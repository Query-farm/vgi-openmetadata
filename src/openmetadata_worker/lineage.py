"""Lineage: the ``lineage_edges`` table and the ``lineage(fqn, ...)`` traversal function."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

from .assets import scan_tags
from .client import SECRET_REQUIREMENT, Endpoint, endpoint, get_json, quote_fqn
from .common import (
    CATALOG_TOOL,
    TSTZ,
    NoArgs,
    catalog_tool_column,
    eq_values,
    get,
    qualified_name,
    rows_to_batch,
    ts,
)
from .meta import column
from .scan import EntityScan, ScanCursor


def _end(side: str) -> list[pa.Field[Any]]:
    return [
        column(f"{side}_id", pa.string(), f"Id of the {side} entity.", nullable=False),
        column(f"{side}_type", pa.string(), f"Entity type of the {side} end: table, dashboard, pipeline, topic, ..."),
        column(f"{side}_fqn", pa.string(), f"FQN of the {side} entity."),
        column(f"{side}_name", pa.string(), f"Name of the {side} entity."),
        column(f"{side}_qualified_name", pa.string(), f"Lower-cased database.schema.table when the {side} is a table."),
        column(f"{side}_column", pa.string(), f"{side.capitalize()} column name (column-level edges), else NULL."),
    ]


EDGE_FIELDS = [
    column(
        "level",
        pa.string(),
        "'dataset' for an entity-to-entity edge, 'column' for a column-to-column edge.",
        nullable=False,
    ),
    *_end("upstream"),
    *_end("downstream"),
    column("pipeline_fqn", pa.string(), "FQN of the pipeline that produces the edge, when recorded."),
    column("transformation", pa.string(), "Column-level transformation expression (columnsLineage.function)."),
    column("sql_query", pa.string(), "SQL that created the edge (query-log lineage), when recorded."),
    column("lineage_source", pa.string(), "How the edge was found: Manual, QueryLineage, DbtLineage, ViewLineage, ..."),
    column("created_at", TSTZ, "When the edge was recorded."),
    column("updated_at", TSTZ, "When the edge was last updated."),
]

LINEAGE_EDGES_SCHEMA = pa.schema([catalog_tool_column(), *EDGE_FIELDS])


def _column_name(column_fqn: Any, entity_fqn: str | None) -> str | None:
    if not column_fqn:
        return None
    text = str(column_fqn)
    if entity_fqn and text.startswith(entity_fqn + "."):
        return text[len(entity_fqn) + 1 :]
    return text.rsplit(".", 1)[-1]


def _end_values(side: str, ref: dict[str, Any]) -> dict[str, Any]:
    fqn = ref.get("fullyQualifiedName")
    etype = ref.get("type")
    return {
        f"{side}_id": ref.get("id"),
        f"{side}_type": etype,
        f"{side}_fqn": fqn,
        f"{side}_name": ref.get("name"),
        f"{side}_qualified_name": qualified_name(fqn) if etype == "table" else None,
    }


def edge_rows(edge: dict[str, Any], nodes: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the entity-level row and one row per column pair of one lineage edge."""
    up = nodes.get(str(edge.get("fromEntity")), {"id": edge.get("fromEntity")})
    down = nodes.get(str(edge.get("toEntity")), {"id": edge.get("toEntity")})
    details = edge.get("lineageDetails") or {}
    common = {
        "catalog_tool": CATALOG_TOOL,
        **_end_values("upstream", up),
        **_end_values("downstream", down),
        "pipeline_fqn": get(details, "pipeline", "fullyQualifiedName"),
        "sql_query": details.get("sqlQuery"),
        "lineage_source": details.get("source"),
        "created_at": ts(details.get("createdAt")),
        "updated_at": ts(details.get("updatedAt")),
    }
    rows = [{**common, "level": "dataset", "upstream_column": None, "downstream_column": None, "transformation": None}]
    for cl in details.get("columnsLineage") or []:
        if not isinstance(cl, dict):
            continue
        to_col = _column_name(cl.get("toColumn"), down.get("fullyQualifiedName"))
        for from_col in cl.get("fromColumns") or []:
            rows.append(
                {
                    **common,
                    "level": "column",
                    "upstream_column": _column_name(from_col, up.get("fullyQualifiedName")),
                    "downstream_column": to_col,
                    "transformation": cl.get("function"),
                }
            )
    return rows


def fetch_lineage(
    ep: Endpoint, entity_type: str, fqn: str, upstream_depth: int, downstream_depth: int
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """``(entity, nodes by id, upstream edges, downstream edges)`` of ``/v1/lineage/{type}/name/{fqn}``."""
    body = get_json(
        ep,
        f"/v1/lineage/{entity_type}/name/{quote_fqn(fqn)}",
        {"upstreamDepth": upstream_depth, "downstreamDepth": downstream_depth},
    )
    body = body if isinstance(body, dict) else {}
    raw_entity = body.get("entity")
    entity: dict[str, Any] = raw_entity if isinstance(raw_entity, dict) else {}
    nodes = {str(n["id"]): n for n in body.get("nodes") or [] if isinstance(n, dict) and n.get("id")}
    if entity.get("id"):
        nodes[str(entity["id"])] = {**entity, "type": entity.get("type") or entity_type}
    ups = [e for e in body.get("upstreamEdges") or [] if isinstance(e, dict)]
    downs = [e for e in body.get("downstreamEdges") or [] if isinstance(e, dict)]
    return entity, nodes, ups, downs


@init_single_worker
@bind_fixed_schema
class LineageEdgesScan(EntityScan[NoArgs]):
    """Rows of ``lineage_edges``: the direct upstream edges of every table (or of the filtered ones)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LINEAGE_EDGES_SCHEMA
    PATH: ClassVar[str] = "/v1/tables"
    ENTITY_TYPE: ClassVar[str] = "table"
    FIELDS: ClassVar[str] = ""
    ID_COLUMN: ClassVar[str | None] = None
    FQN_COLUMN: ClassVar[str | None] = "downstream_fqn"

    class Meta:
        """Scan metadata."""

        name = "lineage_edges_scan"
        description = "Direct table- and column-level lineage edges recorded in OpenMetadata"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Lineage Edges",
            category="lineage",
            keywords="lineage, edges, upstream, downstream, column lineage, dependencies, provenance",
            doc_llm=(
                "One row per direct lineage edge into an OpenMetadata table: level ('dataset' for the entity "
                "edge, 'column' for each fromColumn -> toColumn pair of its columnsLineage), upstream_id / _type / "
                "_fqn / _name / _qualified_name / _column, the same for downstream, pipeline_fqn, transformation, "
                "sql_query, lineage_source, created_at, updated_at. Pushdown: downstream_fqn = / IN asks "
                "/v1/lineage/table/name/{fqn} for those tables' upstream edges only; upstream_fqn = / IN (without "
                "downstream_fqn) asks for those tables' downstream edges. Unfiltered, every table's upstream edges "
                "are fetched (one lineage call per table), so edges into non-table entities (dashboards, ML "
                "models) appear only with an upstream_fqn filter or through lineage(). For multi-hop traversal "
                "use lineage(fqn, ...)."
            ),
            doc_md="Direct lineage edges, table- and column-level.",
            schema=LINEAGE_EDGES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT upstream_qualified_name FROM openmetadata.main.lineage_edges "
                    "WHERE downstream_fqn = 'snowflake_prod.analytics.marts.fct_orders' AND level = 'dataset' "
                    "ORDER BY upstream_qualified_name"
                ),
                description="Tables feeding one table directly (downstream_fqn pushes down)",
            ),
            FunctionExample(
                sql=(
                    "SELECT upstream_column, downstream_column, transformation FROM openmetadata.main.lineage_edges "
                    "WHERE downstream_fqn = 'snowflake_prod.analytics.marts.fct_orders' AND level = 'column' "
                    "ORDER BY downstream_column"
                ),
                description="Column-level lineage into one table",
            ),
        ]

    @classmethod
    def plan(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Like the base plan, plus the lineage direction (``upstream_fqn`` alone flips it)."""
        filters = params.current_pushdown_filters
        ups = eq_values(filters, "upstream_fqn")
        if ups is not None and eq_values(filters, "downstream_fqn") is None:
            if not ups:
                return {"extra": {}, "mode": "empty"}
            return {"extra": {"direction": "downstream"}, "mode": "keys", "by": "name", "keys": [str(u) for u in ups]}
        plan = super().plan(params)
        plan["extra"]["direction"] = "upstream"
        return plan

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch the table's direct upstream (or downstream) edges."""
        fqn = entity.get("fullyQualifiedName")
        if not fqn:
            return []
        upstream = plan["extra"].get("direction", "upstream") == "upstream"
        ent, nodes, ups, downs = fetch_lineage(ep, "table", str(fqn), 1 if upstream else 0, 0 if upstream else 1)
        me = str(ent.get("id") or entity.get("id"))
        edges = (
            [e for e in ups if str(e.get("toEntity")) == me]
            if upstream
            else [e for e in downs if str(e.get("fromEntity")) == me]
        )
        rows: list[dict[str, Any]] = []
        for edge in edges:
            rows.extend(edge_rows(edge, nodes))
        return rows


# ----------------------------------------------------------------------------- lineage()

LINEAGE_ENTITY_TYPES = [
    "table",
    "dashboard",
    "chart",
    "pipeline",
    "topic",
    "mlmodel",
    "container",
    "dashboardDataModel",
    "storedProcedure",
    "searchIndex",
]


@dataclass(slots=True, frozen=True, kw_only=True)
class LineageArgs:
    """Arguments of ``lineage(fqn, entity_type := 'table', upstream_depth := 3, downstream_depth := 3)``."""

    fqn: Annotated[str, Arg(0, doc="FQN of the start entity, e.g. 'snowflake_prod.analytics.marts.fct_orders'.")]
    entity_type: Annotated[
        str,
        Arg("entity_type", default="table", doc="Entity type of the start entity.", choices=LINEAGE_ENTITY_TYPES),
    ]
    upstream_depth: Annotated[
        int, Arg("upstream_depth", default=3, doc="Hops to follow upstream (dependencies); 0 to 10.", ge=0, le=10)
    ]
    downstream_depth: Annotated[
        int, Arg("downstream_depth", default=3, doc="Hops to follow downstream (impact); 0 to 10.", ge=0, le=10)
    ]


LINEAGE_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("start_fqn", pa.string(), "FQN of the start entity (the fqn argument).", nullable=False),
        column("direction", pa.string(), "'upstream' or 'downstream' of the start entity.", nullable=False),
        column("degree", pa.int32(), "Hops from the start entity to the edge's far end (1 = direct).", nullable=False),
        *EDGE_FIELDS,
    ]
)


def _degrees(start: str, edges: list[dict[str, Any]], *, upstream: bool) -> dict[str, int]:
    """Breadth-first hop counts from ``start`` along ``edges`` (towards sources when ``upstream``)."""
    adjacency: dict[str, list[str]] = {}
    for e in edges:
        a, b = str(e.get("fromEntity")), str(e.get("toEntity"))
        src, dst = (b, a) if upstream else (a, b)
        adjacency.setdefault(src, []).append(dst)
    seen = {start: 0}
    queue = deque([start])
    while queue:
        cur = queue.popleft()
        for nxt in adjacency.get(cur, []):
            if nxt not in seen:
                seen[nxt] = seen[cur] + 1
                queue.append(nxt)
    return seen


@init_single_worker
@bind_fixed_schema
class Lineage(TableFunctionGenerator[LineageArgs, ScanCursor]):
    """``lineage(fqn, ...)`` via ``/v1/lineage/{entity_type}/name/{fqn}``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LINEAGE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "lineage"
        description = "Multi-hop upstream and downstream lineage edges of one entity (impact analysis)"
        required_secrets = list(SECRET_REQUIREMENT)
        tags = scan_tags(
            title="Lineage Traversal",
            category="lineage",
            keywords="lineage, impact analysis, upstream, downstream, dependencies, blast radius, root cause",
            doc_llm=(
                "Every lineage edge within upstream_depth hops upstream and downstream_depth hops downstream of "
                "one OpenMetadata entity, from one /v1/lineage/{entity_type}/name/{fqn} call. Arguments: fqn "
                "(start entity FQN), entity_type := 'table' (or dashboard, pipeline, topic, mlmodel, ...), "
                "upstream_depth := 0..10 (default 3), downstream_depth := 0..10 (default 3). Returns start_fqn, "
                "direction ('upstream' | 'downstream'), degree (hops from the start to the edge's far end) and "
                "the lineage_edges columns (level 'dataset' and 'column' rows, upstream_* / downstream_* ends, "
                "pipeline_fqn, transformation, sql_query, lineage_source, created_at, updated_at)."
            ),
            doc_md="Multi-hop lineage traversal from one entity.",
            schema=LINEAGE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT DISTINCT upstream_qualified_name, degree FROM openmetadata.main.lineage("
                    "'snowflake_prod.analytics.marts.fct_orders', downstream_depth := 0) "
                    "WHERE level = 'dataset' ORDER BY degree, upstream_qualified_name"
                ),
                description="Every table a table depends on, nearest first",
            ),
            FunctionExample(
                sql=(
                    "SELECT DISTINCT downstream_type, downstream_fqn FROM openmetadata.main.lineage("
                    "'snowflake_prod.analytics.raw.orders', upstream_depth := 0, downstream_depth := 5) "
                    "WHERE downstream_type = 'dashboard' ORDER BY downstream_fqn"
                ),
                description="Dashboards affected by a change to a raw table",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[LineageArgs]) -> ScanCursor:
        """Nothing to resolve: one call answers the whole traversal."""
        return ScanCursor()

    @classmethod
    def process(cls, params: ProcessParams[LineageArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Fetch the lineage graph once and emit every edge."""
        if state.done:
            out.finish()
            return
        state.done = True
        args = params.args
        ep = endpoint(params.secrets)
        entity, nodes, ups, downs = fetch_lineage(
            ep, args.entity_type, args.fqn, args.upstream_depth, args.downstream_depth
        )
        start = str(entity.get("id"))
        rows: list[dict[str, Any]] = []
        for edges, upstream in ((ups, True), (downs, False)):
            degrees = _degrees(start, edges, upstream=upstream)
            for edge in edges:
                far = str(edge.get("fromEntity") if upstream else edge.get("toEntity"))
                degree = degrees.get(far)
                if degree is None:
                    continue
                for row in edge_rows(edge, nodes):
                    rows.append(
                        {
                            **row,
                            "start_fqn": args.fqn,
                            "direction": "upstream" if upstream else "downstream",
                            "degree": degree,
                        }
                    )
        rows.sort(key=lambda r: (r["direction"], r["degree"], json.dumps(r, default=str, sort_keys=True)))
        out.emit(rows_to_batch(rows, params.output_schema))


LINEAGE_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("lineage_edges", LineageEdgesScan, None),
]
FUNCTIONS: list[type[Any]] = [Lineage]
