"""Table functions: ``search(query, ...)`` and the opt-in ``sample_data(fqn)``."""

from __future__ import annotations

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
from .client import SECRET_REQUIREMENT, endpoint, get_json, quote_fqn
from .common import (
    CATALOG_TOOL,
    STR_LIST,
    catalog_tool_column,
    domain_ref,
    get,
    is_pii,
    owner_names,
    platform_of,
    qualified_name,
    rows_to_batch,
    tag_fqns,
    term_fqns,
    tier_of,
    to_float,
    to_str,
)
from .meta import column
from .scan import SEARCH_PATH, ScanCursor, query_filter, term_clause

# entity_type argument -> OpenMetadata search index.
SEARCH_INDEXES = {
    "all": "all",
    "table": "table_search_index",
    "dashboard": "dashboard_search_index",
    "chart": "chart_search_index",
    "pipeline": "pipeline_search_index",
    "topic": "topic_search_index",
    "mlmodel": "mlmodel_search_index",
    "container": "container_search_index",
    "glossaryTerm": "glossary_term_search_index",
    "tag": "tag_search_index",
}
MAX_SEARCH_RESULTS = 10_000  # Elasticsearch's default max_result_window


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchArgs:
    """Arguments of ``search(query, entity_type := 'table', ...)``."""

    query: Annotated[str, Arg(0, doc="Search text (Elasticsearch query_string syntax); '*' matches everything.")]
    entity_type: Annotated[
        str,
        Arg(
            "entity_type",
            default="table",
            doc="What to search: table, dashboard, chart, pipeline, topic, mlmodel, container, glossaryTerm, tag "
            "or all.",
            choices=list(SEARCH_INDEXES),
        ),
    ]
    service: Annotated[str | None, Arg("service", default=None, doc="Only assets of this service (service name).")]
    database: Annotated[str | None, Arg("database", default=None, doc="Only tables in this database (name).")]
    schema_name: Annotated[str | None, Arg("schema_name", default=None, doc="Only tables in this schema (name).")]
    tag: Annotated[str | None, Arg("tag", default=None, doc="Only assets carrying this tag or term FQN.")]
    owner: Annotated[str | None, Arg("owner", default=None, doc="Only assets owned by this user or team (name).")]
    domain: Annotated[str | None, Arg("domain", default=None, doc="Only assets in this domain (FQN).")]
    tier: Annotated[str | None, Arg("tier", default=None, doc="Only assets of this tier, e.g. 'Tier.Tier1'.")]
    max_results: Annotated[
        int, Arg("max_results", default=100, doc="Maximum hits to return; 1 to 10000.", ge=1, le=MAX_SEARCH_RESULTS)
    ]


SEARCH_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "OpenMetadata id of the hit.", nullable=False),
        column("entity_type", pa.string(), "Entity type: table, dashboard, pipeline, topic, ...", nullable=False),
        column("fqn", pa.string(), "Fully-qualified name."),
        column("name", pa.string(), "Name."),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("platform", pa.string(), "Platform from the service type."),
        column("service", pa.string(), "Service name."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table for tables."),
        column("description", pa.string(), "Description."),
        column("owners", STR_LIST, "Owner names."),
        column("tags", STR_LIST, "Classification tag FQNs."),
        column("glossary_terms", STR_LIST, "Glossary term FQNs."),
        column("tier", pa.string(), "Tier tag FQN."),
        column("domain", pa.string(), "Domain FQN."),
        column("score", pa.float64(), "Relevance score (higher is better)."),
        column("url", pa.string(), "Link in the OpenMetadata UI."),
    ]
)


def search_filter(args: SearchArgs) -> str | None:
    """Build the ``query_filter`` for the named arguments, or None without any."""
    pairs = [
        ("service.name.keyword", args.service),
        ("database.name.keyword", args.database),
        ("databaseSchema.name.keyword", args.schema_name),
        ("tags.tagFQN", args.tag),
        ("owners.name.keyword", args.owner),
        ("domain.fullyQualifiedName", args.domain),
        ("tier.tagFQN", args.tier),
    ]
    clauses = [term_clause(field, [value]) for field, value in pairs if value]
    return query_filter(clauses) if clauses else None


@init_single_worker
@bind_fixed_schema
class Search(TableFunctionGenerator[SearchArgs, ScanCursor]):
    """``search(query, ...)`` over ``/v1/search/query``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SEARCH_SCHEMA

    class Meta:
        """Function metadata."""

        name = "search"
        description = "Full-text search over OpenMetadata's search index with service, tag, owner and tier filters"
        required_secrets = list(SECRET_REQUIREMENT)
        tags = scan_tags(
            title="OpenMetadata Search",
            category="search",
            keywords="search, find, full text, discovery, keyword search, elasticsearch, opensearch",
            doc_llm=(
                "Full-text search through OpenMetadata's Elasticsearch / OpenSearch index (/v1/search/query), "
                "best match first. Arguments: query (query_string syntax, '*' = everything), entity_type := "
                "'table' | dashboard | chart | pipeline | topic | mlmodel | container | glossaryTerm | tag | all, "
                "and optional filters service, database, schema_name, tag (tag or glossary term FQN), owner, "
                "domain, tier, which become the search query_filter; max_results := 1..10000 (default 100). "
                "Returns id, entity_type, fqn, name, display_name, platform, service, qualified_name, description, "
                "owners, tags, glossary_terms, tier, domain, score, url."
            ),
            doc_md="Search the OpenMetadata catalog.",
            schema=SEARCH_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, score FROM openmetadata.main.search('orders', tier := 'Tier.Tier1') "
                    "ORDER BY score DESC"
                ),
                description="Tier 1 tables matching 'orders'",
            ),
            FunctionExample(
                sql=(
                    "SELECT entity_type, fqn FROM openmetadata.main.search('revenue', entity_type := 'all') "
                    "ORDER BY entity_type, fqn"
                ),
                description="Every kind of asset mentioning revenue",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[SearchArgs]) -> ScanCursor:
        """Start at offset 0."""
        return ScanCursor()

    @classmethod
    def process(cls, params: ProcessParams[SearchArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Fetch one page of hits (``from`` / ``size`` paging, relevance order)."""
        args = params.args
        if state.done or state.offset >= args.max_results:
            out.finish()
            return
        ep = endpoint(params.secrets)
        size = min(ep.page_size, args.max_results - state.offset)
        query: dict[str, Any] = {
            "q": args.query,
            "index": SEARCH_INDEXES[args.entity_type],
            "from": state.offset,
            "size": size,
            "deleted": "false",
            "track_total_hits": "true",
        }
        flt = search_filter(args)
        if flt:
            query["query_filter"] = flt
        body = get_json(ep, SEARCH_PATH, query)
        hits = [
            h for h in get(body, "hits", "hits") or [] if isinstance(h, dict) and isinstance(h.get("_source"), dict)
        ]
        state.offset += len(hits)
        state.done = len(hits) < size
        rows = []
        for h in hits:
            src = h["_source"]
            etype = src.get("entityType") or (args.entity_type if args.entity_type != "all" else None)
            fqn = src.get("fullyQualifiedName")
            dom = domain_ref(src)
            rows.append(
                {
                    "catalog_tool": CATALOG_TOOL,
                    "id": src.get("id") or h.get("_id"),
                    "entity_type": etype or "unknown",
                    "fqn": fqn,
                    "name": src.get("name"),
                    "display_name": src.get("displayName"),
                    "platform": platform_of(src.get("serviceType")),
                    "service": get(src, "service", "name"),
                    "qualified_name": qualified_name(fqn) if etype == "table" else None,
                    "description": src.get("description"),
                    "owners": owner_names(src),
                    "tags": tag_fqns(src),
                    "glossary_terms": term_fqns(src),
                    "tier": tier_of(src),
                    "domain": dom.get("fullyQualifiedName") or dom.get("name"),
                    "score": to_float(h.get("_score")),
                    "url": ep.entity_url(etype, fqn),
                }
            )
        out.emit(rows_to_batch(rows, params.output_schema))


# ----------------------------------------------------------------------------- sample_data()


@dataclass(slots=True, frozen=True, kw_only=True)
class SampleArgs:
    """Arguments of ``sample_data(fqn, max_rows := 100)``."""

    fqn: Annotated[str, Arg(0, doc="Table FQN, e.g. 'snowflake_prod.analytics.marts.fct_orders'.")]
    max_rows: Annotated[int, Arg("max_rows", default=100, doc="Maximum sample rows; 1 to 10000.", ge=1, le=10_000)]


SAMPLE_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("table_fqn", pa.string(), "FQN of the sampled table.", nullable=False),
        column("row_number", pa.int32(), "1-based number of the sample row.", nullable=False),
        column("column_name", pa.string(), "Column name.", nullable=False),
        column("value", pa.string(), "The sample value as text (NULL for NULL)."),
    ]
)


@init_single_worker
@bind_fixed_schema
class SampleData(TableFunctionGenerator[SampleArgs, ScanCursor]):
    """``sample_data(fqn)``: sample rows OpenMetadata stored for a table, minus PII-tagged columns."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SAMPLE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "sample_data"
        description = "Opt-in sample rows of one table as (row, column, value); PII-tagged columns are never returned"
        required_secrets = list(SECRET_REQUIREMENT)
        tags = scan_tags(
            title="Table Sample Data",
            category="search",
            keywords="sample data, preview, example rows, data preview, pii safe",
            doc_llm=(
                "The sample rows OpenMetadata's profiler stored for one table (/v1/tables/{id}/sampleData), in long "
                "form: table_fqn, row_number, column_name, value (text). Sample data is only fetched by this "
                "function (no table returns it). Columns tagged with any PII.* classification tag are left out, "
                "and a table tagged PII.* itself returns no rows. Arguments: fqn (table FQN), max_rows := 1..10000 "
                "(default 100). Pivot with PIVOT ... ON column_name USING first(value) to get a wide preview."
            ),
            doc_md="Sample rows of one table, without PII-tagged columns.",
            schema=SAMPLE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT column_name, count(DISTINCT value) AS distinct_values FROM openmetadata.main.sample_data("
                    "'snowflake_prod.analytics.marts.fct_orders') GROUP BY column_name ORDER BY column_name"
                ),
                description="Distinct sample values per non-PII column",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[SampleArgs]) -> ScanCursor:
        """One shot."""
        return ScanCursor()

    @classmethod
    def process(cls, params: ProcessParams[SampleArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Look up the PII tags, then fetch and emit the sample."""
        if state.done:
            out.finish()
            return
        state.done = True
        args = params.args
        ep = endpoint(params.secrets)
        table = get_json(ep, f"/v1/tables/name/{quote_fqn(args.fqn)}", {"fields": "columns,tags"}) or {}
        if is_pii(table):
            out.emit(rows_to_batch([], params.output_schema))
            return
        blocked = {str(c.get("name")) for c in table.get("columns") or [] if isinstance(c, dict) and is_pii(c)}
        body = get_json(ep, f"/v1/tables/{quote_fqn(str(table.get('id')))}/sampleData") or {}
        sample = body.get("sampleData") or {}
        names = [str(c) for c in sample.get("columns") or []]
        rows = []
        for i, values in enumerate((sample.get("rows") or [])[: args.max_rows], start=1):
            for name, value in zip(names, values or [], strict=False):
                if name in blocked:
                    continue
                rows.append(
                    {
                        "catalog_tool": CATALOG_TOOL,
                        "table_fqn": args.fqn,
                        "row_number": i,
                        "column_name": name,
                        "value": to_str(value),
                    }
                )
        out.emit(rows_to_batch(rows, params.output_schema))


FUNCTIONS: list[type[Any]] = [Search, SampleData]
