"""Shared building blocks: Arrow types, batch building, catalog tables, pushdown and FQN helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.filter_v2 import Call, ColumnRef, Literal, StandardFilterFunction
from vgi.table_filter_pushdown import (
    AndFilter,
    ComparisonOp,
    ConstantFilter,
    ExpressionFilter,
    FunctionNode,
    InFilter,
    V2ExpressionFilter,
)
from vgi.table_function import TableFunctionGenerator

from .meta import column

CATALOG_TOOL = "openmetadata"

TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())
STR_MAP = pa.map_(pa.string(), pa.string())


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


def catalog_tool_column() -> pa.Field[Any]:
    """Return the shared ``catalog_tool`` column (always 'openmetadata')."""
    return column(
        "catalog_tool",
        pa.string(),
        "Catalog that produced the row; always 'openmetadata' (UNION key with the other catalog workers).",
        nullable=False,
    )


def rows_to_batch(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
    """Build a batch of ``schema`` (the projected output schema) from row dicts."""
    columns = {f.name: [row.get(f.name) for row in rows] for f in schema}
    return pa.RecordBatch.from_pydict(columns, schema=schema)


def duckdb_type(t: pa.DataType) -> str:
    """Return the DuckDB SQL name of an Arrow type used in this worker's schemas."""
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "VARCHAR"
    if pa.types.is_int64(t):
        return "BIGINT"
    if pa.types.is_int32(t):
        return "INTEGER"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_date32(t):
        return "DATE"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP WITH TIME ZONE" if t.tz else "TIMESTAMP"
    if pa.types.is_map(t):
        return f"MAP({duckdb_type(t.key_type)}, {duckdb_type(t.item_type)})"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_struct(t):
        return (
            "STRUCT("
            + ", ".join(f"{t.field(i).name} {duckdb_type(t.field(i).type)}" for i in range(t.num_fields))
            + ")"
        )
    raise TypeError(f"no DuckDB name for {t}")


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` JSON for a fixed output schema."""
    return json.dumps(
        [
            {
                "name": f.name,
                "type": duckdb_type(f.type),
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in schema
        ]
    )


def catalog_table(
    name: str,
    function: type[TableFunctionGenerator[Any, Any]],
    *,
    comment: str,
    tags: dict[str, str],
    schema_path: list[str],
    primary_key: list[str] | None = None,
) -> Table:
    """Build a catalog table over ``function`` with NOT NULL records for its non-nullable columns."""
    fields: pa.Schema = function.FIXED_SCHEMA  # type: ignore[attr-defined]
    constraints = [
        constraint_record(
            schema_path,
            name,
            f"{f.name}_not_null",
            "NOT_NULL",
            [[f.name]],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
        )
        for f in fields
        if not f.nullable
    ]
    if primary_key:
        constraints.append(
            constraint_record(
                schema_path,
                name,
                "pk",
                "PRIMARY_KEY",
                [[c] for c in primary_key],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
        )
    comments = {
        f.name: (f.metadata or {}).get(b"comment", b"").decode() for f in fields if (f.metadata or {}).get(b"comment")
    }
    return Table(
        name=name,
        function=function,
        comment=comment,
        tags=tags,
        constraints=tuple(constraints),
        column_comments=comments,
    )


# ----------------------------------------------------------------------------- pushdown helpers


def _scalar(value: Any) -> Any:
    return value.as_py() if hasattr(value, "as_py") else value


def _filters(filters: Any) -> list[Any]:
    """Top-level filters, with AND conjunctions flattened (every child must hold)."""
    out: list[Any] = []
    stack = list(filters.filters) if filters else []
    while stack:
        f = stack.pop(0)
        if isinstance(f, AndFilter):
            stack[:0] = list(f.children)
        else:
            out.append(f)
    return out


def eq_values(filters: Any, column_name: str) -> list[Any] | None:
    """Values ``column_name`` is constrained to by top-level ``=`` / ``IN`` filters, else None.

    Several such filters on one column intersect; an empty list means no row matches.
    """
    found: set[Any] | None = None
    for f in _filters(filters):
        if f.column_name != column_name:
            continue
        if isinstance(f, ConstantFilter) and f.op is ComparisonOp.EQ:
            values = {_scalar(f.value)}
        elif isinstance(f, InFilter):
            values = {v for v in f.values.to_pylist() if v is not None}
        else:
            continue
        found = values if found is None else found & values
    return None if found is None else sorted(found, key=str)


def _contains_constant(node: Any, column_name: str) -> str | None:
    """Return the constant of ``list_contains(<column_name>, 'x')`` in a v1 or v2 expression node, else None."""
    if isinstance(node, Call):
        fn = node.function
        name = fn.value if isinstance(fn, StandardFilterFunction) else getattr(fn, "name", None)
        args = node.arguments
        if name in LIST_CONTAINS_NAMES and len(args) == 2:
            ref, lit = args
            if isinstance(ref, ColumnRef) and ref.column_name == column_name and isinstance(lit, Literal):
                value = _scalar(lit.value)
                return None if value is None else str(value)
        return None
    if isinstance(node, FunctionNode) and node.function_name in LIST_CONTAINS_NAMES and len(node.children) == 2:
        consts = [c for c in node.children if c.expr_type.value == "constant"]
        if len(consts) == 1:
            value = _scalar(consts[0].value)  # type: ignore[attr-defined]
            return None if value is None else str(value)
    return None


LIST_CONTAINS_NAMES = ("list_contains", "list_has", "array_has", "array_contains")


def contains_values(filters: Any, column_name: str) -> list[str] | None:
    """Return the constants of top-level ``list_contains(<column_name>, 'x')`` filters (all must hold)."""
    found: list[str] = []
    for f in _filters(filters):
        if isinstance(f, V2ExpressionFilter):
            value = _contains_constant(f.expression, column_name)
        elif isinstance(f, ExpressionFilter) and f.column_name == column_name:
            value = _contains_constant(f.expr, column_name)
        else:
            value = None
        if value is not None:
            found.append(value)
    return found or None


def range_bounds(filters: Any, column_name: str) -> tuple[Any, Any]:
    """Loosest-safe ``(lower, upper)`` bounds from top-level comparisons on ``column_name``.

    Both bounds are inclusive; the SDK re-applies the exact predicates afterwards.
    """
    lower: Any = None
    upper: Any = None
    for f in _filters(filters):
        if f.column_name != column_name or not isinstance(f, ConstantFilter):
            continue
        value = _scalar(f.value)
        if value is None:
            continue
        if f.op in (ComparisonOp.GT, ComparisonOp.GE, ComparisonOp.EQ):
            lower = value if lower is None else max(lower, value)
        if f.op in (ComparisonOp.LT, ComparisonOp.LE, ComparisonOp.EQ):
            upper = value if upper is None else min(upper, value)
    return lower, upper


def to_millis(value: Any) -> int | None:
    """Epoch milliseconds for a pushed-down timestamp/date bound (None passes through)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1000)
    if isinstance(value, date):
        return int(datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp() * 1000)
    return None


def ts(millis: Any) -> datetime | None:
    """OpenMetadata epoch-millisecond times to an aware UTC datetime; 0/None -> None."""
    if millis is None or millis == 0 or isinstance(millis, bool):
        return None
    try:
        value = int(millis)
        # A few endpoints report seconds; anything before 2001 in ms is treated as seconds.
        if value < 10_000_000_000:
            value *= 1000
        return datetime.fromtimestamp(value / 1000.0, tz=UTC)
    except TypeError, ValueError, OverflowError, OSError:
        return None


def to_int(value: Any) -> int | None:
    """Convert to an integer or None (bools and junk become None)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def to_float(value: Any) -> float | None:
    """Convert to a float or None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except TypeError, ValueError:
        return None


def to_str(value: Any) -> str | None:
    """Convert to a string or None (numbers are formatted, dicts / lists become JSON)."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def get(obj: Any, *path: str) -> Any:
    """Walk nested dicts, returning None as soon as a step is missing."""
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


# ----------------------------------------------------------------------------- FQNs and names


def split_fqn(fqn: str | None) -> list[str]:
    """Split an OpenMetadata FQN on dots, honouring double-quoted parts (``svc."my.db".sch.t``)."""
    if not fqn:
        return []
    parts: list[str] = []
    cur: list[str] = []
    quoted = False
    for ch in fqn:
        if ch == '"':
            quoted = not quoted
        elif ch == "." and not quoted:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


def join_fqn(parts: list[str]) -> str:
    """Join FQN parts, quoting any that contain a dot."""
    return ".".join(f'"{p}"' if "." in p else p for p in parts)


def qualified_name(fqn: str | None) -> str | None:
    """Lower-cased ``database.schema.table`` of a table FQN (``service.database.schema.table``)."""
    parts = split_fqn(fqn)
    if len(parts) < 4:
        return None
    return ".".join(parts[1:4]).lower()


def table_fqn_of(fqn: str | None) -> str | None:
    """Return the table FQN (first four parts) of a table / column / test-case FQN."""
    parts = split_fqn(fqn)
    return join_fqn(parts[:4]) if len(parts) >= 4 else None


# OpenMetadata serviceType -> the platform name shared with the other catalog workers.
PLATFORM_NAMES = {
    "AzureSQL": "azure-sql",
    "BigQuery": "bigquery",
    "DeltaLake": "delta-lake",
    "UnityCatalog": "unity-catalog",
    "SingleStore": "singlestore",
    "PinotDB": "pinot",
    "MongoDB": "mongodb",
    "SapHana": "hana",
    "Db2": "db2",
}


def platform_of(service_type: str | None) -> str | None:
    """OpenMetadata ``serviceType`` (``Snowflake``, ``BigQuery``) to a lower-case platform name."""
    if not service_type:
        return None
    return PLATFORM_NAMES.get(service_type, service_type.lower())


# ----------------------------------------------------------------------------- references and labels


def refs(value: Any) -> list[dict[str, Any]]:
    """EntityReference list (or a single reference) as a list of dicts."""
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    return []


def ref_names(value: Any, key: str = "name") -> list[str]:
    """Names (or FQNs with ``key='fullyQualifiedName'``) of a reference list."""
    return [str(r[key]) for r in refs(value) if r.get(key)]


def owners_of(entity: dict[str, Any]) -> list[dict[str, Any]]:
    """Owner references: ``owners`` (1.5+) or the legacy single ``owner``."""
    return refs(entity.get("owners")) or refs(entity.get("owner"))


def owner_names(entity: dict[str, Any]) -> list[str]:
    """Owner names (users and teams) of an entity."""
    return [str(o["name"]) for o in owners_of(entity) if o.get("name")]


def domain_ref(entity: dict[str, Any]) -> dict[str, Any]:
    """Return the entity's domain reference: ``domain`` (1.x) or the first of ``domains`` (1.8+)."""
    found = refs(entity.get("domain")) or refs(entity.get("domains"))
    return found[0] if found else {}


def labels(entity: dict[str, Any]) -> list[dict[str, Any]]:
    """TagLabel dicts of an entity or column."""
    return [t for t in entity.get("tags") or [] if isinstance(t, dict) and t.get("tagFQN")]


def tag_fqns(entity: dict[str, Any]) -> list[str]:
    """Classification tag FQNs (``PII.Sensitive``, ``Tier.Tier1``) of an entity or column."""
    return [str(t["tagFQN"]) for t in labels(entity) if t.get("source", "Classification") != "Glossary"]


def term_fqns(entity: dict[str, Any]) -> list[str]:
    """Glossary term FQNs of an entity or column."""
    return [str(t["tagFQN"]) for t in labels(entity) if t.get("source") == "Glossary"]


def tier_of(entity: dict[str, Any]) -> str | None:
    """Return the ``Tier.*`` tag FQN, from the tags list or the search document's ``tier`` field."""
    tier = entity.get("tier")
    if isinstance(tier, dict) and tier.get("tagFQN"):
        return str(tier["tagFQN"])
    for fqn in tag_fqns(entity):
        if fqn.startswith("Tier."):
            return fqn
    return None


def is_pii(entity: dict[str, Any]) -> bool:
    """Tell whether a column (or table) carries any ``PII.*`` classification tag."""
    return any(f == "PII" or f.startswith("PII.") for f in tag_fqns(entity))


def string_map(value: Any) -> list[tuple[str, str | None]] | None:
    """Convert a JSON object (``extension`` custom properties) to MAP entries; None when absent."""
    if not isinstance(value, dict) or not value:
        return None
    return [(str(k), to_str(v)) for k, v in sorted(value.items())]
