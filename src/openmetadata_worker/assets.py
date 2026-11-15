"""Asset tables: datasets, columns, services, databases, schemas, dashboards, charts, pipelines, topics, models."""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import bind_fixed_schema, init_single_worker

from .client import SECRET_REQUIREMENT, Endpoint
from .common import (
    CATALOG_TOOL,
    STR_LIST,
    STR_MAP,
    TSTZ,
    NoArgs,
    catalog_tool_column,
    domain_ref,
    get,
    is_pii,
    owner_names,
    platform_of,
    qualified_name,
    ref_names,
    result_columns,
    split_fqn,
    string_map,
    tag_fqns,
    term_fqns,
    tier_of,
    to_float,
    to_int,
    ts,
)
from .meta import column, object_tags
from .scan import EntityScan


def scan_tags(
    *, title: str, category: str, keywords: str, doc_llm: str, doc_md: str, schema: pa.Schema
) -> dict[str, str]:
    """Discovery tags plus ``vgi.result_columns_schema`` for a scan."""
    return {
        **object_tags(title=title, category=category, keywords=keywords, doc_llm=doc_llm, doc_md=doc_md),
        "vgi.result_columns_schema": result_columns(schema),
    }


def id_column(what: str) -> pa.Field[Any]:
    """Return the ``id`` column (an OpenMetadata UUID)."""
    return column("id", pa.string(), f"OpenMetadata id (UUID) of the {what}.", nullable=False)


def fqn_column(what: str, example: str) -> pa.Field[Any]:
    """Return the ``fqn`` column."""
    return column("fqn", pa.string(), f"Fully-qualified name of the {what}, e.g. '{example}'.", nullable=False)


def governance_fields(*, tier: bool = True) -> list[pa.Field[Any]]:
    """Owners / tags / glossary terms / tier / domain columns shared by asset tables."""
    fields = [
        column("owners", STR_LIST, "Owner names (users and teams); see the owners table for owner types."),
        column("tags", STR_LIST, "Classification tag FQNs, e.g. 'PII.Sensitive', 'Tier.Tier1'."),
        column("glossary_terms", STR_LIST, "Glossary term FQNs, e.g. 'Business.Revenue'."),
    ]
    if tier:
        fields.append(column("tier", pa.string(), "Tier tag FQN ('Tier.Tier1' .. 'Tier.Tier5'), NULL if untiered."))
    fields += [
        column("domain_id", pa.string(), "Domain id (UUID)."),
        column("domain", pa.string(), "Domain fully-qualified name."),
    ]
    return fields


def audit_fields() -> list[pa.Field[Any]]:
    """Return the ``deleted`` / ``updated_at`` / ``updated_by`` / ``version`` / ``url`` columns."""
    return [
        column(
            "deleted",
            pa.bool_(),
            "True for a soft-deleted entity (only returned when fetched by id / fqn).",
            nullable=False,
        ),
        column("updated_at", TSTZ, "When the entity was last changed in OpenMetadata."),
        column("updated_by", pa.string(), "Who last changed the entity in OpenMetadata."),
        column("version", pa.float64(), "OpenMetadata entity version (0.1, 0.2, ... minor; 1.0 ... major)."),
        column("url", pa.string(), "Link to the entity in the OpenMetadata UI."),
    ]


def governance_values(entity: dict[str, Any]) -> dict[str, Any]:
    """Values of :func:`governance_fields`."""
    dom = domain_ref(entity)
    return {
        "owners": owner_names(entity),
        "tags": tag_fqns(entity),
        "glossary_terms": term_fqns(entity),
        "tier": tier_of(entity),
        "domain_id": dom.get("id"),
        "domain": dom.get("fullyQualifiedName") or dom.get("name"),
    }


def audit_values(ep: Endpoint, entity: dict[str, Any], entity_type: str) -> dict[str, Any]:
    """Values of :func:`audit_fields`."""
    return {
        "deleted": bool(entity.get("deleted")),
        "updated_at": ts(entity.get("updatedAt")),
        "updated_by": entity.get("updatedBy"),
        "version": to_float(entity.get("version")),
        "url": ep.entity_url(entity_type, entity.get("fullyQualifiedName")),
    }


# Search-index fields shared by the data-asset indexes (OpenMetadata 1.x mappings).
ASSET_SEARCH_TERMS = {
    "service": "service.name.keyword",
    "tier": "tier.tagFQN",
    "domain": "domain.fullyQualifiedName",
    "domain_id": "domain.id",
}
ASSET_SEARCH_CONTAINS = {"tags": "tags.tagFQN", "glossary_terms": "tags.tagFQN", "owners": "owners.name.keyword"}

PUSHDOWN_NOTE = (
    "Pushdown: id = / IN and fqn = / IN fetch those entities directly; service, tier, domain and domain_id "
    "= / IN and list_contains(tags | glossary_terms | owners, ...) become an OpenMetadata search query_filter."
)

# ----------------------------------------------------------------------------- datasets

DATASETS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("table"),
        fqn_column("table (service.database.schema.table)", "snowflake_prod.analytics.marts.fct_orders"),
        column(
            "platform", pa.string(), "Data platform from the service type: 'snowflake', 'postgres', 'bigquery', ..."
        ),
        column("service", pa.string(), "Database service name (the OpenMetadata connection), e.g. 'snowflake_prod'."),
        column("database", pa.string(), "Database name."),
        column("schema", pa.string(), "Schema name."),
        column("name", pa.string(), "Table name.", nullable=False),
        column(
            "qualified_name",
            pa.string(),
            "Lower-cased 'database.schema.table' (no service); the join key to dbt / OpenLineage and the other "
            "catalog workers.",
        ),
        column("database_fqn", pa.string(), "FQN of the database (service.database)."),
        column("schema_fqn", pa.string(), "FQN of the schema (service.database.schema)."),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column(
            "subtype", pa.string(), "OpenMetadata tableType: Regular, View, MaterializedView, External, Dynamic, ..."
        ),
        column("description", pa.string(), "Table description (Markdown)."),
        *governance_fields(),
        column("data_products", STR_LIST, "Data product FQNs the table belongs to."),
        column("column_count", pa.int64(), "Number of top-level columns."),
        column("custom_properties", STR_MAP, "Custom property values (the entity's extension), as text."),
        column("source_url", pa.string(), "Link to the table in its source system."),
        *audit_fields(),
    ]
)


def table_identity(entity: dict[str, Any]) -> dict[str, Any]:
    """Platform / service / database / schema / qualified name of a table entity or search document."""
    fqn = entity.get("fullyQualifiedName")
    parts = split_fqn(fqn)
    return {
        "platform": platform_of(entity.get("serviceType")),
        "service": get(entity, "service", "name") or (parts[0] if len(parts) >= 4 else None),
        "database": get(entity, "database", "name") or (parts[1] if len(parts) >= 4 else None),
        "schema": get(entity, "databaseSchema", "name") or (parts[2] if len(parts) >= 4 else None),
        "qualified_name": qualified_name(fqn),
        "database_fqn": get(entity, "database", "fullyQualifiedName"),
        "schema_fqn": get(entity, "databaseSchema", "fullyQualifiedName"),
    }


TABLE_SEARCH_TERMS = {
    **ASSET_SEARCH_TERMS,
    "database": "database.name.keyword",
    "schema": "databaseSchema.name.keyword",
    "database_fqn": "database.fullyQualifiedName",
    "schema_fqn": "databaseSchema.fullyQualifiedName",
    "subtype": "tableType",
}


@init_single_worker
@bind_fixed_schema
class DatasetsScan(EntityScan[NoArgs]):
    """Rows of ``datasets``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATASETS_SCHEMA
    PATH: ClassVar[str] = "/v1/tables"
    ENTITY_TYPE: ClassVar[str] = "table"
    FIELDS: ClassVar[str] = "owners,tags,domain,dataProducts,extension,columns"
    SEARCH_INDEX: ClassVar[str | None] = "table_search_index"
    SEARCH_TERMS: ClassVar[dict[str, str]] = TABLE_SEARCH_TERMS
    SEARCH_CONTAINS: ClassVar[dict[str, str]] = ASSET_SEARCH_CONTAINS

    class Meta:
        """Scan metadata."""

        name = "datasets_scan"
        description = "Every OpenMetadata table with service, qualified name, owners, tags, tier and domain"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Tables",
            category="assets",
            keywords="datasets, tables, views, data catalog, qualified name, service, owners, tags, tier, domain",
            doc_llm=(
                "One row per OpenMetadata table (the shared vocabulary calls it a dataset): id, fqn "
                "(service.database.schema.table), platform, service, database, schema, name, qualified_name "
                "(lower-cased database.schema.table, the cross-worker join key), database_fqn, schema_fqn, "
                "display_name, subtype (tableType), description, owners / tags / glossary_terms (lists), tier, "
                "domain_id, domain, data_products, column_count, custom_properties (`MAP(VARCHAR, VARCHAR)`), "
                "source_url, deleted, "
                "updated_at, updated_by, version, url. Pushdown: id / fqn = or IN fetch tables directly; "
                "service, database, schema, database_fqn, schema_fqn, subtype, tier, domain and domain_id = / IN "
                "and list_contains(tags | glossary_terms | owners, 'x') become a search query_filter."
            ),
            doc_md="OpenMetadata tables with their service, qualified name and governance metadata.",
            schema=DATASETS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT schema, count(*) AS tables FROM openmetadata.main.datasets "
                    "WHERE service = 'snowflake_prod' GROUP BY schema ORDER BY tables DESC"
                ),
                description="Tables per schema in one service (service pushes into the search query_filter)",
            ),
            FunctionExample(
                sql=(
                    "SELECT qualified_name, owners FROM openmetadata.main.datasets "
                    "WHERE tier = 'Tier.Tier1' ORDER BY qualified_name"
                ),
                description="Tier 1 tables and their owners",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per table."""
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName"),
                **table_identity(entity),
                "name": entity.get("name"),
                "display_name": entity.get("displayName"),
                "subtype": entity.get("tableType"),
                "description": entity.get("description"),
                **governance_values(entity),
                "data_products": ref_names(entity.get("dataProducts"), "fullyQualifiedName"),
                "column_count": len(entity["columns"]) if isinstance(entity.get("columns"), list) else None,
                "custom_properties": string_map(entity.get("extension")),
                "source_url": entity.get("sourceUrl"),
                **audit_values(ep, entity, "table"),
            }
        ]


# ----------------------------------------------------------------------------- columns

COLUMNS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("dataset_id", pa.string(), "Id of the table (datasets.id).", nullable=False),
        column("table_fqn", pa.string(), "FQN of the table (datasets.fqn).", nullable=False),
        column("platform", pa.string(), "Data platform of the table."),
        column("service", pa.string(), "Database service name."),
        column("database", pa.string(), "Database name."),
        column("schema", pa.string(), "Schema name."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table of the table."),
        column(
            "column_name", pa.string(), "Column name; nested fields as dotted paths ('address.city').", nullable=False
        ),
        column("fqn", pa.string(), "Column FQN (table FQN + column name)."),
        column(
            "ordinal_position",
            pa.int32(),
            "1-based position in the table (nested fields follow their parent).",
            nullable=False,
        ),
        column("data_type", pa.string(), "OpenMetadata data type: VARCHAR, INT, BIGINT, TIMESTAMP, STRUCT, ARRAY, ..."),
        column("native_data_type", pa.string(), "The source's type as shown by OpenMetadata, e.g. 'varchar(256)'."),
        column(
            "data_length", pa.int32(), "Declared length in characters / bytes for sized types (varchar(256) -> 256)."
        ),
        column("nullable", pa.bool_(), "False when the column is NOT NULL or part of the primary key.", nullable=False),
        column("column_constraint", pa.string(), "Column constraint: NULL, NOT_NULL, UNIQUE or PRIMARY_KEY."),
        column("is_primary_key", pa.bool_(), "True if the column is in the table's primary key.", nullable=False),
        column("description", pa.string(), "Column description (Markdown)."),
        column("tags", STR_LIST, "Classification tag FQNs on the column, e.g. 'PII.Sensitive'."),
        column("glossary_terms", STR_LIST, "Glossary term FQNs on the column."),
        column("is_pii", pa.bool_(), "True when the column carries any PII.* classification tag.", nullable=False),
    ]
)


def _flatten_columns(columns: Any, prefix: str = "") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for col in columns or []:
        if not isinstance(col, dict) or not col.get("name"):
            continue
        out.append({**col, "_path": f"{prefix}{col['name']}"})
        out.extend(_flatten_columns(col.get("children"), f"{prefix}{col['name']}."))
    return out


def primary_key_columns(entity: dict[str, Any]) -> set[str]:
    """Column names in a table's PRIMARY_KEY (column constraints and tableConstraints)."""
    found = {
        str(c["name"])
        for c in entity.get("columns") or []
        if isinstance(c, dict) and c.get("constraint") == "PRIMARY_KEY"
    }
    for tc in entity.get("tableConstraints") or []:
        if isinstance(tc, dict) and tc.get("constraintType") == "PRIMARY_KEY":
            found.update(str(c) for c in tc.get("columns") or [])
    return found


@init_single_worker
@bind_fixed_schema
class ColumnsScan(EntityScan[NoArgs]):
    """Rows of ``columns``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COLUMNS_SCHEMA
    PATH: ClassVar[str] = "/v1/tables"
    ENTITY_TYPE: ClassVar[str] = "table"
    FIELDS: ClassVar[str] = "columns,tags,tableConstraints"
    ID_COLUMN: ClassVar[str | None] = "dataset_id"
    FQN_COLUMN: ClassVar[str | None] = "table_fqn"
    SEARCH_INDEX: ClassVar[str | None] = "table_search_index"
    SEARCH_TERMS: ClassVar[dict[str, str]] = {
        "service": "service.name.keyword",
        "database": "database.name.keyword",
        "schema": "databaseSchema.name.keyword",
    }
    SEARCH_CONTAINS: ClassVar[dict[str, str]] = {"tags": "columns.tags.tagFQN", "glossary_terms": "columns.tags.tagFQN"}

    class Meta:
        """Scan metadata."""

        name = "columns_scan"
        description = "Every column of every OpenMetadata table with type, constraint, tags and PII flag"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Columns",
            category="assets",
            keywords="columns, fields, schema, data types, constraints, primary key, pii, column tags",
            doc_llm=(
                "One row per column of every OpenMetadata table (nested `STRUCT` children as dotted column_name "
                "paths): dataset_id, table_fqn, platform, service, database, schema, qualified_name, column_name, "
                "fqn, ordinal_position, data_type, native_data_type, data_length, nullable, column_constraint, "
                "is_primary_key, description, tags, glossary_terms, is_pii (any PII.* tag). Pushdown: dataset_id "
                "/ table_fqn = or IN fetch those tables only; service, database and schema = / IN and "
                "list_contains(tags | glossary_terms, 'x') become a search query_filter."
            ),
            doc_md="Columns of OpenMetadata tables with types, constraints and tags.",
            schema=COLUMNS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, column_name FROM openmetadata.main.columns "
                    "WHERE list_contains(tags, 'PII.Sensitive') ORDER BY qualified_name, column_name"
                ),
                description="Columns tagged PII.Sensitive",
            ),
            FunctionExample(
                sql=(
                    "SELECT column_name, data_type, nullable FROM openmetadata.main.columns "
                    "WHERE table_fqn = 'snowflake_prod.analytics.marts.fct_orders' ORDER BY ordinal_position"
                ),
                description="Schema of one table (table_fqn fetches only that table)",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per (nested) column."""
        ident = table_identity(entity)
        pk = primary_key_columns(entity)
        rows = []
        for pos, col in enumerate(_flatten_columns(entity.get("columns")), start=1):
            constraint = col.get("constraint")
            is_pk = constraint == "PRIMARY_KEY" or col["_path"] in pk
            rows.append(
                {
                    "catalog_tool": CATALOG_TOOL,
                    "dataset_id": entity.get("id"),
                    "table_fqn": entity.get("fullyQualifiedName"),
                    "platform": ident["platform"],
                    "service": ident["service"],
                    "database": ident["database"],
                    "schema": ident["schema"],
                    "qualified_name": ident["qualified_name"],
                    "column_name": col["_path"],
                    "fqn": col.get("fullyQualifiedName"),
                    "ordinal_position": pos,
                    "data_type": col.get("dataType"),
                    "native_data_type": col.get("dataTypeDisplay"),
                    "data_length": to_int(col.get("dataLength")),
                    "nullable": not (is_pk or constraint == "NOT_NULL"),
                    "column_constraint": constraint,
                    "is_primary_key": is_pk,
                    "description": col.get("description"),
                    "tags": tag_fqns(col),
                    "glossary_terms": term_fqns(col),
                    "is_pii": is_pii(col),
                }
            )
        return rows


# ----------------------------------------------------------------------------- services / databases / schemas

DATABASE_SERVICES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("database service"),
        fqn_column("database service (its name)", "snowflake_prod"),
        column("name", pa.string(), "Service name.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("service_type", pa.string(), "OpenMetadata serviceType: Snowflake, Postgres, BigQuery, ..."),
        column("platform", pa.string(), "Lower-case platform name derived from service_type."),
        column("description", pa.string(), "Service description."),
        *governance_fields(tier=False),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class DatabaseServicesScan(EntityScan[NoArgs]):
    """Rows of ``database_services``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATABASE_SERVICES_SCHEMA
    PATH: ClassVar[str] = "/v1/services/databaseServices"
    ENTITY_TYPE: ClassVar[str] = "databaseService"
    FIELDS: ClassVar[str] = "owners,tags,domain"

    class Meta:
        """Scan metadata."""

        name = "database_services_scan"
        description = "OpenMetadata database services (connections); connection settings are never returned"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Database Services",
            category="assets",
            keywords="database services, connections, sources, snowflake, postgres, bigquery, ingestion",
            doc_llm=(
                "One row per OpenMetadata database service (a configured source connection): id, fqn, name, "
                "display_name, service_type, platform, description, owners, tags, glossary_terms, domain_id, "
                "domain, deleted, updated_at, updated_by, version, url. Connection settings (hosts, users, "
                "passwords) are deliberately not exposed. Pushdown: id / fqn = or IN fetch directly."
            ),
            doc_md="Database services (source connections) registered in OpenMetadata.",
            schema=DATABASE_SERVICES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT platform, count(*) AS services FROM openmetadata.main.database_services "
                    "GROUP BY platform ORDER BY services DESC"
                ),
                description="Database services per platform",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per service."""
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName") or entity.get("name"),
                "name": entity.get("name"),
                "display_name": entity.get("displayName"),
                "service_type": entity.get("serviceType"),
                "platform": platform_of(entity.get("serviceType")),
                "description": entity.get("description"),
                **governance_values(entity),
                **audit_values(ep, entity, "databaseService"),
            }
        ]


DATABASES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("database"),
        fqn_column("database (service.database)", "snowflake_prod.analytics"),
        column("name", pa.string(), "Database name.", nullable=False),
        column("service", pa.string(), "Database service name."),
        column("platform", pa.string(), "Data platform from the service type."),
        column("description", pa.string(), "Database description."),
        *governance_fields(),
        column("source_url", pa.string(), "Link to the database in its source system."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class DatabasesScan(EntityScan[NoArgs]):
    """Rows of ``databases``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATABASES_SCHEMA
    PATH: ClassVar[str] = "/v1/databases"
    ENTITY_TYPE: ClassVar[str] = "database"
    FIELDS: ClassVar[str] = "owners,tags,domain"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"service": "service"}

    class Meta:
        """Scan metadata."""

        name = "databases_scan"
        description = "OpenMetadata databases with their service, owners, tags and domain"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Databases",
            category="assets",
            keywords="databases, catalogs, projects, services, data catalog hierarchy",
            doc_llm=(
                "One row per OpenMetadata database: id, fqn (service.database), name, service, platform, "
                "description, owners, tags, glossary_terms, tier, domain_id, domain, source_url, deleted, "
                "updated_at, updated_by, version, url. Pushdown: id / fqn = or IN fetch directly; service = / IN "
                "becomes the list endpoint's service parameter."
            ),
            doc_md="Databases registered in OpenMetadata.",
            schema=DATABASES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT name, owners FROM openmetadata.main.databases "
                    "WHERE service = 'snowflake_prod' ORDER BY name"
                ),
                description="Databases of one service (service pushes down)",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per database."""
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName"),
                "name": entity.get("name"),
                "service": get(entity, "service", "name"),
                "platform": platform_of(entity.get("serviceType")),
                "description": entity.get("description"),
                **governance_values(entity),
                "source_url": entity.get("sourceUrl"),
                **audit_values(ep, entity, "database"),
            }
        ]


DATABASE_SCHEMAS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("schema"),
        fqn_column("schema (service.database.schema)", "snowflake_prod.analytics.marts"),
        column("name", pa.string(), "Schema name.", nullable=False),
        column("service", pa.string(), "Database service name."),
        column("platform", pa.string(), "Data platform from the service type."),
        column("database", pa.string(), "Database name."),
        column("database_fqn", pa.string(), "FQN of the database (databases.fqn)."),
        column("description", pa.string(), "Schema description."),
        *governance_fields(),
        column("source_url", pa.string(), "Link to the schema in its source system."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class DatabaseSchemasScan(EntityScan[NoArgs]):
    """Rows of ``database_schemas``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATABASE_SCHEMAS_SCHEMA
    PATH: ClassVar[str] = "/v1/databaseSchemas"
    ENTITY_TYPE: ClassVar[str] = "databaseSchema"
    FIELDS: ClassVar[str] = "owners,tags,domain"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"database_fqn": "database"}

    class Meta:
        """Scan metadata."""

        name = "database_schemas_scan"
        description = "OpenMetadata database schemas with their database, owners, tags and domain"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Database Schemas",
            category="assets",
            keywords="schemas, database schemas, namespaces, datasets, data catalog hierarchy",
            doc_llm=(
                "One row per OpenMetadata database schema: id, fqn (service.database.schema), name, service, "
                "platform, database, database_fqn, description, owners, tags, glossary_terms, tier, domain_id, "
                "domain, source_url, deleted, updated_at, updated_by, version, url. Pushdown: id / fqn = or IN "
                "fetch directly; database_fqn = / IN becomes the list endpoint's database parameter."
            ),
            doc_md="Database schemas registered in OpenMetadata.",
            schema=DATABASE_SCHEMAS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT name, description FROM openmetadata.main.database_schemas "
                    "WHERE database_fqn = 'snowflake_prod.analytics' ORDER BY name"
                ),
                description="Schemas of one database (database_fqn pushes down)",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per schema."""
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName"),
                "name": entity.get("name"),
                "service": get(entity, "service", "name"),
                "platform": platform_of(entity.get("serviceType")),
                "database": get(entity, "database", "name"),
                "database_fqn": get(entity, "database", "fullyQualifiedName"),
                "description": entity.get("description"),
                **governance_values(entity),
                "source_url": entity.get("sourceUrl"),
                **audit_values(ep, entity, "databaseSchema"),
            }
        ]


# ----------------------------------------------------------------------------- other data assets


def asset_head(what: str, example: str) -> list[pa.Field[Any]]:
    """Leading columns of a non-table asset."""
    return [
        catalog_tool_column(),
        id_column(what),
        fqn_column(f"{what} (service.name)", example),
        column("name", pa.string(), f"{what.capitalize()} name (the source system's id or key).", nullable=False),
        column("display_name", pa.string(), "Human-readable title shown in the OpenMetadata UI (NULL when unset)."),
        column("service", pa.string(), "Service name the asset was ingested from."),
        column("platform", pa.string(), "Platform from the service type: 'looker', 'airflow', 'kafka', ..."),
        column("description", pa.string(), "Description (Markdown)."),
    ]


def asset_head_values(entity: dict[str, Any]) -> dict[str, Any]:
    """Values of :func:`asset_head`."""
    return {
        "catalog_tool": CATALOG_TOOL,
        "id": entity.get("id"),
        "fqn": entity.get("fullyQualifiedName"),
        "name": entity.get("name"),
        "display_name": entity.get("displayName"),
        "service": get(entity, "service", "name"),
        "platform": platform_of(entity.get("serviceType")),
        "description": entity.get("description"),
    }


DASHBOARDS_SCHEMA = pa.schema(
    [
        *asset_head("dashboard", "looker_prod.sales_overview"),
        column("dashboard_type", pa.string(), "Dashboard or Report."),
        column("charts", STR_LIST, "FQNs of the dashboard's charts."),
        *governance_fields(),
        column("source_url", pa.string(), "Link to the dashboard in its BI tool."),
        *audit_fields(),
    ]
)


class AssetScan(EntityScan[NoArgs]):
    """Non-table assets: search pushdown on service / tier / domain / tags / owners."""

    SEARCH_TERMS: ClassVar[dict[str, str]] = ASSET_SEARCH_TERMS
    SEARCH_CONTAINS: ClassVar[dict[str, str]] = ASSET_SEARCH_CONTAINS


@init_single_worker
@bind_fixed_schema
class DashboardsScan(AssetScan):
    """Rows of ``dashboards``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DASHBOARDS_SCHEMA
    PATH: ClassVar[str] = "/v1/dashboards"
    ENTITY_TYPE: ClassVar[str] = "dashboard"
    FIELDS: ClassVar[str] = "owners,tags,domain,charts"
    SEARCH_INDEX: ClassVar[str | None] = "dashboard_search_index"

    class Meta:
        """Scan metadata."""

        name = "dashboards_scan"
        description = "OpenMetadata dashboards with their charts, owners, tags, tier and domain"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Dashboards",
            category="assets",
            keywords="dashboards, reports, bi, looker, tableau, superset, power bi, metabase",
            doc_llm=(
                "One row per OpenMetadata dashboard: id, fqn, name, display_name, service, platform, description, "
                "dashboard_type, charts (chart FQNs), owners, tags, glossary_terms, tier, domain_id, domain, "
                f"source_url, deleted, updated_at, updated_by, version, url. {PUSHDOWN_NOTE}"
            ),
            doc_md="BI dashboards registered in OpenMetadata.",
            schema=DASHBOARDS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT display_name, len(charts) AS charts FROM openmetadata.main.dashboards "
                    "WHERE service = 'looker_prod' ORDER BY charts DESC"
                ),
                description="Dashboards of one BI service by chart count",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per dashboard."""
        return [
            {
                **asset_head_values(entity),
                "dashboard_type": entity.get("dashboardType"),
                "charts": ref_names(entity.get("charts"), "fullyQualifiedName"),
                **governance_values(entity),
                "source_url": entity.get("sourceUrl"),
                **audit_values(ep, entity, "dashboard"),
            }
        ]


CHARTS_SCHEMA = pa.schema(
    [
        *asset_head("chart", "looker_prod.revenue_by_month"),
        column("chart_type", pa.string(), "Chart type: Line, Bar, Table, Pie, Area, Other, ..."),
        column("dashboards", STR_LIST, "FQNs of the dashboards showing the chart."),
        *governance_fields(),
        column("source_url", pa.string(), "Link to the chart in its BI tool."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class ChartsScan(EntityScan[NoArgs]):
    """Rows of ``charts``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CHARTS_SCHEMA
    PATH: ClassVar[str] = "/v1/charts"
    ENTITY_TYPE: ClassVar[str] = "chart"
    FIELDS: ClassVar[str] = "owners,tags,domain,dashboards"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"service": "service"}

    class Meta:
        """Scan metadata."""

        name = "charts_scan"
        description = "OpenMetadata charts with type, dashboards, owners and tags"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Charts",
            category="assets",
            keywords="charts, visualizations, tiles, bi, dashboards",
            doc_llm=(
                "One row per OpenMetadata chart: id, fqn, name, display_name, service, platform, description, "
                "chart_type, dashboards (dashboard FQNs), owners, tags, glossary_terms, tier, domain_id, domain, "
                "source_url, deleted, updated_at, updated_by, version, url. Pushdown: id / fqn = or IN fetch "
                "directly; service = / IN becomes the list endpoint's service parameter."
            ),
            doc_md="BI charts registered in OpenMetadata.",
            schema=CHARTS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT chart_type, count(*) AS charts FROM openmetadata.main.charts "
                    "GROUP BY chart_type ORDER BY charts DESC"
                ),
                description="Charts per chart type",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per chart."""
        return [
            {
                **asset_head_values(entity),
                "chart_type": entity.get("chartType"),
                "dashboards": ref_names(entity.get("dashboards"), "fullyQualifiedName"),
                **governance_values(entity),
                "source_url": entity.get("sourceUrl"),
                **audit_values(ep, entity, "chart"),
            }
        ]


PIPELINE_TASK = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("display_name", pa.string()),
        pa.field("task_type", pa.string()),
        pa.field("downstream_tasks", STR_LIST),
    ]
)

PIPELINES_SCHEMA = pa.schema(
    [
        *asset_head("pipeline", "airflow_prod.daily_orders"),
        column("tasks", pa.list_(PIPELINE_TASK), "Tasks: name, display_name, task_type and downstream task names."),
        column("schedule_interval", pa.string(), "Schedule (cron or preset) reported by the orchestrator."),
        column("last_run_status", pa.string(), "Status of the latest run: Successful, Failed, Pending, Skipped."),
        column("last_run_at", TSTZ, "When the latest reported run happened."),
        *governance_fields(),
        column("source_url", pa.string(), "Link to the pipeline in its orchestrator."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class PipelinesScan(AssetScan):
    """Rows of ``pipelines``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = PIPELINES_SCHEMA
    PATH: ClassVar[str] = "/v1/pipelines"
    ENTITY_TYPE: ClassVar[str] = "pipeline"
    FIELDS: ClassVar[str] = "owners,tags,domain,tasks,pipelineStatus"
    SEARCH_INDEX: ClassVar[str | None] = "pipeline_search_index"

    class Meta:
        """Scan metadata."""

        name = "pipelines_scan"
        description = "OpenMetadata pipelines with tasks, latest run status, owners and tags"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Pipelines",
            category="assets",
            keywords="pipelines, dags, jobs, airflow, dagster, dbt, fivetran, orchestration, tasks",
            doc_llm=(
                "One row per OpenMetadata pipeline (Airflow DAG, dbt job, Fivetran connector, ...): id, fqn, name, "
                "display_name, service, platform, description, tasks (`STRUCT(name, display_name, "
                "task_type, downstream_tasks)[]`), schedule_interval, last_run_status, last_run_at, owners, tags, "
                f"glossary_terms, tier, domain_id, domain, source_url, deleted, updated_at, updated_by, version, "
                f"url. {PUSHDOWN_NOTE}"
            ),
            doc_md="Data pipelines registered in OpenMetadata with their latest run status.",
            schema=PIPELINES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT fqn, last_run_at FROM openmetadata.main.pipelines "
                    "WHERE last_run_status = 'Failed' ORDER BY last_run_at DESC"
                ),
                description="Pipelines whose latest run failed",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per pipeline."""
        tasks = [
            {
                "name": t.get("name"),
                "display_name": t.get("displayName"),
                "task_type": t.get("taskType"),
                "downstream_tasks": [str(d) for d in t.get("downstreamTasks") or []],
            }
            for t in entity.get("tasks") or []
            if isinstance(t, dict)
        ]
        status = entity.get("pipelineStatus") or {}
        return [
            {
                **asset_head_values(entity),
                "tasks": tasks,
                "schedule_interval": entity.get("scheduleInterval"),
                "last_run_status": status.get("executionStatus"),
                "last_run_at": ts(status.get("timestamp")),
                **governance_values(entity),
                "source_url": entity.get("sourceUrl"),
                **audit_values(ep, entity, "pipeline"),
            }
        ]


TOPICS_SCHEMA = pa.schema(
    [
        *asset_head("topic", "kafka_prod.orders.events"),
        column("partitions", pa.int32(), "Number of partitions."),
        column("replication_factor", pa.int32(), "Number of broker replicas kept of each partition."),
        column("retention_ms", pa.int64(), "Retention time in milliseconds (-1 = forever)."),
        column("max_message_size", pa.int64(), "Maximum message size in bytes."),
        column("cleanup_policies", STR_LIST, "Cleanup policies: delete, compact."),
        column("schema_type", pa.string(), "Message schema type: Avro, Protobuf, JSON, None."),
        *governance_fields(),
        column("source_url", pa.string(), "Link to the topic in its broker UI."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class TopicsScan(AssetScan):
    """Rows of ``topics``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TOPICS_SCHEMA
    PATH: ClassVar[str] = "/v1/topics"
    ENTITY_TYPE: ClassVar[str] = "topic"
    FIELDS: ClassVar[str] = "owners,tags,domain"
    SEARCH_INDEX: ClassVar[str | None] = "topic_search_index"

    class Meta:
        """Scan metadata."""

        name = "topics_scan"
        description = "OpenMetadata messaging topics with partitions, retention, owners and tags"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Topics",
            category="assets",
            keywords="topics, kafka, redpanda, kinesis, streams, messaging, partitions, retention",
            doc_llm=(
                "One row per OpenMetadata topic: id, fqn, name, display_name, service, platform, description, "
                "partitions, replication_factor, retention_ms, max_message_size, cleanup_policies, schema_type, "
                "owners, tags, glossary_terms, tier, domain_id, domain, source_url, deleted, updated_at, "
                f"updated_by, version, url. {PUSHDOWN_NOTE}"
            ),
            doc_md="Messaging topics registered in OpenMetadata.",
            schema=TOPICS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT name, partitions, retention_ms / 86400000 AS retention_days "
                    "FROM openmetadata.main.topics ORDER BY partitions DESC"
                ),
                description="Topics by partition count with retention in days",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per topic."""
        return [
            {
                **asset_head_values(entity),
                "partitions": to_int(entity.get("partitions")),
                "replication_factor": to_int(entity.get("replicationFactor")),
                "retention_ms": to_int(entity.get("retentionTime")),
                "max_message_size": to_int(entity.get("maximumMessageSize")),
                "cleanup_policies": [str(p) for p in entity.get("cleanupPolicies") or []],
                "schema_type": get(entity, "messageSchema", "schemaType"),
                **governance_values(entity),
                "source_url": entity.get("sourceUrl"),
                **audit_values(ep, entity, "topic"),
            }
        ]


ML_MODELS_SCHEMA = pa.schema(
    [
        *asset_head("ML model", "mlflow_prod.churn_model"),
        column("algorithm", pa.string(), "Algorithm, e.g. 'XGBoost'."),
        column("target", pa.string(), "Target (label) variable."),
        column("features", STR_LIST, "Feature names."),
        column("server", pa.string(), "Serving endpoint URL."),
        column("dashboard", pa.string(), "FQN of the dashboard monitoring the model."),
        *governance_fields(),
        column("source_url", pa.string(), "Link to the model in its registry."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class MlModelsScan(AssetScan):
    """Rows of ``ml_models``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ML_MODELS_SCHEMA
    PATH: ClassVar[str] = "/v1/mlmodels"
    ENTITY_TYPE: ClassVar[str] = "mlmodel"
    FIELDS: ClassVar[str] = "owners,tags,domain,dashboard"
    SEARCH_INDEX: ClassVar[str | None] = "mlmodel_search_index"

    class Meta:
        """Scan metadata."""

        name = "ml_models_scan"
        description = "OpenMetadata ML models with algorithm, target, features, owners and tags"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata ML Models",
            category="assets",
            keywords="ml models, machine learning, mlflow, sagemaker, features, model registry",
            doc_llm=(
                "One row per OpenMetadata ML model: id, fqn, name, display_name, service, platform, description, "
                "algorithm, target, features (names), server, dashboard, owners, tags, glossary_terms, tier, "
                f"domain_id, domain, source_url, deleted, updated_at, updated_by, version, url. {PUSHDOWN_NOTE}"
            ),
            doc_md="Machine-learning models registered in OpenMetadata.",
            schema=ML_MODELS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql="SELECT name, algorithm, len(features) AS features FROM openmetadata.main.ml_models ORDER BY name",
                description="ML models with their algorithm and feature count",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per ML model."""
        return [
            {
                **asset_head_values(entity),
                "algorithm": entity.get("algorithm"),
                "target": entity.get("target"),
                "features": [
                    str(f["name"]) for f in entity.get("mlFeatures") or [] if isinstance(f, dict) and f.get("name")
                ],
                "server": entity.get("server"),
                "dashboard": get(entity, "dashboard", "fullyQualifiedName"),
                **governance_values(entity),
                "source_url": entity.get("sourceUrl"),
                **audit_values(ep, entity, "mlmodel"),
            }
        ]


ASSET_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("datasets", DatasetsScan, ["id"]),
    ("columns", ColumnsScan, ["dataset_id", "column_name"]),
    ("database_services", DatabaseServicesScan, ["id"]),
    ("databases", DatabasesScan, ["id"]),
    ("database_schemas", DatabaseSchemasScan, ["id"]),
    ("dashboards", DashboardsScan, ["id"]),
    ("charts", ChartsScan, ["id"]),
    ("pipelines", PipelinesScan, ["id"]),
    ("topics", TopicsScan, ["id"]),
    ("ml_models", MlModelsScan, ["id"]),
]
