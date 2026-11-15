"""Discovery metadata for the ``openmetadata`` catalog: categories, docs and helpers.

DuckDB, coding agents and the ``vgi-lint`` metadata checker read these tags.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "openmetadata"

SOURCE_URL = "https://github.com/Query-farm/vgi-openmetadata"
OPENMETADATA_API_URL = "https://docs.open-metadata.org/latest/main-concepts/metadata-standard/apis"

# Every object's ``vgi.category`` names one of these.
SCHEMA_CATEGORIES = [
    {
        "name": "assets",
        "title": "Data Assets",
        "description": (
            "Database services, databases, schemas, tables (datasets) and columns, dashboards, charts, "
            "pipelines, topics and ML models."
        ),
    },
    {
        "name": "governance",
        "title": "Governance",
        "description": "Owners, users, teams, classifications and tags, glossaries and terms, domains, data products.",
    },
    {
        "name": "lineage",
        "title": "Lineage",
        "description": "Table- and column-level lineage edges and multi-hop lineage traversal.",
    },
    {
        "name": "quality",
        "title": "Quality And Usage",
        "description": "Test suites, test cases and their results, incidents, table profiles, usage and queries.",
    },
    {
        "name": "search",
        "title": "Search",
        "description": "Full-text search over the OpenMetadata search index, and opt-in sample data.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


CATALOG_COMMENT = "OpenMetadata and Collate metadata catalog (assets, governance, lineage, data quality) as SQL tables."

CATALOG_DOC_LLM = (
    "Read-only SQL access to an OpenMetadata (open-source) or Collate (managed) metadata catalog through "
    "its REST API v1. Tables in schema `main`: database_services, databases, database_schemas, datasets "
    "(OpenMetadata tables), columns, dashboards, charts, pipelines, topics, ml_models; owners, users, teams, "
    "classifications, tags, glossaries, glossary_terms, domains, data_products; lineage_edges; test_suites, "
    "assertions (test cases), assertion_results (test case results), incidents, dataset_profiles, usage, "
    "queries. Functions: lineage(fqn, ...) for multi-hop lineage, search(query, ...) for full-text search, "
    "sample_data(fqn) for opt-in sample rows (PII-tagged columns are never returned). Every row has "
    "catalog_tool = 'openmetadata'; tables share column names (qualified_name = lower-cased "
    "database.schema.table, id, platform, description, updated_at, url) with the sibling DataHub worker so "
    "results can be combined with UNION ALL BY NAME. Connect with "
    "CREATE SECRET (TYPE openmetadata, HOST_PORT 'http://localhost:8585/api', TOKEN '<bot JWT>'). "
    "Filters on id / fqn fetch entities directly; filters on service, database, schema, tier, domain, tags "
    "and owners become an Elasticsearch query_filter; time ranges on result / incident timestamps become "
    "startTs / endTs."
)

CATALOG_DOC_MD = (
    "# OpenMetadata\n\n"
    "Query an [OpenMetadata](https://open-metadata.org) or Collate catalog from DuckDB: data assets, "
    "ownership, tags and glossary terms, domains, lineage, data-quality tests and their results, profiles "
    "and usage.\n\n"
    "Connect with an `openmetadata` secret holding the API root (`HOST_PORT`, e.g. "
    "`http://localhost:8585/api`) and a bot JWT (`TOKEN`).\n\n"
    "Read-only. Filters push down into OpenMetadata's search `query_filter`, REST query parameters "
    "and time ranges."
)

SCHEMA_DOC_LLM = (
    "The `main` schema mirrors an OpenMetadata catalog. Start from `datasets` (one row per OpenMetadata "
    "table: id, fqn = service.database.schema.table, qualified_name, service, platform, database, schema, "
    "owners, tags, tier, glossary_terms, domain) and `columns`. Governance: owners (entity -> owner rows), "
    "users, teams, classifications, tags, glossaries, glossary_terms, domains, data_products. Lineage: "
    "lineage_edges (direct table and column edges) and lineage(fqn, upstream_depth, downstream_depth). "
    "Quality: test_suites, assertions (test cases), assertion_results (runs; filter run_at to push a time "
    "range), incidents, dataset_profiles (latest table and column profile), usage (daily query counts) and "
    "queries. search(query, ...) runs full-text search; sample_data(fqn) returns sample rows only on request "
    "and never for PII-tagged columns. Ids are OpenMetadata UUIDs; fqn columns are fully-qualified names."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "OpenMetadata assets, governance metadata, lineage, data quality, profiles and usage, one table per "
    "concept.\n\n"
    "Column names follow the shared data-catalog vocabulary (`catalog_tool`, `id`, `qualified_name`, "
    "`platform`, `description`, `updated_at`, `url`), so results combine with the DataHub worker's tables "
    "through `UNION ALL BY NAME`.\n\n"
    "Filters on ids, FQNs, services, tiers, domains, tags and owners are pushed into OpenMetadata's REST "
    "parameters and search `query_filter`; time ranges on results and incidents become `startTs` / `endTs`."
)

AGENT_TEST_TASKS = [
    {
        "name": "tier1_tables",
        "prompt": ("Using the openmetadata worker, list the qualified names of every Tier 1 table, alphabetically."),
    },
    {
        "name": "pii_columns",
        "prompt": (
            "Using the openmetadata worker, which columns (table qualified name and column name) are "
            "tagged PII.Sensitive?"
        ),
    },
    {
        "name": "failing_tests",
        "prompt": (
            "Using the openmetadata worker, which data-quality test cases are currently failing, and on which tables?"
        ),
    },
    {
        "name": "upstream_of_orders",
        "prompt": ("Using the openmetadata worker, which tables feed directly into the fct_orders table?"),
    },
    {
        "name": "schemas_per_service",
        "prompt": (
            "For each OpenMetadata database service, how many databases and how many database schemas does it have?"
        ),
    },
    {
        "name": "dashboard_charts",
        "prompt": ("Which charts are on the Sales Overview dashboard in OpenMetadata, and what chart type is each?"),
    },
    {
        "name": "failed_pipelines",
        "prompt": ("Which OpenMetadata pipelines failed on their latest run, and what are their task names?"),
    },
    {
        "name": "topic_partitions",
        "prompt": ("How many partitions and what retention in days does each messaging topic in OpenMetadata have?"),
    },
    {
        "name": "model_features",
        "prompt": ("Which features does each ML model registered in OpenMetadata use?"),
    },
    {
        "name": "team_owned_assets",
        "prompt": ("For each OpenMetadata team, how many assets does it own and what type of team is it?"),
    },
    {
        "name": "admin_users",
        "prompt": ("Which human (non-bot) OpenMetadata users are administrators, and which teams are they in?"),
    },
    {
        "name": "pii_tag_usage",
        "prompt": ("List every tag of the PII classification in OpenMetadata with how many times it is applied."),
    },
    {
        "name": "unapproved_terms",
        "prompt": (
            "Which OpenMetadata glossary terms are not approved yet, and which glossary (by display name) are they in?"
        ),
    },
    {
        "name": "sales_data_products",
        "prompt": (
            "List the OpenMetadata data products in the Sales domain with their asset counts, and the "
            "Sales domain's type."
        ),
    },
    {
        "name": "suite_failures",
        "prompt": (
            "For each basic OpenMetadata test suite, how many failed test runs were recorded on or after 2026-11-01?"
        ),
    },
    {
        "name": "open_incidents",
        "prompt": (
            "Which OpenMetadata data-quality incidents are not resolved yet, who are they assigned to, "
            "and on which table?"
        ),
    },
    {
        "name": "largest_profiled",
        "prompt": ("Which profiled table has the most rows according to OpenMetadata's profiler, and how many?"),
    },
    {
        "name": "most_used_tables",
        "prompt": (
            "What are the three most-queried tables in OpenMetadata over the last month, with their "
            "monthly query counts?"
        ),
    },
    {
        "name": "slowest_query",
        "prompt": (
            "What is the SQL text of the slowest query recorded in OpenMetadata and how long did it "
            "take in milliseconds?"
        ),
    },
    {
        "name": "customer_impact",
        "prompt": (
            "Which dashboards and ML models are downstream (at any distance) of the OpenMetadata table "
            "snowflake_prod.analytics.raw.customers?"
        ),
    },
    {
        "name": "search_bob_tier1",
        "prompt": ("Using OpenMetadata search, find the Tier 1 tables owned by bob."),
    },
    {
        "name": "sample_rows",
        "prompt": ("How many sample rows does OpenMetadata hold for snowflake_prod.analytics.marts.fct_orders?"),
    },
    {
        "name": "fqn_helpers",
        "prompt": (
            "Without calling the OpenMetadata server, what are the cross-catalog join key and the "
            "service name of the FQN 'snowflake_prod.ANALYTICS.Marts.FCT_ORDERS'?"
        ),
    },
]

CATALOG_TAGS = {
    "vgi.title": "OpenMetadata Catalog",
    "vgi.subtitle": "OpenMetadata and Collate assets, ownership, glossary, lineage and data quality as SQL tables.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "openmetadata, collate, data catalog, metadata, lineage, data quality, glossary, data governance"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
