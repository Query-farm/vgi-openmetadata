<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Your OpenMetadata Catalog, Lineage And Data Quality in DuckDB

> **vgi-openmetadata** · a [Query.Farm](https://query.farm) VGI worker

Read-only SQL over an [OpenMetadata](https://open-metadata.org) server (open source) or
[Collate](https://www.getcollate.io) (managed OpenMetadata) through its REST API v1: tables and
columns, owners, tags, glossary terms, domains, table- and column-level lineage, data-quality
tests and their results, incidents, profiles, usage and queries. Every row carries
`catalog_tool = 'openmetadata'` and the column names follow the shared data-catalog vocabulary
used by [vgi-datahub](https://github.com/Query-farm/vgi-datahub), so the two catalogs combine with
`UNION ALL BY NAME`.

## Install

Workers are not published to PyPI; run a tagged release straight from GitHub:

```sh
uvx --from git+https://github.com/Query-farm/vgi-openmetadata@v0.1.1 vgi-openmetadata-worker --help
```

From a checkout, `make build` creates `.venv/bin/vgi-openmetadata-worker`.

## Connect to OpenMetadata

```sql
-- DuckDB 2.0
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
-- Haybarn 2.0: INSTALL vgi FROM community; LOAD vgi;

ATTACH 'openmetadata' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-openmetadata@v0.1.1 vgi-openmetadata-worker');

CREATE SECRET om (
  TYPE openmetadata,
  HOST_PORT 'http://localhost:8585/api',   -- or https://<tenant>.getcollate.io/api
  TOKEN '<bot JWT>'                        -- Settings > Bots > ingestion-bot (or a personal access token)
);
```

| Secret field | Meaning |
|---|---|
| `HOST_PORT` | API root, the same `hostPort` the OpenMetadata SDK takes. `/api` is appended when missing. |
| `TOKEN` | JWT sent as `Authorization: Bearer`. Redacted in `duckdb_secrets()`; never logged or kept in scan state. |
| `UI_URL` | Optional UI root for the `url` columns (default: `HOST_PORT` without `/api`). |
| `PAGE_SIZE` | Optional `limit` / `size` per request, 1..1000 (default 100). |

HTTP 429 and 5xx responses are retried with exponential backoff that honours `Retry-After`.
The worker only issues `GET` requests.

## Tables and functions (schema `main`)

| Object | What it returns |
|---|---|
| `datasets` | One row per OpenMetadata table: `id`, `fqn`, `platform`, `service`, `database`, `schema`, `name`, `qualified_name`, `subtype`, `owners`, `tags`, `glossary_terms`, `tier`, `domain`, `data_products`, `custom_properties`, ... |
| `columns` | Every (nested) column: type, constraint, primary key, tags, `is_pii` |
| `database_services`, `databases`, `database_schemas` | The asset hierarchy (connection settings are never exposed) |
| `dashboards`, `charts`, `pipelines`, `topics`, `ml_models` | Other data assets with owners, tags, tier, domain |
| `owners` | One row per (asset, owner) across all asset types |
| `users`, `teams` | People, bots and the team hierarchy |
| `classifications`, `tags` | Tag categories (PII, Tier, ...) and their tags with usage counts |
| `glossaries`, `glossary_terms` | Business glossary with hierarchy, synonyms, related terms, status |
| `domains`, `data_products` | Data-mesh domains, subdomains and data products |
| `lineage_edges` | Direct lineage edges: `level = 'dataset'` rows plus one `level = 'column'` row per column pair |
| `lineage(fqn, entity_type := 'table', upstream_depth := 3, downstream_depth := 3)` | Multi-hop lineage with `direction` and `degree` |
| `test_suites`, `assertions`, `assertion_results` | Test suites, test cases (latest status) and their result history |
| `incidents` | Test-case incidents at their latest state (New / Ack / Assigned / Resolved) |
| `dataset_profiles` | Latest profiler run per table with per-column statistics |
| `usage` | Query counts and percentile ranks (latest day per table, or daily history for one table) |
| `queries` | Query entities with the tables they use |
| `search(query, entity_type := 'table', service, database, schema_name, tag, owner, domain, tier, max_results)` | Full-text search over the OpenMetadata search index |
| `sample_data(fqn, max_rows := 100)` | Opt-in sample rows in long form; PII-tagged columns are never returned, a PII-tagged table returns nothing |
| `fqn_qualified_name(fqn)`, `split_fqn(fqn)` | Offline FQN helpers (no server call) |

Shared vocabulary with the other catalog workers: `datasets` (with lower-cased `qualified_name` =
`database.schema.table`), `columns`, `owners`, `tags`, `glossary_terms`, `domains`,
`lineage_edges`, `assertions` / `assertion_results`, `usage`; common columns `catalog_tool`, `id`,
`platform`, `qualified_name`, `description`, `updated_at`, `url`. OpenMetadata ids are UUIDs; the
`fqn` columns hold fully-qualified names (`service.database.schema.table`).

## What pushes down

| Filter | Becomes |
|---|---|
| `id = / IN`, `fqn = / IN` (and `dataset_id`, `table_fqn`, `username`, `name`, ...) | `GET <collection>/{id}` or `GET <collection>/name/{fqn}`; no listing |
| `service`, `database`, `schema`, `database_fqn`, `schema_fqn`, `subtype`, `tier`, `domain`, `domain_id` `= / IN` on `datasets` (and the asset tables) | an Elasticsearch / OpenSearch `query_filter` on `GET /v1/search/query`, paged with `search_after` |
| `list_contains(tags, 'x')`, `list_contains(glossary_terms, 'x')`, `list_contains(owners, 'x')` | `tags.tagFQN` / `owners.name.keyword` terms in the same `query_filter` |
| `service =` on `databases`, `charts`, `queries`; `database_fqn =` on `database_schemas`; `classification =` on `tags`; `glossary_id =` / `parent_id =` on `glossary_terms`; `domain =` on `data_products`; `suite_type =` on `test_suites`; `is_bot =` / `is_admin =` on `users` | the REST list endpoint's own query parameter |
| `entity_type = / IN` on `owners` | which collections are listed |
| `table_fqn =` / `test_suite_id =` on `assertions`, `assertion_results` | `entityLink=<#E::table::fqn>` / `testSuiteId` |
| `run_at` ranges on `assertion_results`; `updated_at` ranges, `state =`, `test_case_fqn =` on `incidents` | `startTs` / `endTs`, `testCaseResolutionStatusType`, `testCaseFQN` |
| `downstream_fqn =` / `upstream_fqn =` on `lineage_edges` | one `/v1/lineage/table/name/{fqn}` call per table instead of one per table in the catalog |
| `usage_date` ranges with `table_fqn =` on `usage` | `/v1/usage/table/name/{fqn}?date=&days=` (up to 30 days) |

Every predicate is re-applied by DuckDB afterwards, so pushdown only narrows what is fetched.
Cursors (`after`, `search_after`, offsets) live in table-function state, so long scans resume on
any worker process over the HTTP transport.

## Examples

```sql
-- Tier 1 tables and their owners (tier pushes into the search query_filter)
SELECT qualified_name, owners FROM openmetadata.main.datasets WHERE tier = 'Tier.Tier1' ORDER BY 1;

-- PII columns
SELECT qualified_name, column_name FROM openmetadata.main.columns WHERE list_contains(tags, 'PII.Sensitive');

-- What feeds fct_orders, column by column
SELECT upstream_qualified_name, upstream_column, downstream_column, transformation
FROM openmetadata.main.lineage_edges
WHERE downstream_fqn = 'snowflake_prod.analytics.marts.fct_orders' AND level = 'column';

-- Dashboards affected by a change to a raw table
SELECT DISTINCT downstream_fqn FROM openmetadata.main.lineage('snowflake_prod.analytics.raw.customers',
  upstream_depth := 0, downstream_depth := 5) WHERE downstream_type = 'dashboard';

-- Failing tests and their recent history
SELECT qualified_name, name, last_result FROM openmetadata.main.assertions WHERE last_status = 'Failed';
SELECT run_at, status FROM openmetadata.main.assertion_results
WHERE table_fqn = 'snowflake_prod.analytics.marts.fct_orders' AND run_at >= now() - INTERVAL 7 DAY;

-- One catalog view across OpenMetadata and DataHub
SELECT catalog_tool, qualified_name, owners FROM openmetadata.main.datasets
UNION ALL BY NAME
SELECT catalog_tool, qualified_name, owners FROM datahub.main.datasets;
```

## Testing

OpenMetadata needs MySQL/PostgreSQL plus Elasticsearch/OpenSearch (4 GB+), so the tests run
against `test/mock_openmetadata/server.py`: a strict replay of the OpenMetadata **1.6** REST API
over `test/fixtures/openmetadata.json` (regenerate with `python test/fixtures/build_fixture.py`).
It rejects unknown query parameters and `fields=` values, strips unrequested optional fields,
validates the search `query_filter` structure and the fields each index maps, issues opaque
`after` cursors, and requires `startTs` / `endTs` on time series. Three instances run per suite:
one answers every third new request with `429 Retry-After: 0`, one requires a bearer token, and a
strict one refuses unfiltered table / test-case listings, searches without `query_filter`, and
`startTs=0`, which is how the SQL tests prove pushdown. `GET /__requests` exposes the request log,
which the SQL tests read with `read_json` to assert the exact `query_filter`, `startTs` and
`days` the worker sent. No network access is needed.

```sh
make test-unit                  # pytest
make test-sql TRANSPORT=stdio   # also launch, http
make lint-explain               # vgi-lint metadata gate (100/100 at fail_on = info)
make check                      # everything
```

Version notes: pinned to the OpenMetadata 1.6 response shapes. The worker also reads the legacy
single `owner` (pre-1.5) and the plural `domains` (1.8+) fields. `created_at` is not provided:
OpenMetadata entities carry only `updatedAt`.

## Support

Open an issue at https://github.com/Query-farm/vgi-openmetadata/issues. For VGI itself, see
[query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD).
