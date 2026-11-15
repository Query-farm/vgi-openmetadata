"""Data quality and usage: test suites, test cases (assertions), results, incidents, profiles, usage, queries."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .assets import TABLE_SEARCH_TERMS, audit_fields, audit_values, fqn_column, id_column, scan_tags, table_identity
from .client import SECRET_REQUIREMENT, Endpoint, NotFoundError, get_json, quote_fqn
from .common import (
    CATALOG_TOOL,
    STR_LIST,
    STR_MAP,
    TSTZ,
    NoArgs,
    catalog_tool_column,
    eq_values,
    get,
    owner_names,
    qualified_name,
    range_bounds,
    ref_names,
    split_fqn,
    tag_fqns,
    to_float,
    to_int,
    to_millis,
    to_str,
    ts,
)
from .meta import column
from .scan import EntityScan, ScanCursor

DAY_MS = 86_400_000


def parse_entity_link(link: str | None) -> tuple[str | None, str | None]:
    """``(table_fqn, column_name)`` of an entity link ``<#E::table::svc.db.sch.t::columns::col>``."""
    if not link:
        return None, None
    parts = link.strip().removeprefix("<").removesuffix(">").split("::")
    if len(parts) < 3 or parts[0] != "#E" or parts[1] != "table":
        return None, None
    column_name = parts[4] if len(parts) >= 5 and parts[3] == "columns" else None
    return parts[2], column_name


def entity_link(table_fqn: str) -> str:
    """Return the entity link of a table FQN."""
    return f"<#E::table::{table_fqn}>"


def time_window(params: ProcessParams[Any], col: str) -> dict[str, int]:
    """``startTs`` / ``endTs`` (epoch ms) from range filters on ``col``; open ends become 0 / tomorrow."""
    lower, upper = range_bounds(params.current_pushdown_filters, col)
    start = to_millis(lower)
    end = to_millis(upper)
    now = int(datetime.now(tz=UTC).timestamp() * 1000)
    return {"startTs": start if start is not None else 0, "endTs": end if end is not None else now + DAY_MS}


def test_case_sources(params: ProcessParams[Any]) -> list[dict[str, Any]]:
    """Test-case listings narrowed by ``table_fqn`` (entityLink) and ``test_suite_id`` pushdown."""
    filters = params.current_pushdown_filters
    tables = eq_values(filters, "table_fqn")
    suites = eq_values(filters, "test_suite_id")
    combos: list[dict[str, str]] = [{}]
    if tables is not None:
        combos = [{**c, "entityLink": entity_link(str(t)), "includeAllTests": "true"} for c in combos for t in tables]
    if suites is not None:
        combos = [{**c, "testSuiteId": str(s)} for c in combos for s in suites]
    return [
        {
            "path": "/v1/dataQuality/testCases",
            "fields": "testDefinition,testSuite,testCaseResult,owners,tags,incidentId",
            "entity_type": "testCase",
            "params": c,
        }
        for c in combos
    ]


# ----------------------------------------------------------------------------- test suites

TEST_SUITES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("test suite"),
        fqn_column("test suite", "snowflake_prod.analytics.marts.fct_orders.testSuite"),
        column("name", pa.string(), "Test suite name.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("description", pa.string(), "Test suite description."),
        column("suite_type", pa.string(), "'basic' (the table's own suite) or 'logical' (a user-defined grouping)."),
        column("table_fqn", pa.string(), "FQN of the table a basic suite belongs to."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table of that table."),
        column("test_count", pa.int64(), "Number of test cases in the suite."),
        column("success", pa.int64(), "Count of test cases whose latest result is Success."),
        column("failed", pa.int64(), "Count of test cases whose latest result is Failed."),
        column("aborted", pa.int64(), "Count of test cases whose latest result is Aborted."),
        column("queued", pa.int64(), "Count of test cases whose latest result is Queued."),
        column("owners", STR_LIST, "Owner names."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class TestSuitesScan(EntityScan[NoArgs]):
    """Rows of ``test_suites``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TEST_SUITES_SCHEMA
    PATH: ClassVar[str] = "/v1/dataQuality/testSuites"
    ENTITY_TYPE: ClassVar[str] = "testSuite"
    FIELDS: ClassVar[str] = "owners,tests,summary"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"suite_type": "testSuiteType"}

    class Meta:
        """Scan metadata."""

        name = "test_suites_scan"
        description = "OpenMetadata data-quality test suites with their latest result summary"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Test Suites",
            category="quality",
            keywords="test suites, data quality, data tests, checks, success, failed",
            doc_llm=(
                "One row per OpenMetadata test suite: id, fqn, name, display_name, description, suite_type "
                "('basic' = a table's own suite, 'logical' = a grouping), table_fqn, qualified_name, test_count, "
                "success / failed / aborted / queued (latest-result counts), owners, deleted, updated_at, "
                "updated_by, version, url. Pushdown: id / fqn = or IN fetch directly; suite_type = / IN becomes "
                "the testSuiteType parameter."
            ),
            doc_md="Data-quality test suites.",
            schema=TEST_SUITES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, failed, test_count FROM openmetadata.main.test_suites "
                    "WHERE suite_type = 'basic' AND failed > 0 ORDER BY failed DESC"
                ),
                description="Tables with failing tests",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per test suite."""
        basic = bool(entity.get("basic") or entity.get("executable"))
        table = get(entity, "basicEntityReference", "fullyQualifiedName") or get(
            entity, "executableEntityReference", "fullyQualifiedName"
        )
        summary = entity.get("summary") or {}
        tests = entity.get("tests")
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName"),
                "name": entity.get("name"),
                "display_name": entity.get("displayName"),
                "description": entity.get("description"),
                "suite_type": "basic" if basic else "logical",
                "table_fqn": table,
                "qualified_name": qualified_name(table),
                "test_count": len(tests) if isinstance(tests, list) else to_int(summary.get("total")),
                "success": to_int(summary.get("success")),
                "failed": to_int(summary.get("failed")),
                "aborted": to_int(summary.get("aborted")),
                "queued": to_int(summary.get("queued")),
                "owners": owner_names(entity),
                **audit_values(ep, entity, "testSuite"),
            }
        ]


# ----------------------------------------------------------------------------- assertions (test cases)

ASSERTIONS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("test case"),
        fqn_column("test case", "snowflake_prod.analytics.marts.fct_orders.order_id.order_id_unique"),
        column("name", pa.string(), "Test case name.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("table_fqn", pa.string(), "FQN of the tested table (datasets.fqn)."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table of the tested table."),
        column("column_name", pa.string(), "Tested column for column-level tests, else NULL."),
        column("assertion_type", pa.string(), "'table' or 'column'."),
        column(
            "test_definition",
            pa.string(),
            "Test definition, e.g. 'columnValuesToBeUnique', 'tableRowCountToBeBetween'.",
        ),
        column("description", pa.string(), "Test case description."),
        column("parameters", STR_MAP, "Test parameters (name -> value)."),
        column("test_suite_id", pa.string(), "Id of the test suite (test_suites.id)."),
        column("test_suite", pa.string(), "FQN of the test suite."),
        column("last_status", pa.string(), "Latest result: Success, Failed, Aborted or Queued; NULL if never run."),
        column("last_result", pa.string(), "Latest result message."),
        column("last_run_at", TSTZ, "When the latest result was recorded."),
        column("incident_id", pa.string(), "Id of the open incident (incidents.id), if any."),
        column("owners", STR_LIST, "Owner names."),
        column("tags", STR_LIST, "Classification tag FQNs on the test case."),
        *audit_fields(),
    ]
)


def test_case_identity(entity: dict[str, Any]) -> dict[str, Any]:
    """Table / column the test case targets."""
    table, col = parse_entity_link(entity.get("entityLink"))
    if table is None:
        parts = split_fqn(entity.get("fullyQualifiedName"))
        table = ".".join(parts[:4]) if len(parts) >= 5 else None
    return {"table_fqn": table, "qualified_name": qualified_name(table), "column_name": col}


@init_single_worker
@bind_fixed_schema
class AssertionsScan(EntityScan[NoArgs]):
    """Rows of ``assertions``: OpenMetadata test cases."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ASSERTIONS_SCHEMA
    PATH: ClassVar[str] = "/v1/dataQuality/testCases"
    ENTITY_TYPE: ClassVar[str] = "testCase"
    FIELDS: ClassVar[str] = "testDefinition,testSuite,testCaseResult,owners,tags,incidentId"

    class Meta:
        """Scan metadata."""

        name = "assertions_scan"
        description = "OpenMetadata test cases (assertions) with their latest result"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Test Cases",
            category="quality",
            keywords="assertions, test cases, data quality, data tests, checks, failing tests",
            doc_llm=(
                "One row per OpenMetadata test case (the shared vocabulary calls it an assertion): id, fqn, name, "
                "display_name, table_fqn, qualified_name, column_name, assertion_type ('table' | 'column'), "
                "test_definition, description, parameters (`MAP(VARCHAR, VARCHAR)`), test_suite_id, test_suite, "
                "last_status (Success, "
                "Failed, Aborted, Queued), last_result, last_run_at, incident_id, owners, tags, deleted, "
                "updated_at, updated_by, version, url. Pushdown: id / fqn = or IN fetch directly; table_fqn = / IN "
                "becomes entityLink; test_suite_id = / IN becomes testSuiteId. Run history is in "
                "assertion_results."
            ),
            doc_md="Data-quality test cases and their latest status.",
            schema=ASSERTIONS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, name, last_result FROM openmetadata.main.assertions "
                    "WHERE last_status = 'Failed' ORDER BY qualified_name, name"
                ),
                description="Currently failing tests",
            ),
            FunctionExample(
                sql=(
                    "SELECT test_definition, count(*) AS tests FROM openmetadata.main.assertions "
                    "WHERE table_fqn = 'snowflake_prod.analytics.marts.fct_orders' GROUP BY test_definition "
                    "ORDER BY tests DESC"
                ),
                description="Test definitions used on one table (table_fqn pushes down as entityLink)",
            ),
        ]

    @classmethod
    def list_sources(cls, params: ProcessParams[Any]) -> list[dict[str, Any]] | None:
        """Test-case listings narrowed by table and suite pushdown."""
        return test_case_sources(params)

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per test case."""
        ident = test_case_identity(entity)
        result = entity.get("testCaseResult") or {}
        params = [
            (str(p["name"]), to_str(p.get("value")))
            for p in entity.get("parameterValues") or []
            if isinstance(p, dict) and p.get("name")
        ]
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName"),
                "name": entity.get("name"),
                "display_name": entity.get("displayName"),
                **ident,
                "assertion_type": "column" if ident["column_name"] else "table",
                "test_definition": get(entity, "testDefinition", "name")
                or get(entity, "testDefinition", "fullyQualifiedName"),
                "description": entity.get("description"),
                "parameters": params or None,
                "test_suite_id": get(entity, "testSuite", "id"),
                "test_suite": get(entity, "testSuite", "fullyQualifiedName"),
                "last_status": result.get("testCaseStatus"),
                "last_result": result.get("result"),
                "last_run_at": ts(result.get("timestamp")),
                "incident_id": to_str(entity.get("incidentId")),
                "owners": owner_names(entity),
                "tags": tag_fqns(entity),
                **audit_values(ep, entity, "testCase"),
            }
        ]


# ----------------------------------------------------------------------------- assertion results

ASSERTION_RESULTS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("assertion_id", pa.string(), "Id of the test case (assertions.id).", nullable=False),
        column("assertion_fqn", pa.string(), "FQN of the test case (assertions.fqn).", nullable=False),
        column("table_fqn", pa.string(), "FQN of the tested table."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table of the tested table."),
        column("column_name", pa.string(), "Tested column, else NULL."),
        column("test_definition", pa.string(), "Test definition name."),
        column("run_at", TSTZ, "When the result was recorded.", nullable=False),
        column("status", pa.string(), "Success, Failed, Aborted or Queued."),
        column("result", pa.string(), "Result message."),
        column("passed_rows", pa.int64(), "Rows that passed (row-level tests)."),
        column("failed_rows", pa.int64(), "Rows that failed (row-level tests)."),
        column("passed_rows_percentage", pa.float64(), "Percentage (0-100) of the tested rows that passed."),
        column("failed_rows_percentage", pa.float64(), "Percentage (0-100) of the tested rows that failed."),
        column("result_values", STR_MAP, "Measured values (name -> value), e.g. rowCount -> 1200."),
        column("incident_id", pa.string(), "Incident opened for this result, if any."),
    ]
)


@init_single_worker
@bind_fixed_schema
class AssertionResultsScan(EntityScan[NoArgs]):
    """Rows of ``assertion_results``: OpenMetadata test case results."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ASSERTION_RESULTS_SCHEMA
    PATH: ClassVar[str] = "/v1/dataQuality/testCases"
    ENTITY_TYPE: ClassVar[str] = "testCase"
    FIELDS: ClassVar[str] = "testDefinition"
    ID_COLUMN: ClassVar[str | None] = "assertion_id"
    FQN_COLUMN: ClassVar[str | None] = "assertion_fqn"

    class Meta:
        """Scan metadata."""

        name = "assertion_results_scan"
        description = "OpenMetadata test case results over time (run_at ranges push down)"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Test Results",
            category="quality",
            keywords="assertion results, test results, data quality history, failures, pass rate, runs",
            doc_llm=(
                "One row per OpenMetadata test case result: assertion_id, assertion_fqn, table_fqn, "
                "qualified_name, column_name, test_definition, run_at, status (Success, Failed, Aborted, Queued), "
                "result, passed_rows, failed_rows, passed_rows_percentage, failed_rows_percentage, result_values "
                "(`MAP(VARCHAR, VARCHAR)`), incident_id. Pushdown: run_at >, >=, <, <=, = and BETWEEN become "
                "startTs / endTs; "
                "assertion_id / assertion_fqn = or IN fetch only those test cases; table_fqn = / IN becomes "
                "entityLink. Results are fetched per test case from "
                "/v1/dataQuality/testCases/testCaseResults/{fqn}, so filter by table or test case on big servers."
            ),
            doc_md="Data-quality test results over time.",
            schema=ASSERTION_RESULTS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, count(*) FILTER (WHERE status = 'Failed') AS failures, count(*) AS runs "
                    "FROM openmetadata.main.assertion_results WHERE run_at >= TIMESTAMPTZ '2026-11-01 00:00:00+00' "
                    "GROUP BY qualified_name ORDER BY failures DESC"
                ),
                description="Failure counts per table since a date (run_at pushes down as startTs)",
            ),
        ]

    @classmethod
    def extra_plan(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Resolve the run_at window."""
        return time_window(params, "run_at")

    @classmethod
    def list_sources(cls, params: ProcessParams[Any]) -> list[dict[str, Any]] | None:
        """Test-case listings narrowed by table pushdown."""
        sources = test_case_sources(params)
        for s in sources:
            s["fields"] = cls.FIELDS
        return sources

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Every result of one test case in the window."""
        fqn = entity.get("fullyQualifiedName")
        if not fqn:
            return []
        window = plan["extra"]
        try:
            body = get_json(
                ep,
                f"/v1/dataQuality/testCases/testCaseResults/{quote_fqn(str(fqn))}",
                {"startTs": window["startTs"], "endTs": window["endTs"]},
            )
        except NotFoundError:
            return []
        ident = test_case_identity(entity)
        definition = get(entity, "testDefinition", "name")
        rows = []
        for r in (body or {}).get("data") or []:
            if not isinstance(r, dict) or ts(r.get("timestamp")) is None:
                continue
            values = [
                (str(v["name"]), to_str(v.get("value")))
                for v in r.get("testResultValue") or []
                if isinstance(v, dict) and v.get("name")
            ]
            rows.append(
                {
                    "catalog_tool": CATALOG_TOOL,
                    "assertion_id": entity.get("id"),
                    "assertion_fqn": fqn,
                    **ident,
                    "test_definition": definition,
                    "run_at": ts(r.get("timestamp")),
                    "status": r.get("testCaseStatus"),
                    "result": r.get("result"),
                    "passed_rows": to_int(r.get("passedRows")),
                    "failed_rows": to_int(r.get("failedRows")),
                    "passed_rows_percentage": to_float(r.get("passedRowsPercentage")),
                    "failed_rows_percentage": to_float(r.get("failedRowsPercentage")),
                    "result_values": values or None,
                    "incident_id": to_str(r.get("incidentId")),
                }
            )
        return rows


# ----------------------------------------------------------------------------- incidents

INCIDENTS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column(
            "id", pa.string(), "Incident id (the stateId shared by all status records of one incident).", nullable=False
        ),
        column("status_id", pa.string(), "Id of the latest status record."),
        column("test_case_id", pa.string(), "Id of the failing test case (assertions.id)."),
        column("test_case_fqn", pa.string(), "FQN of the failing test case."),
        column("table_fqn", pa.string(), "FQN of the affected table."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table of the affected table."),
        column("state", pa.string(), "Latest state: New, Ack, Assigned or Resolved."),
        column("severity", pa.string(), "Severity1 (highest) .. Severity5, when set."),
        column("assignee", pa.string(), "User the incident is assigned to."),
        column("resolved_by", pa.string(), "User who resolved the incident."),
        column("failure_reason", pa.string(), "Resolution reason: FalsePositive, MissingData, Duplicates, Other, ..."),
        column("failure_comment", pa.string(), "Resolution comment."),
        column("updated_at", TSTZ, "When the latest status was recorded."),
        column("updated_by", pa.string(), "Who recorded the latest status."),
        column("url", pa.string(), "Link to the test case's incident page in the OpenMetadata UI."),
    ]
)

INCIDENTS_PATH = "/v1/dataQuality/testCases/testCaseIncidentStatus"


@init_single_worker
@bind_fixed_schema
class IncidentsScan(EntityScan[NoArgs]):
    """Rows of ``incidents``: the latest status of every test-case incident."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = INCIDENTS_SCHEMA
    PATH: ClassVar[str] = INCIDENTS_PATH
    ENTITY_TYPE: ClassVar[str] = "testCase"
    ID_COLUMN: ClassVar[str | None] = None
    FQN_COLUMN: ClassVar[str | None] = None

    class Meta:
        """Scan metadata."""

        name = "incidents_scan"
        description = "OpenMetadata data-quality incidents with their latest state (time ranges push down)"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Incidents",
            category="quality",
            keywords="incidents, data quality incidents, triage, resolution, assignee, severity",
            doc_llm=(
                "One row per OpenMetadata test-case incident, at its latest status: id (stateId), status_id, "
                "test_case_id, test_case_fqn, table_fqn, qualified_name, state (New, Ack, Assigned, Resolved), "
                "severity, assignee, resolved_by, failure_reason, failure_comment, updated_at, updated_by, url. "
                "Pushdown: updated_at ranges become startTs / endTs; state = becomes "
                "testCaseResolutionStatusType; test_case_fqn = becomes testCaseFQN."
            ),
            doc_md="Data-quality incidents and their triage state.",
            schema=INCIDENTS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, test_case_fqn, assignee FROM openmetadata.main.incidents "
                    "WHERE state <> 'Resolved' ORDER BY updated_at"
                ),
                description="Open incidents, oldest first",
            ),
        ]

    @classmethod
    def extra_plan(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Resolve the window and state / test-case parameters."""
        window: dict[str, Any] = time_window(params, "updated_at")
        filters = params.current_pushdown_filters
        for col, param in (("state", "testCaseResolutionStatusType"), ("test_case_fqn", "testCaseFQN")):
            values = eq_values(filters, col)
            if values is None:
                continue
            if len(values) != 1:
                return {**window, "empty": not values}
            window[param] = str(values[0])
        return window

    @classmethod
    def list_sources(cls, params: ProcessParams[Any]) -> list[dict[str, Any]] | None:
        """One offset-paged listing."""
        return [{"path": INCIDENTS_PATH, "fields": "", "entity_type": "testCase", "params": {}}]

    @classmethod
    def fetch_page(cls, ep: Endpoint, plan: dict[str, Any], state: ScanCursor) -> list[dict[str, Any]]:
        """Offset paging (this endpoint has no ``after`` cursor)."""
        extra = {k: v for k, v in plan["extra"].items() if k != "empty"}
        body = get_json(ep, INCIDENTS_PATH, {**extra, "latest": "true", "limit": ep.page_size, "offset": state.offset})
        data = [d for d in (body or {}).get("data") or [] if isinstance(d, dict)]
        state.offset += len(data)
        total = to_int(get(body, "paging", "total"))
        state.done = len(data) < ep.page_size or (total is not None and state.offset >= total)
        return data

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per incident."""
        details = entity.get("testCaseResolutionStatusDetails") or {}
        test_fqn = get(entity, "testCaseReference", "fullyQualifiedName")
        parts = split_fqn(test_fqn)
        table = ".".join(parts[:4]) if len(parts) >= 5 else None
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": to_str(entity.get("stateId")) or to_str(entity.get("id")),
                "status_id": to_str(entity.get("id")),
                "test_case_id": get(entity, "testCaseReference", "id"),
                "test_case_fqn": test_fqn,
                "table_fqn": table,
                "qualified_name": qualified_name(table),
                "state": entity.get("testCaseResolutionStatusType"),
                "severity": entity.get("severity"),
                "assignee": get(details, "assignee", "name"),
                "resolved_by": get(details, "resolvedBy", "name"),
                "failure_reason": details.get("testCaseFailureReason"),
                "failure_comment": details.get("testCaseFailureComment"),
                "updated_at": ts(entity.get("timestamp")),
                "updated_by": get(entity, "updatedBy", "name") or to_str(entity.get("updatedBy")),
                "url": ep.entity_url("testCase", test_fqn),
            }
        ]


# ----------------------------------------------------------------------------- dataset profiles

FIELD_PROFILE = pa.struct(
    [
        pa.field("column_name", pa.string()),
        pa.field("null_count", pa.int64()),
        pa.field("null_proportion", pa.float64()),
        pa.field("unique_count", pa.int64()),
        pa.field("unique_proportion", pa.float64()),
        pa.field("min", pa.string()),
        pa.field("max", pa.string()),
        pa.field("mean", pa.string()),
        pa.field("median", pa.string()),
        pa.field("stdev", pa.string()),
    ]
)


def table_key_columns() -> list[pa.Field[Any]]:
    """``dataset_id`` / ``table_fqn`` / platform / service / database / schema / qualified_name."""
    return [
        column("dataset_id", pa.string(), "Id of the table (datasets.id).", nullable=False),
        column("table_fqn", pa.string(), "FQN of the table (datasets.fqn).", nullable=False),
        column("platform", pa.string(), "Data platform of the table."),
        column("service", pa.string(), "Database service name."),
        column("database", pa.string(), "Database name."),
        column("schema", pa.string(), "Schema name."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table of the table."),
    ]


def table_key_values(entity: dict[str, Any]) -> dict[str, Any]:
    """Values of :func:`table_key_columns`."""
    ident = table_identity(entity)
    return {
        "catalog_tool": CATALOG_TOOL,
        "dataset_id": entity.get("id"),
        "table_fqn": entity.get("fullyQualifiedName"),
        "platform": ident["platform"],
        "service": ident["service"],
        "database": ident["database"],
        "schema": ident["schema"],
        "qualified_name": ident["qualified_name"],
    }


PROFILES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        *table_key_columns(),
        column("profiled_at", TSTZ, "When the profile was taken.", nullable=False),
        column("row_count", pa.int64(), "Rows in the table."),
        column("column_count", pa.int64(), "Columns in the table."),
        column("size_bytes", pa.int64(), "Table size in bytes."),
        column(
            "column_profiles",
            pa.list_(FIELD_PROFILE),
            "Per-column statistics: STRUCT(column_name, null_count, null_proportion, unique_count, "
            "unique_proportion, min, max, mean, median, stdev); min..stdev as text.",
        ),
    ]
)

TABLE_SCAN_TERMS = {k: TABLE_SEARCH_TERMS[k] for k in ("service", "database", "schema")}


@init_single_worker
@bind_fixed_schema
class DatasetProfilesScan(EntityScan[NoArgs]):
    """Rows of ``dataset_profiles``: the latest table and column profile of each table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = PROFILES_SCHEMA
    PATH: ClassVar[str] = "/v1/tables"
    ENTITY_TYPE: ClassVar[str] = "table"
    FIELDS: ClassVar[str] = ""
    ID_COLUMN: ClassVar[str | None] = "dataset_id"
    FQN_COLUMN: ClassVar[str | None] = "table_fqn"
    SEARCH_INDEX: ClassVar[str | None] = "table_search_index"
    SEARCH_TERMS: ClassVar[dict[str, str]] = TABLE_SCAN_TERMS

    class Meta:
        """Scan metadata."""

        name = "dataset_profiles_scan"
        description = "Latest OpenMetadata profiler results per table, with per-column statistics"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Table Profiles",
            category="quality",
            keywords="profiles, profiler, row count, table size, null proportion, column statistics",
            doc_llm=(
                "One row per profiled OpenMetadata table, from /v1/tables/{fqn}/tableProfile/latest: dataset_id, "
                "table_fqn, platform, service, database, schema, qualified_name, profiled_at, row_count, "
                "column_count, size_bytes, column_profiles (`STRUCT(column_name, null_count, "
                "null_proportion, unique_count, unique_proportion, min, max, mean, median, stdev)[]`). Tables never "
                "profiled have no row. Pushdown: dataset_id / table_fqn = or IN profile only those tables; "
                "service, database and schema = / IN narrow the table search. One profile call per table."
            ),
            doc_md="Latest table and column profiles.",
            schema=PROFILES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, row_count, size_bytes FROM openmetadata.main.dataset_profiles "
                    "WHERE service = 'snowflake_prod' ORDER BY size_bytes DESC LIMIT 10"
                ),
                description="Largest profiled tables in one service",
            ),
            FunctionExample(
                sql=(
                    "SELECT p.qualified_name, c.column_name, c.null_proportion "
                    "FROM openmetadata.main.dataset_profiles p, "
                    "unnest(p.column_profiles) AS u(c) WHERE c.null_proportion > 0.5 ORDER BY c.null_proportion DESC"
                ),
                description="Mostly-NULL columns",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch the table's latest profile."""
        fqn = entity.get("fullyQualifiedName")
        if not fqn:
            return []
        try:
            body = get_json(
                ep, f"/v1/tables/{quote_fqn(str(fqn))}/tableProfile/latest", {"includeColumnProfile": "true"}
            )
        except NotFoundError:
            return []
        profile = (body or {}).get("profile") or {}
        profiled_at = ts(profile.get("timestamp"))
        if profiled_at is None:
            return []
        columns = []
        for col in (body or {}).get("columns") or []:
            p = col.get("profile") if isinstance(col, dict) else None
            if not isinstance(p, dict):
                continue
            columns.append(
                {
                    "column_name": col.get("name"),
                    "null_count": to_int(p.get("nullCount")),
                    "null_proportion": to_float(p.get("nullProportion")),
                    "unique_count": to_int(p.get("uniqueCount")),
                    "unique_proportion": to_float(p.get("uniqueProportion")),
                    "min": to_str(p.get("min")),
                    "max": to_str(p.get("max")),
                    "mean": to_str(p.get("mean")),
                    "median": to_str(p.get("median")),
                    "stdev": to_str(p.get("stddev")),
                }
            )
        return [
            {
                **table_key_values({**entity, **{k: v for k, v in (body or {}).items() if k != "columns"}}),
                "profiled_at": profiled_at,
                "row_count": to_int(profile.get("rowCount")),
                "column_count": to_int(profile.get("columnCount")),
                "size_bytes": to_int(profile.get("sizeInByte")),
                "column_profiles": columns,
            }
        ]


# ----------------------------------------------------------------------------- usage

USAGE_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        *table_key_columns(),
        column("usage_date", pa.date32(), "Day the counts are for.", nullable=False),
        column("time_range", pa.string(), "Granularity of query_count: always 'DAY'.", nullable=False),
        column("query_count", pa.int64(), "Count of queries that used the table on usage_date."),
        column(
            "percentile_rank", pa.float64(), "Percentile (0-100) of the table's daily query count among all tables."
        ),
        column("weekly_query_count", pa.int64(), "Count of queries in the 7 days ending on usage_date."),
        column(
            "weekly_percentile_rank",
            pa.float64(),
            "Percentile (0-100) of the table's 7-day query count among all tables.",
        ),
        column("monthly_query_count", pa.int64(), "Count of queries in the 30 days ending on usage_date."),
        column(
            "monthly_percentile_rank",
            pa.float64(),
            "Percentile (0-100) of the table's 30-day query count among all tables.",
        ),
    ]
)

MAX_USAGE_DAYS = 30


def _usage_row(entity: dict[str, Any], u: dict[str, Any]) -> dict[str, Any] | None:
    try:
        day = date.fromisoformat(str(u.get("date"))[:10])
    except ValueError:
        return None
    return {
        **table_key_values(entity),
        "usage_date": day,
        "time_range": "DAY",
        "query_count": to_int(get(u, "dailyStats", "count")),
        "percentile_rank": to_float(get(u, "dailyStats", "percentileRank")),
        "weekly_query_count": to_int(get(u, "weeklyStats", "count")),
        "weekly_percentile_rank": to_float(get(u, "weeklyStats", "percentileRank")),
        "monthly_query_count": to_int(get(u, "monthlyStats", "count")),
        "monthly_percentile_rank": to_float(get(u, "monthlyStats", "percentileRank")),
    }


@init_single_worker
@bind_fixed_schema
class UsageScan(EntityScan[NoArgs]):
    """Rows of ``usage``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = USAGE_SCHEMA
    PATH: ClassVar[str] = "/v1/tables"
    ENTITY_TYPE: ClassVar[str] = "table"
    FIELDS: ClassVar[str] = "usageSummary"
    ID_COLUMN: ClassVar[str | None] = "dataset_id"
    FQN_COLUMN: ClassVar[str | None] = "table_fqn"
    SEARCH_INDEX: ClassVar[str | None] = "table_search_index"
    SEARCH_TERMS: ClassVar[dict[str, str]] = TABLE_SCAN_TERMS

    class Meta:
        """Scan metadata."""

        name = "usage_scan"
        description = "OpenMetadata table usage: daily query counts and percentile ranks"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Table Usage",
            category="quality",
            keywords="usage, popularity, query counts, percentile rank, most used tables",
            doc_llm=(
                "Query-log usage of OpenMetadata tables: dataset_id, table_fqn, platform, service, database, "
                "schema, qualified_name, usage_date, time_range ('DAY'), query_count, percentile_rank, "
                "weekly_query_count, weekly_percentile_rank, monthly_query_count, monthly_percentile_rank. "
                "Without a table filter: one row per table for its latest usage day (the table's usageSummary). "
                "With dataset_id / table_fqn = or IN: the daily history from /v1/usage/table/name/{fqn} (up to 30 "
                "days ending at the usage_date upper bound; usage_date ranges push down as date / days). service, "
                "database and schema = / IN narrow the table search."
            ),
            doc_md="Table usage from query logs.",
            schema=USAGE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, monthly_query_count FROM openmetadata.main.usage "
                    "ORDER BY monthly_query_count DESC LIMIT 10"
                ),
                description="The ten most-queried tables over the last month",
            ),
            FunctionExample(
                sql=(
                    "SELECT usage_date, query_count FROM openmetadata.main.usage "
                    "WHERE table_fqn = 'snowflake_prod.analytics.marts.fct_orders' ORDER BY usage_date"
                ),
                description="Daily usage history of one table",
            ),
        ]

    @classmethod
    def extra_plan(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """History mode (table filter) and its date window."""
        filters = params.current_pushdown_filters
        history = eq_values(filters, "dataset_id") is not None or eq_values(filters, "table_fqn") is not None
        lower, upper = range_bounds(filters, "usage_date")
        lower = lower.date() if isinstance(lower, datetime) else lower
        upper = upper.date() if isinstance(upper, datetime) else upper
        days = MAX_USAGE_DAYS
        if isinstance(lower, date):
            end = upper if isinstance(upper, date) else datetime.now(tz=UTC).date()
            if end < lower:
                return {"empty": True}
            days = max(1, min(MAX_USAGE_DAYS, (end - lower).days + 1))
        return {
            "history": history,
            "days": days,
            "date": upper.isoformat() if isinstance(upper, date) else None,
        }

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Daily history (history mode) or the latest usage summary."""
        extra = plan["extra"]
        if not extra.get("history"):
            summary = entity.get("usageSummary")
            row = _usage_row(entity, summary) if isinstance(summary, dict) else None
            return [row] if row else []
        fqn = entity.get("fullyQualifiedName")
        if not fqn:
            return []
        try:
            body = get_json(
                ep, f"/v1/usage/table/name/{quote_fqn(str(fqn))}", {"days": extra["days"], "date": extra["date"]}
            )
        except NotFoundError:
            return []
        rows = [_usage_row(entity, u) for u in (body or {}).get("usage") or [] if isinstance(u, dict)]
        return [r for r in rows if r]


# ----------------------------------------------------------------------------- queries

QUERIES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("query"),
        fqn_column("query", "snowflake_prod.3f1c9a..."),
        column("name", pa.string(), "Query name (a checksum unless named).", nullable=False),
        column("query", pa.string(), "SQL text."),
        column("query_date", TSTZ, "When the query ran (query-log queries)."),
        column("duration_ms", pa.float64(), "How long the query ran, in milliseconds."),
        column("service", pa.string(), "Database service the query ran on."),
        column("users", STR_LIST, "OpenMetadata users who ran the query."),
        column("used_by", STR_LIST, "Raw user names from the query log."),
        column("used_in", STR_LIST, "FQNs of the tables the query uses."),
        column("checksum", pa.string(), "Checksum of the SQL text."),
        column("owners", STR_LIST, "Owner names."),
        column("tags", STR_LIST, "Classification tag FQNs on the query."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class QueriesScan(EntityScan[NoArgs]):
    """Rows of ``queries``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = QUERIES_SCHEMA
    PATH: ClassVar[str] = "/v1/queries"
    ENTITY_TYPE: ClassVar[str] = "query"
    FIELDS: ClassVar[str] = "owners,users,queryUsedIn,tags"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"service": "service"}

    class Meta:
        """Scan metadata."""

        name = "queries_scan"
        description = "OpenMetadata query entities (from query logs or curated) with the tables they use"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Queries",
            category="quality",
            keywords="queries, sql, query log, query history, popular queries, used in",
            doc_llm=(
                "One row per OpenMetadata query entity: id, fqn, name, query (SQL), query_date, duration_ms, "
                "service, users, used_by, used_in (table FQNs), checksum, owners, tags, deleted, updated_at, "
                "updated_by, version, url. Pushdown: id / fqn = or IN fetch directly; service = / IN becomes the "
                "service parameter."
            ),
            doc_md="Queries recorded in OpenMetadata.",
            schema=QUERIES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT query, duration_ms FROM openmetadata.main.queries "
                    "WHERE list_contains(used_in, 'snowflake_prod.analytics.marts.fct_orders') "
                    "ORDER BY duration_ms DESC LIMIT 5"
                ),
                description="Slowest queries touching one table",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per query."""
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName"),
                "name": entity.get("name"),
                "query": entity.get("query"),
                "query_date": ts(entity.get("queryDate")),
                "duration_ms": to_float(entity.get("duration")),
                "service": entity.get("service")
                if isinstance(entity.get("service"), str)
                else get(entity, "service", "name"),
                "users": ref_names(entity.get("users")),
                "used_by": [str(u) for u in entity.get("usedBy") or []],
                "used_in": ref_names(entity.get("queryUsedIn"), "fullyQualifiedName"),
                "checksum": entity.get("checksum"),
                "owners": owner_names(entity),
                "tags": tag_fqns(entity),
                **audit_values(ep, entity, "query"),
            }
        ]


QUALITY_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("test_suites", TestSuitesScan, ["id"]),
    ("assertions", AssertionsScan, ["id"]),
    ("assertion_results", AssertionResultsScan, None),
    ("incidents", IncidentsScan, ["id"]),
    ("dataset_profiles", DatasetProfilesScan, ["dataset_id"]),
    ("usage", UsageScan, ["dataset_id", "usage_date"]),
    ("queries", QueriesScan, ["id"]),
]
