"""Write ``openmetadata.json``: a small OpenMetadata 1.6 catalog as REST-API-shaped entities.

Run ``python test/fixtures/build_fixture.py`` after editing; the JSON is committed and read by
``test/mock_openmetadata/server.py``. Entities carry every field (base and optional); the mock
strips optional fields the request's ``fields=`` did not ask for, exactly as OpenMetadata does.
Keys that start with ``_`` are not entities: lineage edges, test case results, incident status
records, usage history, table profiles and sample data, served by their own endpoints.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

OUT = Path(__file__).with_name("openmetadata.json")
NS = uuid.UUID("6f1d3c2a-0000-4000-8000-0000000000aa")
UPDATED = "2026-11-10T12:00:00"


def ms(text: str) -> int:
    """Epoch milliseconds of an ISO UTC timestamp."""
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp() * 1000)


def uid(kind: str, fqn: str) -> str:
    """Deterministic UUID of an entity."""
    return str(uuid.uuid5(NS, f"{kind}:{fqn}"))


def ref(kind: str, fqn: str, name: str | None = None, display: str | None = None) -> dict[str, Any]:
    """An EntityReference."""
    out: dict[str, Any] = {
        "id": uid(kind, fqn),
        "type": kind,
        "name": name or fqn.rsplit(".", 1)[-1],
        "fullyQualifiedName": fqn,
        "deleted": False,
    }
    if display:
        out["displayName"] = display
    return out


def user(name: str) -> dict[str, Any]:
    """A user reference."""
    return ref("user", name, name, name.capitalize())


def team(name: str) -> dict[str, Any]:
    """A team reference."""
    return ref("team", name, name, name.replace("-", " ").title())


def tag(fqn: str, source: str = "Classification") -> dict[str, Any]:
    """A TagLabel."""
    return {"tagFQN": fqn, "source": source, "labelType": "Manual", "state": "Confirmed", "name": fqn.split(".")[-1]}


def term(fqn: str) -> dict[str, Any]:
    """A glossary-term TagLabel."""
    return tag(fqn, "Glossary")


def audit(version: float = 0.2, by: str = "admin") -> dict[str, Any]:
    """Entity bookkeeping fields."""
    return {"version": version, "updatedAt": ms(UPDATED), "updatedBy": by, "deleted": False}


SF = "snowflake_prod"
PG = "postgres_app"
SERVICES = {SF: "Snowflake", PG: "Postgres"}


def database_services() -> list[dict[str, Any]]:
    """Database services (connection config deliberately present: the worker must never expose it)."""
    out = []
    for name, stype in SERVICES.items():
        out.append(
            {
                "id": uid("databaseService", name),
                "name": name,
                "fullyQualifiedName": name,
                "serviceType": stype,
                "description": f"{stype} production warehouse" if name == SF else "Application database",
                "connection": {"config": {"type": stype, "username": "svc_om", "password": "s3cr3t-do-not-leak"}},
                "owners": [team("data-eng")],
                "tags": [],
                "domain": None,
                **audit(),
            }
        )
    return out


def databases() -> list[dict[str, Any]]:
    """Databases."""
    out = []
    for svc, db in ((SF, "analytics"), (PG, "app")):
        fqn = f"{svc}.{db}"
        out.append(
            {
                "id": uid("database", fqn),
                "name": db,
                "fullyQualifiedName": fqn,
                "service": ref("databaseService", svc, svc),
                "serviceType": SERVICES[svc],
                "description": f"The {db} database",
                "owners": [team("data-eng")],
                "tags": [],
                "domain": None,
                **audit(),
            }
        )
    return out


SCHEMAS = [(SF, "analytics", "raw"), (SF, "analytics", "staging"), (SF, "analytics", "marts"), (PG, "app", "public")]


def database_schemas() -> list[dict[str, Any]]:
    """Schemas."""
    return [
        {
            "id": uid("databaseSchema", f"{s}.{d}.{n}"),
            "name": n,
            "fullyQualifiedName": f"{s}.{d}.{n}",
            "service": ref("databaseService", s, s),
            "serviceType": SERVICES[s],
            "database": ref("database", f"{s}.{d}", d),
            "description": f"{n} layer",
            "owners": [],
            "tags": [tag("Tier.Tier1")] if n == "marts" else [],
            "domain": None,
            **audit(),
        }
        for s, d, n in SCHEMAS
    ]


def col(
    table_fqn: str,
    name: str,
    dtype: str,
    display: str,
    pos: int,
    *,
    constraint: str | None = None,
    tags: list[dict[str, Any]] | None = None,
    children: list[dict[str, Any]] | None = None,
    desc: str | None = None,
) -> dict[str, Any]:
    """A Column."""
    out: dict[str, Any] = {
        "name": name,
        "dataType": dtype,
        "dataTypeDisplay": display,
        "fullyQualifiedName": f"{table_fqn}.{name}",
        "ordinalPosition": pos,
        "tags": tags or [],
    }
    if constraint:
        out["constraint"] = constraint
    if dtype == "VARCHAR":
        out["dataLength"] = 256
    if children:
        out["children"] = children
    if desc:
        out["description"] = desc
    return out


PII = [tag("PII.Sensitive")]

ORDERS = f"{SF}.analytics.raw.orders"
CUSTOMERS = f"{SF}.analytics.raw.customers"
STG_ORDERS = f"{SF}.analytics.staging.stg_orders"
FCT_ORDERS = f"{SF}.analytics.marts.fct_orders"
DIM_CUSTOMERS = f"{SF}.analytics.marts.dim_customers"
USERS = f"{PG}.app.public.users"

SALES = {"id": uid("domain", "Sales"), "type": "domain", "name": "Sales", "fullyQualifiedName": "Sales"}


def table(
    fqn: str,
    *,
    table_type: str = "Regular",
    description: str,
    columns: list[dict[str, Any]],
    tags: list[dict[str, Any]],
    owners: list[dict[str, Any]],
    domain: dict[str, Any] | None = None,
    usage: tuple[str, int, int, int] = ("2026-11-14", 0, 0, 0),
    extension: dict[str, Any] | None = None,
    pk: list[str] | None = None,
) -> dict[str, Any]:
    """A Table entity."""
    svc, db, sch, name = fqn.split(".")
    day, daily, weekly, monthly = usage
    return {
        "id": uid("table", fqn),
        "name": name,
        "fullyQualifiedName": fqn,
        "tableType": table_type,
        "description": description,
        "service": ref("databaseService", svc, svc),
        "serviceType": SERVICES[svc],
        "database": ref("database", f"{svc}.{db}", db),
        "databaseSchema": ref("databaseSchema", f"{svc}.{db}.{sch}", sch),
        "columns": columns,
        "tableConstraints": [{"constraintType": "PRIMARY_KEY", "columns": pk}] if pk else [],
        "tags": tags,
        "owners": owners,
        "domain": domain,
        "dataProducts": [ref("dataProduct", "Orders360", "Orders360")] if fqn in (FCT_ORDERS, DIM_CUSTOMERS) else [],
        "extension": extension,
        "usageSummary": {
            "date": day,
            "dailyStats": {"count": daily, "percentileRank": round(daily / 10.0, 1)},
            "weeklyStats": {"count": weekly, "percentileRank": round(weekly / 70.0, 1)},
            "monthlyStats": {"count": monthly, "percentileRank": round(monthly / 300.0, 1)},
        },
        "sourceUrl": f"https://app.snowflake.com/{name}" if svc == SF else None,
        "followers": [],
        **audit(0.3 if fqn == FCT_ORDERS else 0.2),
    }


def tables() -> list[dict[str, Any]]:
    """Tables."""
    return [
        table(
            ORDERS,
            description="Raw orders loaded from the shop",
            columns=[
                col(ORDERS, "order_id", "NUMBER", "number(38,0)", 1, constraint="PRIMARY_KEY"),
                col(ORDERS, "customer_id", "NUMBER", "number(38,0)", 2),
                col(ORDERS, "amount", "DECIMAL", "number(12,2)", 3),
                col(ORDERS, "created_at", "TIMESTAMP", "timestamp_ntz", 4),
            ],
            tags=[tag("Tier.Tier3")],
            owners=[team("data-eng")],
            usage=("2026-11-14", 4, 20, 80),
        ),
        table(
            CUSTOMERS,
            description="Raw customers",
            columns=[
                col(CUSTOMERS, "customer_id", "NUMBER", "number(38,0)", 1, constraint="PRIMARY_KEY"),
                col(CUSTOMERS, "email", "VARCHAR", "varchar(256)", 2, tags=PII),
                col(CUSTOMERS, "name", "VARCHAR", "varchar(256)", 3, tags=PII),
                col(CUSTOMERS, "country", "VARCHAR", "varchar(256)", 4, constraint="NOT_NULL"),
            ],
            tags=[tag("Tier.Tier3")],
            owners=[user("alice")],
            usage=("2026-11-14", 2, 9, 31),
        ),
        table(
            STG_ORDERS,
            table_type="View",
            description="Cleaned orders",
            columns=[
                col(STG_ORDERS, "order_id", "NUMBER", "number(38,0)", 1),
                col(STG_ORDERS, "customer_id", "NUMBER", "number(38,0)", 2),
                col(STG_ORDERS, "amount", "DECIMAL", "number(12,2)", 3),
            ],
            tags=[],
            owners=[team("data-eng")],
            usage=("2026-11-14", 1, 5, 18),
        ),
        table(
            FCT_ORDERS,
            description="One row per order, with revenue",
            columns=[
                col(FCT_ORDERS, "order_id", "NUMBER", "number(38,0)", 1),
                col(FCT_ORDERS, "customer_id", "NUMBER", "number(38,0)", 2, constraint="NOT_NULL"),
                col(
                    FCT_ORDERS,
                    "order_total",
                    "DECIMAL",
                    "number(12,2)",
                    3,
                    tags=[term("Business.Revenue")],
                    desc="Order revenue in USD",
                ),
                col(FCT_ORDERS, "order_date", "DATE", "date", 4),
                col(FCT_ORDERS, "customer_email", "VARCHAR", "varchar(256)", 5, tags=PII),
            ],
            tags=[tag("Tier.Tier1"), term("Business.Revenue")],
            owners=[user("alice"), team("analytics")],
            domain=SALES,
            usage=("2026-11-14", 9, 60, 240),
            extension={"sla_hours": 6, "steward": "alice"},
            pk=["order_id"],
        ),
        table(
            DIM_CUSTOMERS,
            description="Customer dimension",
            columns=[
                col(DIM_CUSTOMERS, "customer_id", "NUMBER", "number(38,0)", 1, constraint="PRIMARY_KEY"),
                col(DIM_CUSTOMERS, "customer_name", "VARCHAR", "varchar(256)", 2, tags=PII),
                col(DIM_CUSTOMERS, "country", "VARCHAR", "varchar(256)", 3),
                col(
                    DIM_CUSTOMERS,
                    "address",
                    "STRUCT",
                    "struct<city:varchar,zip:varchar>",
                    4,
                    children=[
                        col(f"{DIM_CUSTOMERS}.address", "city", "VARCHAR", "varchar", 1),
                        col(f"{DIM_CUSTOMERS}.address", "zip", "VARCHAR", "varchar", 2),
                    ],
                ),
            ],
            tags=[tag("Tier.Tier1")],
            owners=[user("bob")],
            domain=SALES,
            usage=("2026-11-14", 5, 30, 120),
        ),
        table(
            USERS,
            description="Application users",
            columns=[
                col(USERS, "id", "INT", "integer", 1, constraint="PRIMARY_KEY"),
                col(USERS, "email", "VARCHAR", "varchar(256)", 2, tags=PII),
                col(USERS, "created_at", "TIMESTAMP", "timestamp", 3),
            ],
            tags=[tag("Tier.Tier2"), tag("PII.Sensitive")],
            owners=[],
            usage=("2026-11-14", 3, 15, 60),
        ),
    ]


def asset(kind: str, svc: str, stype: str, name: str, **extra: Any) -> dict[str, Any]:
    """A non-table data asset."""
    fqn = f"{svc}.{name}"
    return {
        "id": uid(kind, fqn),
        "name": name,
        "fullyQualifiedName": fqn,
        "service": ref(f"{kind}Service", svc, svc),
        "serviceType": stype,
        "tags": [],
        "owners": [],
        "domain": None,
        "followers": [],
        **audit(),
        **extra,
    }


def dashboards() -> list[dict[str, Any]]:
    """Dashboards."""
    return [
        asset(
            "dashboard",
            "looker_prod",
            "Looker",
            "sales_overview",
            displayName="Sales Overview",
            description="Revenue and orders",
            dashboardType="Dashboard",
            charts=[ref("chart", "looker_prod.revenue_by_month"), ref("chart", "looker_prod.orders_by_country")],
            tags=[tag("Tier.Tier2")],
            owners=[user("alice")],
            domain=SALES,
            sourceUrl="https://looker.example.com/dashboards/12",
        )
    ]


def charts() -> list[dict[str, Any]]:
    """Charts."""
    dash = [ref("dashboard", "looker_prod.sales_overview")]
    return [
        asset(
            "chart",
            "looker_prod",
            "Looker",
            "revenue_by_month",
            displayName="Revenue by month",
            chartType="Line",
            dashboards=dash,
        ),
        asset(
            "chart",
            "looker_prod",
            "Looker",
            "orders_by_country",
            displayName="Orders by country",
            chartType="Bar",
            dashboards=dash,
        ),
    ]


def pipelines() -> list[dict[str, Any]]:
    """Pipelines."""
    return [
        asset(
            "pipeline",
            "airflow_prod",
            "Airflow",
            "daily_orders",
            displayName="Daily orders",
            description="Builds fct_orders",
            scheduleInterval="0 6 * * *",
            tasks=[
                {
                    "name": "extract",
                    "displayName": "Extract",
                    "taskType": "PythonOperator",
                    "downstreamTasks": ["transform"],
                },
                {
                    "name": "transform",
                    "displayName": "Transform",
                    "taskType": "DbtOperator",
                    "downstreamTasks": ["load"],
                },
                {"name": "load", "displayName": "Load", "taskType": "PythonOperator", "downstreamTasks": []},
            ],
            pipelineStatus={"executionStatus": "Failed", "timestamp": ms("2026-11-14T06:20:00")},
            owners=[team("data-eng")],
            tags=[tag("Tier.Tier1")],
            sourceUrl="https://airflow.example.com/dags/daily_orders",
        )
    ]


def topics() -> list[dict[str, Any]]:
    """Topics."""
    return [
        asset(
            "topic",
            "kafka_prod",
            "Kafka",
            "orders_events",
            description="Order events",
            partitions=6,
            replicationFactor=3,
            retentionTime=604800000,
            maximumMessageSize=1048576,
            cleanupPolicies=["delete"],
            messageSchema={"schemaType": "Avro", "schemaText": "{}"},
            owners=[team("data-eng")],
        )
    ]


def mlmodels() -> list[dict[str, Any]]:
    """ML models."""
    return [
        asset(
            "mlmodel",
            "mlflow_prod",
            "Mlflow",
            "churn_model",
            description="Predicts churn",
            algorithm="XGBoost",
            target="churned",
            mlFeatures=[{"name": "order_count"}, {"name": "days_since_last_order"}],
            server="https://models.example.com/churn",
            dashboard=ref("dashboard", "looker_prod.sales_overview"),
            owners=[user("carol")],
            domain=SALES,
        )
    ]


def users() -> list[dict[str, Any]]:
    """Users."""

    def u(name: str, *, admin: bool = False, bot: bool = False, teams: list[str] | None = None) -> dict[str, Any]:
        return {
            "id": uid("user", name),
            "name": name,
            "fullyQualifiedName": name,
            "displayName": name.replace("-", " ").title(),
            "email": f"{name}@example.com",
            "isAdmin": admin,
            "isBot": bot,
            "teams": [team(t) for t in teams or []],
            "roles": [ref("role", "DataSteward", "DataSteward")] if name == "alice" else [],
            **audit(),
        }

    return [
        u("alice", admin=True, teams=["analytics"]),
        u("bob", teams=["analytics"]),
        u("carol", teams=["data-eng"]),
        u("ingestion-bot", bot=True),
    ]


def teams() -> list[dict[str, Any]]:
    """Teams."""

    def t(name: str, ttype: str, parents: list[str], count: int) -> dict[str, Any]:
        return {
            "id": uid("team", name),
            "name": name,
            "fullyQualifiedName": name,
            "displayName": name.replace("-", " ").title(),
            "teamType": ttype,
            "email": f"{name}@example.com",
            "description": f"The {name} team",
            "parents": [team(p) for p in parents],
            "owners": [user("alice")] if name == "analytics" else [],
            "userCount": count,
            "isJoinable": ttype == "Group",
            **audit(),
        }

    return [
        t("Organization", "Organization", [], 4),
        t("analytics", "Group", ["Organization"], 2),
        t("data-eng", "Group", ["Organization"], 1),
    ]


def classifications() -> list[dict[str, Any]]:
    """Classifications."""
    return [
        {
            "id": uid("classification", "PII"),
            "name": "PII",
            "fullyQualifiedName": "PII",
            "description": "Personally identifiable information",
            "mutuallyExclusive": True,
            "termCount": 3,
            "provider": "system",
            "disabled": False,
            **audit(),
        },
        {
            "id": uid("classification", "Tier"),
            "name": "Tier",
            "fullyQualifiedName": "Tier",
            "description": "Business criticality",
            "mutuallyExclusive": True,
            "termCount": 3,
            "provider": "system",
            "disabled": False,
            **audit(),
        },
    ]


def tags() -> list[dict[str, Any]]:
    """Tags."""
    usage = {
        "PII.Sensitive": 7,
        "PII.NonSensitive": 0,
        "PII.None": 0,
        "Tier.Tier1": 4,
        "Tier.Tier2": 2,
        "Tier.Tier3": 2,
    }
    out = []
    for fqn, count in usage.items():
        cls, name = fqn.split(".")
        out.append(
            {
                "id": uid("tag", fqn),
                "name": name,
                "fullyQualifiedName": fqn,
                "description": f"{name} data",
                "classification": ref("classification", cls, cls),
                "style": {"color": "#ff0000"} if fqn == "PII.Sensitive" else None,
                "usageCount": count,
                "provider": "system",
                "disabled": False,
                **audit(),
            }
        )
    return out


def glossaries() -> list[dict[str, Any]]:
    """Glossaries."""
    return [
        {
            "id": uid("glossary", "Business"),
            "name": "Business",
            "fullyQualifiedName": "Business",
            "displayName": "Business Glossary",
            "description": "Company vocabulary",
            "termCount": 3,
            "mutuallyExclusive": False,
            "owners": [user("alice")],
            "reviewers": [user("bob")],
            "tags": [],
            "domain": SALES,
            **audit(),
        }
    ]


def glossary_terms() -> list[dict[str, Any]]:
    """Glossary terms."""
    glossary = ref("glossary", "Business", "Business")

    def gt(fqn: str, status: str, **extra: Any) -> dict[str, Any]:
        return {
            "id": uid("glossaryTerm", fqn),
            "name": fqn.rsplit(".", 1)[-1],
            "fullyQualifiedName": fqn,
            "glossary": glossary,
            "description": f"Definition of {fqn.rsplit('.', 1)[-1]}",
            "synonyms": [],
            "relatedTerms": [],
            "references": [],
            "status": status,
            "owners": [user("alice")],
            "reviewers": [user("bob")],
            "tags": [],
            "domain": SALES,
            **audit(),
            **extra,
        }

    return [
        gt(
            "Business.Revenue",
            "Approved",
            synonyms=["Sales", "Turnover"],
            relatedTerms=[ref("glossaryTerm", "Business.Customer")],
            references=[{"name": "Finance wiki", "endpoint": "https://wiki.example.com/revenue"}],
        ),
        gt("Business.Revenue.Net Revenue", "Draft", parent=ref("glossaryTerm", "Business.Revenue", "Revenue")),
        gt("Business.Customer", "Approved"),
    ]


def domains() -> list[dict[str, Any]]:
    """Domains."""
    return [
        {
            "id": uid("domain", "Sales"),
            "name": "Sales",
            "fullyQualifiedName": "Sales",
            "domainType": "Consumer-aligned",
            "description": "Sales and revenue",
            "owners": [user("alice")],
            "experts": [user("bob")],
            "parent": None,
            **audit(),
        },
        {
            "id": uid("domain", "Sales.Online"),
            "name": "Online",
            "fullyQualifiedName": "Sales.Online",
            "domainType": "Source-aligned",
            "description": "Web shop",
            "owners": [],
            "experts": [],
            "parent": SALES,
            **audit(),
        },
        {
            "id": uid("domain", "Engineering"),
            "name": "Engineering",
            "fullyQualifiedName": "Engineering",
            "domainType": "Source-aligned",
            "description": "Platform data",
            "owners": [team("data-eng")],
            "experts": [],
            "parent": None,
            **audit(),
        },
    ]


def data_products() -> list[dict[str, Any]]:
    """Data products."""
    return [
        {
            "id": uid("dataProduct", "Orders360"),
            "name": "Orders360",
            "fullyQualifiedName": "Orders360",
            "displayName": "Orders 360",
            "description": "Everything about orders",
            "domain": SALES,
            "assets": [ref("table", FCT_ORDERS), ref("table", DIM_CUSTOMERS)],
            "owners": [user("alice")],
            "experts": [user("bob")],
            **audit(),
        }
    ]


def test_suites() -> list[dict[str, Any]]:
    """Test suites (OpenMetadata 1.6 shape: basic + basicEntityReference)."""

    def basic(table_fqn: str, tests: list[str], summary: dict[str, int]) -> dict[str, Any]:
        fqn = f"{table_fqn}.testSuite"
        return {
            "id": uid("testSuite", fqn),
            "name": fqn,
            "fullyQualifiedName": fqn,
            "basic": True,
            "basicEntityReference": ref("table", table_fqn),
            "tests": [ref("testCase", t) for t in tests],
            "summary": {**summary, "total": sum(summary.values())},
            "owners": [],
            **audit(),
        }

    return [
        basic(
            FCT_ORDERS,
            [f"{FCT_ORDERS}.order_id.order_id_unique", f"{FCT_ORDERS}.row_count_between"],
            {"success": 1, "failed": 1, "aborted": 0, "queued": 0},
        ),
        basic(
            DIM_CUSTOMERS,
            [f"{DIM_CUSTOMERS}.customer_id.customer_id_not_null"],
            {"success": 1, "failed": 0, "aborted": 0, "queued": 0},
        ),
        {
            "id": uid("testSuite", "critical_checks"),
            "name": "critical_checks",
            "fullyQualifiedName": "critical_checks",
            "displayName": "Critical checks",
            "description": "Checks that page someone",
            "basic": False,
            "tests": [ref("testCase", f"{FCT_ORDERS}.row_count_between")],
            "summary": {"success": 0, "failed": 1, "aborted": 0, "queued": 0, "total": 1},
            "owners": [team("data-eng")],
            **audit(),
        },
    ]


RESULTS = {
    f"{FCT_ORDERS}.order_id.order_id_unique": [
        ("2026-11-01T06:30:00", "Success", "Found 0 duplicates", [("uniqueCount", "1200"), ("valuesCount", "1200")]),
        ("2026-11-02T06:30:00", "Success", "Found 0 duplicates", [("uniqueCount", "1210"), ("valuesCount", "1210")]),
        ("2026-11-03T06:30:00", "Success", "Found 0 duplicates", [("uniqueCount", "1225"), ("valuesCount", "1225")]),
    ],
    f"{FCT_ORDERS}.row_count_between": [
        ("2026-11-01T06:30:00", "Success", "Row count 1200 within [1000, 5000]", [("rowCount", "1200")]),
        ("2026-11-02T06:30:00", "Failed", "Row count 12 outside [1000, 5000]", [("rowCount", "12")]),
        ("2026-11-03T06:30:00", "Failed", "Row count 0 outside [1000, 5000]", [("rowCount", "0")]),
    ],
    f"{DIM_CUSTOMERS}.customer_id.customer_id_not_null": [
        ("2026-11-02T07:00:00", "Success", "Found 0 nulls", [("nullCount", "0")]),
        ("2026-11-03T07:00:00", "Success", "Found 0 nulls", [("nullCount", "0")]),
    ],
}


def test_cases() -> list[dict[str, Any]]:
    """Test cases, each with its latest result."""

    def tc(
        fqn: str, table_fqn: str, column: str | None, definition: str, params: list[tuple[str, str]], suite: str
    ) -> dict[str, Any]:
        link = f"<#E::table::{table_fqn}" + (f"::columns::{column}>" if column else ">")
        ts_, status, message, values = RESULTS[fqn][-1]
        return {
            "id": uid("testCase", fqn),
            "name": fqn.rsplit(".", 1)[-1],
            "fullyQualifiedName": fqn,
            "entityLink": link,
            "entityFQN": f"{table_fqn}.{column}" if column else table_fqn,
            "description": f"{definition} on {column or 'table'}",
            "testDefinition": ref("testDefinition", definition, definition),
            "testSuite": ref("testSuite", suite, suite),
            "parameterValues": [{"name": n, "value": v} for n, v in params],
            "testCaseResult": {
                "timestamp": ms(ts_),
                "testCaseStatus": status,
                "result": message,
                "testResultValue": [{"name": n, "value": v} for n, v in values],
            },
            "incidentId": uid("incident", fqn) if status == "Failed" else None,
            "owners": [],
            "tags": [],
            **audit(),
        }

    return [
        tc(
            f"{FCT_ORDERS}.order_id.order_id_unique",
            FCT_ORDERS,
            "order_id",
            "columnValuesToBeUnique",
            [],
            f"{FCT_ORDERS}.testSuite",
        ),
        tc(
            f"{FCT_ORDERS}.row_count_between",
            FCT_ORDERS,
            None,
            "tableRowCountToBeBetween",
            [("minValue", "1000"), ("maxValue", "5000")],
            f"{FCT_ORDERS}.testSuite",
        ),
        tc(
            f"{DIM_CUSTOMERS}.customer_id.customer_id_not_null",
            DIM_CUSTOMERS,
            "customer_id",
            "columnValuesToBeNotNull",
            [],
            f"{DIM_CUSTOMERS}.testSuite",
        ),
    ]


def results() -> dict[str, list[dict[str, Any]]]:
    """Test case results per test case FQN."""
    out = {}
    for fqn, runs in RESULTS.items():
        out[fqn] = [
            {
                "id": uid("result", f"{fqn}@{t}"),
                "testCaseFQN": fqn,
                "timestamp": ms(t),
                "testCaseStatus": status,
                "result": message,
                "testResultValue": [{"name": n, "value": v} for n, v in values],
                "passedRows": 1200 if status == "Success" and "unique" in fqn else None,
                "failedRows": 0 if status == "Success" and "unique" in fqn else None,
                "passedRowsPercentage": 100.0 if status == "Success" and "unique" in fqn else None,
                "failedRowsPercentage": 0.0 if status == "Success" and "unique" in fqn else None,
                "incidentId": uid("incident", fqn) if status == "Failed" else None,
            }
            for t, status, message, values in runs
        ]
    return out


def incident_statuses() -> list[dict[str, Any]]:
    """TestCaseResolutionStatus records (several per incident; stateId groups them)."""
    rc = f"{FCT_ORDERS}.row_count_between"
    nn = f"{DIM_CUSTOMERS}.customer_id.customer_id_not_null"

    def st(state_fqn: str, test_fqn: str, when: str, kind: str, **details: Any) -> dict[str, Any]:
        return {
            "id": uid("incidentStatus", f"{state_fqn}@{when}"),
            "stateId": uid("incident", state_fqn),
            "timestamp": ms(when),
            "testCaseResolutionStatusType": kind,
            "testCaseResolutionStatusDetails": details or None,
            "testCaseReference": ref("testCase", test_fqn),
            "severity": "Severity2" if test_fqn == rc else None,
            "updatedBy": user("alice"),
            "updatedAt": ms(when),
        }

    return [
        st(rc, rc, "2026-11-02T06:31:00", "New"),
        st(rc, rc, "2026-11-02T09:00:00", "Assigned", assignee=user("carol")),
        st(f"{nn}#old", nn, "2026-10-20T07:01:00", "New"),
        st(
            f"{nn}#old",
            nn,
            "2026-10-21T10:00:00",
            "Resolved",
            resolvedBy=user("bob"),
            testCaseFailureReason="FalsePositive",
            testCaseFailureComment="Late-arriving rows",
        ),
    ]


def lineage() -> list[dict[str, Any]]:
    """Lineage edges by FQN: (from type, from fqn, to type, to fqn, details)."""

    def e(ft: str, f: str, tt: str, t: str, **details: Any) -> dict[str, Any]:
        return {
            "from": [ft, f],
            "to": [tt, t],
            "lineageDetails": {
                "createdAt": ms("2026-11-05T00:00:00"),
                "updatedAt": ms("2026-11-06T00:00:00"),
                **details,
            },
        }

    return [
        e(
            "table",
            ORDERS,
            "table",
            STG_ORDERS,
            source="ViewLineage",
            sqlQuery="create view stg_orders as select * from raw.orders",
            columnsLineage=[
                {"fromColumns": [f"{ORDERS}.order_id"], "toColumn": f"{STG_ORDERS}.order_id"},
                {"fromColumns": [f"{ORDERS}.amount"], "toColumn": f"{STG_ORDERS}.amount"},
            ],
        ),
        e(
            "table",
            STG_ORDERS,
            "table",
            FCT_ORDERS,
            source="QueryLineage",
            pipeline=ref("pipeline", "airflow_prod.daily_orders"),
            columnsLineage=[
                {"fromColumns": [f"{STG_ORDERS}.order_id"], "toColumn": f"{FCT_ORDERS}.order_id"},
                {
                    "fromColumns": [f"{STG_ORDERS}.amount"],
                    "toColumn": f"{FCT_ORDERS}.order_total",
                    "function": "sum(amount)",
                },
            ],
        ),
        e(
            "table",
            CUSTOMERS,
            "table",
            FCT_ORDERS,
            source="QueryLineage",
            columnsLineage=[{"fromColumns": [f"{CUSTOMERS}.email"], "toColumn": f"{FCT_ORDERS}.customer_email"}],
        ),
        e(
            "table",
            CUSTOMERS,
            "table",
            DIM_CUSTOMERS,
            source="DbtLineage",
            columnsLineage=[{"fromColumns": [f"{CUSTOMERS}.name"], "toColumn": f"{DIM_CUSTOMERS}.customer_name"}],
        ),
        e("table", FCT_ORDERS, "dashboard", "looker_prod.sales_overview", source="DashboardLineage"),
        e("table", DIM_CUSTOMERS, "mlmodel", "mlflow_prod.churn_model", source="Manual"),
    ]


def usage_history() -> dict[str, list[dict[str, Any]]]:
    """Daily usage of fct_orders, 2026-11-10 .. 2026-11-14."""
    counts = [5, 7, 6, 8, 9]
    return {
        FCT_ORDERS: [
            {
                "date": f"2026-11-{10 + i:02d}",
                "dailyStats": {"count": c, "percentileRank": 90.0 + i},
                "weeklyStats": {"count": 40 + i * 5, "percentileRank": 91.0},
                "monthlyStats": {"count": 200 + i * 10, "percentileRank": 95.0},
            }
            for i, c in enumerate(counts)
        ]
    }


def profiles() -> dict[str, dict[str, Any]]:
    """Latest table + column profiles."""

    def p(when: str, rows: int, size: int, columns: dict[str, dict[str, Any]]) -> dict[str, Any]:
        return {
            "profile": {"timestamp": ms(when), "rowCount": rows, "columnCount": len(columns), "sizeInByte": size},
            "columns": {name: {"name": name, "timestamp": ms(when), **c} for name, c in columns.items()},
        }

    return {
        FCT_ORDERS: p(
            "2026-11-14T05:00:00",
            1225,
            98304,
            {
                "order_id": {
                    "nullCount": 0,
                    "nullProportion": 0.0,
                    "uniqueCount": 1225,
                    "uniqueProportion": 1.0,
                    "min": 1,
                    "max": 1225,
                    "mean": 613.0,
                    "median": 613,
                    "stddev": 353.8,
                },
                "customer_email": {
                    "nullCount": 980,
                    "nullProportion": 0.8,
                    "uniqueCount": 200,
                    "uniqueProportion": 0.16,
                },
            },
        ),
        DIM_CUSTOMERS: p(
            "2026-11-14T05:05:00",
            300,
            16384,
            {
                "customer_id": {"nullCount": 0, "nullProportion": 0.0, "uniqueCount": 300, "uniqueProportion": 1.0},
            },
        ),
        ORDERS: p("2026-11-13T05:00:00", 1300, 120000, {}),
    }


def sample_data() -> dict[str, dict[str, Any]]:
    """Sample rows (including PII columns: the worker must drop them)."""
    return {
        FCT_ORDERS: {
            "columns": ["order_id", "customer_id", "order_total", "order_date", "customer_email"],
            "rows": [
                [1, 10, 99.5, "2026-11-01", "ann@example.com"],
                [2, 11, 12.0, "2026-11-01", "ben@example.com"],
                [3, 10, None, "2026-11-02", "ann@example.com"],
            ],
        },
        USERS: {"columns": ["id", "email", "created_at"], "rows": [[1, "x@example.com", "2026-01-01"]]},
    }


def queries() -> list[dict[str, Any]]:
    """Query entities."""

    def q(n: int, sql: str, used: list[str], duration: float, by: str) -> dict[str, Any]:
        fqn = f"{SF}.q{n:04d}"
        return {
            "id": uid("query", fqn),
            "name": f"q{n:04d}",
            "fullyQualifiedName": fqn,
            "query": sql,
            "queryDate": ms("2026-11-14T08:00:00"),
            "duration": duration,
            "service": SF,
            "users": [user(by)],
            "usedBy": [by.upper()],
            "queryUsedIn": [ref("table", u) for u in used],
            "checksum": f"chk{n}",
            "owners": [],
            "tags": [],
            **audit(),
        }

    return [
        q(1, "select sum(order_total) from marts.fct_orders", [FCT_ORDERS], 1520.0, "alice"),
        q(
            2,
            "select * from marts.fct_orders join marts.dim_customers using (customer_id)",
            [FCT_ORDERS, DIM_CUSTOMERS],
            4210.5,
            "bob",
        ),
    ]


def main() -> None:
    """Write the fixture."""
    data = {
        "openmetadata_version": "1.6.2",
        "collections": {
            "services/databaseServices": database_services(),
            "databases": databases(),
            "databaseSchemas": database_schemas(),
            "tables": tables(),
            "dashboards": dashboards(),
            "charts": charts(),
            "pipelines": pipelines(),
            "topics": topics(),
            "mlmodels": mlmodels(),
            "users": users(),
            "teams": teams(),
            "classifications": classifications(),
            "tags": tags(),
            "glossaries": glossaries(),
            "glossaryTerms": glossary_terms(),
            "domains": domains(),
            "dataProducts": data_products(),
            "dataQuality/testSuites": test_suites(),
            "dataQuality/testCases": test_cases(),
            "queries": queries(),
        },
        "_lineage": lineage(),
        "_testCaseResults": results(),
        "_incidentStatuses": incident_statuses(),
        "_usage": usage_history(),
        "_profiles": profiles(),
        "_sampleData": sample_data(),
    }
    OUT.write_text(json.dumps(data, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
