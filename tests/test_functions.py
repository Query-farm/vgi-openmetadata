"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pytest

from openmetadata_worker.client import (
    NotFoundError,
    OpenMetadataError,
    endpoint_from_secret,
    get_json,
    retry_delay,
)
from openmetadata_worker.common import (
    is_pii,
    platform_of,
    qualified_name,
    split_fqn,
    table_fqn_of,
    tier_of,
    ts,
)
from openmetadata_worker.functions import SearchArgs, search_filter
from openmetadata_worker.lineage import _degrees, edge_rows
from openmetadata_worker.quality import parse_entity_link
from openmetadata_worker.scan import query_filter, term_clause


def test_split_fqn_honours_quotes() -> None:
    assert split_fqn('svc."my.db".sch.t') == ["svc", "my.db", "sch", "t"]
    assert split_fqn("a.b.c.d.col") == ["a", "b", "c", "d", "col"]
    assert split_fqn(None) == []


def test_qualified_name_drops_service_and_lowercases() -> None:
    assert qualified_name("Snowflake_Prod.ANALYTICS.Marts.FCT") == "analytics.marts.fct"
    assert qualified_name('svc."my.db".s.t') == "my.db.s.t"
    assert qualified_name("svc.db") is None
    assert table_fqn_of("svc.db.s.t.col.test") == "svc.db.s.t"


def test_platform_names() -> None:
    assert platform_of("Snowflake") == "snowflake"
    assert platform_of("BigQuery") == "bigquery"
    assert platform_of("UnityCatalog") == "unity-catalog"
    assert platform_of(None) is None


def test_labels() -> None:
    entity: dict[str, Any] = {
        "tags": [{"tagFQN": "PII.Sensitive"}, {"tagFQN": "Tier.Tier2", "source": "Classification"}]
    }
    assert is_pii(entity)
    assert tier_of(entity) == "Tier.Tier2"
    assert tier_of({"tier": {"tagFQN": "Tier.Tier1"}}) == "Tier.Tier1"
    assert not is_pii({"tags": [{"tagFQN": "PIIx.Other"}]})


def test_ts_handles_millis_seconds_and_junk() -> None:
    assert ts(1793577600000) == datetime(2026, 11, 2, tzinfo=UTC)
    assert ts(1793577600) == datetime(2026, 11, 2, tzinfo=UTC)
    assert ts(0) is None
    assert ts("x") is None


def test_entity_link() -> None:
    assert parse_entity_link("<#E::table::s.d.sc.t::columns::c>") == ("s.d.sc.t", "c")
    assert parse_entity_link("<#E::table::s.d.sc.t>") == ("s.d.sc.t", None)
    assert parse_entity_link("<#E::topic::k.t>") == (None, None)


def test_endpoint_from_secret() -> None:
    ep = endpoint_from_secret({"host_port": "http://localhost:8585", "token": "t"})
    assert ep.api_url == "http://localhost:8585/api"
    assert ep.ui_url == "http://localhost:8585"
    assert ep.page_size == 100
    ep = endpoint_from_secret({"host_port": "https://x.getcollate.io/api/", "ui_url": "https://ui", "page_size": "7"})
    assert (ep.api_url, ep.ui_url, ep.page_size) == ("https://x.getcollate.io/api", "https://ui", 7)
    assert ep.entity_url("table", "a.b c") == "https://ui/table/a.b%20c"
    with pytest.raises(OpenMetadataError, match="no openmetadata secret"):
        endpoint_from_secret(None)
    with pytest.raises(OpenMetadataError, match="must start with http"):
        endpoint_from_secret({"host_port": "localhost"})
    with pytest.raises(OpenMetadataError, match="between 1 and 1000"):
        endpoint_from_secret({"host_port": "http://h", "page_size": "0"})


def test_retry_delay() -> None:
    assert retry_delay("3", 1) == 3.0
    assert retry_delay("junk", 1) == 1.0
    assert retry_delay(None, 10) == 60.0


def test_query_filter_shape() -> None:
    q = json.loads(query_filter([term_clause("service.name.keyword", ["a", "b"])]))
    assert q == {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [{"term": {"service.name.keyword": "a"}}, {"term": {"service.name.keyword": "b"}}]
                        }
                    }
                ]
            }
        }
    }


def test_search_filter_from_named_args() -> None:
    args = SearchArgs(
        query="*",
        entity_type="table",
        service="s",
        database=None,
        schema_name=None,
        tag=None,
        owner="bob",
        domain=None,
        tier=None,
        max_results=10,
    )
    flt = search_filter(args)
    assert flt is not None
    fields = [next(iter(c["bool"]["should"][0]["term"])) for c in json.loads(flt)["query"]["bool"]["must"]]
    assert fields == ["service.name.keyword", "owners.name.keyword"]
    none = SearchArgs(
        query="*",
        entity_type="table",
        service=None,
        database=None,
        schema_name=None,
        tag=None,
        owner=None,
        domain=None,
        tier=None,
        max_results=10,
    )
    assert search_filter(none) is None


def test_edge_rows_and_degrees() -> None:
    nodes = {
        "a": {"id": "a", "type": "table", "name": "a", "fullyQualifiedName": "s.d.x.a"},
        "b": {"id": "b", "type": "table", "name": "b", "fullyQualifiedName": "s.d.x.b"},
        "c": {"id": "c", "type": "dashboard", "name": "c", "fullyQualifiedName": "bi.c"},
    }
    edge = {
        "fromEntity": "a",
        "toEntity": "b",
        "lineageDetails": {
            "columnsLineage": [{"fromColumns": ["s.d.x.a.k", "s.d.x.a.v"], "toColumn": "s.d.x.b.k", "function": "f"}]
        },
    }
    rows = edge_rows(edge, nodes)
    assert [r["level"] for r in rows] == ["dataset", "column", "column"]
    assert rows[1]["upstream_column"] == "k" and rows[2]["upstream_column"] == "v"
    assert rows[0]["downstream_qualified_name"] == "d.x.b"
    edges: list[dict[str, Any]] = [edge, {"fromEntity": "b", "toEntity": "c"}]
    assert _degrees("a", edges, upstream=False) == {"a": 0, "b": 1, "c": 2}
    assert _degrees("c", edges, upstream=True) == {"c": 0, "b": 1, "a": 2}


def test_get_json_retries_429_and_maps_404(mock_om: str) -> None:
    ep = endpoint_from_secret({"host_port": mock_om, "page_size": "2"})
    # Each new request may get one 429 first; get_json must retry through it.
    for i in range(1, 7):
        body = get_json(ep, "/v1/tables", {"limit": i, "fields": "owners"})
        assert len(body["data"]) == min(i, 6)
    with pytest.raises(NotFoundError):
        get_json(ep, "/v1/tables/name/no.such.table.here")
    with pytest.raises(OpenMetadataError, match=r"HTTP 400.*Invalid field name"):
        get_json(ep, "/v1/tables", {"fields": "bogus"})


def test_catalog_shape() -> None:
    from openmetadata_worker.worker import OpenmetadataWorker

    schema = OpenmetadataWorker.catalog.schemas[0]
    tables = {t.name for t in schema.tables}
    assert {
        "datasets",
        "columns",
        "owners",
        "tags",
        "glossary_terms",
        "domains",
        "lineage_edges",
        "assertions",
        "assertion_results",
        "usage",
    } <= tables
    names = {f.Meta.name for f in schema.functions}  # type: ignore[attr-defined]
    assert names == {"lineage", "search", "sample_data", "fqn_qualified_name", "split_fqn"}
    for t in schema.tables:
        fields = t.function.FIXED_SCHEMA  # type: ignore[union-attr]
        assert fields.field("catalog_tool").nullable is False, t.name
        assert pa.types.is_string(fields.field("catalog_tool").type)
