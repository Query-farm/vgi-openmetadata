"""Strict OpenMetadata 1.6 REST replay server for the tests (no MySQL / Elasticsearch needed).

It serves ``GET /api/v1/...`` from ``test/fixtures/openmetadata.json`` and is strict about what
it accepts, the way the real server is:

* every route has a fixed set of query parameters; an unknown parameter is HTTP 400;
* ``fields=`` must name fields the entity type has (``Invalid field name`` 400 otherwise), and
  optional fields that were not requested are stripped from the response;
* ``limit`` must be 1..1000000, ``include`` one of non-deleted / deleted / all, ``after`` an
  opaque cursor this server issued;
* ``/v1/search/query`` takes a known ``index`` and a ``query_filter`` that must be
  ``{"query":{"bool":{"must":[...]}}}`` of ``term`` / ``terms`` / ``bool.should`` clauses on
  fields the index maps; ``search_after`` needs a ``sort_field``; ``from + size <= 10000``;
* time-series endpoints require ``startTs`` / ``endTs`` epoch milliseconds; usage ``days`` is
  1..30.

Environment:

* ``MOCK_TOKEN=<t>``: require ``Authorization: Bearer <t>`` (401 otherwise).
* ``MOCK_FLAKY=1``: answer the first sighting of every third distinct request with
  ``429 Retry-After: 0``, so the retry path runs in every suite.
* ``MOCK_STRICT_PUSHDOWN=1``: refuse unfiltered scans so SQL tests can prove pushdown: a
  ``/v1/tables`` listing, a search without ``query_filter``, a test-case listing without
  ``entityLink`` and a time-series call with ``startTs=0`` are 400s.
* ``GET /__requests`` returns the log of ``{path, params}`` received; ``POST /__reset`` clears it.

Prints ``PORT:<n>`` on stdout once listening (``--port 0`` picks a free port).
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import os
import sys
import threading
import urllib.parse
from collections import deque
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = json.loads((ROOT / "test/fixtures/openmetadata.json").read_text())
COLLECTIONS: dict[str, list[dict[str, Any]]] = FIXTURE["collections"]

# Optional fields per collection (OpenMetadata 1.6 ``FIELDS`` lists). Everything else is a base field.
OPTIONAL_FIELDS = {
    "tables": "tableConstraints,tablePartition,usageSummary,owners,customMetrics,columns,tags,followers,joins,"
    "schemaDefinition,dataModel,extension,testSuite,domain,dataProducts,lifeCycle,sourceHash,votes",
    "services/databaseServices": "pipelines,owners,tags,domain,followers,dataProducts",
    "databases": "owners,databaseSchemas,usageSummary,location,tags,extension,domain,dataProducts,votes,followers,"
    "sourceHash",
    "databaseSchemas": "owners,tables,usageSummary,tags,extension,domain,dataProducts,votes,followers,sourceHash",
    "dashboards": "owners,charts,followers,tags,usageSummary,extension,dataModels,domain,dataProducts,votes,sourceHash",
    "charts": "owners,followers,tags,domain,dataProducts,votes,extension,sourceHash,dashboards",
    "pipelines": "owners,tasks,pipelineStatus,followers,tags,extension,domain,sourceHash,dataProducts,votes,"
    "usageSummary",
    "topics": "owners,followers,tags,extension,domain,dataProducts,votes,sourceHash",
    "mlmodels": "owners,dashboard,followers,tags,usageSummary,extension,domain,dataProducts,votes,sourceHash",
    "users": "profile,roles,teams,follows,owns,domain,personas,defaultPersona",
    "teams": "owners,profile,users,owns,defaultRoles,parents,children,policies,userCount,childrenCount,domain",
    "classifications": "termCount,usageCount",
    "tags": "children,usageCount",
    "glossaries": "owners,tags,reviewers,usageCount,termCount,domain,extension,votes",
    "glossaryTerms": "children,relatedTerms,reviewers,owners,tags,usageCount,domain,extension,childrenCount",
    "domains": "children,owners,parent,experts",
    "dataProducts": "domain,owners,experts,assets,extension",
    "dataQuality/testSuites": "owners,tests,summary,pipelines",
    "dataQuality/testCases": "owners,testSuite,testDefinition,testSuites,incidentId,testCaseResult,tags",
    "queries": "owners,followers,users,votes,tags,queryUsedIn",
}
OPTIONAL = {k: set(v.split(",")) for k, v in OPTIONAL_FIELDS.items()}

# List filter parameters per collection: parameter -> how it matches an entity.
COMMON_LIST = {"fields", "limit", "before", "after", "include"}
LIST_FILTERS: dict[str, dict[str, str]] = {
    "tables": {
        "database": "database.fullyQualifiedName",
        "databaseSchema": "databaseSchema.fullyQualifiedName",
        "includeEmptyTestSuite": "",
    },
    "services/databaseServices": {"domain": "domain.fullyQualifiedName"},
    "databases": {"service": "service.name"},
    "databaseSchemas": {"database": "database.fullyQualifiedName"},
    "dashboards": {"service": "service.name"},
    "charts": {"service": "service.name"},
    "pipelines": {"service": "service.name"},
    "topics": {"service": "service.name"},
    "mlmodels": {"service": "service.name"},
    "users": {"isBot": "isBot", "isAdmin": "isAdmin", "team": ""},
    "teams": {"parentTeam": "", "isJoinable": "isJoinable"},
    "classifications": {"disabled": "disabled"},
    "tags": {"parent": "classification.name", "disabled": "disabled"},
    "glossaries": {},
    "glossaryTerms": {"glossary": "glossary.id", "parent": "parent.id", "directChildrenOf": ""},
    "domains": {},
    "dataProducts": {"domain": "domain.fullyQualifiedName"},
    "dataQuality/testSuites": {"testSuiteType": "", "includeEmptyTestSuites": ""},
    "dataQuality/testCases": {
        "entityLink": "",
        "testSuiteId": "testSuite.id",
        "includeAllTests": "",
        "orderByLastExecutionDate": "",
    },
    "queries": {"service": "service", "entityId": ""},
}
GET_PARAMS = {"fields", "include"}

# Search indexes and the fields their query_filter may use.
TABLE_INDEX_FIELDS = {
    "service.name.keyword",
    "database.name.keyword",
    "databaseSchema.name.keyword",
    "database.fullyQualifiedName",
    "databaseSchema.fullyQualifiedName",
    "tableType",
    "tier.tagFQN",
    "domain.fullyQualifiedName",
    "domain.id",
    "tags.tagFQN",
    "owners.name.keyword",
    "columns.tags.tagFQN",
    "serviceType",
    "fullyQualifiedName",
    "entityType",
}
ASSET_INDEX_FIELDS = {
    "service.name.keyword",
    "tier.tagFQN",
    "domain.fullyQualifiedName",
    "domain.id",
    "tags.tagFQN",
    "owners.name.keyword",
    "serviceType",
    "fullyQualifiedName",
    "entityType",
    "database.name.keyword",
    "databaseSchema.name.keyword",
}
INDEXES = {
    "table_search_index": ("tables", "table", TABLE_INDEX_FIELDS),
    "dashboard_search_index": ("dashboards", "dashboard", ASSET_INDEX_FIELDS),
    "chart_search_index": ("charts", "chart", ASSET_INDEX_FIELDS),
    "pipeline_search_index": ("pipelines", "pipeline", ASSET_INDEX_FIELDS),
    "topic_search_index": ("topics", "topic", ASSET_INDEX_FIELDS),
    "mlmodel_search_index": ("mlmodels", "mlmodel", ASSET_INDEX_FIELDS),
    "container_search_index": ("containers", "container", ASSET_INDEX_FIELDS),
    "glossary_term_search_index": ("glossaryTerms", "glossaryTerm", ASSET_INDEX_FIELDS),
    "tag_search_index": ("tags", "tag", ASSET_INDEX_FIELDS),
}
ALL_INDEX = [
    "table_search_index",
    "dashboard_search_index",
    "chart_search_index",
    "pipeline_search_index",
    "topic_search_index",
    "mlmodel_search_index",
    "glossary_term_search_index",
    "tag_search_index",
]
SEARCH_PARAMS = {
    "q",
    "index",
    "from",
    "size",
    "deleted",
    "query_filter",
    "post_filter",
    "sort_field",
    "sort_order",
    "track_total_hits",
    "search_after",
    "fetch_source",
    "include_source_fields",
}

ENTITY_TYPES = {
    "table": "tables",
    "dashboard": "dashboards",
    "chart": "charts",
    "pipeline": "pipelines",
    "topic": "topics",
    "mlmodel": "mlmodels",
    "container": "containers",
    "dashboardDataModel": "dashboardDataModels",
    "storedProcedure": "storedProcedures",
    "searchIndex": "searchIndexes",
}

TOKEN = os.environ.get("MOCK_TOKEN") or None
FLAKY = os.environ.get("MOCK_FLAKY") == "1"
STRICT = os.environ.get("MOCK_STRICT_PUSHDOWN") == "1"
LOG: deque[dict[str, Any]] = deque(maxlen=5000)
LOCK = threading.Lock()
SEEN: dict[str, int] = {}


class HttpError(Exception):
    """An HTTP error response."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def bad(message: str) -> HttpError:
    """A 400."""
    return HttpError(400, message)


# ----------------------------------------------------------------------------- helpers


def by_fqn(collection: str) -> dict[str, dict[str, Any]]:
    """Entities of a collection by FQN."""
    return {e["fullyQualifiedName"]: e for e in COLLECTIONS.get(collection, [])}


def by_id(collection: str) -> dict[str, dict[str, Any]]:
    """Entities of a collection by id."""
    return {e["id"]: e for e in COLLECTIONS.get(collection, [])}


def resolve(doc: Any, path: list[str]) -> list[Any]:
    """Every leaf value at a dotted path (lists are traversed)."""
    if not path:
        return doc if isinstance(doc, list) else [doc]
    if isinstance(doc, list):
        return [v for item in doc for v in resolve(item, path)]
    if not isinstance(doc, dict):
        return []
    return resolve(doc.get(path[0]), path[1:])


def project(collection: str, entity: dict[str, Any], fields_param: str | None) -> dict[str, Any]:
    """Validate ``fields`` and strip optional fields that were not requested."""
    optional = OPTIONAL[collection]
    requested = {f.strip() for f in (fields_param or "").split(",") if f.strip()}
    unknown = requested - optional
    if unknown:
        raise bad(f"Invalid field name {sorted(unknown)[0]}")
    out = {k: copy.deepcopy(v) for k, v in entity.items() if k not in optional or k in requested}
    if collection == "tables" and "columns" in out and "tags" not in requested:
        for c in out["columns"]:
            c.pop("tags", None)
    return out


def int_param(params: dict[str, str], name: str, lo: int, hi: int, default: int | None = None) -> int:
    """An integer query parameter within [lo, hi]."""
    raw = params.get(name)
    if raw is None:
        if default is None:
            raise bad(f"query param {name} is required")
        return default
    try:
        value = int(raw)
    except ValueError as err:
        raise bad(f"query param {name} must be an integer, got {raw!r}") from err
    if not lo <= value <= hi:
        raise bad(f"query param {name} must be between {lo} and {hi}")
    return value


def bool_param(params: dict[str, str], name: str) -> bool | None:
    """A boolean query parameter."""
    raw = params.get(name)
    if raw is None:
        return None
    if raw not in ("true", "false"):
        raise bad(f"query param {name} must be true or false, got {raw!r}")
    return raw == "true"


def check_params(params: dict[str, str], allowed: set[str]) -> None:
    """Reject unknown query parameters."""
    unknown = set(params) - allowed
    if unknown:
        raise bad(f"Unrecognized query parameter: {sorted(unknown)[0]}")


def cursor(offset: int) -> str:
    """An opaque ``after`` cursor."""
    return base64.urlsafe_b64encode(f"om:{offset}".encode()).decode()


def uncursor(value: str) -> int:
    """Decode an ``after`` cursor."""
    try:
        text = base64.urlsafe_b64decode(value.encode()).decode()
    except ValueError as err:
        raise bad("invalid after cursor") from err
    if not text.startswith("om:"):
        raise bad("invalid after cursor")
    return int(text[3:])


# ----------------------------------------------------------------------------- list / get


def list_entities(collection: str, params: dict[str, str]) -> dict[str, Any]:
    """``GET /v1/<collection>``."""
    filters = LIST_FILTERS[collection]
    check_params(params, COMMON_LIST | set(filters))
    limit = int_param(params, "limit", 1, 1_000_000, 10)
    include = params.get("include", "non-deleted")
    if include not in ("non-deleted", "deleted", "all"):
        raise bad(f"invalid include {include!r}")
    if STRICT and collection == "tables" and not (set(params) & set(filters)):
        raise bad("strict mode: unfiltered /v1/tables listing refused (push a filter down)")
    if STRICT and collection == "dataQuality/testCases" and "entityLink" not in params:
        raise bad("strict mode: test case listing without entityLink refused")
    rows = list(COLLECTIONS.get(collection, []))
    if include == "non-deleted":
        rows = [r for r in rows if not r.get("deleted")]
    for param, value in params.items():
        path = filters.get(param)
        if param == "entityLink":
            rows = [r for r in rows if match_link(r, value, params.get("includeAllTests") == "true")]
        elif param == "testSuiteType":
            if value not in ("basic", "logical"):
                raise bad(f"invalid testSuiteType {value!r}")
            rows = [r for r in rows if bool(r.get("basic")) == (value == "basic")]
        elif param in ("isBot", "isAdmin", "isJoinable", "disabled"):
            wanted = bool_param(params, param)
            rows = [r for r in rows if bool(r.get(path or param)) == wanted]
        elif path:
            rows = [r for r in rows if value in [str(v) for v in resolve(r, path.split("."))]]
    rows.sort(key=lambda r: r.get("fullyQualifiedName") or r.get("name") or r["id"])
    start = uncursor(params["after"]) if params.get("after") else 0
    page = rows[start : start + limit]
    paging: dict[str, Any] = {"total": len(rows)}
    if start + limit < len(rows):
        paging["after"] = cursor(start + limit)
    if start:
        paging["before"] = cursor(max(0, start - limit))
    return {"data": [project(collection, r, params.get("fields")) for r in page], "paging": paging}


def match_link(test_case: dict[str, Any], link: str, include_all: bool) -> bool:
    """Does a test case's entityLink match a requested table link (columns too with includeAllTests)?"""
    if not (link.startswith("<#E::") and link.endswith(">")):
        raise bad(f"invalid entityLink {link!r}")
    mine = test_case.get("entityLink", "")
    if mine == link:
        return True
    return include_all and mine.startswith(link[:-1] + "::columns::")


def get_entity(collection: str, key: str, params: dict[str, str], *, by_name: bool) -> dict[str, Any]:
    """``GET /v1/<collection>/{id}`` or ``/name/{fqn}``."""
    check_params(params, GET_PARAMS)
    found = (by_fqn if by_name else by_id)(collection).get(key)
    if found is None or (found.get("deleted") and params.get("include", "non-deleted") == "non-deleted"):
        raise HttpError(404, f"{collection} instance for {key} not found")
    return project(collection, found, params.get("fields"))


# ----------------------------------------------------------------------------- search


def search_doc(entity: dict[str, Any], entity_type: str) -> dict[str, Any]:
    """The search-index document of an entity: everything, plus ``tier`` and ``entityType``."""
    doc = copy.deepcopy(entity)
    doc["entityType"] = entity_type
    tier = next((t for t in entity.get("tags") or [] if t.get("tagFQN", "").startswith("Tier.")), None)
    doc["tier"] = tier
    doc.pop("connection", None)
    return doc


def clause_matches(clause: Any, doc: dict[str, Any], fields: set[str]) -> bool:
    """Evaluate one query_filter clause."""
    if not isinstance(clause, dict) or len(clause) != 1:
        raise bad(f"unsupported query_filter clause {clause!r}")
    kind, body = next(iter(clause.items()))
    if kind == "bool":
        if not isinstance(body, dict) or set(body) - {"should", "must", "minimum_should_match"}:
            raise bad("query_filter bool supports must / should only")
        must = body.get("must") or []
        should = body.get("should") or []
        return all(clause_matches(c, doc, fields) for c in must) and (
            not should or any(clause_matches(c, doc, fields) for c in should)
        )
    if kind in ("term", "terms"):
        if not isinstance(body, dict) or len(body) != 1:
            raise bad(f"{kind} needs exactly one field")
        field, value = next(iter(body.items()))
        if field not in fields:
            raise bad(f"field {field!r} is not mapped in this index")
        values = value if kind == "terms" else [value]
        if kind == "terms" and not isinstance(value, list):
            raise bad("terms needs a list")
        path = field.removesuffix(".keyword").split(".")
        have = {str(v) for v in resolve(doc, path) if v is not None}
        return any(str(v) in have for v in values)
    raise bad(f"unsupported query_filter clause type {kind!r}")


def search(params: dict[str, str]) -> dict[str, Any]:
    """``GET /v1/search/query``."""
    check_params(params, SEARCH_PARAMS)
    index = params.get("index", "table_search_index")
    if index != "all" and index not in INDEXES:
        raise bad(f"unknown index {index!r}")
    frm = int_param(params, "from", 0, 10_000, 0)
    size = int_param(params, "size", 0, 10_000, 10)
    if frm + size > 10_000:
        raise bad("Result window is too large, from + size must be less than or equal to: [10000]")
    raw_filter = params.get("query_filter")
    if STRICT and not raw_filter:
        raise bad("strict mode: search without query_filter refused")
    must: list[Any] = []
    if raw_filter:
        try:
            parsed = json.loads(raw_filter)
        except json.JSONDecodeError as err:
            raise bad(f"query_filter is not JSON: {err}") from err
        if set(parsed) != {"query"} or set(parsed["query"]) != {"bool"} or set(parsed["query"]["bool"]) != {"must"}:
            raise bad('query_filter must be {"query":{"bool":{"must":[...]}}}')
        must = parsed["query"]["bool"]["must"]
        if not isinstance(must, list):
            raise bad("query_filter must is not a list")
    indexes = ALL_INDEX if index == "all" else [index]
    q = params.get("q", "*").strip()
    deleted = params.get("deleted", "false") == "true"
    hits = []
    for idx in indexes:
        collection, etype, fields = INDEXES[idx]
        for entity in COLLECTIONS.get(collection, []):
            if bool(entity.get("deleted")) != deleted:
                continue
            doc = search_doc(entity, etype)
            if not all(clause_matches(c, doc, fields) for c in must):
                continue
            score = text_score(q, doc)
            if score <= 0:
                continue
            hits.append({"_index": idx, "_id": doc["id"], "_score": score, "_source": doc})
    sort_field = params.get("sort_field", "_score")
    sort_order = params.get("sort_order", "desc")
    if sort_order not in ("asc", "desc"):
        raise bad("sort_order must be asc or desc")
    if sort_field == "_score":
        hits.sort(key=lambda h: (-h["_score"], h["_source"]["fullyQualifiedName"]))
        for h in hits:
            h["sort"] = [h["_score"], h["_source"]["fullyQualifiedName"]]
    elif sort_field == "fullyQualifiedName":
        hits.sort(key=lambda h: h["_source"]["fullyQualifiedName"], reverse=sort_order == "desc")
        for h in hits:
            h["sort"] = [h["_source"]["fullyQualifiedName"]]
    else:
        raise bad(f"sort_field {sort_field!r} is not sortable")
    after = params.get("search_after")
    if after:
        if sort_field != "fullyQualifiedName":
            raise bad("search_after needs sort_field=fullyQualifiedName in this replay")
        hits = [h for h in hits if (h["sort"][0] > after) == (sort_order == "asc") and h["sort"][0] != after]
        if frm:
            raise bad("from must be 0 with search_after")
    total = len(hits)
    page = hits[frm : frm + size]
    return {
        "took": 1,
        "timed_out": False,
        "hits": {
            "total": {"value": total, "relation": "eq"},
            "max_score": page[0]["_score"] if page else None,
            "hits": page,
        },
    }


def text_score(q: str, doc: dict[str, Any]) -> float:
    """1.0 for ``*``; otherwise the number of query words found in the doc's text fields."""
    if q in ("", "*"):
        return 1.0
    text = " ".join(
        str(v)
        for v in [
            doc.get("name"),
            doc.get("displayName"),
            doc.get("description"),
            doc.get("fullyQualifiedName"),
            *[c.get("name") for c in doc.get("columns") or []],
        ]
        if v
    ).lower()
    words = [w for w in q.lower().replace("*", " ").split() if w]
    return float(sum(text.count(w) for w in words))


# ----------------------------------------------------------------------------- lineage


def lineage(entity_type: str, fqn: str, params: dict[str, str]) -> dict[str, Any]:
    """``GET /v1/lineage/{entityType}/name/{fqn}``."""
    check_params(params, {"upstreamDepth", "downstreamDepth"})
    collection = ENTITY_TYPES.get(entity_type)
    if collection is None:
        raise bad(f"unknown entity type {entity_type!r}")
    up_depth = int_param(params, "upstreamDepth", 0, 10, 1)
    down_depth = int_param(params, "downstreamDepth", 0, 10, 1)
    start = by_fqn(collection).get(fqn)
    if start is None:
        raise HttpError(404, f"{entity_type} instance for {fqn} not found")

    def node(kind: str, name: str) -> dict[str, Any]:
        ent = by_fqn(ENTITY_TYPES[kind])[name]
        return {
            "id": ent["id"],
            "type": kind,
            "name": ent["name"],
            "fullyQualifiedName": name,
            "displayName": ent.get("displayName"),
            "deleted": False,
        }

    edges = [
        {"fromEntity": node(*e["from"]), "toEntity": node(*e["to"]), "lineageDetails": e["lineageDetails"]}
        for e in FIXTURE["_lineage"]
    ]
    nodes: dict[str, dict[str, Any]] = {}

    def walk(upstream: bool, depth: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        frontier = {start["id"]}
        seen = set(frontier)
        for _ in range(depth):
            nxt = set()
            for e in edges:
                near, far = (e["toEntity"], e["fromEntity"]) if upstream else (e["fromEntity"], e["toEntity"])
                if near["id"] in frontier:
                    out.append(e)
                    nodes[far["id"]] = far
                    if far["id"] not in seen:
                        seen.add(far["id"])
                        nxt.add(far["id"])
            frontier = nxt
        return out

    def wire(e: dict[str, Any]) -> dict[str, Any]:
        return {
            "fromEntity": e["fromEntity"]["id"],
            "toEntity": e["toEntity"]["id"],
            "lineageDetails": e["lineageDetails"],
        }

    ups = [wire(e) for e in walk(True, up_depth)]
    downs = [wire(e) for e in walk(False, down_depth)]
    nodes.pop(start["id"], None)
    return {
        "entity": {
            "id": start["id"],
            "type": entity_type,
            "name": start["name"],
            "fullyQualifiedName": fqn,
            "deleted": False,
        },
        "nodes": sorted(nodes.values(), key=lambda n: n["fullyQualifiedName"]),
        "upstreamEdges": ups,
        "downstreamEdges": downs,
    }


# ----------------------------------------------------------------------------- time series and table extras


def window(params: dict[str, str]) -> tuple[int, int]:
    """Required ``startTs`` / ``endTs``."""
    start = int_param(params, "startTs", 0, 2**62)
    end = int_param(params, "endTs", 0, 2**62)
    if STRICT and start == 0:
        raise bad("strict mode: startTs=0 refused (push a time range down)")
    return start, end


def test_case_results(fqn: str, params: dict[str, str]) -> dict[str, Any]:
    """``GET /v1/dataQuality/testCases/testCaseResults/{fqn}``."""
    check_params(params, {"startTs", "endTs"})
    start, end = window(params)
    if fqn not in by_fqn("dataQuality/testCases"):
        raise HttpError(404, f"testCase instance for {fqn} not found")
    rows = [r for r in FIXTURE["_testCaseResults"].get(fqn, []) if start <= r["timestamp"] <= end]
    rows.sort(key=lambda r: -r["timestamp"])
    return {"data": rows, "paging": {"total": len(rows)}}


def incidents(params: dict[str, str]) -> dict[str, Any]:
    """``GET /v1/dataQuality/testCases/testCaseIncidentStatus``."""
    check_params(
        params,
        {
            "startTs",
            "endTs",
            "latest",
            "testCaseResolutionStatusType",
            "assignee",
            "testCaseFQN",
            "limit",
            "offset",
            "include",
            "testCaseId",
        },
    )
    start, end = window(params)
    limit = int_param(params, "limit", 1, 1_000_000, 10)
    offset = int_param(params, "offset", 0, 1_000_000_000, 0)
    latest = bool_param(params, "latest") or False
    kind = params.get("testCaseResolutionStatusType")
    if kind is not None and kind not in ("New", "Ack", "Assigned", "Resolved"):
        raise bad(f"invalid testCaseResolutionStatusType {kind!r}")
    rows = [r for r in FIXTURE["_incidentStatuses"] if start <= r["timestamp"] <= end]
    if latest:
        newest: dict[str, dict[str, Any]] = {}
        for r in rows:
            if r["stateId"] not in newest or r["timestamp"] > newest[r["stateId"]]["timestamp"]:
                newest[r["stateId"]] = r
        rows = list(newest.values())
    if kind:
        rows = [r for r in rows if r["testCaseResolutionStatusType"] == kind]
    if params.get("testCaseFQN"):
        rows = [r for r in rows if r["testCaseReference"]["fullyQualifiedName"] == params["testCaseFQN"]]
    rows.sort(key=lambda r: (-r["timestamp"], r["id"]))
    return {"data": rows[offset : offset + limit], "paging": {"total": len(rows), "offset": offset, "limit": limit}}


def table_profile(fqn: str, params: dict[str, str]) -> dict[str, Any]:
    """``GET /v1/tables/{fqn}/tableProfile/latest``."""
    check_params(params, {"includeColumnProfile"})
    table = by_fqn("tables").get(fqn)
    if table is None:
        raise HttpError(404, f"table instance for {fqn} not found")
    with_columns = bool_param(params, "includeColumnProfile")
    out = project("tables", table, "columns")
    prof = FIXTURE["_profiles"].get(fqn)
    if prof:
        out["profile"] = prof["profile"]
        if with_columns is not False:
            for c in out["columns"]:
                if c["name"] in prof["columns"]:
                    c["profile"] = prof["columns"][c["name"]]
    return out


def sample(table_id: str, params: dict[str, str]) -> dict[str, Any]:
    """``GET /v1/tables/{id}/sampleData``."""
    check_params(params, set())
    table = by_id("tables").get(table_id)
    if table is None:
        raise HttpError(404, f"table instance for {table_id} not found")
    out = project("tables", table, "columns")
    out["sampleData"] = FIXTURE["_sampleData"].get(table["fullyQualifiedName"], {"columns": [], "rows": []})
    return out


def usage(fqn: str, params: dict[str, str]) -> dict[str, Any]:
    """``GET /v1/usage/table/name/{fqn}``."""
    check_params(params, {"days", "date"})
    table = by_fqn("tables").get(fqn)
    if table is None:
        raise HttpError(404, f"table instance for {fqn} not found")
    days = int_param(params, "days", 1, 30, 1)
    try:
        end = date.fromisoformat(params["date"]) if params.get("date") else datetime.now(tz=UTC).date()
    except ValueError as err:
        raise bad("date must be YYYY-MM-DD") from err
    first = end - timedelta(days=days - 1)
    rows = [u for u in FIXTURE["_usage"].get(fqn, []) if first <= date.fromisoformat(u["date"]) <= end]
    return {
        "entity": {"id": table["id"], "type": "table", "fullyQualifiedName": fqn, "name": table["name"]},
        "usage": rows,
    }


# ----------------------------------------------------------------------------- routing


def route(path: str, params: dict[str, str]) -> Any:
    """Dispatch one GET below ``/api``."""
    if not path.startswith("/v1/"):
        raise HttpError(404, f"no route {path}")
    rest = path[len("/v1/") :]
    parts = [urllib.parse.unquote(p) for p in rest.split("/")]
    if rest == "search/query":
        return search(params)
    if parts[0] == "lineage" and len(parts) == 4 and parts[2] == "name":
        return lineage(parts[1], parts[3], params)
    if rest.startswith("dataQuality/testCases/testCaseResults/") and len(parts) == 4:
        return test_case_results(parts[3], params)
    if rest == "dataQuality/testCases/testCaseIncidentStatus":
        return incidents(params)
    if parts[0] == "usage" and len(parts) == 4 and parts[1] == "table" and parts[2] == "name":
        return usage(parts[3], params)
    if parts[0] == "tables" and len(parts) == 4 and parts[2:] == ["tableProfile", "latest"]:
        return table_profile(parts[1], params)
    if parts[0] == "tables" and len(parts) == 3 and parts[2] == "sampleData":
        return sample(parts[1], params)
    for collection in sorted(OPTIONAL, key=len, reverse=True):
        segs = collection.split("/")
        if parts[: len(segs)] != segs:
            continue
        tail = parts[len(segs) :]
        if not tail:
            return list_entities(collection, params)
        if len(tail) == 2 and tail[0] == "name":
            return get_entity(collection, tail[1], params, by_name=True)
        if len(tail) == 1:
            return get_entity(collection, tail[0], params, by_name=False)
    raise HttpError(404, f"no route {path}")


class Handler(BaseHTTPRequestHandler):
    """Serve the replay API."""

    def log_message(self, fmt: str, *args: Any) -> None:
        print(fmt % args, file=sys.stderr)

    def _send(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        if self.path == "/__reset":
            with LOCK:
                LOG.clear()
            self._send(200, {"ok": True})
        else:
            self._send(405, {"code": 405, "message": "read-only replay server"})

    def do_GET(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/__requests":
            with LOCK:
                self._send(200, list(LOG))
            return
        raw = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if any(len(v) > 1 for v in raw.values()):
            self._send(400, {"code": 400, "message": "repeated query parameter"})
            return
        params = {k: v[0] for k, v in raw.items()}
        if TOKEN and self.headers.get("Authorization") != f"Bearer {TOKEN}":
            self._send(401, {"code": 401, "message": "Not Authorized! Token not present"})
            return
        if not parsed.path.startswith("/api/"):
            self._send(404, {"code": 404, "message": f"no route {parsed.path}"})
            return
        path = parsed.path[len("/api") :]
        key = hashlib.sha1(self.path.encode()).hexdigest()
        if FLAKY:
            with LOCK:
                if key not in SEEN:
                    SEEN[key] = len(SEEN)
                    if SEEN[key] % 3 == 2:
                        self._send(429, {"code": 429, "message": "Too many requests"}, {"Retry-After": "0"})
                        return
        with LOCK:
            LOG.append({"path": urllib.parse.unquote(path), "params": params})
        try:
            body = route(path, params)
        except HttpError as err:
            self._send(err.status, {"code": err.status, "message": err.message})
            return
        self._send(200, body)


def main() -> None:
    """Serve until killed."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
