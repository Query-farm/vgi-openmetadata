"""The entity scan every catalog table is built on.

A scan has a *plan*, resolved once in ``initial_state`` from the pushed-down filters:

* ``=`` / ``IN`` on the id column fetches those entities by id (``GET <path>/{id}``);
  on the FQN column, by name (``GET <path>/name/{fqn}``). No listing at all.
* ``=`` / ``IN`` (and ``list_contains`` on list columns) on columns the search index
  knows becomes an Elasticsearch / OpenSearch ``query_filter`` on ``GET /v1/search/query``,
  paged with ``search_after`` over a ``fullyQualifiedName`` sort.
* ``=`` / ``IN`` on columns the REST list endpoint takes as a query parameter
  (``service``, ``database``, ``testSuiteId``, ...) becomes that parameter; an ``IN`` list
  runs one listing per value.
* Everything else is a plain REST listing (``GET <path>?limit=&after=``).

The plan and the cursor (``after`` / ``search_after``) live in table-function state, so a
long scan resumes on any worker process over HTTP. The SDK re-applies every predicate to
the rows afterwards, so pushdown only ever narrows what is fetched.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi.table_function import OutputCollector, ProcessParams, TableFunctionGenerator
from vgi_rpc import ArrowSerializableDataclass

from .client import Endpoint, NotFoundError, endpoint, get_json, quote_fqn
from .common import contains_values, eq_values, rows_to_batch

MAX_EMPTY_PAGES = 50
SEARCH_PATH = "/v1/search/query"


@dataclass(kw_only=True)
class ScanCursor(ArrowSerializableDataclass):
    """The resolved plan (JSON) and the position in it."""

    plan_json: str = "{}"
    after: str = ""
    offset: int = 0
    done: bool = False


def term_clause(field: str, values: list[Any]) -> dict[str, Any]:
    """One ``must`` clause: any of ``values`` on ``field`` (a ``bool.should`` of ``term`` queries)."""
    return {"bool": {"should": [{"term": {field: v}} for v in values]}}


def query_filter(clauses: list[dict[str, Any]]) -> str:
    """Build the ``query_filter`` JSON OpenMetadata's search endpoint takes."""
    return json.dumps({"query": {"bool": {"must": clauses}}}, separators=(",", ":"))


def search_page(
    ep: Endpoint, index: str, q: str, filter_json: str | None, after: str, size: int
) -> tuple[list[dict[str, Any]], str]:
    """One page of ``/v1/search/query``: ``(hits, next search_after or '')``."""
    params: dict[str, Any] = {
        "q": q,
        "index": index,
        "from": 0,
        "size": size,
        "deleted": "false",
        "sort_field": "fullyQualifiedName",
        "sort_order": "asc",
        "track_total_hits": "true",
    }
    if filter_json:
        params["query_filter"] = filter_json
    if after:
        params["search_after"] = after
    body = get_json(ep, SEARCH_PATH, params)
    hits = [h for h in ((body or {}).get("hits") or {}).get("hits") or [] if isinstance(h, dict)]
    nxt = ""
    if len(hits) >= size:
        sort = hits[-1].get("sort")
        if isinstance(sort, list) and sort:
            nxt = ",".join(str(s) for s in sort)
    return hits, nxt


def param_value(value: Any) -> str:
    """Render a pushed-down constant as a query-parameter string (booleans as ``true`` / ``false``)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class EntityScan[A](TableFunctionGenerator[A, ScanCursor]):
    """List, search or fetch OpenMetadata entities and turn each into rows."""

    FIXED_SCHEMA: ClassVar[pa.Schema]
    #: REST collection, e.g. ``/v1/tables``.
    PATH: ClassVar[str]
    #: OpenMetadata entity type (``table``) for UI links.
    ENTITY_TYPE: ClassVar[str]
    #: ``fields=`` requested on list and get calls.
    FIELDS: ClassVar[str] = ""
    #: Fixed list query parameters (``include``, ``isBot``, ...).
    LIST_EXTRA: ClassVar[dict[str, str]] = {}
    #: Column whose ``=`` / ``IN`` values are entity ids (UUIDs) to fetch directly.
    ID_COLUMN: ClassVar[str | None] = "id"
    #: Column whose ``=`` / ``IN`` values are FQNs to fetch directly.
    FQN_COLUMN: ClassVar[str | None] = "fqn"
    #: Columns pushed into list query parameters: column -> parameter.
    LIST_PARAMS: ClassVar[dict[str, str]] = {}
    #: Search index used when a search-mapped column is filtered (None: no search mode).
    SEARCH_INDEX: ClassVar[str | None] = None
    #: Scalar columns pushed into the search ``query_filter``: column -> index field.
    SEARCH_TERMS: ClassVar[dict[str, str]] = {}
    #: List columns whose ``list_contains`` filters go into the ``query_filter``.
    SEARCH_CONTAINS: ClassVar[dict[str, str]] = {}

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Rows for one entity."""
        raise NotImplementedError

    @classmethod
    def extra_plan(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Scan-specific plan entries resolved from pushdown (time ranges, ...)."""
        return {}

    @classmethod
    def list_sources(cls, params: ProcessParams[Any]) -> list[dict[str, Any]] | None:
        """Override to list several collections (``[{path, fields, params, entity_type}]``)."""
        return None

    # ----------------------------------------------------------------- plan

    @classmethod
    def plan(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Resolve pushdown into ``{mode, ...}``."""
        filters = params.current_pushdown_filters
        plan: dict[str, Any] = {"extra": cls.extra_plan(params)}
        if plan["extra"].get("empty"):
            return {**plan, "mode": "empty"}
        if cls.ID_COLUMN:
            ids = eq_values(filters, cls.ID_COLUMN)
            if ids is not None:
                return {**plan, "mode": "keys", "by": "id", "keys": [str(i) for i in ids]}
        if cls.FQN_COLUMN:
            fqns = eq_values(filters, cls.FQN_COLUMN)
            if fqns is not None:
                return {**plan, "mode": "keys", "by": "name", "keys": [str(f) for f in fqns]}
        if cls.SEARCH_INDEX:
            clauses: list[dict[str, Any]] = []
            for col, field in cls.SEARCH_TERMS.items():
                values = eq_values(filters, col)
                if values is None:
                    continue
                if not values:
                    return {**plan, "mode": "empty"}
                clauses.append(term_clause(field, [str(v) for v in values]))
            for col, field in cls.SEARCH_CONTAINS.items():
                clauses.extend(term_clause(field, [v]) for v in contains_values(filters, col) or [])
            if clauses:
                return {**plan, "mode": "search", "index": cls.SEARCH_INDEX, "filter": query_filter(clauses)}
        sources = cls.list_sources(params)
        if sources is None:
            per_param: list[list[tuple[str, str]]] = []
            for col, param in cls.LIST_PARAMS.items():
                values = eq_values(filters, col)
                if values is None:
                    continue
                if not values:
                    return {**plan, "mode": "empty"}
                per_param.append([(param, param_value(v)) for v in values])
            sources = [
                {"path": cls.PATH, "fields": cls.FIELDS, "entity_type": cls.ENTITY_TYPE, "params": dict(combo)}
                for combo in itertools.product(*per_param)
            ]
        if not sources:
            return {**plan, "mode": "empty"}
        return {**plan, "mode": "list", "sources": sources}

    # ----------------------------------------------------------------- fetching

    @classmethod
    def fetch_page(cls, ep: Endpoint, plan: dict[str, Any], state: ScanCursor) -> list[dict[str, Any]]:
        """Fetch the next page of entities and advance ``state``."""
        mode = plan["mode"]
        if mode == "keys":
            keys = plan["keys"][state.offset : state.offset + ep.page_size]
            state.offset += ep.page_size
            state.done = state.offset >= len(plan["keys"])
            out = []
            for key in keys:
                path = f"{cls.PATH}/{quote_fqn(key)}" if plan["by"] == "id" else f"{cls.PATH}/name/{quote_fqn(key)}"
                try:
                    entity = get_json(ep, path, {"fields": cls.FIELDS or None, "include": "all"})
                except NotFoundError:
                    continue
                if isinstance(entity, dict):
                    out.append({**entity, "_entity_type": cls.ENTITY_TYPE})
            return out
        if mode == "search":
            hits, nxt = search_page(ep, plan["index"], "*", plan["filter"], state.after, ep.page_size)
            state.after = nxt
            state.done = not nxt
            return [
                {**h["_source"], "_entity_type": cls.ENTITY_TYPE} for h in hits if isinstance(h.get("_source"), dict)
            ]
        # list
        source = plan["sources"][state.offset]
        query: dict[str, Any] = {
            **cls.LIST_EXTRA,
            **source.get("extra", {}),
            **source["params"],
            "fields": source["fields"] or None,
            "limit": ep.page_size,
        }
        if state.after:
            query["after"] = state.after
        body = get_json(ep, source["path"], query) or {}
        data = [d for d in body.get("data") or [] if isinstance(d, dict)]
        next_after = (body.get("paging") or {}).get("after")
        if next_after and data:
            state.after = str(next_after)
        else:
            state.after = ""
            state.offset += 1
            state.done = state.offset >= len(plan["sources"])
        return [{**d, "_entity_type": source["entity_type"]} for d in data]

    # ----------------------------------------------------------------- table-function protocol

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> ScanCursor:
        """Resolve the plan from the pushed-down filters."""
        plan = cls.plan(params)
        return ScanCursor(plan_json=json.dumps(plan, separators=(",", ":")), done=plan["mode"] == "empty")

    @classmethod
    def process(cls, params: ProcessParams[Any], state: ScanCursor, out: OutputCollector) -> None:
        """Fetch pages until one yields rows (or the scan ends) and emit them."""
        plan = json.loads(state.plan_json)
        ep = endpoint(params.secrets)
        for _ in range(MAX_EMPTY_PAGES):
            if state.done:
                out.finish()
                return
            rows: list[dict[str, Any]] = []
            for entity in cls.fetch_page(ep, plan, state):
                rows.extend(cls.build_rows(ep, entity, plan))
            if rows:
                out.emit(rows_to_batch(rows, params.output_schema))
                return
        out.emit(rows_to_batch([], params.output_schema))
