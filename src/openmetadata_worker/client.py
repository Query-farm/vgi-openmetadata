"""OpenMetadata REST client: endpoint from an ``openmetadata`` DuckDB secret, retries, errors.

Every table and function reads one ``openmetadata`` secret::

    -- self-hosted OpenMetadata (the API root, including /api)
    CREATE SECRET om (TYPE openmetadata, HOST_PORT 'http://localhost:8585/api', TOKEN '<bot JWT>');
    -- Collate (managed OpenMetadata)
    CREATE SECRET om (TYPE openmetadata, HOST_PORT 'https://acme.getcollate.io/api', TOKEN '<bot JWT>');

``HOST_PORT`` is the same value the OpenMetadata ingestion framework and Python SDK take
(``hostPort``). A missing ``/api`` suffix is added. ``UI_URL`` (default: ``HOST_PORT``
without ``/api``) is used to build each row's ``url``. ``PAGE_SIZE`` (default 100) is the
``limit`` / ``size`` of every list and search request. The token is sent as
``Authorization: Bearer``, read on every call, and never kept in table-function state
(which can travel back to DuckDB over HTTP).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi.arguments import SecretLookupEntry
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

SECRET_TYPE = "openmetadata"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 6
MAX_BACKOFF_SECONDS = 60.0
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
USER_AGENT = "vgi-openmetadata-worker/0.1"
DOC_URL = "https://github.com/Query-farm/vgi-openmetadata#connect-to-openmetadata"


def _doc_field(name: str, doc: str, *, redact: bool = False) -> pa.Field[Any]:
    metadata = {"vgi_doc": doc}
    if redact:
        metadata["redact"] = "true"
    return pa.field(name, pa.string(), metadata=metadata)


OPENMETADATA_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description=(
        "Connection to an OpenMetadata server (open-source or Collate): HOST_PORT (the API root, "
        "e.g. 'http://localhost:8585/api') and a bot or personal-access JWT TOKEN."
    ),
    schema=pa.schema(
        [
            _doc_field(
                "host_port",
                "OpenMetadata API root, e.g. 'http://localhost:8585/api' or 'https://<tenant>.getcollate.io/api'. "
                "'/api' is appended when missing; requests go to <host_port>/v1/...",
            ),
            _doc_field(
                "token",
                "JWT of an OpenMetadata bot (Settings > Bots) or a personal access token, sent as "
                "'Authorization: Bearer <token>'. Leave unset only for a server with authentication disabled.",
                redact=True,
            ),
            _doc_field("ui_url", "OpenMetadata UI root for row links (url column); default: HOST_PORT without /api."),
            _doc_field("page_size", "Entities requested per list / search page; 1 to 1000, default 100."),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)

# Every scan asks DuckDB for the openmetadata secret at bind time.
SECRET_REQUIREMENT = [SecretLookupEntry(secret_type=SECRET_TYPE)]


class OpenMetadataError(RuntimeError):
    """OpenMetadata returned an error, or the worker is not configured to reach it."""


class NotFoundError(OpenMetadataError):
    """OpenMetadata answered 404 for an entity looked up by id or name."""


@dataclass(frozen=True, slots=True)
class Endpoint:
    """API root, UI root, page size and bearer token (never logged)."""

    api_url: str
    ui_url: str
    token: str | None
    page_size: int

    def entity_url(self, entity_type: str | None, fqn: str | None) -> str | None:
        """Link to an entity's page in the OpenMetadata UI."""
        if not fqn or not entity_type:
            return None
        path = UI_PATHS.get(entity_type)
        if path is None:
            return None
        return f"{self.ui_url}/{path}/{urllib.parse.quote(fqn, safe='')}"


# OpenMetadata entity type -> the UI route segment.
UI_PATHS = {
    "table": "table",
    "databaseService": "service/databaseServices",
    "database": "database",
    "databaseSchema": "databaseSchema",
    "dashboard": "dashboard",
    "chart": "chart",
    "pipeline": "pipeline",
    "topic": "topic",
    "mlmodel": "mlmodel",
    "user": "users",
    "team": "settings/members/teams",
    "classification": "tags",
    "tag": "tags",
    "glossary": "glossary",
    "glossaryTerm": "glossary",
    "domain": "domain",
    "dataProduct": "dataProduct",
    "testSuite": "test-suites",
    "testCase": "test-case",
    "query": "query",
    "container": "container",
    "storedProcedure": "storedProcedure",
    "searchIndex": "searchIndex",
    "dashboardDataModel": "dashboardDataModel",
}


def _field(secret: dict[str, Any], name: str) -> str | None:
    value = secret.get(name)
    if value is None:
        return None
    py = value.as_py() if hasattr(value, "as_py") else value
    text = str(py).strip() if py is not None else ""
    return text or None


def endpoint_from_secret(secret: dict[str, Any] | None) -> Endpoint:
    """Resolve the API endpoint from an ``openmetadata`` secret's fields.

    Args:
        secret: The resolved secret's fields (Arrow scalars or plain values), or None.

    Returns:
        The endpoint.

    Raises:
        OpenMetadataError: No secret, no HOST_PORT, a malformed URL, or a bad PAGE_SIZE.

    """
    if not secret:
        raise OpenMetadataError(
            "no openmetadata secret found. Create one first: "
            "CREATE SECRET (TYPE openmetadata, HOST_PORT 'http://localhost:8585/api', TOKEN '<bot JWT>')"
        )
    host = _field(secret, "host_port")
    if not host:
        raise OpenMetadataError("the openmetadata secret needs HOST_PORT, e.g. 'http://localhost:8585/api'")
    if not host.startswith(("http://", "https://")):
        raise OpenMetadataError(f"openmetadata secret host_port must start with http:// or https://, got {host!r}")
    root = host.rstrip("/").removesuffix("/v1")
    api = root if root.endswith("/api") else f"{root}/api"
    ui = _field(secret, "ui_url")
    if ui and not ui.startswith(("http://", "https://")):
        raise OpenMetadataError(f"openmetadata secret ui_url must start with http:// or https://, got {ui!r}")
    ui_root = ui.rstrip("/") if ui else api.removesuffix("/api")
    raw_size = _field(secret, "page_size")
    try:
        page_size = int(raw_size) if raw_size else DEFAULT_PAGE_SIZE
    except ValueError as err:
        raise OpenMetadataError(f"openmetadata secret page_size must be an integer, got {raw_size!r}") from err
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise OpenMetadataError(f"openmetadata secret page_size must be between 1 and {MAX_PAGE_SIZE}, got {page_size}")
    return Endpoint(api_url=api, ui_url=ui_root, token=_field(secret, "token"), page_size=page_size)


def endpoint(secrets: Any) -> Endpoint:
    """Resolve the endpoint from a call's resolved secrets (``params.secrets``).

    Args:
        secrets: The call's ``ResolvedSecrets``.

    Returns:
        The endpoint.

    Raises:
        OpenMetadataError: Several different openmetadata secrets apply, or none is usable.

    """
    found = list(secrets.of_type(SECRET_TYPE)) if secrets is not None else []
    keys = ("host_port", "token", "ui_url", "page_size")
    distinct = {tuple(_field(s, f) for f in keys): s for s in found}
    found = list(distinct.values())
    if len(found) > 1:
        raise OpenMetadataError(
            "more than one openmetadata secret is defined; keep one per session (DROP SECRET the others)"
        )
    return endpoint_from_secret(found[0] if found else None)


def retry_delay(retry_after: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt``: ``Retry-After`` if given, else exponential backoff."""
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), MAX_BACKOFF_SECONDS)
        except ValueError:
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


def quote_fqn(fqn: str) -> str:
    """Percent-encode an FQN for use as one path segment (OpenMetadata's ``/name/{fqn}`` routes)."""
    return urllib.parse.quote(fqn, safe="")


def get_json(ep: Endpoint, path: str, params: dict[str, Any] | None = None) -> Any:
    """GET ``<api_url><path>`` and decode the JSON body.

    429 and 5xx responses are retried with exponential backoff honouring ``Retry-After``.

    Args:
        ep: The endpoint.
        path: Path below the API root, starting with ``/v1/``.
        params: Query parameters; None values are dropped.

    Returns:
        The decoded JSON body.

    Raises:
        NotFoundError: HTTP 404.
        OpenMetadataError: Any other HTTP failure after retries, or an unreachable server.

    """
    query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None}, doseq=True)
    url = f"{ep.api_url}{path}" + (f"?{query}" if query else "")
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    if ep.token:
        headers["Authorization"] = f"Bearer {ep.token}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                return json.load(response)
        except urllib.error.HTTPError as err:
            if err.code == 404:
                raise NotFoundError(f"OpenMetadata GET {path} returned HTTP 404") from err
            if (err.code != 429 and err.code < 500) or attempt == MAX_ATTEMPTS:
                detail = err.read(300).decode(errors="replace").strip()
                hint = " (check the openmetadata secret's TOKEN)" if err.code in (401, 403) else ""
                raise OpenMetadataError(f"OpenMetadata GET {path} returned HTTP {err.code}{hint}: {detail}") from err
            delay = retry_delay(err.headers.get("Retry-After") if err.headers else None, attempt)
            log.info("openmetadata: HTTP %s on %s, retrying in %.1fs", err.code, path, delay)
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == MAX_ATTEMPTS:
                raise OpenMetadataError(
                    f"cannot reach OpenMetadata at {ep.api_url}: {getattr(err, 'reason', err)}"
                ) from err
            time.sleep(retry_delay(None, attempt))
    raise OpenMetadataError(f"OpenMetadata GET {path} failed")  # pragma: no cover - loop returns or raises
