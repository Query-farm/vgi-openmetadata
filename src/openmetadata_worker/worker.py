"""Assemble the ``openmetadata`` catalog and run the worker.

Run it:

    vgi-openmetadata-worker                      # stdio: what DuckDB launches by default
    vgi-openmetadata-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'openmetadata' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-openmetadata-worker');
    CREATE SECRET (TYPE openmetadata, HOST_PORT 'http://localhost:8585/api', TOKEN '<bot JWT>');
    SELECT qualified_name FROM openmetadata.main.datasets WHERE service = 'snowflake_prod';
"""

from __future__ import annotations

import json
from typing import Any

from vgi import Worker
from vgi.catalog import Catalog, Schema, Table

from .assets import ASSET_TABLES
from .client import OPENMETADATA_SECRET_TYPE
from .common import catalog_table
from .functions import FUNCTIONS as SEARCH_FUNCTIONS
from .governance import GOVERNANCE_TABLES
from .lineage import FUNCTIONS as LINEAGE_FUNCTIONS
from .lineage import LINEAGE_TABLES
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .quality import QUALITY_TABLES
from .scalars import SCALARS

ALL_TABLES = [*ASSET_TABLES, *GOVERNANCE_TABLES, *LINEAGE_TABLES, *QUALITY_TABLES]
DOMAIN_TAG = {
    "assets": "data catalog",
    "governance": "data governance",
    "lineage": "data lineage",
    "quality": "data quality",
}


def _examples(objects: list[type[Any]], *, first_only: bool = False) -> list[dict[str, str]]:
    out = []
    for fn in objects:
        examples = list(getattr(fn.Meta, "examples", []))
        for ex in examples[:1] if first_only else examples:
            out.append({"description": ex.description, "sql": ex.sql})
    return out


def _stamp_example_queries(objects: list[type[Any]]) -> None:
    """Publish each object's examples as ``vgi.example_queries``."""
    for fn in objects:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(_examples([fn]))}


def _tables() -> list[Table]:
    tables = []
    for name, scan, pk in ALL_TABLES:
        category = scan.Meta.tags["vgi.category"]
        tags = {**scan.Meta.tags, "domain": DOMAIN_TAG.get(category, "data catalog")}
        tables.append(
            catalog_table(name, scan, comment=scan.Meta.description, tags=tags, schema_path=["main"], primary_key=pk)
        )
    return tables


_SCANS = [scan for _, scan, _ in ALL_TABLES]
OBJECTS: list[type[Any]] = [*LINEAGE_FUNCTIONS, *SEARCH_FUNCTIONS, *SCALARS]
_stamp_example_queries(_SCANS + OBJECTS)


class OpenmetadataWorker(Worker):
    """The worker process: the ``openmetadata`` catalog with one ``main`` schema."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="OpenMetadata assets, governance metadata, lineage, data quality, profiles and usage.",
                functions=OBJECTS,
                tables=_tables(),
                tags={
                    "vgi.title": "OpenMetadata Metadata",
                    "domain": "data catalog",
                    "vgi.keywords": keywords_array(
                        "openmetadata, tables, columns, owners, glossary, domains, lineage, test cases, usage, search"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    # Each object's first example only: agent tools cap a tag at 8,000 characters.
                    "vgi.example_queries": json.dumps(_examples(_SCANS + OBJECTS, first_only=True)),
                },
            )
        ],
    )

    secret_types = [OPENMETADATA_SECRET_TYPE]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    OpenmetadataWorker.main()


if __name__ == "__main__":
    main()
