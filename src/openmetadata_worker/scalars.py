"""Offline FQN scalars: take apart OpenMetadata fully-qualified names without calling the server.

They apply the same rules as the tables, so ``fqn_qualified_name(fqn)`` equals
``datasets.qualified_name`` and joins to the other catalog / lineage / BI workers.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .common import qualified_name, split_fqn
from .meta import object_tags

FQN_DOC = "An OpenMetadata fully-qualified name (datasets.fqn, lineage_edges.upstream_fqn, ...); NULL gives NULL."
QUOTED = 'snowflake_prod."my.db".Marts.FCT_ORDERS'


class FqnQualifiedName(ScalarFunction):
    """``fqn_qualified_name(fqn)``."""

    class Meta:
        """Function metadata."""

        name = "fqn_qualified_name"
        description = "The lower-cased database.schema.table join key of an OpenMetadata table FQN"
        tags = {
            **object_tags(
                title="FQN Qualified Name",
                category="assets",
                keywords="fqn, fully qualified name, qualified name, join key, normalize, table name",
                doc_llm=(
                    "Return the cross-worker join key of an OpenMetadata table (or column / test case) FQN "
                    "'service.database.schema.table[...]': the lower-cased 'database.schema.table', dropping the "
                    "service. Double-quoted parts (names containing dots) are unquoted. Same rule as "
                    "datasets.qualified_name, so use it to join FQNs from lineage_edges, assertions or your own data "
                    "to dbt, OpenLineage, DataHub and Looker tables. Runs locally: no OpenMetadata call, no secret "
                    "needed. NULL, or an FQN with fewer than four parts, gives NULL."
                ),
                doc_md=(
                    "## fqn_qualified_name(fqn)\n\n"
                    "The lower-cased `database.schema.table` of a table FQN.\n\n"
                    "Offline; NULL for anything shorter than service.database.schema.table."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "The join key of a table FQN, of a column FQN and of a quoted FQN",
                        "sql": (
                            "SELECT openmetadata.main.fqn_qualified_name("
                            "'snowflake_prod.ANALYTICS.Marts.FCT_ORDERS') AS table_key, "
                            "openmetadata.main.fqn_qualified_name("
                            "'snowflake_prod.analytics.marts.fct_orders.order_id') AS column_key, "
                            f"openmetadata.main.fqn_qualified_name('{QUOTED}') AS quoted_key"
                        ),
                        "expected_result": [
                            ["analytics.marts.fct_orders", "analytics.marts.fct_orders", "my.db.marts.fct_orders"]
                        ],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT openmetadata.main.fqn_qualified_name(upstream_fqn) AS source, count(*) AS consumers "
                    "FROM openmetadata.main.lineage_edges WHERE level = 'dataset' AND upstream_type = 'table' "
                    "GROUP BY ALL ORDER BY consumers DESC"
                ),
                description="Most-consumed upstream tables by qualified name",
            ),
        ]

    @classmethod
    def compute(cls, fqn: Annotated[pa.StringArray, Param(doc=FQN_DOC)]) -> Annotated[pa.StringArray, Returns()]:
        """Qualified name of every FQN."""
        return pa.array([None if f is None else qualified_name(f) for f in fqn.to_pylist()], type=pa.string())


class SplitFqn(ScalarFunction):
    """``split_fqn(fqn)``."""

    class Meta:
        """Function metadata."""

        name = "split_fqn"
        description = "Split an OpenMetadata FQN into its parts, honouring double-quoted names"
        tags = {
            **object_tags(
                title="Split FQN",
                category="assets",
                keywords="fqn, split, parse, parts, service, database, schema, table, column",
                doc_llm=(
                    "Split an OpenMetadata fully-qualified name on dots into a `VARCHAR[]` list, keeping "
                    "double-quoted parts (names that contain dots) whole and unquoted: "
                    "'svc.\"my.db\".sch.t' -> [svc, my.db, sch, t]. For a table FQN the parts are service, database, "
                    "schema, table; a column FQN adds the column. Index with [1], [2], ... Offline; NULL gives NULL."
                ),
                doc_md=(
                    "## split_fqn(fqn)\n\n"
                    "The dot-separated parts of an FQN as a list.\n\n"
                    "Quoted parts stay whole; offline."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Service and table of a table FQN, and the parts of a quoted FQN",
                        "sql": (
                            "SELECT p[1] AS service, p[4] AS table_name, len(q) AS parts, q[2] AS quoted_part FROM "
                            "(SELECT openmetadata.main.split_fqn('snowflake_prod.analytics.marts.fct_orders') AS p, "
                            f"openmetadata.main.split_fqn('{QUOTED}') AS q)"
                        ),
                        "expected_result": [["snowflake_prod", "fct_orders", 4, "my.db"]],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT openmetadata.main.split_fqn(fqn)[1] AS service, count(*) AS tests "
                    "FROM openmetadata.main.assertions GROUP BY ALL ORDER BY tests DESC"
                ),
                description="Test cases per database service",
            ),
        ]

    @classmethod
    def compute(
        cls, fqn: Annotated[pa.StringArray, Param(doc=FQN_DOC)]
    ) -> Annotated[pa.Array[Any], Returns(arrow_type=pa.list_(pa.string()))]:
        """Parts of every FQN."""
        return pa.array([None if f is None else split_fqn(f) for f in fqn.to_pylist()], type=pa.list_(pa.string()))


SCALARS = [FqnQualifiedName, SplitFqn]
