"""Governance tables: owners, users, teams, classifications, tags, glossaries, terms, domains, data products."""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .assets import audit_fields, audit_values, fqn_column, id_column, scan_tags
from .client import SECRET_REQUIREMENT, Endpoint
from .common import (
    CATALOG_TOOL,
    STR_LIST,
    NoArgs,
    catalog_tool_column,
    domain_ref,
    eq_values,
    get,
    owner_names,
    owners_of,
    qualified_name,
    ref_names,
    tag_fqns,
    to_int,
)
from .meta import column
from .scan import EntityScan

# ----------------------------------------------------------------------------- owners

# entity_type -> (collection path, the entity type's name in this worker's tables)
OWNED_COLLECTIONS = {
    "table": "/v1/tables",
    "database": "/v1/databases",
    "databaseSchema": "/v1/databaseSchemas",
    "databaseService": "/v1/services/databaseServices",
    "dashboard": "/v1/dashboards",
    "chart": "/v1/charts",
    "pipeline": "/v1/pipelines",
    "topic": "/v1/topics",
    "mlmodel": "/v1/mlmodels",
}

OWNERS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("entity_id", pa.string(), "Id of the owned entity (datasets.id, dashboards.id, ...).", nullable=False),
        column(
            "entity_type",
            pa.string(),
            "Owned entity type: table, database, databaseSchema, databaseService, dashboard, chart, pipeline, "
            "topic, mlmodel.",
            nullable=False,
        ),
        column("entity_fqn", pa.string(), "FQN of the owned entity."),
        column("entity_name", pa.string(), "Name of the owned entity."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table when the entity is a table."),
        column("owner_id", pa.string(), "Id of the owner (users.id or teams.id).", nullable=False),
        column("owner_type", pa.string(), "'user' or 'team'.", nullable=False),
        column("owner_name", pa.string(), "Owner's name (user name or team name)."),
        column("owner_display_name", pa.string(), "Owner's display name."),
    ]
)


@init_single_worker
@bind_fixed_schema
class OwnersScan(EntityScan[NoArgs]):
    """Rows of ``owners``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OWNERS_SCHEMA
    PATH: ClassVar[str] = "/v1/tables"
    ENTITY_TYPE: ClassVar[str] = "table"
    FIELDS: ClassVar[str] = "owners"
    ID_COLUMN: ClassVar[str | None] = None
    FQN_COLUMN: ClassVar[str | None] = None

    class Meta:
        """Scan metadata."""

        name = "owners_scan"
        description = "One row per (entity, owner) across OpenMetadata data assets"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Owners",
            category="governance",
            keywords="owners, ownership, stewards, accountability, users, teams, data governance",
            doc_llm=(
                "One row per owner of every OpenMetadata data asset (tables, databases, schemas, database "
                "services, dashboards, charts, pipelines, topics, ML models): entity_id, entity_type, entity_fqn, "
                "entity_name, qualified_name (tables), owner_id, owner_type ('user' | 'team'), owner_name, "
                "owner_display_name. Assets without owners have no rows; find them with "
                "len(datasets.owners) = 0. Pushdown: entity_type = / IN limits which collections are listed."
            ),
            doc_md="Who owns each OpenMetadata data asset.",
            schema=OWNERS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT owner_name, count(*) AS tables FROM openmetadata.main.owners "
                    "WHERE entity_type = 'table' GROUP BY owner_name ORDER BY tables DESC"
                ),
                description="How many tables each user or team owns",
            ),
        ]

    @classmethod
    def list_sources(cls, params: ProcessParams[Any]) -> list[dict[str, Any]] | None:
        """One listing per owned collection, narrowed by ``entity_type`` pushdown."""
        wanted = eq_values(params.current_pushdown_filters, "entity_type")
        types = [t for t in OWNED_COLLECTIONS if wanted is None or t in {str(w) for w in wanted}]
        return [{"path": OWNED_COLLECTIONS[t], "fields": "owners", "entity_type": t, "params": {}} for t in types]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per owner."""
        etype = entity.get("_entity_type")
        fqn = entity.get("fullyQualifiedName")
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "entity_id": entity.get("id"),
                "entity_type": etype,
                "entity_fqn": fqn,
                "entity_name": entity.get("name"),
                "qualified_name": qualified_name(fqn) if etype == "table" else None,
                "owner_id": o.get("id"),
                "owner_type": o.get("type") or "user",
                "owner_name": o.get("name"),
                "owner_display_name": o.get("displayName"),
            }
            for o in owners_of(entity)
            if o.get("id")
        ]


# ----------------------------------------------------------------------------- users / teams

USERS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("user"),
        column("username", pa.string(), "Login name (the user's OpenMetadata name).", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("email", pa.string(), "Email address."),
        column("is_admin", pa.bool_(), "True for OpenMetadata administrators."),
        column("is_bot", pa.bool_(), "True for bot users (ingestion and automation identities)."),
        column("teams", STR_LIST, "Names of the teams the user belongs to."),
        column("roles", STR_LIST, "Names of the roles assigned directly to the user."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class UsersScan(EntityScan[NoArgs]):
    """Rows of ``users``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = USERS_SCHEMA
    PATH: ClassVar[str] = "/v1/users"
    ENTITY_TYPE: ClassVar[str] = "user"
    FIELDS: ClassVar[str] = "teams,roles"
    FQN_COLUMN: ClassVar[str | None] = "username"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"is_bot": "isBot", "is_admin": "isAdmin"}

    class Meta:
        """Scan metadata."""

        name = "users_scan"
        description = "OpenMetadata users and bots with their teams and roles"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Users",
            category="governance",
            keywords="users, people, bots, accounts, teams, roles, admins",
            doc_llm=(
                "One row per OpenMetadata user: id, username, display_name, email, is_admin, is_bot, teams, roles, "
                "deleted, updated_at, updated_by, version, url. Pushdown: id / username = or IN fetch directly; "
                "is_bot and is_admin = become the isBot / isAdmin list parameters."
            ),
            doc_md="Users (and bots) registered in OpenMetadata.",
            schema=USERS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql="SELECT username, teams FROM openmetadata.main.users WHERE NOT is_bot ORDER BY username",
                description="Human users and their teams",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per user."""
        name = entity.get("name")
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "username": name,
                "display_name": entity.get("displayName"),
                "email": entity.get("email"),
                "is_admin": entity.get("isAdmin"),
                "is_bot": entity.get("isBot"),
                "teams": ref_names(entity.get("teams")),
                "roles": ref_names(entity.get("roles")),
                **audit_values(ep, {**entity, "fullyQualifiedName": name}, "user"),
            }
        ]


TEAMS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("team"),
        column("name", pa.string(), "Team name.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("description", pa.string(), "Team description."),
        column("team_type", pa.string(), "Organization, BusinessUnit, Division, Department or Group."),
        column("email", pa.string(), "Team email address."),
        column("parents", STR_LIST, "Names of the parent teams."),
        column("owners", STR_LIST, "Owner names of the team."),
        column("user_count", pa.int64(), "Number of users in the team."),
        column("is_joinable", pa.bool_(), "True if users can join the team themselves."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class TeamsScan(EntityScan[NoArgs]):
    """Rows of ``teams``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TEAMS_SCHEMA
    PATH: ClassVar[str] = "/v1/teams"
    ENTITY_TYPE: ClassVar[str] = "team"
    FIELDS: ClassVar[str] = "owners,parents,userCount"
    FQN_COLUMN: ClassVar[str | None] = "name"

    class Meta:
        """Scan metadata."""

        name = "teams_scan"
        description = "OpenMetadata teams with type, parents, owners and member count"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Teams",
            category="governance",
            keywords="teams, groups, departments, business units, organization, hierarchy",
            doc_llm=(
                "One row per OpenMetadata team: id, name, display_name, description, team_type (Organization, "
                "BusinessUnit, Division, Department, Group), email, parents, owners, user_count, is_joinable, "
                "deleted, updated_at, updated_by, version, url. Pushdown: id / name = or IN fetch directly."
            ),
            doc_md="Teams (the OpenMetadata organization hierarchy).",
            schema=TEAMS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql="SELECT name, team_type, user_count FROM openmetadata.main.teams ORDER BY user_count DESC",
                description="Teams by size",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per team."""
        name = entity.get("name")
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "name": name,
                "display_name": entity.get("displayName"),
                "description": entity.get("description"),
                "team_type": entity.get("teamType"),
                "email": entity.get("email"),
                "parents": ref_names(entity.get("parents")),
                "owners": owner_names(entity),
                "user_count": to_int(entity.get("userCount")),
                "is_joinable": entity.get("isJoinable"),
                **audit_values(ep, {**entity, "fullyQualifiedName": name}, "team"),
            }
        ]


# ----------------------------------------------------------------------------- classifications / tags

CLASSIFICATIONS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("classification"),
        column("name", pa.string(), "Classification name, e.g. 'PII', 'Tier'.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("description", pa.string(), "Classification description."),
        column("mutually_exclusive", pa.bool_(), "True if an entity can carry only one tag of the classification."),
        column("term_count", pa.int64(), "Number of tags in the classification."),
        column("provider", pa.string(), "'system' (built in) or 'user'."),
        column("disabled", pa.bool_(), "True if the classification is disabled."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class ClassificationsScan(EntityScan[NoArgs]):
    """Rows of ``classifications``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CLASSIFICATIONS_SCHEMA
    PATH: ClassVar[str] = "/v1/classifications"
    ENTITY_TYPE: ClassVar[str] = "classification"
    FIELDS: ClassVar[str] = "termCount"
    FQN_COLUMN: ClassVar[str | None] = "name"

    class Meta:
        """Scan metadata."""

        name = "classifications_scan"
        description = "OpenMetadata classifications (tag categories such as PII and Tier)"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Classifications",
            category="governance",
            keywords="classifications, tag categories, pii, tier, sensitivity, taxonomy",
            doc_llm=(
                "One row per OpenMetadata classification (tag category): id, name, display_name, description, "
                "mutually_exclusive, term_count, provider ('system' | 'user'), disabled, deleted, updated_at, "
                "updated_by, version, url. Tags inside a classification are in the tags table. Pushdown: id / "
                "name = or IN fetch directly."
            ),
            doc_md="Classifications: the tag categories (PII, Tier, ...).",
            schema=CLASSIFICATIONS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql="SELECT name, term_count FROM openmetadata.main.classifications ORDER BY name",
                description="Classifications and how many tags each has",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per classification."""
        name = entity.get("name")
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "name": name,
                "display_name": entity.get("displayName"),
                "description": entity.get("description"),
                "mutually_exclusive": entity.get("mutuallyExclusive"),
                "term_count": to_int(entity.get("termCount")),
                "provider": entity.get("provider"),
                "disabled": entity.get("disabled"),
                **audit_values(ep, {**entity, "fullyQualifiedName": name}, "classification"),
            }
        ]


TAGS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("tag"),
        fqn_column("tag (classification.tag)", "PII.Sensitive"),
        column("name", pa.string(), "Tag name within its classification.", nullable=False),
        column("classification", pa.string(), "Classification name (classifications.name)."),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("description", pa.string(), "Tag description."),
        column("color", pa.string(), "Display color (hex), when set."),
        column("usage_count", pa.int64(), "Number of entities and columns carrying the tag."),
        column("provider", pa.string(), "'system' (built in) or 'user'."),
        column("disabled", pa.bool_(), "True if the tag is disabled."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class TagsScan(EntityScan[NoArgs]):
    """Rows of ``tags``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TAGS_SCHEMA
    PATH: ClassVar[str] = "/v1/tags"
    ENTITY_TYPE: ClassVar[str] = "tag"
    FIELDS: ClassVar[str] = "usageCount"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"classification": "parent"}

    class Meta:
        """Scan metadata."""

        name = "tags_scan"
        description = "OpenMetadata classification tags with usage counts"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Tags",
            category="governance",
            keywords="tags, labels, classification tags, pii, tier, usage count",
            doc_llm=(
                "One row per OpenMetadata classification tag: id, fqn ('PII.Sensitive'), name, classification, "
                "display_name, description, color, usage_count, provider, disabled, deleted, updated_at, "
                "updated_by, version, url. The fqn is what datasets.tags / columns.tags contain. Pushdown: id / "
                "fqn = or IN fetch directly; classification = / IN becomes the list endpoint's parent parameter."
            ),
            doc_md="Tags inside OpenMetadata classifications.",
            schema=TAGS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT fqn, usage_count FROM openmetadata.main.tags WHERE classification = 'PII' "
                    "ORDER BY usage_count DESC"
                ),
                description="PII tags by how often they are applied",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per tag."""
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName"),
                "name": entity.get("name"),
                "classification": get(entity, "classification", "name"),
                "display_name": entity.get("displayName"),
                "description": entity.get("description"),
                "color": get(entity, "style", "color"),
                "usage_count": to_int(entity.get("usageCount")),
                "provider": entity.get("provider"),
                "disabled": entity.get("disabled"),
                **audit_values(ep, entity, "tag"),
            }
        ]


# ----------------------------------------------------------------------------- glossaries / terms

GLOSSARIES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("glossary"),
        column("name", pa.string(), "Glossary name.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("description", pa.string(), "Glossary description."),
        column("term_count", pa.int64(), "Number of terms in the glossary."),
        column("mutually_exclusive", pa.bool_(), "True if an entity can carry only one term of the glossary."),
        column("owners", STR_LIST, "Owner names."),
        column("reviewers", STR_LIST, "Reviewer names."),
        column("tags", STR_LIST, "Classification tag FQNs on the glossary."),
        column("domain", pa.string(), "Domain fully-qualified name."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class GlossariesScan(EntityScan[NoArgs]):
    """Rows of ``glossaries``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GLOSSARIES_SCHEMA
    PATH: ClassVar[str] = "/v1/glossaries"
    ENTITY_TYPE: ClassVar[str] = "glossary"
    FIELDS: ClassVar[str] = "owners,tags,reviewers,termCount,domain"
    FQN_COLUMN: ClassVar[str | None] = "name"

    class Meta:
        """Scan metadata."""

        name = "glossaries_scan"
        description = "OpenMetadata business glossaries with owners, reviewers and term counts"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Glossaries",
            category="governance",
            keywords="glossaries, business glossary, vocabulary, terms, reviewers, data governance",
            doc_llm=(
                "One row per OpenMetadata glossary: id, name, display_name, description, term_count, "
                "mutually_exclusive, owners, reviewers, tags, domain, deleted, updated_at, updated_by, version, url. "
                "Terms are in glossary_terms. Pushdown: id / name = or IN fetch directly."
            ),
            doc_md="Business glossaries.",
            schema=GLOSSARIES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql="SELECT name, term_count, reviewers FROM openmetadata.main.glossaries ORDER BY name",
                description="Glossaries with their size and reviewers",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per glossary."""
        name = entity.get("name")
        dom = domain_ref(entity)
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "name": name,
                "display_name": entity.get("displayName"),
                "description": entity.get("description"),
                "term_count": to_int(entity.get("termCount")),
                "mutually_exclusive": entity.get("mutuallyExclusive"),
                "owners": owner_names(entity),
                "reviewers": ref_names(entity.get("reviewers")),
                "tags": tag_fqns(entity),
                "domain": dom.get("fullyQualifiedName") or dom.get("name"),
                **audit_values(ep, {**entity, "fullyQualifiedName": name}, "glossary"),
            }
        ]


TERM_REFERENCE = pa.struct([pa.field("name", pa.string()), pa.field("endpoint", pa.string())])

GLOSSARY_TERMS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("glossary term"),
        fqn_column("glossary term (glossary.parent.term)", "Business.Revenue.Net Revenue"),
        column("name", pa.string(), "Term name.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("glossary", pa.string(), "Glossary name (glossaries.name)."),
        column("glossary_id", pa.string(), "Glossary id (glossaries.id)."),
        column("parent_id", pa.string(), "Id of the parent term, NULL for a top-level term."),
        column("parent_fqn", pa.string(), "FQN of the parent term."),
        column("description", pa.string(), "Term definition (Markdown)."),
        column("synonyms", STR_LIST, "Alternative words that mean the same as the term."),
        column("related_terms", STR_LIST, "FQNs of related terms."),
        column("reference_links", pa.list_(TERM_REFERENCE), "External references: STRUCT(name, endpoint)."),
        column("status", pa.string(), "Approval status: Draft, In Review, Approved, Deprecated, Rejected."),
        column("owners", STR_LIST, "Owner names."),
        column("reviewers", STR_LIST, "Reviewer names."),
        column("tags", STR_LIST, "Classification tag FQNs on the term."),
        column("domain_id", pa.string(), "Id (UUID) of the term's domain (domains.id)."),
        column("domain", pa.string(), "Domain fully-qualified name."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class GlossaryTermsScan(EntityScan[NoArgs]):
    """Rows of ``glossary_terms``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GLOSSARY_TERMS_SCHEMA
    PATH: ClassVar[str] = "/v1/glossaryTerms"
    ENTITY_TYPE: ClassVar[str] = "glossaryTerm"
    FIELDS: ClassVar[str] = "owners,relatedTerms,reviewers,tags,domain"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"glossary_id": "glossary", "parent_id": "parent"}

    class Meta:
        """Scan metadata."""

        name = "glossary_terms_scan"
        description = "OpenMetadata glossary terms with hierarchy, synonyms, related terms and status"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Glossary Terms",
            category="governance",
            keywords="glossary terms, business terms, definitions, synonyms, semantic layer, vocabulary",
            doc_llm=(
                "One row per OpenMetadata glossary term: id, fqn (glossary.parent.term), name, display_name, "
                "glossary, glossary_id, parent_id, parent_fqn, description, synonyms, related_terms, reference_links "
                "(`STRUCT(name, endpoint)[]`), status, owners, reviewers, tags, domain_id, domain, deleted, "
                "updated_at, updated_by, version, url. datasets.glossary_terms holds term FQNs. Pushdown: id / "
                "fqn = or IN fetch directly; glossary_id and parent_id = / IN become list parameters."
            ),
            doc_md="Business glossary terms.",
            schema=GLOSSARY_TERMS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT fqn, status FROM openmetadata.main.glossary_terms WHERE status <> 'Approved' ORDER BY fqn"
                ),
                description="Glossary terms still awaiting approval",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per term."""
        dom = domain_ref(entity)
        references = [
            {"name": r.get("name"), "endpoint": r.get("endpoint")}
            for r in entity.get("references") or []
            if isinstance(r, dict)
        ]
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName"),
                "name": entity.get("name"),
                "display_name": entity.get("displayName"),
                "glossary": get(entity, "glossary", "name"),
                "glossary_id": get(entity, "glossary", "id"),
                "parent_id": get(entity, "parent", "id"),
                "parent_fqn": get(entity, "parent", "fullyQualifiedName"),
                "description": entity.get("description"),
                "synonyms": [str(s) for s in entity.get("synonyms") or []],
                "related_terms": ref_names(entity.get("relatedTerms"), "fullyQualifiedName"),
                "reference_links": references,
                "status": entity.get("status"),
                "owners": owner_names(entity),
                "reviewers": ref_names(entity.get("reviewers")),
                "tags": tag_fqns(entity),
                "domain_id": dom.get("id"),
                "domain": dom.get("fullyQualifiedName") or dom.get("name"),
                **audit_values(ep, entity, "glossaryTerm"),
            }
        ]


# ----------------------------------------------------------------------------- domains / data products

DOMAINS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("domain"),
        fqn_column("domain (parent.child for subdomains)", "Sales"),
        column("name", pa.string(), "Domain name.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("description", pa.string(), "Domain description."),
        column("domain_type", pa.string(), "Aggregate, Consumer-aligned or Source-aligned."),
        column("parent_domain_id", pa.string(), "Id of the parent domain, NULL for a top-level domain."),
        column("parent_domain", pa.string(), "FQN of the parent domain."),
        column("owners", STR_LIST, "Owner names."),
        column("experts", STR_LIST, "Names of the domain's experts."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class DomainsScan(EntityScan[NoArgs]):
    """Rows of ``domains``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DOMAINS_SCHEMA
    PATH: ClassVar[str] = "/v1/domains"
    ENTITY_TYPE: ClassVar[str] = "domain"
    FIELDS: ClassVar[str] = "owners,parent,experts"

    class Meta:
        """Scan metadata."""

        name = "domains_scan"
        description = "OpenMetadata domains and subdomains with type, owners and experts"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Domains",
            category="governance",
            keywords="domains, subdomains, data mesh, business domains, experts",
            doc_llm=(
                "One row per OpenMetadata domain or subdomain: id, fqn, name, display_name, description, "
                "domain_type (Aggregate, Consumer-aligned, Source-aligned), parent_domain_id, parent_domain, "
                "owners, experts, deleted, updated_at, updated_by, version, url. datasets.domain holds the domain "
                "FQN. Pushdown: id / fqn = or IN fetch directly."
            ),
            doc_md="Data-mesh domains.",
            schema=DOMAINS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT d.fqn, count(t.id) AS tables FROM openmetadata.main.domains d "
                    "LEFT JOIN openmetadata.main.datasets t ON t.domain = d.fqn GROUP BY d.fqn ORDER BY tables DESC"
                ),
                description="Tables per domain",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per domain."""
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName") or entity.get("name"),
                "name": entity.get("name"),
                "display_name": entity.get("displayName"),
                "description": entity.get("description"),
                "domain_type": entity.get("domainType"),
                "parent_domain_id": get(entity, "parent", "id"),
                "parent_domain": get(entity, "parent", "fullyQualifiedName"),
                "owners": owner_names(entity),
                "experts": ref_names(entity.get("experts")),
                **audit_values(ep, entity, "domain"),
            }
        ]


DATA_PRODUCTS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        id_column("data product"),
        fqn_column("data product", "Sales.Orders 360"),
        column("name", pa.string(), "Data product name.", nullable=False),
        column("display_name", pa.string(), "Human-readable name shown in the OpenMetadata UI (NULL when unset)."),
        column("description", pa.string(), "Data product description."),
        column("domain_id", pa.string(), "Id of the owning domain."),
        column("domain", pa.string(), "FQN of the owning domain."),
        column("assets", STR_LIST, "FQNs of the assets in the data product."),
        column("asset_count", pa.int64(), "Number of assets in the data product."),
        column("owners", STR_LIST, "Owner names."),
        column("experts", STR_LIST, "Names of the data product's experts."),
        *audit_fields(),
    ]
)


@init_single_worker
@bind_fixed_schema
class DataProductsScan(EntityScan[NoArgs]):
    """Rows of ``data_products``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATA_PRODUCTS_SCHEMA
    PATH: ClassVar[str] = "/v1/dataProducts"
    ENTITY_TYPE: ClassVar[str] = "dataProduct"
    FIELDS: ClassVar[str] = "domain,owners,experts,assets"
    LIST_PARAMS: ClassVar[dict[str, str]] = {"domain": "domain"}

    class Meta:
        """Scan metadata."""

        name = "data_products_scan"
        description = "OpenMetadata data products with their domain and assets"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="OpenMetadata Data Products",
            category="governance",
            keywords="data products, data mesh, domains, assets, ownership",
            doc_llm=(
                "One row per OpenMetadata data product: id, fqn, name, display_name, description, domain_id, "
                "domain, assets (asset FQNs), asset_count, owners, experts, deleted, updated_at, updated_by, "
                "version, url. Pushdown: id / fqn = or IN fetch directly; domain = / IN becomes the list "
                "endpoint's domain parameter."
            ),
            doc_md="Data products inside domains.",
            schema=DATA_PRODUCTS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT name, asset_count FROM openmetadata.main.data_products WHERE domain = 'Sales' "
                    "ORDER BY asset_count DESC"
                ),
                description="Data products of the Sales domain by size",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per data product."""
        dom = domain_ref(entity)
        assets = ref_names(entity.get("assets"), "fullyQualifiedName")
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("id"),
                "fqn": entity.get("fullyQualifiedName") or entity.get("name"),
                "name": entity.get("name"),
                "display_name": entity.get("displayName"),
                "description": entity.get("description"),
                "domain_id": dom.get("id"),
                "domain": dom.get("fullyQualifiedName") or dom.get("name"),
                "assets": assets,
                "asset_count": len(assets),
                "owners": owner_names(entity),
                "experts": ref_names(entity.get("experts")),
                **audit_values(ep, entity, "dataProduct"),
            }
        ]


GOVERNANCE_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("owners", OwnersScan, ["entity_id", "owner_id"]),
    ("users", UsersScan, ["id"]),
    ("teams", TeamsScan, ["id"]),
    ("classifications", ClassificationsScan, ["id"]),
    ("tags", TagsScan, ["id"]),
    ("glossaries", GlossariesScan, ["id"]),
    ("glossary_terms", GlossaryTermsScan, ["id"]),
    ("domains", DomainsScan, ["id"]),
    ("data_products", DataProductsScan, ["id"]),
]
