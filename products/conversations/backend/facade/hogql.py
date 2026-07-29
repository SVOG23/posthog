"""
HogQL facade for conversations: the federated ticket side tables, plus the lazy joins that
power `system.support_tickets.tags` and `system.support_tickets.assignee`.

A ticket's tags and assignee are normalized away from the ticket row, so a flat mapping over
`posthog_conversations_ticket` can't see them. These expose them at query time instead of
storing a copy on the row.

The raw federated tables (`_ticket_tagged_items`, `_ticket_assignments`, `_ticket_assignee_roles`)
have no `team_id` column and should not be reachable directly from the SQL editor - they exist
only so the lazy join subqueries below can be resolved by the planner.
"""

from posthog.hogql import ast
from posthog.hogql.base import Expr
from posthog.hogql.context import HogQLContext
from posthog.hogql.database.lazy_join_tags import TICKET_ASSIGNEE, TICKET_TAGS
from posthog.hogql.database.models import (
    DANGEROUS_NoTeamIdCheckTable,
    FieldOrTable,
    IntegerDatabaseField,
    LazyJoin,
    LazyJoinToAdd,
    LazyTable,
    LazyTableToAdd,
    StringArrayDatabaseField,
    StringDatabaseField,
    UUIDDatabaseField,
)
from posthog.hogql.database.postgres_table import PostgresTable
from posthog.hogql.errors import ResolutionError
from posthog.hogql.parser import parse_expr, parse_select


class _TicketScopedPostgresTable(PostgresTable, DANGEROUS_NoTeamIdCheckTable):
    """PostgresTable variant for ticket side tables that lack a `team_id` column.

    The framework's auto-injected `team_id = X` guard is bypassed because the column doesn't
    exist. Security is preserved instead via a predicate (set on the class) that scopes through
    `ticket_id`, relying on the framework re-applying its team_id guard to the inner
    `system.support_tickets` reference.

    Direct top-level SELECT remains safe because the predicate prunes rows whose FK doesn't
    resolve to a team-scoped ticket.
    """

    predicates: list[Expr] = [parse_expr("ticket_id IN (SELECT id FROM system.support_tickets)")]


ticket_tagged_items: _TicketScopedPostgresTable = _TicketScopedPostgresTable(
    name="_ticket_tagged_items",
    postgres_table_name="posthog_taggeditem",
    description="Internal federated junction table (PostgreSQL `posthog_taggeditem`) of tag-to-ticket links; not for direct querying - use `system.support_tickets.tags`.",
    fields={
        "id": UUIDDatabaseField(name="id", description="Primary key of the tagged-item junction row."),
        "tag_id": UUIDDatabaseField(name="tag_id", description="Tag applied to the ticket; join to `system.tags.id`."),
        "ticket_id": UUIDDatabaseField(
            name="ticket_id",
            nullable=True,
            description="Ticket the tag is applied to; join to `system.support_tickets.id`.",
        ),
    },
)

ticket_assignments: _TicketScopedPostgresTable = _TicketScopedPostgresTable(
    name="_ticket_assignments",
    postgres_table_name="posthog_conversations_ticket_assignment",
    description="Internal federated table (PostgreSQL `posthog_conversations_ticket_assignment`) of ticket assignments; not for direct querying - use `system.support_tickets.assignee`.",
    fields={
        "id": UUIDDatabaseField(name="id", description="Assignment UUID."),
        "ticket_id": UUIDDatabaseField(
            name="ticket_id", description="Ticket assigned; join to `system.support_tickets.id`."
        ),
        "user_id": IntegerDatabaseField(
            name="user_id", nullable=True, description="User the ticket is assigned to, if assigned to a user."
        ),
        "role_id": UUIDDatabaseField(
            name="role_id", nullable=True, description="Role the ticket is assigned to, if assigned to a role."
        ),
    },
)


class _TicketAssigneeRolesTable(PostgresTable, DANGEROUS_NoTeamIdCheckTable):
    """Roles (`ee_role`) are organization-scoped, so there is no `team_id` to guard on.

    Scoped instead to the roles actually referenced by this team's ticket assignments, which
    resolves through `_ticket_assignments` and in turn through `system.support_tickets`, where
    the framework re-applies the team_id guard.
    """

    predicates: list[Expr] = [parse_expr("id IN (SELECT role_id FROM system._ticket_assignments)")]


ticket_assignee_roles: _TicketAssigneeRolesTable = _TicketAssigneeRolesTable(
    name="_ticket_assignee_roles",
    postgres_table_name="ee_role",
    description="Internal federated table (PostgreSQL `ee_role`) naming the roles this team's tickets are assigned to; not for direct querying - use `system.support_tickets.assignee`.",
    fields={
        "id": UUIDDatabaseField(name="id", description="Role UUID."),
        "name": StringDatabaseField(name="name", description="Role name, e.g. 'Team Support'."),
    },
)


def _ticket_tags_select() -> ast.SelectQuery | ast.SelectSetQuery:
    return parse_select(
        """
        SELECT
            tti.ticket_id AS ticket_id,
            arraySort(arrayDistinct(groupArray(t.name))) AS names
        FROM system._ticket_tagged_items AS tti
        INNER JOIN system.tags AS t ON t.id = tti.tag_id
        GROUP BY tti.ticket_id
        """
    )


def _ticket_assignee_select() -> ast.SelectQuery | ast.SelectSetQuery:
    return parse_select(
        """
        SELECT
            ta.ticket_id AS ticket_id,
            ta.user_id AS user_id,
            ta.role_id AS role_id,
            nullIf(r.name, '') AS role_name
        FROM system._ticket_assignments AS ta
        LEFT JOIN system._ticket_assignee_roles AS r ON r.id = assumeNotNull(ta.role_id)
        """
    )


class _TicketTagsTable(LazyTable):
    description: str = (
        "Internal aggregating table backing `system.support_tickets.tags`: the distinct, sorted tag names per ticket."
    )
    fields: dict[str, FieldOrTable] = {
        "ticket_id": UUIDDatabaseField(
            name="ticket_id", description="Ticket these tags belong to; join to `system.support_tickets.id`."
        ),
        "names": StringArrayDatabaseField(
            name="names",
            description="Distinct, sorted tag names applied to the ticket. Filter with has(tags.names, 'x') or hasAny(tags.names, ['x', 'y']).",
        ),
    }

    def lazy_select(
        self, table_to_add: LazyTableToAdd, context: HogQLContext, node: ast.SelectQuery
    ) -> ast.SelectQuery | ast.SelectSetQuery:
        return _ticket_tags_select()

    def to_printed_clickhouse(self, context: HogQLContext) -> str:
        return "ticket_tags"

    def to_printed_hogql(self) -> str:
        return "ticket_tags"


class _TicketAssigneeTable(LazyTable):
    description: str = (
        "Internal table backing `system.support_tickets.assignee`: who a ticket is assigned to, user or role."
    )
    fields: dict[str, FieldOrTable] = {
        "ticket_id": UUIDDatabaseField(
            name="ticket_id", description="Ticket this assignment belongs to; join to `system.support_tickets.id`."
        ),
        "user_id": IntegerDatabaseField(
            name="user_id", nullable=True, description="User the ticket is assigned to, if assigned to a user."
        ),
        "role_id": UUIDDatabaseField(
            name="role_id", nullable=True, description="Role the ticket is assigned to, if assigned to a role."
        ),
        "role_name": StringDatabaseField(
            name="role_name",
            nullable=True,
            description="Name of the role the ticket is assigned to, e.g. 'Team Support'.",
        ),
    }

    def lazy_select(
        self, table_to_add: LazyTableToAdd, context: HogQLContext, node: ast.SelectQuery
    ) -> ast.SelectQuery | ast.SelectSetQuery:
        return _ticket_assignee_select()

    def to_printed_clickhouse(self, context: HogQLContext) -> str:
        return "ticket_assignee"

    def to_printed_hogql(self) -> str:
        return "ticket_assignee"


def _join_on_ticket_id(select: ast.SelectQuery | ast.SelectSetQuery, join_to_add: LazyJoinToAdd) -> ast.JoinExpr:
    return ast.JoinExpr(
        alias=join_to_add.to_table,
        table=select,
        join_type="LEFT JOIN",
        constraint=ast.JoinConstraint(
            constraint_type="ON",
            expr=ast.CompareOperation(
                op=ast.CompareOperationOp.Eq,
                left=ast.Field(chain=[join_to_add.from_table, "id"]),
                right=ast.Field(chain=[join_to_add.to_table, "ticket_id"]),
            ),
        ),
    )


def ticket_tags_join(join_to_add: LazyJoinToAdd, context: HogQLContext, node: ast.SelectQuery) -> ast.JoinExpr:
    if not join_to_add.fields_accessed:
        raise ResolutionError("No fields requested from `support_tickets.tags`")
    return _join_on_ticket_id(_ticket_tags_select(), join_to_add)


def ticket_assignee_join(join_to_add: LazyJoinToAdd, context: HogQLContext, node: ast.SelectQuery) -> ast.JoinExpr:
    if not join_to_add.fields_accessed:
        raise ResolutionError("No fields requested from `support_tickets.assignee`")
    return _join_on_ticket_id(_ticket_assignee_select(), join_to_add)


ticket_tags_lazy_join: LazyJoin = LazyJoin(
    from_field=["id"],
    join_table=_TicketTagsTable(),
    resolver=TICKET_TAGS,
)

ticket_assignee_lazy_join: LazyJoin = LazyJoin(
    from_field=["id"],
    join_table=_TicketAssigneeTable(),
    resolver=TICKET_ASSIGNEE,
)
