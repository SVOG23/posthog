"""
HogQL facade for conversations: the federated ticket side tables, plus the lazy joins that
power `system.support_tickets.tags` and `system.support_tickets.assignee`.

A ticket's tags and assignee are normalized away from the ticket row, so a flat mapping over
`posthog_conversations_ticket` can't see them. These expose them at query time instead of
storing a copy on the row.

None of the three side tables has a `team_id` column, so each is scoped by a predicate instead of
the framework's usual guard. That keeps a direct top-level SELECT safe, which matters because
filtering through them is the cheap way to query tickets by tag or assignee: referencing a lazy
join field in a WHERE clause stops ClickHouse pushing the ticket table's own filters down to
Postgres, so it reads every ticket across every team and filters locally.
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


class _TicketTaggedItemsTable(_TicketScopedPostgresTable):
    """`posthog_taggeditem` links tags to every taggable object across every team, and tickets are
    a small minority of its rows.

    The `ticket_id IN (...)` scoping is a subquery, which ClickHouse cannot push down to Postgres,
    so on its own the federated read streams the entire table over the wire and filters in
    ClickHouse. `ticket_id IS NOT NULL` is a single-column predicate that does push down, and
    Postgres serves it from the partial index on (tag_id, ticket_id) WHERE ticket_id IS NOT NULL,
    so only ticket links are read. It is not redundant with the scoping above: one guards tenant
    isolation, the other keeps the read off the rest of the table.
    """

    predicates: list[Expr] = [
        parse_expr("ticket_id IN (SELECT id FROM system.support_tickets)"),
        parse_expr("ticket_id IS NOT NULL"),
    ]


ticket_tagged_items: _TicketTaggedItemsTable = _TicketTaggedItemsTable(
    name="support_ticket_tags",
    access_scope="ticket",
    access_control_id_field="ticket_id",
    postgres_table_name="posthog_taggeditem",
    description="Tag-to-ticket links, one row per tag on a ticket. Join to `system.tags` for names. Filtering tickets through this table (`id IN (SELECT ticket_id FROM ...)`) reads far less than the `support_tickets.tags` lazy join, which is better suited to ad-hoc queries.",
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
    name="support_ticket_assignments",
    access_scope="ticket",
    access_control_id_field="ticket_id",
    postgres_table_name="posthog_conversations_ticket_assignment",
    description="Ticket assignments, one row per assigned ticket, to a user or a role but never both.",
    fields={
        "id": UUIDDatabaseField(name="id", description="Assignment UUID."),
        "ticket_id": UUIDDatabaseField(
            name="ticket_id", description="Ticket assigned; join to `system.support_tickets.id`."
        ),
        "user_id": IntegerDatabaseField(
            name="user_id", nullable=True, description="User the ticket is assigned to, if assigned to a user."
        ),
        "role_id": UUIDDatabaseField(
            name="role_id",
            nullable=True,
            description=(
                "Role the ticket is assigned to, if assigned to a role. Match it with "
                "role_id IN (SELECT id FROM system.support_ticket_roles WHERE name = '...'); joining on it "
                "directly needs assumeNotNull(role_id), because ClickHouse rejects a nullable join key."
            ),
        ),
    },
)


class _TicketAssigneeRolesTable(PostgresTable, DANGEROUS_NoTeamIdCheckTable):
    """Roles (`ee_role`) are organization-scoped, so there is no `team_id` to guard on.

    Scoped instead to the roles actually referenced by this team's ticket assignments, which
    resolves through `_ticket_assignments` and in turn through `system.support_tickets`, where
    the framework re-applies the team_id guard.
    """

    predicates: list[Expr] = [parse_expr("id IN (SELECT role_id FROM system.support_ticket_assignments)")]


ticket_assignee_roles: _TicketAssigneeRolesTable = _TicketAssigneeRolesTable(
    name="support_ticket_roles",
    access_scope="ticket",
    postgres_table_name="ee_role",
    description="Roles this team's tickets are assigned to, one row per role. Join to `system.support_ticket_assignments.role_id`.",
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
        FROM system.support_ticket_tags AS tti
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
        FROM system.support_ticket_assignments AS ta
        LEFT JOIN system.support_ticket_roles AS r ON r.id = assumeNotNull(ta.role_id)
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
