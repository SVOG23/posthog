from posthog.test.base import BaseTest

from parameterized import parameterized

from posthog.models import Tag

from products.conversations.backend.models import Ticket, TicketAssignment
from products.conversations.backend.models.constants import Channel, Status

from ee.models.rbac.role import Role


class TestTicketDenormalizedFields(BaseTest):
    def setUp(self):
        super().setUp()
        self.ticket = Ticket.objects.create_with_number(
            team=self.team,
            channel_source=Channel.WIDGET,
            widget_session_id="session-1",
            distinct_id="user-1",
            status=Status.NEW,
        )

    def _tag(self, name: str) -> None:
        tag, _ = Tag.objects.get_or_create(name=name, team_id=self.team.id)
        self.ticket.tagged_items.create(tag=tag)

    @parameterized.expand(
        [
            ("short_names", ["urgent", "billing"]),
            ("max_length_names", ["a" * 255, "b" * 255]),
        ]
    )
    def test_tagging_and_untagging_syncs_tag_names(self, _name, names):
        for name in names:
            self._tag(name)
        self.ticket.refresh_from_db()
        assert self.ticket.tag_names == sorted(names)

        self.ticket.tagged_items.filter(tag__name=names[-1]).delete()
        self.ticket.refresh_from_db()
        assert self.ticket.tag_names == sorted(names[:-1])

    def test_assigning_user_syncs_assignee_columns(self):
        TicketAssignment.objects.create(ticket=self.ticket, user=self.user)
        self.ticket.refresh_from_db()
        assert self.ticket.assignee_user_id == self.user.id
        assert self.ticket.assignee_role_id is None
        assert self.ticket.assignee_role_name is None

    def test_assigning_role_syncs_name_and_unassign_clears_it(self):
        role = Role.objects.create(name="Team Support", organization=self.organization)
        assignment = TicketAssignment.objects.create(ticket=self.ticket, role=role)
        self.ticket.refresh_from_db()
        assert self.ticket.assignee_role_id == role.id
        assert self.ticket.assignee_role_name == "Team Support"
        assert self.ticket.assignee_user_id is None

        assignment.delete()
        self.ticket.refresh_from_db()
        assert self.ticket.assignee_role_id is None
        assert self.ticket.assignee_role_name is None

    def test_reassigning_from_user_to_role_leaves_no_stale_user(self):
        TicketAssignment.objects.create(ticket=self.ticket, user=self.user)
        role = Role.objects.create(name="Team Support", organization=self.organization)

        TicketAssignment.objects.update_or_create(
            ticket_id=self.ticket.id, defaults={"user_id": None, "role_id": role.id}
        )

        self.ticket.refresh_from_db()
        assert self.ticket.assignee_user_id is None
        assert self.ticket.assignee_role_id == role.id
        assert self.ticket.assignee_role_name == "Team Support"

    def test_renaming_role_updates_denormalized_name_on_assigned_tickets(self):
        role = Role.objects.create(name="Team Support", organization=self.organization)
        TicketAssignment.objects.create(ticket=self.ticket, role=role)

        role.name = "Team Support & Success"
        role.save()

        self.ticket.refresh_from_db()
        assert self.ticket.assignee_role_name == "Team Support & Success"
