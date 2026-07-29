from django.db import migrations
from django.db.models import Q


def backfill_denormalized_tags_assignee(apps, schema_editor):
    """
    Populate the denormalized tag_names / assignee_* columns for existing tickets from
    the TaggedItem and TicketAssignment side tables. Going forward, signals keep them
    in sync; this is the one-time catch-up for rows created before those columns existed.

    Only tickets that actually carry a tag or an assignment are visited. Every other row
    already holds the right empty value from the AddField default, so walking the whole
    table would rewrite it for nothing — and this runs in a single transaction.
    """
    Ticket = apps.get_model("conversations", "Ticket")
    TaggedItem = apps.get_model("posthog", "TaggedItem")
    TicketAssignment = apps.get_model("conversations", "TicketAssignment")
    Role = apps.get_model("ee", "Role")

    role_names = dict(Role.objects.values_list("id", "name"))
    needs_backfill = (
        Ticket.objects.filter(Q(tagged_items__isnull=False) | Q(assignment__isnull=False)).distinct().order_by("id")
    )

    batch_size = 500
    last_id = None

    while True:
        batch = needs_backfill if last_id is None else needs_backfill.filter(id__gt=last_id)
        tickets = list(batch[:batch_size])
        if not tickets:
            break

        ticket_ids = [ticket.id for ticket in tickets]

        tags_by_ticket: dict = {}
        for ticket_id, tag_name in TaggedItem.objects.filter(ticket_id__in=ticket_ids).values_list(
            "ticket_id", "tag__name"
        ):
            tags_by_ticket.setdefault(ticket_id, []).append(tag_name)

        assignment_by_ticket = {a.ticket_id: a for a in TicketAssignment.objects.filter(ticket_id__in=ticket_ids)}

        for ticket in tickets:
            ticket.tag_names = sorted(tags_by_ticket.get(ticket.id, []))
            assignment = assignment_by_ticket.get(ticket.id)
            if assignment is not None:
                ticket.assignee_user_id = assignment.user_id
                ticket.assignee_role_id = assignment.role_id
                ticket.assignee_role_name = role_names.get(assignment.role_id) if assignment.role_id else None

        Ticket.objects.bulk_update(
            tickets,
            ["tag_names", "assignee_user_id", "assignee_role_id", "assignee_role_name"],
            batch_size=batch_size,
        )

        last_id = ticket_ids[-1]


class Migration(migrations.Migration):
    dependencies = [
        ("conversations", "0055_ticket_denormalized_tags_assignee"),
        ("posthog", "1032_remove_taggeditem_exactly_one_related_object_and_more"),  # TaggedItem.ticket FK
        ("ee", "0014_roles_memberships_and_resource_access"),  # Role model
    ]

    operations = [
        migrations.RunPython(backfill_denormalized_tags_assignee, migrations.RunPython.noop),
    ]
