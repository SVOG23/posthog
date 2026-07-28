from django.contrib.postgres.fields import ArrayField
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("conversations", "0054_ticket_sla_snooze_asc_indexes"),
    ]

    operations = [
        migrations.AddField(
            model_name="ticket",
            name="tag_names",
            field=ArrayField(models.CharField(max_length=200), blank=True, default=list, null=True, size=None),
        ),
        migrations.AddField(
            model_name="ticket",
            name="assignee_user_id",
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="ticket",
            name="assignee_role_id",
            field=models.UUIDField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="ticket",
            name="assignee_role_name",
            field=models.CharField(blank=True, max_length=200, null=True),
        ),
    ]
