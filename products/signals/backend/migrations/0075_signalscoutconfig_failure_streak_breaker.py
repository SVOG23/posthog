from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("signals", "0074_signalreport_charts")]

    operations = [
        migrations.AddField(
            model_name="signalscoutconfig",
            name="consecutive_failure_count",
            field=models.PositiveIntegerField(db_default=0, default=0),
        ),
        migrations.AddField(
            model_name="signalscoutconfig",
            name="auto_paused_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="signalscoutconfig",
            name="auto_pause_reason",
            field=models.TextField(blank=True, db_default="", default=""),
        ),
    ]
