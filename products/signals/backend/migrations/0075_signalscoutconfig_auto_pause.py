from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("signals", "0074_signalreport_charts"),
    ]

    operations = [
        migrations.AddField(
            model_name="signalscoutconfig",
            name="auto_pause_exempt",
            field=models.BooleanField(db_default=False, default=False),
        ),
        migrations.AddField(
            model_name="signalscoutconfig",
            name="auto_pause_reason",
            field=models.CharField(
                blank=True,
                choices=[("inactive", "No output or engagement")],
                max_length=40,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="signalscoutconfig",
            name="auto_pause_warned_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="signalscoutconfig",
            name="auto_paused_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
