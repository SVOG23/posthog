from datetime import timedelta
from typing import Any

from posthog.test.base import BaseTest

from django.apps import apps
from django.utils import timezone

from parameterized import parameterized

from posthog.models.activity_logging.activity_log import ActivityLog

from products.signals.backend.models import SignalReport, SignalReportArtefact, SignalScoutConfig, SignalScoutRun
from products.signals.backend.scout_harness.config_registry import register_missing_configs
from products.signals.backend.scout_harness.inactivity import (
    COLD_START_GRACE,
    INACTIVITY_WINDOW,
    MIN_RUNS_IN_WINDOW,
    WARNING_GRACE,
    sweep_inactive_scouts,
)
from products.skills.backend.models.skills import LLMSkill

SKILL = "signals-scout-quiet"


class TestScoutInactivitySweep(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.now = timezone.now()
        self.config = self._config()

    def _config(self, **overrides: Any) -> SignalScoutConfig:
        config = SignalScoutConfig.objects.create(team=self.team, skill_name=SKILL, **overrides)
        # `created_at` is auto_now_add, so age the row past the cold-start grace out of band.
        SignalScoutConfig.all_teams.filter(pk=config.pk).update(
            created_at=self.now - COLD_START_GRACE - timedelta(days=1)
        )
        config.refresh_from_db()
        return config

    def _runs(self, count: int, *, age: timedelta, **output: Any) -> list[SignalScoutRun]:
        Task = apps.get_model("tasks", "Task")
        TaskRun = apps.get_model("tasks", "TaskRun")
        runs = []
        for _ in range(count):
            task = Task.objects.create(
                team=self.team,
                title="scout run",
                description="scout run",
                origin_product=Task.OriginProduct.SIGNALS_SCOUT,
            )
            run = SignalScoutRun.objects.create(
                task_run=TaskRun.objects.create(task=task, team=self.team),
                team=self.team,
                scout_config=self.config,
                skill_name=SKILL,
                skill_version=1,
                **output,
            )
            SignalScoutRun.all_teams.filter(pk=run.pk).update(created_at=self.now - age)
            runs.append(run)
        return runs

    def _report(self) -> SignalReport:
        return SignalReport.objects.create(team=self.team, title="A report", summary="Something")

    def _silent_runs(self) -> None:
        self._runs(MIN_RUNS_IN_WINDOW, age=INACTIVITY_WINDOW / 2)

    def _reload(self) -> SignalScoutConfig:
        return SignalScoutConfig.all_teams.get(pk=self.config.pk)

    def test_silent_scout_is_warned_then_paused_after_the_grace_period(self) -> None:
        self._silent_runs()

        outcome = sweep_inactive_scouts(now=self.now)
        warned = self._reload()
        assert [c.pk for c in outcome.warned] == [self.config.pk]
        assert warned.enabled is True
        assert warned.auto_pause_warned_at is not None
        assert warned.auto_paused_at is None

        # Still silent a day later: warned, not yet due to pause.
        assert not sweep_inactive_scouts(now=self.now + timedelta(days=1)).paused

        outcome = sweep_inactive_scouts(now=self.now + WARNING_GRACE)
        paused = self._reload()
        assert [c.pk for c in outcome.paused] == [self.config.pk]
        assert paused.enabled is False
        assert paused.auto_paused_at is not None
        assert paused.auto_pause_reason == SignalScoutConfig.AutoPauseReason.NO_OUTPUT

    def test_a_scout_whose_older_reports_went_unread_is_paused_as_ignored(self) -> None:
        # Separated from `no_output` because the two want different fixes: retune a scout whose
        # reports nobody picks up, retire one that never finds anything.
        report = self._report()
        self._runs(1, age=INACTIVITY_WINDOW + timedelta(days=5), emitted_report_ids=[str(report.id)])
        self._silent_runs()

        sweep_inactive_scouts(now=self.now)
        sweep_inactive_scouts(now=self.now + WARNING_GRACE)

        assert self._reload().auto_pause_reason == SignalScoutConfig.AutoPauseReason.IGNORED

    def test_pause_is_activity_logged_without_pinning_it_on_a_user(self) -> None:
        self._silent_runs()
        sweep_inactive_scouts(now=self.now)
        ActivityLog.objects.filter(scope="SignalScoutConfig").delete()

        sweep_inactive_scouts(now=self.now + WARNING_GRACE)

        entry = ActivityLog.objects.get(scope="SignalScoutConfig", item_id=str(self.config.id))
        assert entry.user is None
        detail = entry.detail or {}
        assert detail.get("trigger", {}).get("job_type") == "signals_scout_inactivity_sweep"

    def test_warning_alone_is_not_activity_logged(self) -> None:
        # The warning is UI state, not a config change — it must not spam the audit log.
        self._silent_runs()
        ActivityLog.objects.filter(scope="SignalScoutConfig").delete()

        sweep_inactive_scouts(now=self.now)

        assert not ActivityLog.objects.filter(scope="SignalScoutConfig", item_id=str(self.config.id)).exists()

    @parameterized.expand(
        [
            ("findings", {"emitted_finding_ids": ["finding-1"], "emitted_count": 1}),
            ("reports", {"emitted_report_ids": ["a3f0a1de-0000-4000-8000-000000000001"]}),
            ("edits", {"edited_report_ids": ["a3f0a1de-0000-4000-8000-000000000002"]}),
        ]
    )
    def test_output_on_any_emit_channel_keeps_a_scout_running(self, _name: str, output: dict) -> None:
        # `emitted_count` covers only `emit_finding`, so judging on it alone would read every
        # report-channel scout as silent and pause it.
        self._runs(MIN_RUNS_IN_WINDOW - 1, age=INACTIVITY_WINDOW / 2)
        self._runs(1, age=INACTIVITY_WINDOW / 2, **output)

        outcome = sweep_inactive_scouts(now=self.now)

        assert outcome.warned == []
        assert self._reload().auto_pause_warned_at is None

    @parameterized.expand(
        [
            ("log_artefact", SignalReportArtefact.ArtefactType.NOTE, None),
            ("dismissal_artefact", SignalReportArtefact.ArtefactType.DISMISSAL, None),
            ("user_driven_status", None, SignalReport.Status.SUPPRESSED),
        ]
    )
    def test_engagement_with_an_older_report_keeps_a_silent_scout_running(
        self, _name: str, artefact_type: str | None, status: str | None
    ) -> None:
        report = self._report()
        # Authored before the window, so the run itself is no longer output — only what happened to
        # the report since counts.
        self._runs(1, age=INACTIVITY_WINDOW + timedelta(days=5), emitted_report_ids=[str(report.id)])
        self._silent_runs()
        if artefact_type is not None:
            SignalReportArtefact.objects.create(
                team=self.team, report=report, type=artefact_type, content="{}", created_by=self.user
            )
        if status is not None:
            SignalReport.objects.filter(pk=report.pk).update(status=status, updated_at=self.now - timedelta(days=1))

        outcome = sweep_inactive_scouts(now=self.now)

        assert outcome.warned == []
        assert self._reload().auto_pause_warned_at is None

    @parameterized.expand(
        [
            # A pipeline assessment, never a person's work.
            ("status_judgment", SignalReportArtefact.ArtefactType.PRIORITY_JUDGMENT),
            # The right type, but written by the pipeline: grouping appends a symmetric `related_to`
            # when a resolved report recurs, and autostart appends `task_run`, both unattributed.
            ("unattributed_log", SignalReportArtefact.ArtefactType.RELATED_TO),
        ]
    )
    def test_pipeline_artefacts_are_not_engagement(self, _name: str, artefact_type: str) -> None:
        report = self._report()
        self._runs(1, age=INACTIVITY_WINDOW + timedelta(days=5), emitted_report_ids=[str(report.id)])
        self._silent_runs()
        SignalReportArtefact.objects.create(
            team=self.team,
            report=report,
            type=artefact_type,
            content="{}",
        )

        assert [c.pk for c in sweep_inactive_scouts(now=self.now).warned] == [self.config.pk]

    def test_a_scout_that_recovers_loses_its_warning(self) -> None:
        self._silent_runs()
        sweep_inactive_scouts(now=self.now)
        self._runs(1, age=timedelta(hours=1), emitted_finding_ids=["finding-1"], emitted_count=1)

        outcome = sweep_inactive_scouts(now=self.now + timedelta(days=1))

        recovered = self._reload()
        assert outcome.recovered == 1
        assert recovered.auto_pause_warned_at is None
        assert recovered.enabled is True

    @parameterized.expand(
        [
            ("exempt", {"auto_pause_exempt": True}),
            ("dry_run", {"emit": False}),
            ("already_disabled", {"enabled": False}),
        ]
    )
    def test_scouts_the_sweep_must_not_touch(self, _name: str, overrides: dict) -> None:
        SignalScoutConfig.all_teams.filter(pk=self.config.pk).update(**overrides)
        self._silent_runs()

        assert sweep_inactive_scouts(now=self.now).warned == []

    def test_a_cold_start_scout_is_left_alone(self) -> None:
        SignalScoutConfig.all_teams.filter(pk=self.config.pk).update(created_at=self.now - timedelta(days=1))
        self._silent_runs()

        assert sweep_inactive_scouts(now=self.now).warned == []

    def test_a_scout_that_has_barely_run_is_left_alone(self) -> None:
        # Sparse runs (a monthly cron, or a team that spent its budget elsewhere) say nothing about
        # what the scout would have found.
        self._runs(MIN_RUNS_IN_WINDOW - 1, age=INACTIVITY_WINDOW / 2)

        assert sweep_inactive_scouts(now=self.now).warned == []

    def test_a_resumed_scout_gets_a_full_fresh_window(self) -> None:
        # Without the reset stamp the sweep would judge the resumed scout on the same silent runs
        # that paused it, so a resume would buy a week rather than a fresh window.
        self._silent_runs()
        sweep_inactive_scouts(now=self.now)
        sweep_inactive_scouts(now=self.now + WARNING_GRACE)
        resumed_at = self.now + WARNING_GRACE + timedelta(hours=1)
        SignalScoutConfig.all_teams.filter(pk=self.config.pk).update(
            enabled=True,
            auto_paused_at=None,
            auto_pause_reason=None,
            auto_pause_warned_at=None,
            auto_pause_reset_at=resumed_at,
        )

        outcome = sweep_inactive_scouts(now=resumed_at + timedelta(days=1))

        assert outcome.warned == []
        assert self._reload().auto_pause_warned_at is None

    def test_pause_survives_lazy_seed_reconciliation(self) -> None:
        # Configs are re-reconciled on every coordinator tick; a pause that gets quietly re-enabled
        # there would put the scout straight back on the schedule.
        LLMSkill.objects.create(team=self.team, name=SKILL, description="Quiet", body="Look around")
        self._silent_runs()
        sweep_inactive_scouts(now=self.now)
        sweep_inactive_scouts(now=self.now + WARNING_GRACE)

        register_missing_configs(self.team.id)

        reloaded = self._reload()
        assert reloaded.enabled is False
        assert reloaded.auto_paused_at is not None
