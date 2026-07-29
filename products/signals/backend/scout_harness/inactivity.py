"""Inactivity sweep: warn, then auto-pause scouts that produce nothing anyone uses.

`SignalScoutConfig.enabled` only ever moves by hand, so a scout that surfaces nothing keeps
spending sandbox runs on its cadence indefinitely. This module is the missing stop: once a day
(`tasks.pause_inactive_signal_scouts`, deliberately not the 30-minute coordinator tick, which is
kept short-lived and bounded) it decides whether each enabled scout is still earning its runs.

A scout counts as **productive** if either half holds over the window:

- *output* — any run in the window recorded something on any of the three emit channels
  (`emitted_finding_ids`, `emitted_report_ids`, `edited_report_ids`). All three matter: a
  report-channel scout writes through `emit_report` / `edit_report`, so judging on the finding
  tally alone would read every one of them as silent.
- *engagement* — someone acted on a report the scout wrote or edited before the window: a log
  artefact (note, dismissal, task run, commit, code review, reviewer change…) landed on it inside
  the window, or the report reached a state only a human action produces. Reports the scout touched
  *inside* the window are excluded from this half — they are already covered by the output half, and
  including them would count the scout's own writes as engagement with itself. Client-side opens
  aren't persisted server-side, so they can't count either way.

Neither half is retroactive: a scout that goes quiet is warned first, and only paused if it is still
quiet a grace period later. Re-enabling a scout clears the pause (see
`SignalScoutConfigUpdateSerializer.update`), and the pause survives lazy-seed reconciliation because
that path only ever fills in *missing* config rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from django.utils import timezone

import structlog

from posthog.models.activity_logging.activity_log import Trigger
from posthog.models.activity_logging.model_activity import ActivityTriggerContext

from products.signals.backend.models import SignalReport, SignalReportArtefact, SignalScoutConfig, SignalScoutRun

logger = structlog.get_logger(__name__)

# How far back productivity is judged. Long enough that a daily scout is assessed on a couple of
# weeks of runs rather than a bad afternoon, short enough that the waste stops mattering in days.
INACTIVITY_WINDOW = timedelta(days=14)

# Breathing room between the warning and the pause: the team gets a full window's worth of notice on
# the fleet page, and a scout that surfaces something in the meantime clears its own warning.
WARNING_GRACE = timedelta(days=7)

# Cold start: a config younger than one window has no full window of evidence behind it, and a brand
# new scout deserves the chance to find its first finding.
COLD_START_GRACE = INACTIVITY_WINDOW

# A scout is only judged once it has actually had a fair number of attempts in the window. Guards
# the sparse cases — a monthly cron, or a scout whose team spent its daily budget elsewhere — where
# "no output" says more about how rarely it ran than about what it found.
MIN_RUNS_IN_WINDOW = 5

# How far back to look for the reports a scout has touched. Bounds the run scan on a table that grows
# a row per scout per interval forever; a report nobody engaged with in three months isn't about to
# rescue the scout that wrote it.
TOUCHED_REPORT_LOOKBACK = timedelta(days=90)

# Artefact types that mean someone did work on a report. The status artefacts (safety / actionability
# / priority judgments, repo selection) are excluded — the pipeline appends those on its own, so they
# would read as engagement on a report nobody ever looked at.
_ENGAGEMENT_ARTEFACT_TYPES: frozenset[str] = SignalReportArtefact.LOG_ARTEFACT_TYPES | frozenset(
    {SignalReportArtefact.ArtefactType.DISMISSAL}
)

# Statuses no pipeline transition produces: archiving, resolving, and deleting a report are all
# user-driven, which is what makes "report is in this state, and moved recently" a durable
# engagement signal even when the action left no artefact behind.
_ENGAGED_REPORT_STATUSES: frozenset[str] = frozenset(
    {
        SignalReport.Status.SUPPRESSED,
        SignalReport.Status.RESOLVED,
        SignalReport.Status.DELETED,
    }
)

_SWEEP_JOB_TYPE = "signals_scout_inactivity_sweep"


@dataclass
class SweepOutcome:
    """What one sweep did, for logging and analytics."""

    considered: int = 0
    warned: list[SignalScoutConfig] = field(default_factory=list)
    paused: list[SignalScoutConfig] = field(default_factory=list)
    recovered: int = 0


def sweep_inactive_scouts(now: datetime | None = None) -> SweepOutcome:
    """Warn or pause every enabled scout that produced nothing anyone engaged with.

    Idempotent: re-running the same day re-derives the same verdict from the runs and reports
    themselves, and a config already warned/paused is only advanced when the grace period has
    actually elapsed.
    """
    now = now or timezone.now()
    outcome = SweepOutcome()

    # A fleet-wide sweep is genuinely cross-team, so it reads through the unscoped manager; every
    # per-team query below is then keyed on the `team_id` carried by the config rows themselves.
    candidates = (
        SignalScoutConfig.all_teams.filter(
            enabled=True,
            # A dry-run scout emits nothing by design, so the productivity test can't say anything
            # about it — `emit_finding` is a no-op there and never records output on the run.
            emit=True,
            auto_pause_exempt=False,
            created_at__lte=now - COLD_START_GRACE,
        )
        .order_by("team_id", "skill_name")
        .iterator()
    )
    by_team: dict[int, list[SignalScoutConfig]] = {}
    for config in candidates:
        by_team.setdefault(config.team_id, []).append(config)

    for team_id, configs in by_team.items():
        outcome.considered += len(configs)
        try:
            productive, judgeable = _assess_team(team_id, [c.skill_name for c in configs], now)
        except Exception:
            # One team's data problem must not cost the rest of the fleet its sweep.
            logger.exception("signals_scout inactivity sweep: team assessment failed", team_id=team_id)
            continue
        for config in configs:
            if config.skill_name in productive:
                # It surfaced something again — drop any pending warning so a productive scout is
                # never one quiet fortnight away from a pause it already worked off.
                if config.auto_pause_warned_at is not None:
                    _clear_warning(config)
                    outcome.recovered += 1
                continue
            if config.skill_name not in judgeable:
                continue
            if config.auto_pause_warned_at is None:
                _warn(config, now)
                outcome.warned.append(config)
            elif now - config.auto_pause_warned_at >= WARNING_GRACE:
                _pause(config, now)
                outcome.paused.append(config)

    return outcome


def _assess_team(team_id: int, skill_names: list[str], now: datetime) -> tuple[set[str], set[str]]:
    """Return `(productive skills, judgeable skills)` for one team.

    Judgeable means the scout ran often enough in the window for "it found nothing" to mean
    anything; productive means it passed either half of the productivity test.
    """
    window_start = now - INACTIVITY_WINDOW
    runs = SignalScoutRun.all_teams.filter(
        team_id=team_id,
        skill_name__in=skill_names,
        created_at__gte=now - TOUCHED_REPORT_LOOKBACK,
    ).values_list("skill_name", "created_at", "emitted_finding_ids", "emitted_report_ids", "edited_report_ids")

    runs_in_window: dict[str, int] = {}
    productive: set[str] = set()
    # Reports each scout touched *before* the window — the only ones the engagement half reads, so a
    # scout's own in-window writes can't be mistaken for someone engaging with them.
    touched_before_window: dict[str, set[str]] = {}
    for skill_name, created_at, finding_ids, report_ids, edited_ids in runs:
        emitted_anything = bool(finding_ids) or bool(report_ids) or bool(edited_ids)
        if created_at >= window_start:
            runs_in_window[skill_name] = runs_in_window.get(skill_name, 0) + 1
            if emitted_anything:
                productive.add(skill_name)
            continue
        touched = {str(report_id) for report_id in (report_ids or []) + (edited_ids or [])}
        if touched:
            touched_before_window.setdefault(skill_name, set()).update(touched)

    judgeable = {name for name, count in runs_in_window.items() if count >= MIN_RUNS_IN_WINDOW}
    pending = {
        name: reports for name, reports in touched_before_window.items() if name in judgeable and name not in productive
    }
    if pending:
        engaged = _engaged_report_ids(team_id, set().union(*pending.values()), window_start)
        productive.update(name for name, reports in pending.items() if reports & engaged)
    return productive, judgeable


def _engaged_report_ids(team_id: int, report_ids: set[str], window_start: datetime) -> set[str]:
    """Of `report_ids`, the ones someone acted on since `window_start`."""
    if not report_ids:
        return set()
    engaged = {
        str(report_id)
        for report_id in SignalReportArtefact.objects.filter(
            team_id=team_id,
            report_id__in=report_ids,
            type__in=_ENGAGEMENT_ARTEFACT_TYPES,
            created_at__gte=window_start,
        ).values_list("report_id", flat=True)
    }
    engaged |= {
        str(report_id)
        for report_id in SignalReport.objects.filter(
            team_id=team_id,
            id__in=report_ids,
            status__in=_ENGAGED_REPORT_STATUSES,
            updated_at__gte=window_start,
        ).values_list("id", flat=True)
    }
    return engaged


def _warn(config: SignalScoutConfig, now: datetime) -> None:
    config.auto_pause_warned_at = now
    # `auto_pause_warned_at` is activity-signal-excluded, so this stamp alone writes no audit entry —
    # the warning is a UI state, not a change to how the scout is configured.
    config.save(update_fields=["auto_pause_warned_at", "updated_at"])
    logger.info(
        "signals_scout inactivity sweep: warned",
        team_id=config.team_id,
        skill_name=config.skill_name,
    )


def _clear_warning(config: SignalScoutConfig) -> None:
    config.auto_pause_warned_at = None
    config.save(update_fields=["auto_pause_warned_at", "updated_at"])


def _pause(config: SignalScoutConfig, now: datetime) -> None:
    config.enabled = False
    config.auto_paused_at = now
    config.auto_pause_reason = SignalScoutConfig.AutoPauseReason.INACTIVE
    config.auto_pause_warned_at = None
    # No acting user: the sweep is the actor, so the activity entry is attributed to the job via the
    # trigger rather than pinned on whoever happened to enable the scout months ago.
    trigger = Trigger(
        job_type=_SWEEP_JOB_TYPE,
        job_id=str(config.id),
        payload={"skill_name": config.skill_name, "reason": config.auto_pause_reason},
    )
    with ActivityTriggerContext(trigger):
        config.save(
            update_fields=[
                "enabled",
                "auto_paused_at",
                "auto_pause_reason",
                "auto_pause_warned_at",
                "updated_at",
            ]
        )
    logger.info(
        "signals_scout inactivity sweep: paused",
        team_id=config.team_id,
        skill_name=config.skill_name,
    )
