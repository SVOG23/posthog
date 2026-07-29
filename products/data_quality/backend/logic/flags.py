from typing import TYPE_CHECKING

from django.conf import settings

from posthog.ph_client import feature_enabled_or_false

if TYPE_CHECKING:
    from posthog.models.team import Team

DATA_QUALITY_CHECKS_FEATURE_FLAG = "data-quality-checks"


def is_data_quality_checks_enabled(team: "Team") -> bool:
    """The `data-quality-checks` flag check, org-keyed. Canonical home for the check -- gate any
    data-quality surface (API, information_schema tables, MCP tools) through here."""
    if settings.DEBUG:
        return True
    return feature_enabled_or_false(
        DATA_QUALITY_CHECKS_FEATURE_FLAG,
        str(team.organization_id),
        groups={"organization": str(team.organization_id)},
        group_properties={"organization": {"id": str(team.organization_id)}},
        send_feature_flag_events=False,
    )
