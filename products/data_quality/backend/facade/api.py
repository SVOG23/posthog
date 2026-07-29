"""
Facade for data_quality.

The only module this product's presentation layer (and external code) may import. It re-exports the
logic surface and model classes so the isolation boundary stays clean: presentation never reaches
into ``logic`` or ``models`` directly.
"""

from ..logic.checks import (
    checks_for_subject,
    ensure_name_available,
    soft_delete_check,
    start_check_suite,
    subject_health,
    update_check,
    upsert_check,
    validate_check,
)
from ..logic.compiler import compile_check, related_subject_ref
from ..logic.contracts import CheckPlan, CompiledCheck, Evaluation, SubjectRef
from ..logic.errors import CheckConfigError, SubjectUnresolvableError
from ..logic.health import CheckStatusRow, roll_up_health
from ..logic.registry import UnknownCheckTypeError, all_specs, get_spec
from ..logic.serialization import compute_fingerprint, from_config_entry, to_config_entry
from ..logic.spec import CheckConfig, CheckTypeSpec
from ..logic.subject_access import denied_subject_names, is_subject_denied, referenced_subject_names
from ..logic.subjects import resolve_subject
from .models import DataQualityCheck, DataQualityCheckRun, DataQualitySuiteRun

__all__ = [
    "CheckConfig",
    "CheckConfigError",
    "CheckPlan",
    "CheckStatusRow",
    "CheckTypeSpec",
    "CompiledCheck",
    "DataQualityCheck",
    "DataQualityCheckRun",
    "DataQualitySuiteRun",
    "Evaluation",
    "SubjectRef",
    "SubjectUnresolvableError",
    "UnknownCheckTypeError",
    "all_specs",
    "checks_for_subject",
    "compile_check",
    "compute_fingerprint",
    "denied_subject_names",
    "ensure_name_available",
    "from_config_entry",
    "get_spec",
    "is_subject_denied",
    "referenced_subject_names",
    "related_subject_ref",
    "resolve_subject",
    "roll_up_health",
    "soft_delete_check",
    "start_check_suite",
    "subject_health",
    "to_config_entry",
    "update_check",
    "upsert_check",
    "validate_check",
]
