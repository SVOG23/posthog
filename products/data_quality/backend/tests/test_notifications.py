from uuid import uuid4

from posthog.test.base import BaseTest
from unittest.mock import patch

from parameterized import parameterized

from posthog.constants import AvailableFeature
from posthog.models import User

from products.data_modeling.backend.facade.models import DataWarehouseSavedQuery
from products.data_quality.backend.facade.enums import (
    CheckRunStatus,
    CheckSeverity,
    CheckType,
    SubjectType,
    SuiteRunTrigger,
)
from products.data_quality.backend.logic.notifications import _QueryAccessResolver
from products.data_quality.backend.logic.runner import run_check
from products.data_quality.backend.models import DataQualityCheck, DataQualitySuiteRun
from products.notifications.backend.facade.enums import TargetType

RUNNER_QUERY = "products.data_quality.backend.logic.runner.execute_hogql_query"
CREATE_NOTIFICATION = "products.data_quality.backend.logic.notifications.create_notification"


class _Response:
    def __init__(self, failure_count: int) -> None:
        self.columns = ["failure_count", "observed_value"]
        self.results = [[failure_count, failure_count]]


class TestDataQualityNotifications(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.view = DataWarehouseSavedQuery.objects.create(team=self.team, name="orders", query={"kind": "HogQLQuery"})
        self.suite_run = DataQualitySuiteRun.objects.for_team(self.team.id).create(
            team=self.team, trigger=SuiteRunTrigger.MANUAL
        )

    def _check(self, **kwargs) -> DataQualityCheck:
        defaults = {
            "team": self.team,
            "subject_type": SubjectType.VIEW,
            "subject_uuid": self.view.id,
            "subject_name": "orders",
            "check_type": CheckType.NOT_NULL,
            "column_name": "customer_id",
            "fingerprint": uuid4().hex,
        }
        return DataQualityCheck.objects.for_team(self.team.id).create(**{**defaults, **kwargs})

    @parameterized.expand(
        [
            ("first_failure", "", CheckSeverity.ERROR, 3, 1),
            ("still_failing", CheckRunStatus.FAILED, CheckSeverity.ERROR, 3, 0),
            ("recovered_then_failed_again", CheckRunStatus.PASSED, CheckSeverity.ERROR, 3, 1),
            ("warn_severity_failure", "", CheckSeverity.WARN, 3, 0),
            ("passing", "", CheckSeverity.ERROR, 0, 0),
            ("recovery", CheckRunStatus.FAILED, CheckSeverity.ERROR, 0, 0),
        ]
    )
    def test_only_a_pass_to_fail_edge_on_an_error_check_notifies(
        self, _name, previous_status: str, severity: CheckSeverity, failure_count: int, expected_calls: int
    ) -> None:
        check = self._check(last_status=previous_status, severity=severity)

        with patch(CREATE_NOTIFICATION) as create_notification:
            with patch(RUNNER_QUERY, return_value=_Response(failure_count)):
                run_check(check, self.suite_run, self.team)

        assert create_notification.call_count == expected_calls

    def test_the_notification_names_the_subject_and_the_failing_row_count(self) -> None:
        check = self._check()

        with patch(CREATE_NOTIFICATION) as create_notification:
            with patch(RUNNER_QUERY, return_value=_Response(4)):
                run_check(check, self.suite_run, self.team)

        payload = create_notification.call_args.args[0]
        assert payload.title == "Data quality check failed on orders"
        assert "4 failing rows" in payload.body
        assert payload.resource_id == str(check.id)

    def test_recipients_are_filtered_to_members_who_can_see_warehouse_objects(self) -> None:
        # The body names a table and column, so it must not reach members denied warehouse access.
        check = self._check()

        with patch(CREATE_NOTIFICATION) as create_notification:
            with patch(RUNNER_QUERY, return_value=_Response(4)):
                run_check(check, self.suite_run, self.team)

        assert create_notification.call_args.args[0].resource_type == "warehouse_objects"

    @patch("products.notifications.backend.resolvers.UserAccessControl")
    def test_members_without_query_access_do_not_get_the_failing_row_count(self, mock_uac_cls) -> None:
        # The body's failing-row count is a count oracle over warehouse rows the run-history API gates
        # behind query access, so a member with warehouse access but no query access must be dropped.
        self.organization.available_product_features = [{"key": AvailableFeature.ACCESS_CONTROL}]
        self.organization.save()
        denied = User.objects.create_and_join(self.organization, "no-query@test.com", "password")

        class FakeUAC:
            def __init__(self, user, team) -> None:
                self._user_id = user.id

            @property
            def access_controls_supported(self) -> bool:
                return True

            def check_access_level_for_resource(self, resource, level) -> bool:
                # Everyone can see warehouse objects; only the denied user lacks query access.
                return resource != "query" or self._user_id != denied.id

        mock_uac_cls.side_effect = FakeUAC

        resolved = _QueryAccessResolver(self.team).resolve(TargetType.TEAM, str(self.team.id), self.team.id)

        assert self.user.id in resolved
        assert denied.id not in resolved

    def test_a_notification_failure_does_not_fail_the_run(self) -> None:
        check = self._check()

        with patch(CREATE_NOTIFICATION, side_effect=RuntimeError("notifications down")):
            with patch(RUNNER_QUERY, return_value=_Response(4)):
                outcome = run_check(check, self.suite_run, self.team)

        assert outcome.status == CheckRunStatus.FAILED
