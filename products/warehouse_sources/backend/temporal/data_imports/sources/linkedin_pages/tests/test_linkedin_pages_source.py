import json
import datetime as dt
from typing import Any, Optional

import pytest
from unittest import mock

from requests import Response

from posthog.schema import DataWarehouseSourceCategory, ReleaseStatus, SourceFieldInputConfig

from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.linkedinpages import (
    LinkedinPagesSourceConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.linkedin_pages import (
    LinkedinPagesClient,
    LinkedinPagesResumeConfig,
    LinkedinPagesTokenRefreshError,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.settings import (
    ENDPOINTS,
    LINKEDIN_PAGES_ENDPOINTS,
    EndpointKind,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.source import LinkedinPagesSource
from products.warehouse_sources.backend.types import ExternalDataSourceType

PROBE_PATCH = "products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.source.probe_credentials"
PIPELINE_PATCH = (
    "products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.source.linkedin_pages_source"
)
SESSION_PATCH = (
    "products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.linkedin_pages"
    ".make_tracked_session"
)

STATS_ENDPOINTS = sorted(
    name for name, config in LINKEDIN_PAGES_ENDPOINTS.items() if config.kind is EndpointKind.TIME_SERIES
)
FULL_REFRESH_ENDPOINTS = sorted(
    name for name, config in LINKEDIN_PAGES_ENDPOINTS.items() if config.kind is not EndpointKind.TIME_SERIES
)


def _inputs(schema_name: str = "page_statistics", **overrides: Any) -> mock.MagicMock:
    defaults: dict[str, Any] = {
        "schema_name": schema_name,
        "schema_id": "schema-1",
        "source_id": "source-1",
        "team_id": 1,
        "should_use_incremental_field": False,
        "db_incremental_field_last_value": None,
        "db_incremental_field_earliest_value": None,
        "incremental_field": None,
        "incremental_field_type": None,
        "job_id": "job-1",
        "logger": mock.MagicMock(),
        "reset_pipeline": False,
        "api_version": None,
    }
    defaults.update(overrides)
    return mock.MagicMock(**defaults)


def _error_from_status(status: int, body: dict[str, Any]) -> str:
    """Raise the client's own error for a status and return its message.

    Keeps `get_non_retryable_errors` honest: the keys have to match what the transport actually
    raises, not a message written from memory.
    """
    response = Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    session = mock.MagicMock()
    session.post.return_value = _ok_token()
    session.get.side_effect = [response, response]

    with mock.patch(SESSION_PATCH, return_value=session):
        client = LinkedinPagesClient("cid", "csecret", "rtoken")
        with pytest.raises(Exception) as excinfo:
            client.request("/organizationPageStatistics", {"q": "organization"})
    return str(excinfo.value)


def _ok_token() -> Response:
    response = Response()
    response.status_code = 200
    response._content = json.dumps({"access_token": "at_1"}).encode()
    return response


class TestLinkedinPagesSource:
    def setup_method(self) -> None:
        self.source = LinkedinPagesSource()
        self.team_id = 123
        self.config = LinkedinPagesSourceConfig(
            client_id="cid",
            client_secret="csecret",
            refresh_token="rtoken",
            organization_id=None,
        )

    def test_source_type(self) -> None:
        assert self.source.source_type == ExternalDataSourceType.LINKEDINPAGES

    def test_get_source_config_ships_released(self) -> None:
        config = self.source.get_source_config

        assert config.name.value == "LinkedinPages"
        assert config.label == "LinkedIn Pages"
        assert config.category == DataWarehouseSourceCategory.COMMUNICATION
        assert config.releaseStatus == ReleaseStatus.ALPHA
        assert not config.unreleasedSource
        assert config.iconPath == "/static/services/linkedin_pages.png"

    @pytest.mark.parametrize(
        "name, required, secret",
        [
            ("client_id", True, False),
            ("client_secret", True, True),
            ("refresh_token", True, True),
            ("organization_id", False, False),
        ],
    )
    def test_credential_fields(self, name: str, required: bool, secret: bool) -> None:
        fields = {
            field.name: field
            for field in self.source.get_source_config.fields
            if isinstance(field, SourceFieldInputConfig)
        }

        assert fields[name].required is required
        assert fields[name].secret is secret

    def test_api_version_metadata(self) -> None:
        assert self.source.default_version in self.source.supported_versions
        assert len(self.source.supported_versions) == 1
        # LinkedIn requires a dated version header on every request.
        assert self.source.default_version.isdigit()
        assert self.source.api_docs_url.startswith("https://")

    def test_get_schemas_lists_every_endpoint(self) -> None:
        schemas = self.source.get_schemas(self.config, self.team_id)

        assert {schema.name for schema in schemas} == set(ENDPOINTS)

    @pytest.mark.parametrize("name", STATS_ENDPOINTS)
    def test_statistics_endpoints_are_incremental_on_date(self, name: str) -> None:
        schema = next(s for s in self.source.get_schemas(self.config, self.team_id) if s.name == name)

        assert schema.supports_incremental is True
        assert {field["field"] for field in schema.incremental_fields} == {"date"}

    @pytest.mark.parametrize("name", FULL_REFRESH_ENDPOINTS)
    def test_endpoints_without_a_server_side_filter_are_full_refresh(self, name: str) -> None:
        schema = next(s for s in self.source.get_schemas(self.config, self.team_id) if s.name == name)

        assert schema.supports_incremental is False
        assert schema.incremental_fields == []

    @pytest.mark.parametrize(
        "names, expected",
        [
            (["posts"], {"posts"}),
            (["posts", "organizations"], {"posts", "organizations"}),
            (["nonexistent"], set()),
        ],
    )
    def test_get_schemas_filtered_by_names(self, names: list[str], expected: set[str]) -> None:
        assert {s.name for s in self.source.get_schemas(self.config, self.team_id, names=names)} == expected

    def test_canonical_descriptions_cover_every_endpoint(self) -> None:
        descriptions = self.source.get_canonical_descriptions()

        assert set(descriptions) == set(ENDPOINTS)

    @pytest.mark.parametrize(
        "status, body",
        [
            (401, {"serviceErrorCode": 65601, "message": "Invalid access token"}),
            (403, {"serviceErrorCode": 100, "message": "Not enough permissions"}),
            (404, {"code": "RESOURCE_NOT_FOUND", "message": "unknown organization"}),
        ],
    )
    def test_non_retryable_errors_match_what_the_transport_raises(self, status: int, body: dict[str, Any]) -> None:
        message = _error_from_status(status, body)

        assert any(key in message for key in self.source.get_non_retryable_errors())

    def test_token_refresh_failure_is_non_retryable(self) -> None:
        session = mock.MagicMock()
        failed_token = Response()
        failed_token.status_code = 400
        failed_token._content = b'{"error":"invalid_grant"}'
        session.post.return_value = failed_token

        with mock.patch(SESSION_PATCH, return_value=session):
            client = LinkedinPagesClient("cid", "csecret", "rtoken")
            with pytest.raises(LinkedinPagesTokenRefreshError) as excinfo:
                client.request("/organizations/1", {})

        assert any(key in str(excinfo.value) for key in self.source.get_non_retryable_errors())

    @pytest.mark.parametrize(
        "probe_result, schema_name, expected_valid",
        [
            ((True, 200), None, True),
            ((True, 200), "posts", True),
            ((False, 401), None, False),
            ((False, None), None, False),
            # A valid token missing the admin scope must not block source creation, but it does
            # block the table that needs it.
            ((False, 403), None, True),
            ((False, 403), "page_statistics", False),
        ],
    )
    def test_validate_credentials(
        self, probe_result: tuple[bool, Optional[int]], schema_name: Optional[str], expected_valid: bool
    ) -> None:
        with mock.patch(PROBE_PATCH, return_value=probe_result):
            is_valid, message = self.source.validate_credentials(self.config, self.team_id, schema_name=schema_name)

        assert is_valid is expected_valid
        assert (message is None) is expected_valid

    def test_validate_credentials_probes_with_the_resolved_api_version(self) -> None:
        with mock.patch(PROBE_PATCH, return_value=(True, 200)) as probe:
            self.source.validate_credentials(self.config, self.team_id)

        assert probe.call_args.kwargs["api_version"] == self.source.default_version

    def test_resumable_manager_is_namespaced_per_endpoint(self) -> None:
        manager = self.source.get_resumable_source_manager(_inputs("posts"))
        other = self.source.get_resumable_source_manager(_inputs("page_statistics"))

        assert isinstance(manager, ResumableSourceManager)
        assert manager._data_class is LinkedinPagesResumeConfig
        # A window start and a page token are not interchangeable, so the slots must differ.
        assert manager._key != other._key

    def test_source_for_pipeline_plumbs_arguments(self) -> None:
        inputs = _inputs(
            schema_name="page_statistics",
            should_use_incremental_field=True,
            db_incremental_field_last_value=dt.date(2026, 5, 1),
        )
        manager = mock.MagicMock()

        with mock.patch(PIPELINE_PATCH) as pipeline:
            self.source.source_for_pipeline(self.config, manager, inputs)

        kwargs = pipeline.call_args.kwargs
        assert kwargs["client_id"] == "cid"
        assert kwargs["refresh_token"] == "rtoken"
        assert kwargs["endpoint"] == "page_statistics"
        assert kwargs["resumable_source_manager"] is manager
        assert kwargs["should_use_incremental_field"] is True
        assert kwargs["db_incremental_field_last_value"] == dt.date(2026, 5, 1)
        assert kwargs["api_version"] == self.source.default_version

    def test_source_for_pipeline_drops_the_watermark_on_a_full_refresh(self) -> None:
        inputs = _inputs(should_use_incremental_field=False, db_incremental_field_last_value=dt.date(2026, 5, 1))

        with mock.patch(PIPELINE_PATCH) as pipeline:
            self.source.source_for_pipeline(self.config, mock.MagicMock(), inputs)

        assert pipeline.call_args.kwargs["db_incremental_field_last_value"] is None

    def test_documented_tables_are_published_without_credentials(self) -> None:
        # The endpoint catalog is static, so posthog.com can render it.
        assert self.source.lists_tables_without_credentials is True

        tables = self.source.get_documented_tables()

        assert {table["name"] for table in tables} == set(ENDPOINTS)
