from types import SimpleNamespace
from typing import Any, cast

from unittest import mock

from parameterized import parameterized

from posthog.schema import ReleaseStatus, SourceFieldInputConfig, SourceFieldInputConfigType

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline.typings import SourceInputs
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.youtubeanalytics import (
    YouTubeAnalyticsSourceConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.youtube_analytics.settings import (
    CHANNEL_DAILY,
    DEMOGRAPHICS,
    ENDPOINTS,
    REVISION_LOOKBACK_SECONDS,
    TOP_VIDEOS,
    YOUTUBE_ANALYTICS_REPORTS,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.youtube_analytics.source import (
    YouTubeAnalyticsSource,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.youtube_analytics.youtube_analytics import (
    YouTubeAnalyticsResumeConfig,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType

MODULE = "products.warehouse_sources.backend.temporal.data_imports.sources.youtube_analytics.source"


def _config(**overrides: Any) -> YouTubeAnalyticsSourceConfig:
    values: dict[str, Any] = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "refresh_token": "refresh-token",
        "channel_id": None,
        "start_date": None,
    }
    values.update(overrides)
    return YouTubeAnalyticsSourceConfig(**values)


class TestYouTubeAnalyticsSource:
    def setup_method(self) -> None:
        self.source = YouTubeAnalyticsSource()
        self.team_id = 123
        self.config = _config()

    def test_source_type(self) -> None:
        assert self.source.source_type == ExternalDataSourceType.YOUTUBEANALYTICS

    def test_source_config_is_released_as_alpha(self) -> None:
        config = self.source.get_source_config

        assert config.releaseStatus == ReleaseStatus.ALPHA
        assert not config.unreleasedSource
        assert config.iconPath == "/static/services/youtube_analytics.png"
        assert config.docsUrl == "https://posthog.com/docs/cdp/sources/youtube-analytics"

    @parameterized.expand(
        [
            ("client_id", SourceFieldInputConfigType.TEXT, True, False),
            ("client_secret", SourceFieldInputConfigType.PASSWORD, True, True),
            ("refresh_token", SourceFieldInputConfigType.PASSWORD, True, True),
            ("channel_id", SourceFieldInputConfigType.TEXT, False, False),
            ("start_date", SourceFieldInputConfigType.TEXT, False, False),
        ]
    )
    def test_customer_supplied_oauth_fields(
        self, name: str, field_type: SourceFieldInputConfigType, required: bool, secret: bool
    ) -> None:
        inputs = [f for f in self.source.get_source_config.fields if isinstance(f, SourceFieldInputConfig)]
        field = next(f for f in inputs if f.name == name)

        assert field.type == field_type
        assert field.required is required
        assert field.secret is secret

    def test_pins_the_vendor_api_version_it_calls(self) -> None:
        assert self.source.supported_versions == ("v2",)
        assert self.source.default_version == "v2"
        assert (self.source.api_docs_url or "").startswith("https://")

    def test_get_schemas_returns_every_report(self) -> None:
        schemas = self.source.get_schemas(self.config, self.team_id)
        assert {schema.name for schema in schemas} == set(ENDPOINTS)

    @parameterized.expand([(name,) for name in ENDPOINTS])
    def test_every_report_is_incremental_on_day(self, endpoint: str) -> None:
        schema = next(s for s in self.source.get_schemas(self.config, self.team_id) if s.name == endpoint)

        assert schema.supports_incremental is True
        assert schema.supports_append is False
        assert [field["field"] for field in schema.incremental_fields] == ["day"]
        # YouTube restates recent days, so incremental runs must re-read a trailing window.
        assert schema.default_incremental_lookback_seconds == REVISION_LOOKBACK_SECONDS

    @parameterized.expand(
        [
            (CHANNEL_DAILY, ["day"]),
            (TOP_VIDEOS, ["day", "video"]),
            (DEMOGRAPHICS, ["day", "ageGroup", "gender"]),
        ]
    )
    def test_primary_keys_include_every_breakdown_dimension(self, endpoint: str, expected: list[str]) -> None:
        schema = next(s for s in self.source.get_schemas(self.config, self.team_id) if s.name == endpoint)
        assert schema.detected_primary_keys == expected

    def test_get_schemas_filtered_by_names(self) -> None:
        schemas = self.source.get_schemas(self.config, self.team_id, names=[TOP_VIDEOS])
        assert [schema.name for schema in schemas] == [TOP_VIDEOS]

    def test_get_schemas_filtered_unknown_name_returns_empty(self) -> None:
        assert self.source.get_schemas(self.config, self.team_id, names=["nonexistent"]) == []

    def test_lists_tables_without_credentials(self) -> None:
        # The report catalog is static, so public docs can render it without connecting.
        assert self.source.lists_tables_without_credentials is True

    def test_canonical_descriptions_cover_every_report(self) -> None:
        descriptions = self.source.get_canonical_descriptions()

        assert set(descriptions) == set(ENDPOINTS)
        for endpoint, entry in descriptions.items():
            columns = entry.get("columns") or {}
            expected_columns = {"day", *YOUTUBE_ANALYTICS_REPORTS[endpoint].dimensions}
            assert expected_columns <= set(columns)

    @parameterized.expand(
        [
            ("400 Client Error: Bad Request for url: https://oauth2.googleapis.com/token",),
            ("401 Client Error: Unauthorized for url: https://oauth2.googleapis.com/token",),
            ("403 Client Error: Forbidden for url: https://youtubeanalytics.googleapis.com",),
        ]
    )
    def test_auth_failures_are_non_retryable(self, expected_key: str) -> None:
        assert expected_key in self.source.get_non_retryable_errors()

    @parameterized.expand(
        [
            ("valid", (True, None), True, None),
            ("rejected", (False, "Google rejected the OAuth credentials."), False, "Google rejected the OAuth"),
        ]
    )
    def test_validate_credentials(
        self, _name: str, mock_return: tuple[bool, str | None], expected_valid: bool, expected_fragment: str | None
    ) -> None:
        with mock.patch(f"{MODULE}.validate_youtube_analytics_credentials") as mock_validate:
            mock_validate.return_value = mock_return

            is_valid, error = self.source.validate_credentials(_config(channel_id="UC123"), self.team_id)

        assert is_valid is expected_valid
        if expected_fragment is None:
            assert error is None
        else:
            assert error is not None and expected_fragment in error
        mock_validate.assert_called_once_with(
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
            channel_id="UC123",
            api_version="v2",
        )

    def test_get_resumable_source_manager_is_bound_to_resume_config(self) -> None:
        manager = self.source.get_resumable_source_manager(mock.MagicMock())

        assert isinstance(manager, ResumableSourceManager)
        assert manager._data_class is YouTubeAnalyticsResumeConfig

    def test_source_for_pipeline_plumbs_inputs(self) -> None:
        manager = mock.MagicMock(spec=ResumableSourceManager)
        logger = mock.MagicMock()
        inputs = SimpleNamespace(
            schema_name=CHANNEL_DAILY,
            team_id=self.team_id,
            job_id="job-1",
            logger=logger,
            api_version=None,
            should_use_incremental_field=True,
            incremental_field="day",
            db_incremental_field_last_value="2026-07-01",
        )

        with mock.patch(f"{MODULE}.youtube_analytics_source") as mock_source:
            response = self.source.source_for_pipeline(
                _config(channel_id="UC123", start_date="2026-01-01"), manager, cast(SourceInputs, inputs)
            )

        mock_source.assert_called_once_with(
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
            channel_id="UC123",
            start_date="2026-01-01",
            endpoint=CHANNEL_DAILY,
            api_version="v2",
            logger=logger,
            resumable_source_manager=manager,
            should_use_incremental_field=True,
            db_incremental_field_last_value="2026-07-01",
        )
        assert response is mock_source.return_value

    def test_source_for_pipeline_drops_watermark_on_full_refresh(self) -> None:
        manager = mock.MagicMock(spec=ResumableSourceManager)
        inputs = SimpleNamespace(
            schema_name=TOP_VIDEOS,
            team_id=self.team_id,
            job_id="job-2",
            logger=mock.MagicMock(),
            api_version=None,
            should_use_incremental_field=False,
            incremental_field=None,
            db_incremental_field_last_value="2026-07-01",
        )

        with mock.patch(f"{MODULE}.youtube_analytics_source") as mock_source:
            self.source.source_for_pipeline(self.config, manager, cast(SourceInputs, inputs))

        assert mock_source.call_args.kwargs["db_incremental_field_last_value"] is None

    def test_source_for_pipeline_honors_a_pinned_api_version(self) -> None:
        manager = mock.MagicMock(spec=ResumableSourceManager)
        inputs = SimpleNamespace(
            schema_name=CHANNEL_DAILY,
            team_id=self.team_id,
            job_id="job-3",
            logger=mock.MagicMock(),
            api_version="v3",
            should_use_incremental_field=False,
            incremental_field=None,
            db_incremental_field_last_value=None,
        )

        with mock.patch(f"{MODULE}.youtube_analytics_source") as mock_source:
            self.source.source_for_pipeline(self.config, manager, cast(SourceInputs, inputs))

        assert mock_source.call_args.kwargs["api_version"] == "v3"
