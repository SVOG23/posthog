from typing import Any

import pytest
from unittest import mock

from posthog.schema import ReleaseStatus, SourceFieldInputConfig, SourceFieldInputConfigType

from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.canonical_descriptions import (
    CANONICAL_DESCRIPTIONS,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.facebook_pages import (
    AUTH_ERROR_PREFIX,
    PERMISSION_ERROR_PREFIX,
    FacebookPagesResumeConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.settings import (
    DEFAULT_API_VERSION,
    ENDPOINTS,
    FACEBOOK_PAGES_ENDPOINTS,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.source import FacebookPagesSource
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.facebookpages import (
    FacebookPagesSourceConfig,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType

SOURCE_MODULE = "products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.source"


class TestFacebookPagesSource:
    def setup_method(self) -> None:
        self.source = FacebookPagesSource()
        self.team_id = 123
        self.config = FacebookPagesSourceConfig(
            page_id="123456789012345",
            app_id="987654321098765",
            app_secret="app-secret",
            access_token="user-token",
        )

    def test_source_type(self) -> None:
        assert self.source.source_type == ExternalDataSourceType.FACEBOOKPAGES

    def test_get_source_config(self) -> None:
        config = self.source.get_source_config

        assert config.name.value == "FacebookPages"
        assert config.unreleasedSource is None
        assert config.releaseStatus == ReleaseStatus.ALPHA
        assert config.iconPath == "/static/services/facebook_pages.png"
        assert [f.name for f in config.fields] == ["page_id", "app_id", "app_secret", "access_token"]

    @pytest.mark.parametrize(
        "field_name, field_type, secret",
        [
            ("page_id", SourceFieldInputConfigType.TEXT, False),
            ("app_id", SourceFieldInputConfigType.TEXT, False),
            ("app_secret", SourceFieldInputConfigType.PASSWORD, True),
            ("access_token", SourceFieldInputConfigType.PASSWORD, True),
        ],
    )
    def test_credential_fields_are_marked_secret(
        self, field_name: str, field_type: SourceFieldInputConfigType, secret: bool
    ) -> None:
        fields = {f.name: f for f in self.source.get_source_config.fields}
        field = fields[field_name]

        assert isinstance(field, SourceFieldInputConfig)
        assert field.type == field_type
        assert field.secret is secret
        assert field.required is True

    def test_api_version_metadata(self) -> None:
        assert self.source.supported_versions == (DEFAULT_API_VERSION,)
        assert self.source.default_version == DEFAULT_API_VERSION
        assert (self.source.api_docs_url or "").startswith("https://")

    def test_lists_tables_without_credentials(self) -> None:
        # get_schemas iterates a static catalog with no I/O, so public docs can render the tables.
        assert self.source.lists_tables_without_credentials is True

    @pytest.mark.parametrize("expected_key", [AUTH_ERROR_PREFIX, PERMISSION_ERROR_PREFIX])
    def test_non_retryable_errors(self, expected_key: str) -> None:
        assert expected_key in self.source.get_non_retryable_errors()

    def test_get_schemas_returns_all_endpoints(self) -> None:
        schemas = self.source.get_schemas(self.config, self.team_id)

        assert {s.name for s in schemas} == set(ENDPOINTS)

    @pytest.mark.parametrize(
        "endpoint, incremental, incremental_field",
        [
            ("page", False, None),
            ("posts", True, "created_time"),
            ("videos", True, "created_time"),
            ("page_insights", True, "end_time"),
        ],
    )
    def test_schema_sync_modes(self, endpoint: str, incremental: bool, incremental_field: str | None) -> None:
        schemas = {s.name: s for s in self.source.get_schemas(self.config, self.team_id)}
        schema = schemas[endpoint]

        assert schema.supports_incremental is incremental
        assert schema.supports_append is False
        assert [f["field"] for f in schema.incremental_fields] == ([incremental_field] if incremental_field else [])

    def test_get_schemas_filtered_by_names(self) -> None:
        schemas = self.source.get_schemas(self.config, self.team_id, names=["posts", "nope"])

        assert [s.name for s in schemas] == ["posts"]

    def test_canonical_descriptions_cover_every_endpoint(self) -> None:
        descriptions = self.source.get_canonical_descriptions()

        assert set(descriptions) == set(ENDPOINTS)
        assert descriptions is CANONICAL_DESCRIPTIONS

    @pytest.mark.parametrize("endpoint", list(FACEBOOK_PAGES_ENDPOINTS))
    def test_canonical_descriptions_cover_the_primary_keys(self, endpoint: str) -> None:
        # The primary key columns are the ones the AI agent most needs described.
        columns = self.source.get_canonical_descriptions()[endpoint].get("columns", {})
        primary_keys = FACEBOOK_PAGES_ENDPOINTS[endpoint].primary_keys or []

        assert set(primary_keys) <= set(columns)

    @pytest.mark.parametrize("mock_return", [(True, None), (False, "Facebook rejected the access token.")])
    @mock.patch(f"{SOURCE_MODULE}.validate_facebook_pages_credentials")
    def test_validate_credentials(self, mock_validate: mock.MagicMock, mock_return: tuple[bool, str | None]) -> None:
        mock_validate.return_value = mock_return

        result = self.source.validate_credentials(self.config, self.team_id, schema_name="posts")

        assert result == mock_return
        mock_validate.assert_called_once_with(
            page_id=self.config.page_id,
            access_token=self.config.access_token,
            app_id=self.config.app_id,
            app_secret=self.config.app_secret,
            api_version=DEFAULT_API_VERSION,
            schema_name="posts",
        )

    def test_get_resumable_source_manager(self) -> None:
        manager = self.source.get_resumable_source_manager(mock.MagicMock())

        assert isinstance(manager, ResumableSourceManager)
        assert manager._data_class is FacebookPagesResumeConfig

    @mock.patch(f"{SOURCE_MODULE}.facebook_pages_source")
    def test_source_for_pipeline_plumbs_arguments(self, mock_source: mock.MagicMock) -> None:
        inputs = mock.MagicMock()
        inputs.schema_name = "posts"
        inputs.api_version = None
        inputs.should_use_incremental_field = True
        inputs.db_incremental_field_last_value = "2024-01-01T00:00:00+0000"
        manager = mock.MagicMock()

        self.source.source_for_pipeline(self.config, manager, inputs)

        kwargs: dict[str, Any] = dict(mock_source.call_args.kwargs)
        assert kwargs["page_id"] == self.config.page_id
        assert kwargs["app_id"] == self.config.app_id
        assert kwargs["app_secret"] == self.config.app_secret
        assert kwargs["access_token"] == self.config.access_token
        assert kwargs["endpoint"] == "posts"
        assert kwargs["api_version"] == DEFAULT_API_VERSION
        assert kwargs["resumable_source_manager"] is manager
        assert kwargs["db_incremental_field_last_value"] == "2024-01-01T00:00:00+0000"

    @mock.patch(f"{SOURCE_MODULE}.facebook_pages_source")
    def test_watermark_is_withheld_when_not_syncing_incrementally(self, mock_source: mock.MagicMock) -> None:
        inputs = mock.MagicMock()
        inputs.schema_name = "posts"
        inputs.api_version = None
        inputs.should_use_incremental_field = False
        inputs.db_incremental_field_last_value = "2024-01-01T00:00:00+0000"

        self.source.source_for_pipeline(self.config, mock.MagicMock(), inputs)

        assert mock_source.call_args.kwargs["db_incremental_field_last_value"] is None

    @mock.patch(f"{SOURCE_MODULE}.facebook_pages_source")
    def test_pinned_api_version_is_honored(self, mock_source: mock.MagicMock) -> None:
        inputs = mock.MagicMock()
        inputs.schema_name = "posts"
        inputs.api_version = "v21.0"
        inputs.should_use_incremental_field = False

        self.source.source_for_pipeline(self.config, mock.MagicMock(), inputs)

        assert mock_source.call_args.kwargs["api_version"] == "v21.0"
