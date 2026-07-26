from typing import Any, Optional

import pytest
from unittest import mock

import structlog

from posthog.schema import (
    DataWarehouseSourceCategory,
    ReleaseStatus,
    SourceFieldInputConfig,
    SourceFieldInputConfigType,
    SourceFieldSelectConfig,
)

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline.typings import SourceInputs
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.instagram import (
    InstagramSourceConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.instagram.instagram import (
    AUTH_ERROR_PREFIX,
    PERMISSION_ERROR_PREFIX,
    InstagramResumeConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.instagram.settings import (
    ENDPOINTS,
    INSTAGRAM_ENDPOINTS,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.instagram.source import InstagramSource
from products.warehouse_sources.backend.types import ExternalDataSourceType

SOURCE_MODULE = "products.warehouse_sources.backend.temporal.data_imports.sources.instagram.source"


def _inputs(schema_name: str = "media", **overrides: Any) -> SourceInputs:
    defaults: dict[str, Any] = {
        "schema_name": schema_name,
        "schema_id": "schema-id",
        "source_id": "source-id",
        "team_id": 1,
        "should_use_incremental_field": False,
        "db_incremental_field_last_value": None,
        "db_incremental_field_earliest_value": None,
        "incremental_field": None,
        "incremental_field_type": None,
        "job_id": "job-id",
        "logger": structlog.get_logger("instagram-tests"),
        "reset_pipeline": False,
    }
    defaults.update(overrides)
    return SourceInputs(**defaults)


class TestInstagramSource:
    def setup_method(self) -> None:
        self.source = InstagramSource()
        self.team_id = 1
        self.config = InstagramSourceConfig(access_token="tok")

    def test_source_type(self) -> None:
        assert self.source.source_type == ExternalDataSourceType.INSTAGRAM

    def test_the_source_ships_visible_and_documented(self) -> None:
        config = self.source.get_source_config

        assert config.name.value == "Instagram"
        assert config.label == "Instagram"
        assert config.category == DataWarehouseSourceCategory.COMMUNICATION
        assert config.releaseStatus == ReleaseStatus.ALPHA
        assert config.unreleasedSource is None
        assert config.iconPath == "/static/services/instagram.png"
        assert config.docsUrl == "https://posthog.com/docs/cdp/sources/instagram"

    def test_source_fields(self) -> None:
        config = self.source.get_source_config

        assert [field.name for field in config.fields] == [
            "login_type",
            "access_token",
            "instagram_account_id",
            "app_secret",
            "start_date",
        ]

    def test_login_type_offers_both_meta_hosts(self) -> None:
        login_type = next(f for f in self.source.get_source_config.fields if f.name == "login_type")

        assert isinstance(login_type, SourceFieldSelectConfig)
        assert login_type.defaultValue == "instagram"
        assert {option.value for option in login_type.options} == {"instagram", "facebook"}

    @pytest.mark.parametrize("field_name,required", [("access_token", True), ("app_secret", False)])
    def test_credentials_are_stored_as_secrets(self, field_name: str, required: bool) -> None:
        field = next(
            f
            for f in self.source.get_source_config.fields
            if isinstance(f, SourceFieldInputConfig) and f.name == field_name
        )

        assert field.type == SourceFieldInputConfigType.PASSWORD
        assert field.secret is True
        assert field.required is required

    def test_the_graph_api_version_is_pinned_to_something_the_code_calls(self) -> None:
        assert self.source.default_version in self.source.supported_versions
        assert self.source.resolve_api_version(None) == "v23.0"
        assert self.source.api_docs_url is not None and self.source.api_docs_url.startswith("https://")

    def test_get_schemas_lists_every_endpoint(self) -> None:
        schemas = self.source.get_schemas(self.config, self.team_id)

        assert [schema.name for schema in schemas] == list(ENDPOINTS)

    def test_get_schemas_honors_the_picker_filter(self) -> None:
        schemas = self.source.get_schemas(self.config, self.team_id, names=["media", "account"])

        assert {schema.name for schema in schemas} == {"media", "account"}

    @pytest.mark.parametrize(
        "endpoint,incremental,field_name",
        [
            ("media", True, "timestamp"),
            ("account_insights", True, "date"),
            ("account", False, None),
            ("stories", False, None),
            ("media_comments", False, None),
            ("media_insights", False, None),
        ],
    )
    def test_only_endpoints_with_a_server_side_time_filter_sync_incrementally(
        self, endpoint: str, incremental: bool, field_name: Optional[str]
    ) -> None:
        schema = next(s for s in self.source.get_schemas(self.config, self.team_id) if s.name == endpoint)

        assert schema.supports_incremental is incremental
        assert [f["field"] for f in schema.incremental_fields] == ([field_name] if field_name else [])

    def test_fan_out_tables_key_on_the_parent_so_rows_stay_unique_table_wide(self) -> None:
        assert INSTAGRAM_ENDPOINTS["media_comments"].primary_keys == ["media_id", "id"]
        assert INSTAGRAM_ENDPOINTS["media_insights"].primary_keys == ["media_id", "metric"]

    def test_the_table_catalog_is_published_without_credentials(self) -> None:
        assert self.source.lists_tables_without_credentials is True

        documented = {table["name"] for table in self.source.get_documented_tables()}
        assert documented == set(ENDPOINTS)

    def test_every_endpoint_is_described_from_the_meta_docs(self) -> None:
        descriptions = self.source.get_canonical_descriptions()

        assert set(descriptions) == set(ENDPOINTS)
        assert all(descriptions[name].get("columns") for name in ENDPOINTS)

    @pytest.mark.parametrize(
        "observed_error",
        [
            f"{AUTH_ERROR_PREFIX}: status=400, code=190, message=Session has expired",
            f"{PERMISSION_ERROR_PREFIX}: status=400, code=10, message=Application does not have permission",
        ],
    )
    def test_auth_and_scope_failures_stop_the_source_instead_of_retrying(self, observed_error: str) -> None:
        assert any(key in observed_error for key in self.source.get_non_retryable_errors())

    def test_a_throttling_error_is_left_retryable(self) -> None:
        observed_error = "Instagram API error (retryable): status=429, code=4, message=rate limited"

        assert not any(key in observed_error for key in self.source.get_non_retryable_errors())

    def test_validate_credentials_passes_the_form_values_through(self) -> None:
        config = InstagramSourceConfig(
            access_token="tok",
            login_type="facebook",
            instagram_account_id="17841",
            app_secret="s3cret",
        )

        with mock.patch(f"{SOURCE_MODULE}.validate_instagram_credentials", return_value=(True, None)) as validate:
            assert self.source.validate_credentials(config, self.team_id) == (True, None)

        assert validate.call_args.kwargs["access_token"] == "tok"
        assert validate.call_args.kwargs["login_type"] == "facebook"
        assert validate.call_args.kwargs["instagram_account_id"] == "17841"
        assert validate.call_args.kwargs["app_secret"] == "s3cret"
        assert validate.call_args.kwargs["api_version"] == "v23.0"

    def test_validate_credentials_surfaces_the_failure_message(self) -> None:
        with mock.patch(f"{SOURCE_MODULE}.validate_instagram_credentials", return_value=(False, "bad token")):
            assert self.source.validate_credentials(self.config, self.team_id) == (False, "bad token")

    def test_the_resume_manager_is_isolated_per_table(self) -> None:
        media = self.source.get_resumable_source_manager(_inputs("media"))
        comments = self.source.get_resumable_source_manager(_inputs("media_comments"))

        assert isinstance(media, ResumableSourceManager)
        assert media._data_class is InstagramResumeConfig
        # Each table checkpoints a URL built for its own edge, so the Redis slots differ.
        assert media._key != comments._key

    def test_source_for_pipeline_forwards_the_incremental_watermark(self) -> None:
        config = InstagramSourceConfig(access_token="tok", instagram_account_id="17841", start_date="2024-01-01")
        inputs = _inputs(
            "media",
            should_use_incremental_field=True,
            db_incremental_field_last_value="2024-06-01T00:00:00+0000",
            api_version="v22.0",
        )

        with mock.patch(f"{SOURCE_MODULE}.instagram_source") as build_source:
            self.source.source_for_pipeline(config, self.source.get_resumable_source_manager(inputs), inputs)

        kwargs = build_source.call_args.kwargs
        assert kwargs["endpoint"] == "media"
        assert kwargs["api_version"] == "v22.0"
        assert kwargs["instagram_account_id"] == "17841"
        assert kwargs["start_date"] == "2024-01-01"
        assert kwargs["should_use_incremental_field"] is True
        assert kwargs["db_incremental_field_last_value"] == "2024-06-01T00:00:00+0000"

    def test_a_full_refresh_run_never_forwards_a_watermark(self) -> None:
        inputs = _inputs("media", should_use_incremental_field=False, db_incremental_field_last_value="2024-06-01")

        with mock.patch(f"{SOURCE_MODULE}.instagram_source") as build_source:
            self.source.source_for_pipeline(self.config, self.source.get_resumable_source_manager(inputs), inputs)

        assert build_source.call_args.kwargs["db_incremental_field_last_value"] is None
