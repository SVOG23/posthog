from typing import Optional, cast

from posthog.schema import (
    DataWarehouseSourceCategory,
    ExternalDataSourceType as SchemaExternalDataSourceType,
    ReleaseStatus,
    SourceConfig,
    SourceFieldInputConfig,
    SourceFieldInputConfigType,
)

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline.typings import (
    SourceInputs,
    SourceResponse,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.base import FieldType, ResumableSource
from products.warehouse_sources.backend.temporal.data_imports.sources.common.canonical_descriptions import (
    CanonicalDescriptions,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.registry import SourceRegistry
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.common.schema import SourceSchema
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.youtubeanalytics import (
    YouTubeAnalyticsSourceConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.youtube_analytics.settings import (
    DAY_INCREMENTAL_FIELDS,
    REQUIRED_SCOPE,
    REVISION_LOOKBACK_SECONDS,
    YOUTUBE_ANALYTICS_REPORTS,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.youtube_analytics.youtube_analytics import (
    YouTubeAnalyticsResumeConfig,
    validate_credentials as validate_youtube_analytics_credentials,
    youtube_analytics_source,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType


@SourceRegistry.register
class YouTubeAnalyticsSource(ResumableSource[YouTubeAnalyticsSourceConfig, YouTubeAnalyticsResumeConfig]):
    api_docs_url = "https://developers.google.com/youtube/analytics/reference/reports/query"
    supported_versions = ("v2",)
    default_version = "v2"

    lists_tables_without_credentials = True  # static report catalog — safe for public docs

    @property
    def source_type(self) -> ExternalDataSourceType:
        return ExternalDataSourceType.YOUTUBEANALYTICS

    def get_non_retryable_errors(self) -> dict[str, str | None]:
        credentials_message = (
            "Google rejected the OAuth credentials. Check the client ID, client secret and refresh token, "
            "then reconnect."
        )
        return {
            "Google rejected the OAuth credentials": credentials_message,
            "Google returned no access token": credentials_message,
            "400 Client Error: Bad Request for url: https://oauth2.googleapis.com/token": credentials_message,
            "401 Client Error: Unauthorized for url: https://oauth2.googleapis.com/token": credentials_message,
            "403 Client Error: Forbidden for url: https://youtubeanalytics.googleapis.com": (
                "The Google account has no access to this channel's analytics. Re-authorize with the "
                "yt-analytics.readonly scope using an account that manages the channel."
            ),
        }

    def get_canonical_descriptions(self) -> CanonicalDescriptions:
        from products.warehouse_sources.backend.temporal.data_imports.sources.youtube_analytics.canonical_descriptions import (
            CANONICAL_DESCRIPTIONS,
        )

        return CANONICAL_DESCRIPTIONS

    def get_schemas(
        self,
        config: YouTubeAnalyticsSourceConfig,
        team_id: int,
        with_counts: bool = False,
        names: list[str] | None = None,
        force_refresh: bool = False,
        api_version: str | None = None,
    ) -> list[SourceSchema]:
        schemas = [
            SourceSchema(
                name=report.name,
                supports_incremental=True,
                supports_append=False,
                incremental_fields=DAY_INCREMENTAL_FIELDS,
                # YouTube keeps restating the most recent days, so re-read a trailing window
                # every incremental run instead of freezing a day at its first-imported value.
                default_incremental_lookback_seconds=REVISION_LOOKBACK_SECONDS,
                detected_primary_keys=report.primary_keys,
            )
            for report in YOUTUBE_ANALYTICS_REPORTS.values()
        ]

        if names is not None:
            names_set = set(names)
            schemas = [schema for schema in schemas if schema.name in names_set]

        return schemas

    def validate_credentials(
        self,
        config: YouTubeAnalyticsSourceConfig,
        team_id: int,
        schema_name: Optional[str] = None,
        api_version: str | None = None,
    ) -> tuple[bool, str | None]:
        return validate_youtube_analytics_credentials(
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            channel_id=config.channel_id,
            api_version=self.resolve_api_version(api_version),
        )

    def get_resumable_source_manager(
        self, inputs: SourceInputs
    ) -> ResumableSourceManager[YouTubeAnalyticsResumeConfig]:
        return ResumableSourceManager[YouTubeAnalyticsResumeConfig](inputs, YouTubeAnalyticsResumeConfig)

    def source_for_pipeline(
        self,
        config: YouTubeAnalyticsSourceConfig,
        resumable_source_manager: ResumableSourceManager[YouTubeAnalyticsResumeConfig],
        inputs: SourceInputs,
    ) -> SourceResponse:
        return youtube_analytics_source(
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            channel_id=config.channel_id,
            start_date=config.start_date,
            endpoint=inputs.schema_name,
            api_version=self.resolve_api_version(inputs.api_version),
            logger=inputs.logger,
            resumable_source_manager=resumable_source_manager,
            should_use_incremental_field=inputs.should_use_incremental_field,
            db_incremental_field_last_value=inputs.db_incremental_field_last_value
            if inputs.should_use_incremental_field
            else None,
        )

    @property
    def get_source_config(self) -> SourceConfig:
        return SourceConfig(
            name=SchemaExternalDataSourceType.YOU_TUBE_ANALYTICS,
            category=DataWarehouseSourceCategory.ANALYTICS,
            label="YouTube Analytics",
            caption=(
                "Pull daily channel metrics — views, watch time, subscribers, traffic sources, geography and "
                "demographics — from the YouTube Analytics API.\n\n"
                "Create an OAuth client in the [Google Cloud console](https://console.cloud.google.com/apis/credentials) "
                "with the YouTube Analytics API enabled, authorize it as the Google account that manages your channel, "
                f"and grant the `{REQUIRED_SCOPE}` scope. Enter the client ID, client secret and the resulting "
                "refresh token below."
            ),
            docsUrl="https://posthog.com/docs/cdp/sources/youtube-analytics",
            iconPath="/static/services/youtube_analytics.png",
            releaseStatus=ReleaseStatus.ALPHA,
            keywords=["youtube", "yt"],
            fields=cast(
                list[FieldType],
                [
                    SourceFieldInputConfig(
                        name="client_id",
                        label="OAuth client ID",
                        type=SourceFieldInputConfigType.TEXT,
                        required=True,
                        placeholder="000000000000-xxxxxxxx.apps.googleusercontent.com",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="client_secret",
                        label="OAuth client secret",
                        type=SourceFieldInputConfigType.PASSWORD,
                        required=True,
                        placeholder="",
                        secret=True,
                    ),
                    SourceFieldInputConfig(
                        name="refresh_token",
                        label="Refresh token",
                        type=SourceFieldInputConfigType.PASSWORD,
                        required=True,
                        placeholder="",
                        secret=True,
                    ),
                    SourceFieldInputConfig(
                        name="channel_id",
                        label="Channel ID",
                        type=SourceFieldInputConfigType.TEXT,
                        required=False,
                        placeholder="Leave blank to use the authorized channel",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="start_date",
                        label="Start date",
                        type=SourceFieldInputConfigType.TEXT,
                        required=False,
                        placeholder="YYYY-MM-DD (defaults to the last 365 days)",
                        secret=False,
                    ),
                ],
            ),
        )
