from typing import Optional, cast

import structlog

from posthog.schema import (
    DataWarehouseSourceCategory,
    ExternalDataSourceType as SchemaExternalDataSourceType,
    ReleaseStatus,
    SourceConfig,
    SourceFieldInputConfig,
    SourceFieldInputConfigType,
    SourceFieldSelectConfig,
    SourceFieldSelectConfigOption,
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
from products.warehouse_sources.backend.temporal.data_imports.sources.common.schema import (
    SourceSchema,
    build_endpoint_schemas,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.instagram import (
    InstagramSourceConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.instagram.instagram import (
    AUTH_ERROR_PREFIX,
    PERMISSION_ERROR_PREFIX,
    InstagramResumeConfig,
    instagram_source,
    validate_credentials as validate_instagram_credentials,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.instagram.settings import (
    ENDPOINTS,
    INCREMENTAL_FIELDS,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType

logger = structlog.get_logger(__name__)


@SourceRegistry.register
class InstagramSource(ResumableSource[InstagramSourceConfig, InstagramResumeConfig]):
    api_docs_url = "https://developers.facebook.com/docs/instagram-platform"
    # Meta pins the Graph API by URL path segment and keeps each version alive for
    # roughly two years, so the pin is a real choice rather than a constant.
    supported_versions = ("v22.0", "v23.0")
    default_version = "v23.0"

    lists_tables_without_credentials = True

    @property
    def connection_host_fields(self) -> list[str]:
        # Both decide where the stored token is sent: `login_type` picks the Graph host and
        # `instagram_account_id` is spliced into the request path. Changing either while
        # reusing a preserved token could retarget it, so both force credential re-entry.
        return ["login_type", "instagram_account_id"]

    @property
    def source_type(self) -> ExternalDataSourceType:
        return ExternalDataSourceType.INSTAGRAM

    @property
    def get_source_config(self) -> SourceConfig:
        return SourceConfig(
            name=SchemaExternalDataSourceType.INSTAGRAM,
            category=DataWarehouseSourceCategory.COMMUNICATION,
            label="Instagram",
            caption="""Pull posts, stories, comments and insights from an Instagram professional (Business or Creator) account into the PostHog Data warehouse.

Create a Meta app, then generate a **long-lived access token** for the account you want to sync. Which login you use decides the rest:
- **Instagram Login** — the token comes from Instagram directly. Leave the account ID blank to sync the token's own account.
- **Facebook Login** — the token comes from the Facebook Page linked to the Instagram account, so you also need the Instagram account ID.

Grant `instagram_basic` and `instagram_manage_insights` (plus `pages_show_list` and `pages_read_engagement` for Facebook Login), or the insights tables stay empty. Fill in **App secret** only if your Meta app requires a proof of the app secret on API calls. Long-lived tokens expire after 60 days, so refresh yours and update this connection before then.""",
            iconPath="/static/services/instagram.png",
            docsUrl="https://posthog.com/docs/cdp/sources/instagram",
            keywords=["ig", "meta", "social"],
            releaseStatus=ReleaseStatus.ALPHA,
            fields=cast(
                list[FieldType],
                [
                    SourceFieldSelectConfig(
                        name="login_type",
                        label="Login type",
                        required=True,
                        defaultValue="instagram",
                        options=[
                            SourceFieldSelectConfigOption(label="Instagram Login", value="instagram"),
                            SourceFieldSelectConfigOption(label="Facebook Login", value="facebook"),
                        ],
                    ),
                    SourceFieldInputConfig(
                        name="access_token",
                        label="Access token",
                        type=SourceFieldInputConfigType.PASSWORD,
                        required=True,
                        placeholder="",
                        secret=True,
                    ),
                    SourceFieldInputConfig(
                        name="instagram_account_id",
                        label="Instagram account ID",
                        type=SourceFieldInputConfigType.TEXT,
                        required=False,
                        placeholder="17841400000000000",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="app_secret",
                        label="App secret",
                        type=SourceFieldInputConfigType.PASSWORD,
                        required=False,
                        placeholder="",
                        secret=True,
                    ),
                    SourceFieldInputConfig(
                        name="start_date",
                        label="Start date",
                        type=SourceFieldInputConfigType.TEXT,
                        required=False,
                        placeholder="2024-01-01",
                        secret=False,
                    ),
                ],
            ),
        )

    def get_non_retryable_errors(self) -> dict[str, str | None]:
        return {
            AUTH_ERROR_PREFIX: (
                "Your Instagram access token is invalid or has expired. Long-lived tokens last 60 days — "
                "generate a new one and reconnect."
            ),
            PERMISSION_ERROR_PREFIX: (
                "Your Instagram access token is missing the permissions this sync needs. Grant "
                "instagram_basic and instagram_manage_insights, then reconnect."
            ),
        }

    def get_canonical_descriptions(self) -> CanonicalDescriptions:
        from products.warehouse_sources.backend.temporal.data_imports.sources.instagram.canonical_descriptions import (  # noqa: PLC0415
            CANONICAL_DESCRIPTIONS,
        )

        return CANONICAL_DESCRIPTIONS

    def get_schemas(
        self,
        config: InstagramSourceConfig,
        team_id: int,
        with_counts: bool = False,
        names: list[str] | None = None,
        force_refresh: bool = False,
        api_version: str | None = None,
    ) -> list[SourceSchema]:
        return build_endpoint_schemas(ENDPOINTS, INCREMENTAL_FIELDS, names)

    def validate_credentials(
        self,
        config: InstagramSourceConfig,
        team_id: int,
        schema_name: Optional[str] = None,
        api_version: str | None = None,
    ) -> tuple[bool, str | None]:
        return validate_instagram_credentials(
            access_token=config.access_token,
            login_type=config.login_type,
            api_version=self.resolve_api_version(api_version),
            logger=logger,
            instagram_account_id=config.instagram_account_id,
            app_secret=config.app_secret,
        )

    def get_resumable_source_manager(self, inputs: SourceInputs) -> ResumableSourceManager[InstagramResumeConfig]:
        # Each endpoint checkpoints a URL built for its own edge, so a retry that lands on
        # a different table must not pick up the previous table's cursor.
        return ResumableSourceManager[InstagramResumeConfig](
            inputs, InstagramResumeConfig, namespace=inputs.schema_name
        )

    def source_for_pipeline(
        self,
        config: InstagramSourceConfig,
        resumable_source_manager: ResumableSourceManager[InstagramResumeConfig],
        inputs: SourceInputs,
    ) -> SourceResponse:
        return instagram_source(
            access_token=config.access_token,
            login_type=config.login_type,
            api_version=self.resolve_api_version(inputs.api_version),
            endpoint=inputs.schema_name,
            logger=inputs.logger,
            resumable_source_manager=resumable_source_manager,
            instagram_account_id=config.instagram_account_id,
            app_secret=config.app_secret,
            start_date=config.start_date,
            should_use_incremental_field=inputs.should_use_incremental_field,
            db_incremental_field_last_value=inputs.db_incremental_field_last_value
            if inputs.should_use_incremental_field
            else None,
        )
