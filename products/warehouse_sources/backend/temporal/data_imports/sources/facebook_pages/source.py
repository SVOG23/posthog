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
from products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.facebook_pages import (
    AUTH_ERROR_PREFIX,
    PERMISSION_ERROR_PREFIX,
    FacebookPagesResumeConfig,
    facebook_pages_source,
    validate_credentials as validate_facebook_pages_credentials,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.settings import (
    DEFAULT_API_VERSION,
    ENDPOINTS,
    FACEBOOK_PAGES_ENDPOINTS,
    INCREMENTAL_FIELDS,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.facebookpages import (
    FacebookPagesSourceConfig,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType


@SourceRegistry.register
class FacebookPagesSource(ResumableSource[FacebookPagesSourceConfig, FacebookPagesResumeConfig]):
    supported_versions = (DEFAULT_API_VERSION,)
    default_version = DEFAULT_API_VERSION
    api_docs_url = "https://developers.facebook.com/docs/graph-api/"

    lists_tables_without_credentials = True  # static endpoint catalog — safe for public docs

    @property
    def source_type(self) -> ExternalDataSourceType:
        return ExternalDataSourceType.FACEBOOKPAGES

    @property
    def get_source_config(self) -> SourceConfig:
        return SourceConfig(
            name=SchemaExternalDataSourceType.FACEBOOK_PAGES,
            category=DataWarehouseSourceCategory.COMMUNICATION,
            label="Facebook Pages",
            keywords=["facebook", "meta", "graph api"],
            caption="""Pull your Facebook Page's profile, posts, videos, and daily insights into the PostHog Data warehouse.

You need a Meta app with the **pages_read_engagement**, **pages_read_user_content**, and **read_insights** permissions, plus an access token for someone who administers the Page.

1. In [Meta for Developers](https://developers.facebook.com/apps/), open your app and copy its **App ID** and **App secret** from **Settings → Basic**.
2. Generate a user access token in the [Graph API Explorer](https://developers.facebook.com/tools/explorer/), granting the permissions above.
3. Find your **Page ID** under **About → Page transparency** on the Page itself.

PostHog exchanges the token for a long-lived Page access token on every sync, so a short-lived token from the explorer works.""",
            iconPath="/static/services/facebook_pages.png",
            docsUrl="https://posthog.com/docs/cdp/sources/facebook-pages",
            releaseStatus=ReleaseStatus.ALPHA,
            fields=cast(
                list[FieldType],
                [
                    SourceFieldInputConfig(
                        name="page_id",
                        label="Page ID",
                        type=SourceFieldInputConfigType.TEXT,
                        required=True,
                        placeholder="123456789012345",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="app_id",
                        label="App ID",
                        type=SourceFieldInputConfigType.TEXT,
                        required=True,
                        placeholder="987654321098765",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="app_secret",
                        label="App secret",
                        type=SourceFieldInputConfigType.PASSWORD,
                        required=True,
                        placeholder="",
                        secret=True,
                    ),
                    SourceFieldInputConfig(
                        name="access_token",
                        label="Access token",
                        type=SourceFieldInputConfigType.PASSWORD,
                        required=True,
                        placeholder="",
                        secret=True,
                    ),
                ],
            ),
        )

    def get_non_retryable_errors(self) -> dict[str, str | None]:
        return {
            AUTH_ERROR_PREFIX: (
                "Facebook rejected the access token. It may have expired or been revoked — generate a new "
                "one for your Meta app and reconnect."
            ),
            PERMISSION_ERROR_PREFIX: (
                "Your Meta app is missing a permission needed to sync this table. Grant "
                "pages_read_engagement, pages_read_user_content, and read_insights, then reconnect."
            ),
        }

    def get_canonical_descriptions(self) -> CanonicalDescriptions:
        from products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.canonical_descriptions import (
            CANONICAL_DESCRIPTIONS,
        )

        return CANONICAL_DESCRIPTIONS

    def get_schemas(
        self,
        config: FacebookPagesSourceConfig,
        team_id: int,
        with_counts: bool = False,
        names: list[str] | None = None,
        force_refresh: bool = False,
        api_version: str | None = None,
    ) -> list[SourceSchema]:
        schemas = [
            SourceSchema(
                name=endpoint,
                supports_incremental=bool(INCREMENTAL_FIELDS.get(endpoint)),
                supports_append=False,
                incremental_fields=INCREMENTAL_FIELDS.get(endpoint, []),
                description=FACEBOOK_PAGES_ENDPOINTS[endpoint].description,
            )
            for endpoint in ENDPOINTS
        ]

        if names is not None:
            names_set = set(names)
            schemas = [s for s in schemas if s.name in names_set]

        return schemas

    def validate_credentials(
        self,
        config: FacebookPagesSourceConfig,
        team_id: int,
        schema_name: Optional[str] = None,
        api_version: str | None = None,
    ) -> tuple[bool, str | None]:
        return validate_facebook_pages_credentials(
            page_id=config.page_id,
            access_token=config.access_token,
            app_id=config.app_id,
            app_secret=config.app_secret,
            api_version=self.resolve_api_version(api_version),
            schema_name=schema_name,
        )

    def get_resumable_source_manager(self, inputs: SourceInputs) -> ResumableSourceManager[FacebookPagesResumeConfig]:
        return ResumableSourceManager[FacebookPagesResumeConfig](inputs, FacebookPagesResumeConfig)

    def source_for_pipeline(
        self,
        config: FacebookPagesSourceConfig,
        resumable_source_manager: ResumableSourceManager[FacebookPagesResumeConfig],
        inputs: SourceInputs,
    ) -> SourceResponse:
        return facebook_pages_source(
            page_id=config.page_id,
            access_token=config.access_token,
            app_id=config.app_id,
            app_secret=config.app_secret,
            endpoint=inputs.schema_name,
            logger=inputs.logger,
            resumable_source_manager=resumable_source_manager,
            api_version=self.resolve_api_version(inputs.api_version),
            should_use_incremental_field=inputs.should_use_incremental_field,
            db_incremental_field_last_value=inputs.db_incremental_field_last_value
            if inputs.should_use_incremental_field
            else None,
        )
