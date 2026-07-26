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
from products.warehouse_sources.backend.temporal.data_imports.sources.common.schema import (
    SourceSchema,
    build_endpoint_schemas,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.linkedinpages import (
    LinkedinPagesSourceConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.linkedin_pages import (
    LINKEDIN_API_VERSION,
    LinkedinPagesResumeConfig,
    linkedin_pages_source,
    probe_credentials,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.settings import (
    ENDPOINTS,
    INCREMENTAL_FIELDS,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType


@SourceRegistry.register
class LinkedinPagesSource(ResumableSource[LinkedinPagesSourceConfig, LinkedinPagesResumeConfig]):
    api_docs_url = "https://learn.microsoft.com/en-us/linkedin/marketing/community-management/organizations/organization-lookup-api"

    supported_versions = (LINKEDIN_API_VERSION,)
    default_version = LINKEDIN_API_VERSION

    lists_tables_without_credentials = True  # static endpoint catalog — safe for public docs

    @property
    def source_type(self) -> ExternalDataSourceType:
        return ExternalDataSourceType.LINKEDINPAGES

    @property
    def get_source_config(self) -> SourceConfig:
        return SourceConfig(
            name=SchemaExternalDataSourceType.LINKEDIN_PAGES,
            category=DataWarehouseSourceCategory.COMMUNICATION,
            keywords=["linkedin company pages", "linkedin organization"],
            label="LinkedIn Pages",
            caption="""Pull your LinkedIn company page statistics, follower growth and posts into the PostHog Data warehouse.

You need a LinkedIn app with access to the Community Management API, authorized by a member who administers the page. Grant the `rw_organization_admin` and `r_organization_social` scopes, then paste the app's client ID and secret along with a refresh token from that authorization.""",
            iconPath="/static/services/linkedin_pages.png",
            docsUrl="https://posthog.com/docs/cdp/sources/linkedin-pages",
            releaseStatus=ReleaseStatus.ALPHA,
            fields=cast(
                list[FieldType],
                [
                    SourceFieldInputConfig(
                        name="client_id",
                        label="Client ID",
                        type=SourceFieldInputConfigType.TEXT,
                        required=True,
                        placeholder="",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="client_secret",
                        label="Client secret",
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
                        name="organization_id",
                        label="Organization ID (optional)",
                        type=SourceFieldInputConfigType.TEXT,
                        required=False,
                        placeholder="Leave blank to sync every page you administer",
                        secret=False,
                    ),
                ],
            ),
        )

    def get_canonical_descriptions(self) -> CanonicalDescriptions:
        from products.warehouse_sources.backend.temporal.data_imports.sources.linkedin_pages.canonical_descriptions import (
            CANONICAL_DESCRIPTIONS,
        )

        return CANONICAL_DESCRIPTIONS

    def get_non_retryable_errors(self) -> dict[str, str | None]:
        return {
            "Failed to refresh the LinkedIn access token": "PostHog could not refresh your LinkedIn access token. Refresh tokens expire after a year — re-authorize the LinkedIn app and paste a new refresh token.",
            "LinkedIn returned no access token": "LinkedIn accepted the request but returned no access token. Re-authorize the LinkedIn app and paste a new refresh token.",
            "LinkedIn API error (401)": "LinkedIn rejected the credentials. Re-authorize the app and paste a new refresh token.",
            "LinkedIn API error (403)": "LinkedIn denied access. The authorized member must administer the page, and the app needs the rw_organization_admin and r_organization_social scopes.",
            # Stable LinkedIn error codes — none of these can be recovered by retrying.
            "REVOKED_ACCESS_TOKEN": "The LinkedIn access token was revoked. Re-authorize the app and paste a new refresh token.",
            "RESTRICTED_MEMBER": "LinkedIn has restricted the member who authorized this app, so PostHog can no longer read your page data. Resolve the restriction with LinkedIn, then re-authorize.",
            "RESOURCE_NOT_FOUND": "LinkedIn could not find the requested page. Check the organization ID, or leave it blank to sync every page the authorized member administers.",
        }

    def get_schemas(
        self,
        config: LinkedinPagesSourceConfig,
        team_id: int,
        with_counts: bool = False,
        names: list[str] | None = None,
        force_refresh: bool = False,
        api_version: str | None = None,
    ) -> list[SourceSchema]:
        # Only the statistics finders advertise incremental fields — see settings.py.
        return build_endpoint_schemas(ENDPOINTS, INCREMENTAL_FIELDS, names)

    def validate_credentials(
        self,
        config: LinkedinPagesSourceConfig,
        team_id: int,
        schema_name: Optional[str] = None,
        api_version: str | None = None,
    ) -> tuple[bool, str | None]:
        is_valid, status = probe_credentials(
            config.client_id,
            config.client_secret,
            config.refresh_token,
            api_version=self.resolve_api_version(api_version),
        )
        if is_valid:
            return True, None

        # A 403 means the token itself is genuine but this member lacks the organization-admin
        # scope. That is a per-table problem, so it must not block source creation.
        if status == 403:
            if schema_name is None:
                return True, None
            return False, (
                "LinkedIn denied access to this table. The authorized member must administer the page, "
                "and the app needs the rw_organization_admin and r_organization_social scopes."
            )

        return False, "Invalid LinkedIn credentials. Check the client ID, client secret and refresh token."

    def get_resumable_source_manager(self, inputs: SourceInputs) -> ResumableSourceManager[LinkedinPagesResumeConfig]:
        # Endpoints store different cursor shapes (a window start vs a page token), so keep their
        # resume slots apart — a retry that switches endpoints must not load the other's state.
        return ResumableSourceManager[LinkedinPagesResumeConfig](inputs, LinkedinPagesResumeConfig).with_namespace(
            inputs.schema_name
        )

    def source_for_pipeline(
        self,
        config: LinkedinPagesSourceConfig,
        resumable_source_manager: ResumableSourceManager[LinkedinPagesResumeConfig],
        inputs: SourceInputs,
    ) -> SourceResponse:
        return linkedin_pages_source(
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            organization_id=config.organization_id,
            endpoint=inputs.schema_name,
            resumable_source_manager=resumable_source_manager,
            logger=inputs.logger,
            should_use_incremental_field=inputs.should_use_incremental_field,
            db_incremental_field_last_value=inputs.db_incremental_field_last_value
            if inputs.should_use_incremental_field
            else None,
            api_version=self.resolve_api_version(inputs.api_version),
        )
