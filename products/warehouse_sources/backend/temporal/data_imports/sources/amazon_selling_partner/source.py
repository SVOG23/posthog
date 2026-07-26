from typing import Optional, cast

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
from products.warehouse_sources.backend.temporal.data_imports.sources.amazon_selling_partner.amazon_selling_partner import (
    AmazonSellingPartnerResumeConfig,
    amazon_selling_partner_source,
    validate_credentials as validate_amazon_selling_partner_credentials,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.amazon_selling_partner.settings import (
    ENDPOINTS,
    INCREMENTAL_FIELDS,
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
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.amazonsellingpartner import (
    AmazonSellingPartnerSourceConfig,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType


@SourceRegistry.register
class AmazonSellingPartnerSource(ResumableSource[AmazonSellingPartnerSourceConfig, AmazonSellingPartnerResumeConfig]):
    # Every SP-API operation carries its own version (orders v0, finances 2024-06-19,
    # reports 2021-06-30), so there is no single version to pin at the source level.
    api_docs_url = "https://developer-docs.amazon.com/sp-api/docs/welcome"

    lists_tables_without_credentials = True  # static endpoint catalog — safe for public docs

    @property
    def source_type(self) -> ExternalDataSourceType:
        return ExternalDataSourceType.AMAZONSELLINGPARTNER

    def get_non_retryable_errors(self) -> dict[str, str | None]:
        return {
            "400 Client Error: Bad Request for url: https://api.amazon.com/auth/o2/token": "Amazon rejected your Login with Amazon credentials. The refresh token may have been revoked — reauthorize the app in Seller Central and reconnect.",
            "401 Client Error: Unauthorized for url: https://api.amazon.com/auth/o2/token": "Amazon could not issue an access token. Check your Login with Amazon client ID and secret.",
            "401 Client Error: Unauthorized for url: https://sellingpartnerapi-": "Amazon rejected the access token. Reauthorize the app in Seller Central and reconnect.",
            "403 Client Error: Forbidden for url: https://sellingpartnerapi-": "Amazon denied access to this data. Check that your app has the roles this table needs, and that the seller has authorized them.",
        }

    def get_retryable_errors(self) -> set[str]:
        return {"Amazon Selling Partner API error (retryable)"}

    @property
    def get_source_config(self) -> SourceConfig:
        return SourceConfig(
            name=SchemaExternalDataSourceType.AMAZON_SELLING_PARTNER,
            category=DataWarehouseSourceCategory.E_COMMERCE,
            label="Amazon Selling Partner",
            caption="""Connect your Amazon seller account to pull orders, financial transactions, FBA inventory, and sales and traffic data into the PostHog Data warehouse.

You need a registered Selling Partner API app. A private app can self-authorize from Seller Central, which gives you the client ID, client secret, and refresh token to paste here — no redirect needed. Pick the region your seller account belongs to, and list the marketplace IDs you want to sync separated by commas.""",
            iconPath="/static/services/amazon_selling_partner.png",
            docsUrl="https://posthog.com/docs/cdp/sources/amazon-selling-partner",
            releaseStatus=ReleaseStatus.ALPHA,
            keywords=["sp-api", "amazon seller", "seller central", "fba"],
            fields=cast(
                list[FieldType],
                [
                    SourceFieldSelectConfig(
                        name="region",
                        label="Region",
                        required=True,
                        defaultValue="na",
                        options=[
                            SourceFieldSelectConfigOption(label="North America", value="na"),
                            SourceFieldSelectConfigOption(label="Europe", value="eu"),
                            SourceFieldSelectConfigOption(label="Far East", value="fe"),
                        ],
                    ),
                    SourceFieldInputConfig(
                        name="marketplace_ids",
                        label="Marketplace IDs",
                        type=SourceFieldInputConfigType.TEXT,
                        required=True,
                        placeholder="ATVPDKIKX0DER",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="client_id",
                        label="LWA client ID",
                        type=SourceFieldInputConfigType.TEXT,
                        required=True,
                        placeholder="amzn1.application-oa2-client...",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="client_secret",
                        label="LWA client secret",
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
                        placeholder="Atzr|...",
                        secret=True,
                    ),
                ],
            ),
        )

    def get_canonical_descriptions(self) -> CanonicalDescriptions:
        from products.warehouse_sources.backend.temporal.data_imports.sources.amazon_selling_partner.canonical_descriptions import (
            CANONICAL_DESCRIPTIONS,
        )

        return CANONICAL_DESCRIPTIONS

    def get_schemas(
        self,
        config: AmazonSellingPartnerSourceConfig,
        team_id: int,
        with_counts: bool = False,
        names: list[str] | None = None,
        force_refresh: bool = False,
        api_version: str | None = None,
    ) -> list[SourceSchema]:
        return build_endpoint_schemas(ENDPOINTS, INCREMENTAL_FIELDS, names)

    def validate_credentials(
        self,
        config: AmazonSellingPartnerSourceConfig,
        team_id: int,
        schema_name: Optional[str] = None,
        api_version: str | None = None,
    ) -> tuple[bool, str | None]:
        return validate_amazon_selling_partner_credentials(
            config.region, config.client_id, config.client_secret, config.refresh_token
        )

    def get_resumable_source_manager(
        self, inputs: SourceInputs
    ) -> ResumableSourceManager[AmazonSellingPartnerResumeConfig]:
        # Page tokens and report windows are incompatible cursors, so each schema keeps
        # its resume state in its own slot.
        return ResumableSourceManager[AmazonSellingPartnerResumeConfig](
            inputs, AmazonSellingPartnerResumeConfig
        ).with_namespace(inputs.schema_name)

    def source_for_pipeline(
        self,
        config: AmazonSellingPartnerSourceConfig,
        resumable_source_manager: ResumableSourceManager[AmazonSellingPartnerResumeConfig],
        inputs: SourceInputs,
    ) -> SourceResponse:
        return amazon_selling_partner_source(
            region=config.region,
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            marketplace_ids=config.marketplace_ids,
            endpoint=inputs.schema_name,
            logger=inputs.logger,
            resumable_source_manager=resumable_source_manager,
            should_use_incremental_field=inputs.should_use_incremental_field,
            db_incremental_field_last_value=inputs.db_incremental_field_last_value
            if inputs.should_use_incremental_field
            else None,
        )
