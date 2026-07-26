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
from products.warehouse_sources.backend.temporal.data_imports.sources.common.base import FieldType, ResumableSource
from products.warehouse_sources.backend.temporal.data_imports.sources.common.canonical_descriptions import (
    CanonicalDescriptions,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.registry import SourceRegistry
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.common.schema import SourceSchema
from products.warehouse_sources.backend.temporal.data_imports.sources.ebay.ebay import (
    EbayResumeConfig,
    check_endpoint_permissions as check_ebay_endpoint_permissions,
    ebay_source,
    validate_credentials as validate_ebay_credentials,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.ebay.settings import ENDPOINTS, INCREMENTAL_FIELDS
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.ebay import EbaySourceConfig
from products.warehouse_sources.backend.types import ExternalDataSourceType

MARKETPLACES: list[tuple[str, str]] = [
    ("EBAY_US", "United States"),
    ("EBAY_GB", "United Kingdom"),
    ("EBAY_DE", "Germany"),
    ("EBAY_AU", "Australia"),
    ("EBAY_CA", "Canada"),
    ("EBAY_FR", "France"),
    ("EBAY_IT", "Italy"),
    ("EBAY_ES", "Spain"),
    ("EBAY_NL", "Netherlands"),
    ("EBAY_BE", "Belgium"),
    ("EBAY_IE", "Ireland"),
    ("EBAY_AT", "Austria"),
    ("EBAY_CH", "Switzerland"),
    ("EBAY_PL", "Poland"),
    ("EBAY_HK", "Hong Kong"),
    ("EBAY_SG", "Singapore"),
    ("EBAY_MOTORS", "eBay Motors"),
]


@SourceRegistry.register
class EbaySource(ResumableSource[EbaySourceConfig, EbayResumeConfig]):
    api_docs_url = "https://developer.ebay.com/api-docs/static/ebay-rest-landing.html"

    lists_tables_without_credentials = True  # static endpoint catalog — safe for public docs

    @property
    def source_type(self) -> ExternalDataSourceType:
        return ExternalDataSourceType.EBAY

    @property
    def get_source_config(self) -> SourceConfig:
        return SourceConfig(
            name=SchemaExternalDataSourceType.EBAY,
            category=DataWarehouseSourceCategory.E_COMMERCE,
            label="eBay",
            caption="""Pull your eBay seller data — orders, monetary transactions, payouts and inventory — into the PostHog Data warehouse.

Seller data is only readable with a user token, so you need an eBay developer application and a one-time authorization from the selling account:

1. Create an application keyset in the [eBay developer program](https://developer.ebay.com/my/keys) and copy its **App ID (client ID)** and **Cert ID (client secret)**.
2. Run eBay's authorization code flow for the selling account and exchange the code for a **refresh token**. Grant the `sell.fulfillment.readonly`, `sell.finances` and `sell.inventory.readonly` scopes.
3. Paste all three values below, then pick the marketplace the account sells on.

Refresh tokens last 18 months, but eBay revokes them when the seller changes their password. Reconnect here if syncs start failing.""",
            iconPath="/static/services/ebay.png",
            docsUrl="https://posthog.com/docs/cdp/sources/ebay",
            releaseStatus=ReleaseStatus.ALPHA,
            keywords=["marketplace", "ebay seller"],
            fields=cast(
                list[FieldType],
                [
                    SourceFieldSelectConfig(
                        name="environment",
                        label="Environment",
                        required=True,
                        defaultValue="production",
                        options=[
                            SourceFieldSelectConfigOption(label="Production", value="production"),
                            SourceFieldSelectConfigOption(label="Sandbox", value="sandbox"),
                        ],
                    ),
                    SourceFieldInputConfig(
                        name="client_id",
                        label="App ID (client ID)",
                        type=SourceFieldInputConfigType.TEXT,
                        required=True,
                        placeholder="",
                        secret=False,
                    ),
                    SourceFieldInputConfig(
                        name="client_secret",
                        label="Cert ID (client secret)",
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
                        placeholder="v^1.1#...",
                        secret=True,
                    ),
                    SourceFieldSelectConfig(
                        name="marketplace_id",
                        label="Marketplace",
                        required=True,
                        defaultValue="EBAY_US",
                        options=[
                            SourceFieldSelectConfigOption(label=label, value=value) for value, label in MARKETPLACES
                        ],
                    ),
                ],
            ),
        )

    def get_non_retryable_errors(self) -> dict[str, str | None]:
        return {
            "400 Client Error: Bad Request for url: https://api.ebay.com/identity/v1/oauth2/token": (
                "eBay rejected your refresh token. It may have expired or been revoked. Reauthorize the selling "
                "account and paste the new refresh token."
            ),
            "401 Client Error: Unauthorized for url: https://api.ebay.com/identity/v1/oauth2/token": (
                "eBay rejected your App ID or Cert ID. Check the keyset in your eBay developer account and reconnect."
            ),
            "401 Client Error: Unauthorized for url: https://api.ebay.com": (
                "Your eBay authorization is no longer valid. Reauthorize the selling account and paste the new "
                "refresh token."
            ),
            "403 Client Error: Forbidden for url: https://api.ebay.com": (
                "Your eBay authorization is missing the scope needed for this data. Reauthorize the selling account "
                "granting sell.fulfillment.readonly, sell.finances and sell.inventory.readonly, then reconnect."
            ),
        }

    def get_retryable_errors(self) -> set[str]:
        # The tracked transport already backs off on eBay's per-app call limit before giving
        # up, so a surfaced 429 is transient rather than a failure worth alerting on.
        return {"429 Client Error"}

    def get_canonical_descriptions(self) -> CanonicalDescriptions:
        from products.warehouse_sources.backend.temporal.data_imports.sources.ebay.canonical_descriptions import (  # noqa: PLC0415
            CANONICAL_DESCRIPTIONS,
        )

        return CANONICAL_DESCRIPTIONS

    def get_schemas(
        self,
        config: EbaySourceConfig,
        team_id: int,
        with_counts: bool = False,
        names: list[str] | None = None,
        force_refresh: bool = False,
        api_version: str | None = None,
    ) -> list[SourceSchema]:
        schemas = [
            SourceSchema(
                name=endpoint,
                supports_incremental=len(INCREMENTAL_FIELDS[endpoint]) > 0,
                supports_append=len(INCREMENTAL_FIELDS[endpoint]) > 0,
                incremental_fields=INCREMENTAL_FIELDS[endpoint],
            )
            for endpoint in ENDPOINTS
        ]

        if names is not None:
            names_set = set(names)
            schemas = [s for s in schemas if s.name in names_set]

        return schemas

    def validate_credentials(
        self,
        config: EbaySourceConfig,
        team_id: int,
        schema_name: Optional[str] = None,
        api_version: str | None = None,
    ) -> tuple[bool, str | None]:
        is_valid, is_forbidden = validate_ebay_credentials(
            environment=config.environment,
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            marketplace_id=config.marketplace_id,
            schema_name=schema_name,
        )
        if is_valid:
            return True, None

        # A 403 means the token is genuine but that scope wasn't granted. Sellers commonly
        # authorize only the APIs they want, so accept it at source-create and reject it
        # only when validating a specific table.
        if is_forbidden and schema_name is None:
            return True, None

        if is_forbidden:
            return False, f"Your eBay authorization is missing the scope required to sync '{schema_name}'"

        return False, "Invalid eBay credentials"

    def get_endpoint_permissions(
        self, config: EbaySourceConfig, team_id: int, endpoints: list[str], api_version: str | None = None
    ) -> dict[str, str | None]:
        return check_ebay_endpoint_permissions(
            environment=config.environment,
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            marketplace_id=config.marketplace_id,
            endpoints=endpoints,
        )

    def get_resumable_source_manager(self, inputs: SourceInputs) -> ResumableSourceManager[EbayResumeConfig]:
        # Endpoints store incompatible cursors (filter window + offset vs parent offset),
        # so each keeps its resume state in its own slot.
        return ResumableSourceManager[EbayResumeConfig](inputs, EbayResumeConfig).with_namespace(inputs.schema_name)

    def source_for_pipeline(
        self,
        config: EbaySourceConfig,
        resumable_source_manager: ResumableSourceManager[EbayResumeConfig],
        inputs: SourceInputs,
    ) -> SourceResponse:
        return ebay_source(
            environment=config.environment,
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            marketplace_id=config.marketplace_id,
            endpoint=inputs.schema_name,
            logger=inputs.logger,
            resumable_source_manager=resumable_source_manager,
            should_use_incremental_field=inputs.should_use_incremental_field,
            db_incremental_field_last_value=inputs.db_incremental_field_last_value
            if inputs.should_use_incremental_field
            else None,
            incremental_field=inputs.incremental_field,
        )
