from typing import Any, Optional

from unittest.mock import MagicMock, patch

from parameterized import parameterized

from posthog.schema import DataWarehouseSourceCategory, ReleaseStatus, SourceFieldInputConfig, SourceFieldSelectConfig

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline.typings import SourceInputs
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.ebay.ebay import EbayResumeConfig
from products.warehouse_sources.backend.temporal.data_imports.sources.ebay.settings import EBAY_ENDPOINTS, ENDPOINTS
from products.warehouse_sources.backend.temporal.data_imports.sources.ebay.source import EbaySource
from products.warehouse_sources.backend.temporal.data_imports.sources.generated_configs.ebay import EbaySourceConfig
from products.warehouse_sources.backend.types import ExternalDataSourceType

VALIDATE_PATH = "products.warehouse_sources.backend.temporal.data_imports.sources.ebay.source.validate_ebay_credentials"
SOURCE_PATH = "products.warehouse_sources.backend.temporal.data_imports.sources.ebay.source.ebay_source"


def _config(environment: str = "production", marketplace_id: str = "EBAY_US") -> EbaySourceConfig:
    return EbaySourceConfig.from_dict(
        {
            "environment": environment,
            "client_id": "app-id",
            "client_secret": "cert-id",
            "refresh_token": "refresh",
            "marketplace_id": marketplace_id,
        }
    )


def _inputs(
    schema_name: str = "orders",
    should_use_incremental_field: bool = False,
    last_value: Any = None,
    incremental_field: Optional[str] = None,
) -> SourceInputs:
    return SourceInputs(
        schema_name=schema_name,
        schema_id="schema-1",
        source_id="source-1",
        team_id=1,
        should_use_incremental_field=should_use_incremental_field,
        db_incremental_field_last_value=last_value,
        db_incremental_field_earliest_value=None,
        incremental_field=incremental_field,
        incremental_field_type=None,
        job_id="job-1",
        logger=MagicMock(),
        reset_pipeline=False,
    )


class TestEbaySource:
    def test_source_type(self) -> None:
        assert EbaySource().source_type == ExternalDataSourceType.EBAY

    def test_source_config_shape(self) -> None:
        config = EbaySource().get_source_config
        # A finished source must be visible: unreleasedSource hides the connector entirely.
        assert config.unreleasedSource is None
        assert config.releaseStatus == ReleaseStatus.ALPHA
        assert config.category == DataWarehouseSourceCategory.E_COMMERCE
        assert config.docsUrl == "https://posthog.com/docs/cdp/sources/ebay"
        assert [f.name for f in config.fields] == [
            "environment",
            "client_id",
            "client_secret",
            "refresh_token",
            "marketplace_id",
        ]

    @parameterized.expand([("client_secret", True), ("refresh_token", True), ("client_id", False)])
    def test_credential_fields_secrecy(self, name: str, expected_secret: bool) -> None:
        field = next(f for f in EbaySource().get_source_config.fields if f.name == name)
        assert isinstance(field, SourceFieldInputConfig)
        assert field.secret is expected_secret

    def test_environment_options_match_the_transport_hosts(self) -> None:
        # An option the transport can't resolve to a host would fail every sync for that choice.
        from products.warehouse_sources.backend.temporal.data_imports.sources.ebay.settings import EBAY_HOSTS

        field = next(f for f in EbaySource().get_source_config.fields if f.name == "environment")
        assert isinstance(field, SourceFieldSelectConfig)
        assert {option.value for option in field.options} == set(EBAY_HOSTS)

    def test_get_schemas_lists_every_endpoint_with_its_incremental_support(self) -> None:
        schemas = EbaySource().get_schemas(_config(), team_id=1)
        assert [s.name for s in schemas] == list(ENDPOINTS)
        assert {s.name for s in schemas if s.supports_incremental} == {"orders", "transactions", "payouts"}
        orders = next(s for s in schemas if s.name == "orders")
        assert [f["field"] for f in orders.incremental_fields] == ["lastModifiedDate", "creationDate"]

    def test_get_schemas_filters_by_name(self) -> None:
        schemas = EbaySource().get_schemas(_config(), team_id=1, names=["payouts"])
        assert [s.name for s in schemas] == ["payouts"]

    def test_documented_tables_render_without_credentials(self) -> None:
        # Public docs call get_schemas with a placeholder config, so discovery must do no I/O.
        tables = EbaySource().get_documented_tables()
        assert {t["name"] for t in tables} == set(ENDPOINTS)
        assert all(t["description"] for t in tables)

    def test_canonical_descriptions_cover_every_endpoint(self) -> None:
        descriptions = EbaySource().get_canonical_descriptions()
        assert set(descriptions) == set(ENDPOINTS)
        for endpoint, entry in descriptions.items():
            columns = entry.get("columns") or {}
            # Every primary key must be documented, since it anchors the table's grain.
            assert set(EBAY_ENDPOINTS[endpoint].primary_keys) <= set(columns)

    @parameterized.expand(
        [
            ("valid", (True, False), None, (True, None)),
            # A seller who only granted some scopes must still be able to create the source.
            ("forbidden_at_create", (False, True), None, (True, None)),
            ("forbidden_for_schema", (False, True), "transactions", (False, None)),
            ("invalid", (False, False), None, (False, "Invalid eBay credentials")),
            ("invalid_for_schema", (False, False), "orders", (False, "Invalid eBay credentials")),
        ]
    )
    def test_validate_credentials(
        self,
        _name: str,
        probe_result: tuple[bool, bool],
        schema_name: Optional[str],
        expected: tuple[bool, Optional[str]],
    ) -> None:
        with patch(VALIDATE_PATH, return_value=probe_result):
            is_valid, message = EbaySource().validate_credentials(_config(), team_id=1, schema_name=schema_name)

        assert is_valid is expected[0]
        if expected[1] is not None:
            assert message == expected[1]
        elif is_valid:
            assert message is None
        else:
            assert message is not None and schema_name is not None and schema_name in message

    def test_get_endpoint_permissions_delegates_to_the_shared_probe(self) -> None:
        with patch(
            "products.warehouse_sources.backend.temporal.data_imports.sources.ebay.source"
            ".check_ebay_endpoint_permissions",
            return_value={"orders": None},
        ) as probe:
            assert EbaySource().get_endpoint_permissions(_config(), team_id=1, endpoints=["orders"]) == {"orders": None}

        assert probe.call_args.kwargs["endpoints"] == ["orders"]

    def test_resumable_manager_is_bound_to_the_resume_config_and_namespaced(self) -> None:
        # Windowed endpoints and the offers fan-out store incompatible cursors, so a shared
        # slot would let one endpoint load the other's state after a retry.
        manager = EbaySource().get_resumable_source_manager(_inputs("offers"))
        assert isinstance(manager, ResumableSourceManager)
        assert manager._data_class is EbayResumeConfig
        assert manager._namespace == "offers"

    def test_non_retryable_errors_cover_auth_and_scope_failures(self) -> None:
        errors = EbaySource().get_non_retryable_errors()
        assert any("identity/v1/oauth2/token" in key for key in errors)
        assert all(message for message in errors.values())

    def test_source_for_pipeline_passes_the_users_cursor_through(self) -> None:
        inputs = _inputs(
            "orders", should_use_incremental_field=True, last_value="2026-01-01", incremental_field="creationDate"
        )
        with patch(SOURCE_PATH) as mock_source:
            EbaySource().source_for_pipeline(_config(), MagicMock(), inputs)

        kwargs = mock_source.call_args.kwargs
        assert kwargs["endpoint"] == "orders"
        assert kwargs["incremental_field"] == "creationDate"
        assert kwargs["db_incremental_field_last_value"] == "2026-01-01"
        assert kwargs["marketplace_id"] == "EBAY_US"
        assert kwargs["environment"] == "production"

    def test_source_for_pipeline_drops_the_watermark_on_a_full_refresh(self) -> None:
        # Passing a stale watermark on a full refresh would filter rows out of a run the
        # user asked to be complete.
        inputs = _inputs("orders", should_use_incremental_field=False, last_value="2026-01-01")
        with patch(SOURCE_PATH) as mock_source:
            EbaySource().source_for_pipeline(_config(), MagicMock(), inputs)

        assert mock_source.call_args.kwargs["db_incremental_field_last_value"] is None
