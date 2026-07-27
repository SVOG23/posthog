import base64
import dataclasses
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any, Optional

import requests
from structlog.types import FilteringBoundLogger

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline.typings import SourceResponse
from products.warehouse_sources.backend.temporal.data_imports.sources.common.http import make_tracked_session
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.ebay.settings import (
    DEFAULT_BACKFILL_DAYS,
    EBAY_ENDPOINTS,
    EBAY_HOSTS,
    INCREMENTAL_OVERLAP_SECONDS,
    MAX_FILTER_WINDOW_DAYS,
    EbayEndpointConfig,
)

OAUTH_TOKEN_PATH = "/identity/v1/oauth2/token"
REQUEST_TIMEOUT_SECONDS = 60

# One filter window, expressed as (start, end). `None` marks an endpoint with no date
# filter, which is walked as a single unfiltered pass.
Window = Optional[tuple[datetime, datetime]]


class EbayAuthenticationError(Exception):
    pass


@dataclasses.dataclass
class EbayResumeConfig:
    # Start of the filter window being paginated, formatted exactly as it is sent to eBay.
    # `None` for endpoints without a date filter.
    window_start: Optional[str] = None
    # Row offset reached inside that window (or inside the unfiltered listing).
    offset: int = 0
    # Offset into the parent listing for fan-out endpoints (offers walk inventory items).
    parent_offset: int = 0


def host_for_environment(environment: str) -> str:
    host = EBAY_HOSTS.get(environment)
    if host is None:
        raise ValueError(f"Invalid eBay environment: {environment}")
    return host


def format_datetime(value: datetime) -> str:
    """eBay expects UTC ISO 8601 with milliseconds and a literal Z, e.g. 2026-01-15T10:00:00.000Z."""
    utc = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def coerce_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return None


def resolve_filter_field(
    config: EbayEndpointConfig, incremental_field: Optional[str], should_use_incremental_field: bool
) -> Optional[str]:
    """eBay's filter field name for this run — the user's chosen cursor when it maps to one."""
    if config.default_filter_field is None:
        return None
    if should_use_incremental_field and incremental_field:
        return config.filter_fields.get(incremental_field, config.default_filter_field)
    return config.default_filter_field


def build_windows(
    config: EbayEndpointConfig,
    now: datetime,
    should_use_incremental_field: bool = False,
    db_incremental_field_last_value: Any = None,
) -> list[Window]:
    """Split the range to sync into windows eBay will accept.

    Sell API date filters are capped at 90 days, so a backfill is a series of windows
    rather than one request. Window starts are derived only from the stored watermark, so
    they stay identical across retries of the same job — that is what makes a saved
    `window_start` resumable.
    """
    if config.default_filter_field is None:
        return [None]

    start = now - timedelta(days=DEFAULT_BACKFILL_DAYS)
    if should_use_incremental_field:
        last_value = coerce_datetime(db_incremental_field_last_value)
        if last_value is not None:
            # Overlap the cursor slightly: eBay warns that in-flight orders can be written
            # with a timestamp just before the one we last saw.
            start = last_value - timedelta(seconds=INCREMENTAL_OVERLAP_SECONDS)

    # A watermark ahead of now would make an inverted range, which eBay rejects.
    start = min(start, now)

    windows: list[Window] = []
    cursor = start
    while cursor < now:
        end = min(cursor + timedelta(days=MAX_FILTER_WINDOW_DAYS), now)
        windows.append((cursor, end))
        cursor = end

    if not windows:
        windows.append((start, now))
    return windows


def window_key(window: Window) -> Optional[str]:
    return None if window is None else format_datetime(window[0])


class EbayClient:
    """Bearer-token eBay Sell API client that re-mints the short-lived user access token.

    eBay user access tokens last two hours, which a large backfill can outlive, so a 401
    is treated as an expiry once per request before it is surfaced as an error.
    """

    def __init__(
        self,
        host: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        marketplace_id: str,
        logger: Optional[FilteringBoundLogger] = None,
    ) -> None:
        self._host = host
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._marketplace_id = marketplace_id
        self._logger = logger
        # Order, transaction, and payout responses carry buyer and seller PII (addresses,
        # notes, payout instruments) that the name-based scrubbers cannot reliably sanitise,
        # so keep these bodies out of the shared HTTP sample store.
        self._session = make_tracked_session(redact_values=(client_secret, refresh_token), capture=False)
        # The token exchange body carries both the refresh token and a freshly minted
        # access token, neither of which the name-based scrubbers recognise.
        self._auth_session = make_tracked_session(redact_values=(client_secret, refresh_token), capture=False)
        self._token: Optional[str] = None

    def mint_token(self) -> str:
        credentials = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        response = self._auth_session.post(
            f"{self._host}{OAUTH_TOKEN_PATH}",
            data={"grant_type": "refresh_token", "refresh_token": self._refresh_token},
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        if not token:
            raise EbayAuthenticationError("eBay did not return an access token for the supplied refresh token")
        self._token = str(token)
        return self._token

    def _headers(self) -> dict[str, str]:
        if self._token is None:
            self.mint_token()
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": self._marketplace_id,
        }

    def get(self, path: str, params: dict[str, str], allow_not_found: bool = False) -> dict[str, Any]:
        url = f"{self._host}{path}"
        response = self._session.get(url, params=params, headers=self._headers(), timeout=REQUEST_TIMEOUT_SECONDS)

        if response.status_code == 401:
            self._token = None
            response = self._session.get(url, params=params, headers=self._headers(), timeout=REQUEST_TIMEOUT_SECONDS)

        # getOffers answers 404 for a SKU that has no offers yet, which is an empty result
        # rather than a failure.
        if allow_not_found and response.status_code == 404:
            return {}

        if not response.ok:
            if self._logger is not None:
                self._logger.error(f"eBay API error: status={response.status_code}, body={response.text}, url={url}")
            response.raise_for_status()

        return response.json()


def validate_credentials(
    environment: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    marketplace_id: str,
    schema_name: Optional[str] = None,
) -> tuple[bool, bool]:
    """Probe eBay to confirm the OAuth credentials work.

    Returns ``(is_valid, is_forbidden)``. ``is_forbidden`` separates a 403 (the token is
    real but the grant is missing the scope for this resource) from a 401 or a failed
    token exchange, so the caller can accept scope gaps at source-create time.
    """
    try:
        host = host_for_environment(environment)
    except ValueError:
        return False, False

    config = EBAY_ENDPOINTS.get(schema_name) if schema_name else None
    if config is None:
        config = EBAY_ENDPOINTS["orders"]
    # Fan-out endpoints need a parent id we don't have here, so probe the parent listing.
    if config.parent is not None:
        config = EBAY_ENDPOINTS[config.parent]

    client = EbayClient(host, client_id, client_secret, refresh_token, marketplace_id)
    try:
        client.mint_token()
    except Exception:
        return False, False

    try:
        client.get(config.path, {"limit": "1"})
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        return False, status == 403
    except Exception:
        return False, False

    return True, False


def check_endpoint_permissions(
    environment: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    marketplace_id: str,
    endpoints: list[str],
) -> dict[str, Optional[str]]:
    """Probe each endpoint with one shared token. ``None`` = reachable, else the reason.

    Only a 403 counts as a scope problem — a throttle, 5xx or network blip leaves the table
    reachable so it is retried at sync time rather than reported as a permission error.
    """
    permissions: dict[str, Optional[str]] = dict.fromkeys(endpoints)
    try:
        client = EbayClient(host_for_environment(environment), client_id, client_secret, refresh_token, marketplace_id)
        client.mint_token()
    except Exception:
        return permissions

    for endpoint in endpoints:
        config = EBAY_ENDPOINTS.get(endpoint)
        if config is None:
            continue
        if config.parent is not None:
            config = EBAY_ENDPOINTS[config.parent]
        try:
            client.get(config.path, {"limit": "1"})
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                permissions[endpoint] = "Your eBay authorization is missing the scope required for this table"
        except Exception:
            continue

    return permissions


def _paginate(
    client: EbayClient,
    config: EbayEndpointConfig,
    base_params: dict[str, str],
    start_offset: int = 0,
    allow_not_found: bool = False,
) -> Iterator[tuple[list[dict[str, Any]], int]]:
    """Walk an eBay limit/offset listing, yielding each page and the offset it ended at."""
    offset = start_offset
    while True:
        params = {**base_params, "limit": str(config.page_limit), "offset": str(offset)}
        data = client.get(config.path, params, allow_not_found=allow_not_found)
        items = data.get(config.data_key) or []
        offset += len(items)

        yield items, offset

        # eBay signals more pages with a `next` href; an empty page ends the walk even if
        # the href is present, so a missing/odd `next` can never loop forever.
        if not items or not data.get("next"):
            return


def _windowed_rows(
    client: EbayClient,
    config: EbayEndpointConfig,
    resumable_source_manager: ResumableSourceManager[EbayResumeConfig],
    resume: Optional[EbayResumeConfig],
    logger: FilteringBoundLogger,
    now: datetime,
    should_use_incremental_field: bool,
    db_incremental_field_last_value: Any,
    incremental_field: Optional[str],
) -> Iterator[list[dict[str, Any]]]:
    windows = build_windows(config, now, should_use_incremental_field, db_incremental_field_last_value)
    filter_field = resolve_filter_field(config, incremental_field, should_use_incremental_field)

    start_index = 0
    start_offset = 0
    if resume is not None:
        for index, window in enumerate(windows):
            if window_key(window) == resume.window_start:
                start_index = index
                start_offset = max(resume.offset, 0)
                logger.debug(f"eBay: resuming {config.name} at window {resume.window_start} offset {start_offset}")
                break

    for index in range(start_index, len(windows)):
        window = windows[index]
        base_params: dict[str, str] = dict(config.extra_params)
        if window is not None and filter_field is not None:
            base_params["filter"] = f"{filter_field}:[{format_datetime(window[0])}..{format_datetime(window[1])}]"

        offset = start_offset if index == start_index else 0
        for items, next_offset in _paginate(client, config, base_params, start_offset=offset):
            if not items:
                continue
            yield items
            # Saved only after the batch is yielded, so a crash re-yields it instead of
            # skipping it — the merge dedupes on the primary key.
            resumable_source_manager.save_state(EbayResumeConfig(window_start=window_key(window), offset=next_offset))


def _fanout_rows(
    client: EbayClient,
    config: EbayEndpointConfig,
    resumable_source_manager: ResumableSourceManager[EbayResumeConfig],
    resume: Optional[EbayResumeConfig],
    logger: FilteringBoundLogger,
) -> Iterator[list[dict[str, Any]]]:
    assert config.parent is not None and config.parent_field is not None and config.parent_query_param is not None
    parent_config = EBAY_ENDPOINTS[config.parent]
    parent_offset = max(resume.parent_offset, 0) if resume is not None else 0
    if parent_offset:
        logger.debug(f"eBay: resuming {config.name} at parent offset {parent_offset}")

    for parents, next_parent_offset in _paginate(client, parent_config, {}, start_offset=parent_offset):
        for parent in parents:
            value = parent.get(config.parent_field)
            if not value:
                continue
            for items, _ in _paginate(client, config, {config.parent_query_param: str(value)}, allow_not_found=True):
                if items:
                    yield items

        # Checkpoint per parent page: a resumed attempt redoes at most one page of parents.
        resumable_source_manager.save_state(EbayResumeConfig(parent_offset=next_parent_offset))


def get_rows(
    environment: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    marketplace_id: str,
    endpoint: str,
    logger: FilteringBoundLogger,
    resumable_source_manager: ResumableSourceManager[EbayResumeConfig],
    should_use_incremental_field: bool = False,
    db_incremental_field_last_value: Any = None,
    incremental_field: Optional[str] = None,
) -> Iterator[list[dict[str, Any]]]:
    config = EBAY_ENDPOINTS[endpoint]
    client = EbayClient(
        host_for_environment(environment), client_id, client_secret, refresh_token, marketplace_id, logger
    )
    resume = resumable_source_manager.load_state() if resumable_source_manager.can_resume() else None

    if config.parent is not None:
        yield from _fanout_rows(client, config, resumable_source_manager, resume, logger)
    else:
        yield from _windowed_rows(
            client,
            config,
            resumable_source_manager,
            resume,
            logger,
            datetime.now(UTC),
            should_use_incremental_field,
            db_incremental_field_last_value,
            incremental_field,
        )

    # The stream finished; leaving the last checkpoint would make the next attempt resume
    # mid-stream instead of starting from the new watermark.
    resumable_source_manager.clear_state()


def ebay_source(
    environment: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    marketplace_id: str,
    endpoint: str,
    logger: FilteringBoundLogger,
    resumable_source_manager: ResumableSourceManager[EbayResumeConfig],
    should_use_incremental_field: bool = False,
    db_incremental_field_last_value: Any = None,
    incremental_field: Optional[str] = None,
) -> SourceResponse:
    config = EBAY_ENDPOINTS[endpoint]

    return SourceResponse(
        name=endpoint,
        items=lambda: get_rows(
            environment=environment,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            marketplace_id=marketplace_id,
            endpoint=endpoint,
            logger=logger,
            resumable_source_manager=resumable_source_manager,
            should_use_incremental_field=should_use_incremental_field,
            db_incremental_field_last_value=db_incremental_field_last_value,
            incremental_field=incremental_field,
        ),
        primary_keys=config.primary_keys,
        sort_mode=config.sort_mode,
        partition_count=1,
        partition_size=1,
        partition_mode="datetime" if config.partition_key else None,
        partition_format="month" if config.partition_key else None,
        partition_keys=[config.partition_key] if config.partition_key else None,
    )
