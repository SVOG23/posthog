import re
import hmac
import json
import hashlib
import dataclasses
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime
from typing import Any, Optional
from urllib.parse import urlencode

import requests
import structlog
from dateutil import parser as dateutil_parser
from structlog.types import FilteringBoundLogger
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline.typings import SourceResponse
from products.warehouse_sources.backend.temporal.data_imports.sources.common.http import make_tracked_session
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.facebook_pages.settings import (
    DEFAULT_API_VERSION,
    DEFAULT_INSIGHTS_LOOKBACK_DAYS,
    FACEBOOK_PAGES_ENDPOINTS,
    GRAPH_API_HOST,
    INSIGHTS_WINDOW_DAYS,
    PAGE_INSIGHTS_METRICS,
    FacebookPagesEndpointConfig,
)

# The credential probe runs outside a job context, so it logs to the module logger rather than
# the per-job one the pipeline threads through.
PROBE_LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)

REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
MAX_RETRY_AFTER_SECONDS = 60
EDGE_PAGE_SIZE = 100

# How many times an insights window may be re-requested after dropping metrics Meta rejected.
MAX_METRIC_DROP_ATTEMPTS = 3

SECONDS_PER_DAY = 24 * 60 * 60

# Stable prefixes so `get_non_retryable_errors` can match on them — Graph answers an expired
# token with HTTP 400, not 401, so the HTTP status alone can't drive the classification.
AUTH_ERROR_PREFIX = "Facebook Graph API authentication failed"
PERMISSION_ERROR_PREFIX = "Facebook Graph API permission denied"

# https://developers.facebook.com/docs/graph-api/guides/error-handling
AUTH_ERROR_CODES = frozenset({102, 190, 458, 459, 460, 463, 464, 467})
PERMISSION_ERROR_CODES = frozenset({3, 10, 200, 299, 803})
# Throttling (4/17/32/613/341) and Meta-side blips (1/2) — all worth another attempt.
RETRYABLE_ERROR_CODES = frozenset({1, 2, 4, 17, 32, 341, 613})


class FacebookPagesRetryableError(Exception):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class FacebookPagesAuthError(Exception):
    pass


class FacebookPagesPermissionError(Exception):
    pass


class FacebookPagesAPIError(Exception):
    pass


@dataclasses.dataclass
class FacebookPagesResumeConfig:
    # Edges: the `paging.cursors.after` cursor and the `since` filter pinned at sync start.
    after: str | None = None
    since: int | None = None
    # Insights: start of the next window to fetch, and the end of the overall range (both
    # unix seconds). The range end is pinned at sync start so resumes stay deterministic.
    window_since: int | None = None
    range_until: int | None = None


def appsecret_proof(app_secret: str, access_token: str) -> str:
    """Meta's proof-of-app-secret: HMAC-SHA256 of the access token keyed with the app secret.

    Apps with "Require app secret" enabled reject any call without it, and sending it is
    harmless otherwise, so every request carries one.
    """
    return hmac.new(app_secret.encode("utf-8"), access_token.encode("utf-8"), hashlib.sha256).hexdigest()


def graph_url(api_version: str, path: str) -> str:
    return f"{GRAPH_API_HOST}/{api_version}/{path.lstrip('/')}"


def _parse_retry_after(response: requests.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if raw and raw.strip().isdigit():
        return min(float(raw.strip()), MAX_RETRY_AFTER_SECONDS)
    return None


def _retry_wait(retry_state: RetryCallState) -> float:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, FacebookPagesRetryableError) and exc.retry_after is not None:
        return exc.retry_after
    return wait_exponential_jitter(initial=1, max=30)(retry_state)


def raise_for_graph_error(response: requests.Response, logger: FilteringBoundLogger) -> None:
    """Turn a failed Graph response into the right exception class.

    Classification is driven by Meta's numeric error code rather than the HTTP status or the
    `type` field: Graph returns 400 with `type: "OAuthException"` for throttling, expired
    tokens, and plain bad parameters alike, so only the code separates them.
    """
    if response.ok:
        return

    try:
        body = response.json()
    except ValueError:
        body = {}

    error = (body or {}).get("error") or {}
    code = error.get("code")
    subcode = error.get("error_subcode")
    message = error.get("message") or response.text

    if response.status_code == 429 or response.status_code >= 500 or code in RETRYABLE_ERROR_CODES:
        retry_after = _parse_retry_after(response) if response.status_code == 429 else None
        raise FacebookPagesRetryableError(
            f"Facebook Graph API error (retryable): status={response.status_code}, code={code}, message={message}",
            retry_after=retry_after,
        )

    if response.status_code == 401 or code in AUTH_ERROR_CODES:
        raise FacebookPagesAuthError(f"{AUTH_ERROR_PREFIX}: {message} (code={code}, subcode={subcode})")

    if response.status_code == 403 or code in PERMISSION_ERROR_CODES:
        raise FacebookPagesPermissionError(f"{PERMISSION_ERROR_PREFIX}: {message} (code={code})")

    logger.error(f"Facebook Graph API error: status={response.status_code}, code={code}, message={message}")
    raise FacebookPagesAPIError(
        f"Facebook Graph API error: status={response.status_code}, code={code}, message={message}"
    )


def _fetch_json_once(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    access_token: str,
    app_secret: str,
    logger: FilteringBoundLogger,
) -> dict[str, Any]:
    query = {**params, "appsecret_proof": appsecret_proof(app_secret, access_token)}
    # The token rides in the Authorization header, never the query string, so it stays out of
    # request logs and captured samples.
    response = session.get(
        f"{url}?{urlencode(query)}",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    raise_for_graph_error(response, logger)
    data = response.json()
    return data if isinstance(data, dict) else {"data": data}


_fetch_json = retry(
    retry=retry_if_exception_type((FacebookPagesRetryableError, requests.ReadTimeout, requests.ConnectionError)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=_retry_wait,
    reraise=True,
)(_fetch_json_once)


def resolve_page_access_token(
    session: requests.Session,
    api_version: str,
    app_id: str,
    app_secret: str,
    page_id: str,
    access_token: str,
    logger: FilteringBoundLogger,
) -> str:
    """Turn whatever token the customer pasted into a Page access token.

    Both steps are best-effort upgrades: a customer who already pasted a long-lived Page token
    can't exchange it and doesn't need to, so a failure here leaves the original token in place
    and lets the real data request report the problem.
    """
    token = access_token

    try:
        exchanged = _fetch_json(
            session,
            graph_url(api_version, "oauth/access_token"),
            {
                "grant_type": "fb_exchange_token",
                "client_id": app_id,
                "client_secret": app_secret,
                "fb_exchange_token": access_token,
            },
            access_token,
            app_secret,
            logger,
        )
        if exchanged.get("access_token"):
            token = str(exchanged["access_token"])
    except Exception as e:
        logger.debug(f"Facebook Pages: could not exchange for a long-lived token, using the supplied one: {e}")

    try:
        page = _fetch_json(
            session, graph_url(api_version, page_id), {"fields": "access_token"}, token, app_secret, logger
        )
        if page.get("access_token"):
            token = str(page["access_token"])
    except Exception as e:
        logger.debug(f"Facebook Pages: could not read a Page access token, using the user token: {e}")

    return token


def _to_epoch_seconds(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Cannot interpret incremental value as a timestamp: {value!r}")
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(aware.timestamp())
    if isinstance(value, date):
        return int(datetime.combine(value, datetime.min.time(), tzinfo=UTC).timestamp())
    if isinstance(value, str) and value.strip():
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
        parsed = dateutil_parser.parse(stripped)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return int(parsed.timestamp())
    raise ValueError(f"Cannot interpret incremental value as a timestamp: {value!r}")


def _row_epoch(item: dict[str, Any], field_name: str) -> int | None:
    raw = item.get(field_name)
    if raw is None:
        return None
    try:
        return _to_epoch_seconds(raw)
    except (ValueError, TypeError, OverflowError):
        return None


def _now_seconds() -> int:
    return int(datetime.now(UTC).timestamp())


def unsupported_metrics(message: str, metrics: Sequence[str]) -> list[str]:
    """Metric names the error message calls out, matched on whole tokens.

    `page_impressions` is a prefix of `page_impressions_unique`, so a substring test would drop
    metrics Meta never complained about.
    """
    return [m for m in metrics if re.search(rf"(?<![A-Za-z0-9_]){re.escape(m)}(?![A-Za-z0-9_])", message)]


def flatten_insights(payload: dict[str, Any], page_id: str) -> list[dict[str, Any]]:
    """One row per metric/period/end_time.

    Graph nests a `values` time series under each metric. Breakdown metrics carry a dict value
    instead of a number; those go to `value_json` so `value` stays a clean numeric column.
    """
    rows: list[dict[str, Any]] = []
    for metric in payload.get("data") or []:
        if not isinstance(metric, dict):
            continue
        for point in metric.get("values") or []:
            if not isinstance(point, dict) or not point.get("end_time"):
                continue
            raw_value: Any = point.get("value")
            is_numeric = isinstance(raw_value, int | float) and not isinstance(raw_value, bool)
            rows.append(
                {
                    "page_id": page_id,
                    "name": metric.get("name"),
                    "period": metric.get("period"),
                    "title": metric.get("title"),
                    "description": metric.get("description"),
                    "end_time": point.get("end_time"),
                    "value": float(raw_value) if is_numeric else None,
                    "value_json": None if is_numeric or raw_value is None else json.dumps(raw_value),
                }
            )
    return rows


def _get_object_rows(
    session: requests.Session,
    api_version: str,
    page_id: str,
    access_token: str,
    app_secret: str,
    config: FacebookPagesEndpointConfig,
    logger: FilteringBoundLogger,
) -> Iterator[list[dict[str, Any]]]:
    data = _fetch_json(
        session,
        graph_url(api_version, page_id),
        {"fields": ",".join(config.fields)},
        access_token,
        app_secret,
        logger,
    )
    if data:
        yield [data]


def _get_edge_rows(
    session: requests.Session,
    api_version: str,
    page_id: str,
    access_token: str,
    app_secret: str,
    config: FacebookPagesEndpointConfig,
    logger: FilteringBoundLogger,
    resumable_source_manager: ResumableSourceManager[FacebookPagesResumeConfig],
    db_incremental_field_last_value: Any,
) -> Iterator[list[dict[str, Any]]]:
    """Walk a Page edge newest-first with the `after` cursor, bounded by `since`.

    Graph only guarantees the `since` filter on the first request of a cursor walk, so the loop
    also stops client-side once a whole page predates the watermark — otherwise every
    incremental run would page back through the Page's entire history.
    """
    resume = resumable_source_manager.load_state() if resumable_source_manager.can_resume() else None

    if resume is not None and resume.after:
        after: str | None = resume.after
        since = resume.since
        logger.debug(f"Facebook Pages: resuming {config.name} from cursor after={after}")
    else:
        after = None
        since = _to_epoch_seconds(db_incremental_field_last_value) if db_incremental_field_last_value else None

    url = graph_url(api_version, f"{page_id}/{config.edge}")
    timestamp_field = config.timestamp_field

    while True:
        params: dict[str, str] = {"fields": ",".join(config.fields), "limit": str(EDGE_PAGE_SIZE)}
        if since is not None:
            params["since"] = str(since)
        if after:
            params["after"] = after

        data = _fetch_json(session, url, params, access_token, app_secret, logger)
        items = [item for item in (data.get("data") or []) if isinstance(item, dict)]

        if since is not None and timestamp_field is not None:
            # `>=` keeps the boundary row; merge dedupes it on the primary key.
            fresh = [item for item in items if (_row_epoch(item, timestamp_field) or 0) >= since]
        else:
            fresh = items

        if fresh:
            yield [{**item, "page_id": page_id} for item in fresh]

        if since is not None and not fresh:
            break

        paging = data.get("paging") or {}
        after = (paging.get("cursors") or {}).get("after")
        if not paging.get("next") or not after or not items:
            break

        # Saved AFTER yielding so a crash re-yields the last page instead of skipping it.
        resumable_source_manager.save_state(FacebookPagesResumeConfig(after=after, since=since))


def _fetch_insights_window(
    session: requests.Session,
    url: str,
    metrics: list[str],
    since: int,
    until: int,
    access_token: str,
    app_secret: str,
    logger: FilteringBoundLogger,
) -> tuple[dict[str, Any], list[str]]:
    """Fetch one insights window, dropping metrics Meta rejects and retrying.

    Meta retires Page metrics on its own schedule and rejects the whole request when one is no
    longer available, naming it in the error. Dropping the named metrics keeps the rest of the
    table syncing instead of failing every run until the pin is updated.
    """
    remaining = list(metrics)

    for _ in range(MAX_METRIC_DROP_ATTEMPTS):
        if not remaining:
            return {}, remaining
        try:
            payload = _fetch_json(
                session,
                url,
                {
                    "metric": ",".join(remaining),
                    "period": "day",
                    "since": str(since),
                    "until": str(until),
                },
                access_token,
                app_secret,
                logger,
            )
            return payload, remaining
        except FacebookPagesAPIError as e:
            rejected = unsupported_metrics(str(e), remaining)
            if not rejected:
                raise
            logger.warning(f"Facebook Pages: dropping metrics Meta no longer supports: {', '.join(rejected)}")
            remaining = [m for m in remaining if m not in rejected]

    return {}, remaining


def _get_insights_rows(
    session: requests.Session,
    api_version: str,
    page_id: str,
    access_token: str,
    app_secret: str,
    config: FacebookPagesEndpointConfig,
    logger: FilteringBoundLogger,
    resumable_source_manager: ResumableSourceManager[FacebookPagesResumeConfig],
    db_incremental_field_last_value: Any,
) -> Iterator[list[dict[str, Any]]]:
    """Walk the metric time series forward in windows Meta will accept (<= 93 days each)."""
    resume = resumable_source_manager.load_state() if resumable_source_manager.can_resume() else None

    if resume is not None and resume.window_since is not None and resume.range_until is not None:
        window_since, range_until = resume.window_since, resume.range_until
        logger.debug(f"Facebook Pages: resuming {config.name} from window_since={window_since}")
    else:
        range_until = _now_seconds()
        if db_incremental_field_last_value:
            window_since = min(_to_epoch_seconds(db_incremental_field_last_value), range_until)
        else:
            window_since = range_until - DEFAULT_INSIGHTS_LOOKBACK_DAYS * SECONDS_PER_DAY

    url = graph_url(api_version, f"{page_id}/{config.edge}")
    metrics = list(PAGE_INSIGHTS_METRICS)

    while window_since < range_until:
        window_until = min(window_since + INSIGHTS_WINDOW_DAYS * SECONDS_PER_DAY, range_until)
        payload, metrics = _fetch_insights_window(
            session, url, metrics, window_since, window_until, access_token, app_secret, logger
        )

        if not metrics:
            logger.warning("Facebook Pages: no supported Page insights metrics remain, stopping")
            return

        rows = flatten_insights(payload, page_id)
        if rows:
            yield rows

        window_since = window_until
        if window_since >= range_until:
            break

        resumable_source_manager.save_state(
            FacebookPagesResumeConfig(window_since=window_since, range_until=range_until)
        )


def get_rows(
    page_id: str,
    access_token: str,
    app_id: str,
    app_secret: str,
    endpoint: str,
    api_version: str,
    logger: FilteringBoundLogger,
    resumable_source_manager: ResumableSourceManager[FacebookPagesResumeConfig],
    should_use_incremental_field: bool = False,
    db_incremental_field_last_value: Any = None,
) -> Iterator[list[dict[str, Any]]]:
    config = FACEBOOK_PAGES_ENDPOINTS[endpoint]
    last_value = db_incremental_field_last_value if should_use_incremental_field else None

    session = make_tracked_session(redact_values=(access_token, app_secret))
    token = resolve_page_access_token(session, api_version, app_id, app_secret, page_id, access_token, logger)

    if config.style == "object":
        yield from _get_object_rows(session, api_version, page_id, token, app_secret, config, logger)
    elif config.style == "edge":
        yield from _get_edge_rows(
            session, api_version, page_id, token, app_secret, config, logger, resumable_source_manager, last_value
        )
    else:
        yield from _get_insights_rows(
            session, api_version, page_id, token, app_secret, config, logger, resumable_source_manager, last_value
        )


def validate_credentials(
    page_id: str,
    access_token: str,
    app_id: str,
    app_secret: str,
    api_version: str,
    schema_name: Optional[str] = None,
) -> tuple[bool, str | None]:
    """Probe the Page node to confirm the token can actually see the configured Page.

    A scoped probe (``schema_name`` set) hits that endpoint's own edge instead, so the schema
    picker reports the missing permission for the table the user is about to sync rather than a
    generic failure.
    """
    if not page_id.strip():
        return False, "Enter the numeric ID of the Facebook Page you want to sync"

    logger = PROBE_LOGGER
    session = make_tracked_session(redact_values=(access_token, app_secret))

    try:
        token = resolve_page_access_token(session, api_version, app_id, app_secret, page_id, access_token, logger)

        config = FACEBOOK_PAGES_ENDPOINTS.get(schema_name) if schema_name else None
        if config is not None and config.style == "edge":
            _fetch_json(
                session,
                graph_url(api_version, f"{page_id}/{config.edge}"),
                {"fields": "id", "limit": "1"},
                token,
                app_secret,
                logger,
            )
        elif config is not None and config.style == "insights":
            _fetch_json(
                session,
                graph_url(api_version, f"{page_id}/{config.edge}"),
                {"metric": PAGE_INSIGHTS_METRICS[0], "period": "day"},
                token,
                app_secret,
                logger,
            )
        else:
            _fetch_json(session, graph_url(api_version, page_id), {"fields": "id,name"}, token, app_secret, logger)
    except FacebookPagesAuthError:
        return False, ("Facebook rejected the access token. Generate a new token for your app and reconnect.")
    except (FacebookPagesPermissionError, FacebookPagesAPIError, FacebookPagesRetryableError) as e:
        return False, str(e)
    except requests.exceptions.RequestException as e:
        return False, str(e)

    return True, None


def facebook_pages_source(
    page_id: str,
    access_token: str,
    app_id: str,
    app_secret: str,
    endpoint: str,
    logger: FilteringBoundLogger,
    resumable_source_manager: ResumableSourceManager[FacebookPagesResumeConfig],
    api_version: str = DEFAULT_API_VERSION,
    should_use_incremental_field: bool = False,
    db_incremental_field_last_value: Optional[Any] = None,
) -> SourceResponse:
    config = FACEBOOK_PAGES_ENDPOINTS[endpoint]

    return SourceResponse(
        name=endpoint,
        items=lambda: get_rows(
            page_id=page_id,
            access_token=access_token,
            app_id=app_id,
            app_secret=app_secret,
            endpoint=endpoint,
            api_version=api_version,
            logger=logger,
            resumable_source_manager=resumable_source_manager,
            should_use_incremental_field=should_use_incremental_field,
            db_incremental_field_last_value=db_incremental_field_last_value,
        ),
        primary_keys=config.primary_keys,
        # Page edges come back newest-first and cursor pagination can't be reordered; insights
        # are walked forward window by window.
        sort_mode=config.sort_mode,
        partition_count=1,
        partition_size=1,
        partition_mode="datetime" if config.partition_key else None,
        partition_format=config.partition_format if config.partition_key else None,
        partition_keys=[config.partition_key] if config.partition_key else None,
    )
