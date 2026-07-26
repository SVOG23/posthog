import hmac
import hashlib
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, Optional, cast
from urllib.parse import parse_qs, urlparse

import pytest
from unittest import mock

import structlog

from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.instagram.instagram import (
    AUTH_ERROR_PREFIX,
    MAX_RETRY_ATTEMPTS,
    PERMISSION_ERROR_PREFIX,
    InstagramAuthError,
    InstagramBadRequestError,
    InstagramClient,
    InstagramPermissionError,
    InstagramResumeConfig,
    InstagramRetryableError,
    _account_insight_plan,
    _insight_points,
    _insight_windows,
    _normalize_timestamp,
    _to_unix_seconds,
    _with_query_param,
    get_rows,
    instagram_source,
    validate_credentials,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.instagram.settings import (
    ACCOUNT_INSIGHT_METRICS,
    INSTAGRAM_ENDPOINTS,
    MEDIA_INSIGHT_METRICS,
)

MODULE = "products.warehouse_sources.backend.temporal.data_imports.sources.instagram.instagram"
LOGGER = structlog.get_logger("instagram-tests")


class FakeResponse:
    def __init__(self, status_code: int = 200, payload: Any = None, text: str = "", repeat: bool = False) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text or ("" if payload is None else str(payload))
        # Retryable responses are served on every matching request, so a test can watch
        # the retry loop exhaust itself instead of falling through to the next queued one.
        self.repeat = repeat

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload


class FakeSession:
    """Stands in for the tracked session, matching queued responses by URL substring."""

    def __init__(self, responses: Optional[list[tuple[str, FakeResponse]]] = None) -> None:
        self.responses: list[tuple[str, FakeResponse]] = list(responses or [])
        self.requested_urls: list[str] = []
        self.headers: dict[str, str] = {}
        self.default_response = FakeResponse(200, {"data": []})

    def get(self, url: str, headers: Optional[dict[str, str]] = None, timeout: Optional[int] = None) -> FakeResponse:
        self.requested_urls.append(url)
        for index, (fragment, response) in enumerate(self.responses):
            if fragment in url:
                if not response.repeat:
                    self.responses.pop(index)
                return response
        return self.default_response

    def params(self, index: int) -> dict[str, list[str]]:
        return parse_qs(urlparse(self.requested_urls[index]).query)


class FakeResumableSourceManager(ResumableSourceManager[InstagramResumeConfig]):
    """Resume manager backed by memory instead of Redis."""

    def __init__(self, state: Optional[InstagramResumeConfig] = None) -> None:
        self.state = state
        self.saved: list[InstagramResumeConfig] = []

    def can_resume(self) -> bool:
        return self.state is not None

    def load_state(self) -> Optional[InstagramResumeConfig]:
        return self.state

    def save_state(self, data: InstagramResumeConfig) -> None:
        self.saved.append(data)
        self.state = data

    def clear_state(self) -> None:
        self.state = None


def _page(rows: Iterable[dict[str, Any]], after: Optional[str] = None) -> dict[str, Any]:
    body: dict[str, Any] = {"data": list(rows)}
    if after is not None:
        body["paging"] = {"cursors": {"after": after}, "next": "https://graph.instagram.com/next"}
    return body


def _collect(
    endpoint: str,
    session: FakeSession,
    manager: Optional[FakeResumableSourceManager] = None,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    resume_manager = manager or FakeResumableSourceManager()
    with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
        batches = list(
            get_rows(
                access_token="tok",
                login_type="instagram",
                api_version="v23.0",
                endpoint=endpoint,
                logger=LOGGER,
                resumable_source_manager=resume_manager,
                **kwargs,
            )
        )
    return [row for batch in batches for row in batch]


class TestInstagramTransport:
    def test_build_url_targets_the_host_for_the_login_type(self) -> None:
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=FakeSession()):
            instagram = InstagramClient("tok", "instagram", "v23.0", LOGGER)
            facebook = InstagramClient("tok", "facebook", "v23.0", LOGGER)

        assert instagram.build_url("me/media").startswith("https://graph.instagram.com/v23.0/me/media")
        assert facebook.build_url("123/media").startswith("https://graph.facebook.com/v23.0/123/media")

    def test_unknown_login_type_is_rejected(self) -> None:
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=FakeSession()):
            with pytest.raises(ValueError, match="Unknown Instagram login type"):
                InstagramClient("tok", "threads", "v23.0", LOGGER)

    def test_app_secret_produces_the_hmac_proof_meta_expects(self) -> None:
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=FakeSession()):
            client = InstagramClient("tok", "instagram", "v23.0", LOGGER, app_secret="s3cret")

        expected = hmac.new(b"s3cret", b"tok", hashlib.sha256).hexdigest()
        assert client.appsecret_proof() == expected
        assert f"appsecret_proof={expected}" in client.build_url("me")

    def test_no_app_secret_means_no_proof_param(self) -> None:
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=FakeSession()):
            client = InstagramClient("tok", "instagram", "v23.0", LOGGER)

        assert client.appsecret_proof() is None
        assert "appsecret_proof" not in client.build_url("me")

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://x/me/media?limit=100", ["CUR"]),
            ("https://x/me/media?limit=100&after=OLD", ["CUR"]),
        ],
    )
    def test_with_query_param_replaces_rather_than_appends(self, url: str, expected: list[str]) -> None:
        updated = _with_query_param(url, "after", "CUR")
        assert parse_qs(urlparse(updated).query)["after"] == expected
        assert parse_qs(urlparse(updated).query)["limit"] == ["100"]

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("2024-03-01T18:10:00+0000", "2024-03-01T18:10:00+00:00"),
            ("2024-03-01T18:10:00.500+0000", "2024-03-01T18:10:00.500000+00:00"),
            ("2024-03-01T18:10:00+00:00", "2024-03-01T18:10:00+00:00"),
            ("not-a-timestamp", "not-a-timestamp"),
            ("", ""),
            (None, None),
            (17, 17),
        ],
    )
    def test_normalize_timestamp(self, value: Any, expected: Any) -> None:
        assert _normalize_timestamp(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, None),
            ("", None),
            ("2024-01-01", 1704067200),
            ("2024-01-01T00:00:00+0000", 1704067200),
            (datetime(2024, 1, 1, tzinfo=UTC), 1704067200),
            (datetime(2024, 1, 1), 1704067200),
            (1704067200, 1704067200),
            ("nonsense", None),
        ],
    )
    def test_to_unix_seconds(self, value: Any, expected: Optional[int]) -> None:
        assert _to_unix_seconds(value) == expected

    @pytest.mark.parametrize(
        "since,until,expected",
        [
            (0, 0, []),
            (100, 50, []),
            (0, 10 * 86400, [(0, 864000)]),
            (0, 45 * 86400, [(0, 30 * 86400), (30 * 86400, 45 * 86400)]),
        ],
    )
    def test_insight_windows_never_exceed_meta_s_30_day_limit(
        self, since: int, until: int, expected: list[tuple[int, int]]
    ) -> None:
        windows = _insight_windows(since, until)
        assert windows == expected
        assert all(end - start <= 30 * 86400 for start, end in windows)

    @pytest.mark.parametrize(
        "entry,expected",
        [
            ({"time_series": [{"value": 1, "end_time": "t"}]}, [{"value": 1, "end_time": "t"}]),
            ({"values": [{"value": 2, "end_time": "t"}]}, [{"value": 2, "end_time": "t"}]),
            ({"total_value": {"value": 3}}, [{"value": 3, "end_time": None}]),
            ({"values": []}, []),
            ({}, []),
        ],
    )
    def test_insight_points_handles_every_shape_meta_returns(
        self, entry: dict[str, Any], expected: list[dict[str, Any]]
    ) -> None:
        assert _insight_points(entry) == expected

    @pytest.mark.parametrize(
        "status_code,payload,expected",
        [
            (400, {"error": {"code": 190, "message": "expired"}}, InstagramAuthError),
            (401, {"error": {"code": 0, "message": "bad token"}}, InstagramAuthError),
            (400, {"error": {"code": 10, "message": "no permission"}}, InstagramPermissionError),
            (400, {"error": {"code": 200, "message": "missing scope"}}, InstagramPermissionError),
            (403, {"error": {"code": 0, "message": "forbidden"}}, InstagramPermissionError),
            (400, {"error": {"code": 100, "message": "bad metric"}}, InstagramBadRequestError),
            (404, None, InstagramBadRequestError),
        ],
    )
    def test_permanent_errors_are_classified_from_status_and_meta_error_code(
        self, status_code: int, payload: Any, expected: type[Exception]
    ) -> None:
        session = FakeSession([("me", FakeResponse(status_code, payload, text="body"))])
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            client = InstagramClient("tok", "instagram", "v23.0", LOGGER)

        with pytest.raises(expected):
            client.get(client.build_url("me"))

        # A permanent failure must not be retried — Meta's throttle budget is per app.
        assert len(session.requested_urls) == 1

    @pytest.mark.parametrize(
        "status_code,payload",
        [
            (429, {"error": {"code": 4, "message": "throttled"}}),
            # Meta reports most of its throttling as an HTTP 400 body, which a
            # status-code retry layer would never notice.
            (400, {"error": {"code": 4, "message": "app limit"}}),
            (400, {"error": {"code": 32, "message": "page limit"}}),
            (400, {"error": {"code": 2, "message": "transient"}}),
            (503, {"error": {"code": 1, "message": "unknown"}}),
        ],
    )
    def test_throttling_and_transient_errors_are_retried_then_surfaced(self, status_code: int, payload: Any) -> None:
        session = FakeSession([("me", FakeResponse(status_code, payload, repeat=True))])
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            client = InstagramClient("tok", "instagram", "v23.0", LOGGER)

        with pytest.raises(InstagramRetryableError):
            client.get(client.build_url("me"))

        assert len(session.requested_urls) == MAX_RETRY_ATTEMPTS

    @pytest.mark.parametrize(
        "status_code,code,prefix",
        [
            (400, 190, AUTH_ERROR_PREFIX),
            (400, 10, PERMISSION_ERROR_PREFIX),
        ],
    )
    def test_permanent_failures_carry_the_prefix_the_source_matches_on(
        self, status_code: int, code: int, prefix: str
    ) -> None:
        session = FakeSession([("me", FakeResponse(status_code, {"error": {"code": code, "message": "nope"}}))])
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            client = InstagramClient("tok", "instagram", "v23.0", LOGGER)

        with pytest.raises(Exception) as error:
            client.get(client.build_url("me"))

        assert prefix in str(error.value)

    def test_a_throttled_request_is_retried_and_then_succeeds(self) -> None:
        session = FakeSession(
            [
                ("me", FakeResponse(429, {"error": {"code": 4, "message": "throttled"}})),
                ("me", FakeResponse(200, {"id": "1"})),
            ]
        )
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            client = InstagramClient("tok", "instagram", "v23.0", LOGGER)
            assert client.get(client.build_url("me")) == {"id": "1"}

        assert len(session.requested_urls) == 2


class TestInstagramEdges:
    def test_media_pages_until_meta_stops_returning_a_cursor(self) -> None:
        session = FakeSession(
            [
                ("me/media", FakeResponse(200, _page([{"id": "1", "timestamp": "2024-03-01T18:10:00+0000"}], "CUR"))),
                ("after=CUR", FakeResponse(200, _page([{"id": "2", "timestamp": "2024-02-01T00:00:00+0000"}]))),
            ]
        )
        manager = FakeResumableSourceManager()

        rows = _collect("media", session, manager)

        assert [row["id"] for row in rows] == ["1", "2"]
        # Meta's `+0000` offset is rewritten so the cursor/partition column parses.
        assert rows[0]["timestamp"] == "2024-03-01T18:10:00+00:00"
        assert len(session.requested_urls) == 2

    def test_media_stops_when_the_cursor_is_missing_even_though_next_is_present(self) -> None:
        session = FakeSession(
            [("me/media", FakeResponse(200, {"data": [{"id": "1"}], "paging": {"next": "https://x/next"}}))]
        )

        rows = _collect("media", session)

        assert [row["id"] for row in rows] == ["1"]
        assert len(session.requested_urls) == 1

    def test_incremental_media_sync_asks_meta_for_the_window_after_the_watermark(self) -> None:
        session = FakeSession()

        _collect(
            "media",
            session,
            should_use_incremental_field=True,
            db_incremental_field_last_value="2024-01-01T00:00:00+0000",
        )

        assert session.params(0)["since"] == ["1704067200"]

    def test_a_full_refresh_media_sync_sends_no_time_window(self) -> None:
        session = FakeSession()

        _collect("media", session, should_use_incremental_field=False, db_incremental_field_last_value="2024-01-01")

        assert "since" not in session.params(0)

    def test_start_date_bounds_the_first_media_sync(self) -> None:
        session = FakeSession()

        _collect("media", session, start_date="2024-01-01")

        assert session.params(0)["since"] == ["1704067200"]

    def test_the_next_page_is_checkpointed_only_after_its_rows_are_yielded(self) -> None:
        session = FakeSession(
            [
                ("me/media", FakeResponse(200, _page([{"id": "1"}], "CUR"))),
                ("after=CUR", FakeResponse(200, _page([{"id": "2"}]))),
            ]
        )
        manager = FakeResumableSourceManager()

        _collect("media", session, manager)

        assert len(manager.saved) == 1
        assert "after=CUR" in manager.saved[0].next_url

    def test_a_saved_checkpoint_restarts_mid_stream_instead_of_from_page_one(self) -> None:
        session = FakeSession([("after=CUR", FakeResponse(200, _page([{"id": "2"}])))])
        manager = FakeResumableSourceManager(
            InstagramResumeConfig(next_url="https://graph.instagram.com/v23.0/me/media?limit=100&after=CUR")
        )

        rows = _collect("media", session, manager)

        assert [row["id"] for row in rows] == ["2"]
        assert session.requested_urls == ["https://graph.instagram.com/v23.0/me/media?limit=100&after=CUR"]

    def test_the_account_endpoint_yields_the_node_itself(self) -> None:
        session = FakeSession([("me?", FakeResponse(200, {"id": "17841", "username": "posthog"}))])

        rows = _collect("account", session)

        assert rows == [{"id": "17841", "username": "posthog"}]

    def test_an_account_id_replaces_the_me_alias(self) -> None:
        session = FakeSession([("17841", FakeResponse(200, {"id": "17841"}))])

        _collect("account", session, instagram_account_id="17841")

        assert "/v23.0/17841?" in session.requested_urls[0]


class TestInstagramFanOut:
    def test_comments_carry_the_parent_media_id(self) -> None:
        session = FakeSession(
            [
                ("me/media", FakeResponse(200, _page([{"id": "m1"}, {"id": "m2"}]))),
                ("m1/comments", FakeResponse(200, _page([{"id": "c1", "timestamp": "2024-03-01T18:10:00+0000"}]))),
                ("m2/comments", FakeResponse(200, _page([{"id": "c2"}]))),
            ]
        )

        rows = _collect("media_comments", session)

        assert [(row["media_id"], row["id"]) for row in rows] == [("m1", "c1"), ("m2", "c2")]
        assert rows[0]["timestamp"] == "2024-03-01T18:10:00+00:00"

    def test_a_post_with_comments_disabled_is_skipped_rather_than_failing_the_table(self) -> None:
        session = FakeSession(
            [
                ("me/media", FakeResponse(200, _page([{"id": "m1"}, {"id": "m2"}]))),
                ("m1/comments", FakeResponse(400, {"error": {"code": 100, "message": "comments disabled"}})),
                ("m2/comments", FakeResponse(200, _page([{"id": "c2"}]))),
            ]
        )

        rows = _collect("media_comments", session)

        assert [row["id"] for row in rows] == ["c2"]

    @pytest.mark.parametrize("product_type", ["FEED", "REELS", "STORY"])
    def test_media_insights_request_the_metric_set_for_the_media_type(self, product_type: str) -> None:
        session = FakeSession(
            [("me/media", FakeResponse(200, _page([{"id": "m1", "media_product_type": product_type}])))]
        )

        _collect("media_insights", session)

        requested = session.params(1)["metric"][0].split(",")
        assert requested == list(MEDIA_INSIGHT_METRICS[product_type])

    def test_media_insights_are_emitted_one_row_per_metric(self) -> None:
        session = FakeSession(
            [
                (
                    "me/media",
                    FakeResponse(
                        200,
                        _page([{"id": "m1", "media_product_type": "FEED", "timestamp": "2024-03-01T18:10:00+0000"}]),
                    ),
                ),
                (
                    "m1/insights",
                    FakeResponse(
                        200,
                        {
                            "data": [
                                {"name": "reach", "period": "lifetime", "values": [{"value": 10}]},
                                {"name": "likes", "period": "lifetime", "total_value": {"value": 4}},
                                {"name": "shares", "period": "lifetime"},
                            ]
                        },
                    ),
                ),
            ]
        )

        rows = _collect("media_insights", session)

        assert [(row["metric"], row["value"]) for row in rows] == [("reach", 10), ("likes", 4)]
        assert {row["media_id"] for row in rows} == {"m1"}
        assert rows[0]["media_timestamp"] == "2024-03-01T18:10:00+00:00"

    def test_a_post_meta_refuses_to_report_on_is_skipped(self) -> None:
        session = FakeSession(
            [
                ("me/media", FakeResponse(200, _page([{"id": "m1", "media_product_type": "FEED"}]))),
                ("m1/insights", FakeResponse(400, {"error": {"code": 100, "message": "unsupported metric"}})),
            ]
        )

        assert _collect("media_insights", session) == []

    def test_the_parent_page_is_checkpointed_once_its_children_are_drained(self) -> None:
        session = FakeSession(
            [
                ("me/media", FakeResponse(200, _page([{"id": "m1"}], "CUR"))),
                ("m1/comments", FakeResponse(200, _page([{"id": "c1"}]))),
                ("after=CUR", FakeResponse(200, _page([{"id": "m2"}]))),
                ("m2/comments", FakeResponse(200, _page([{"id": "c2"}]))),
            ]
        )
        manager = FakeResumableSourceManager()

        rows = _collect("media_comments", session, manager)

        assert [row["id"] for row in rows] == ["c1", "c2"]
        assert len(manager.saved) == 1
        assert "after=CUR" in manager.saved[0].next_url


class TestInstagramAccountInsights:
    def test_the_request_plan_groups_every_metric_under_its_window(self) -> None:
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=FakeSession()):
            client = InstagramClient("tok", "instagram", "v23.0", LOGGER)

        plan = _account_insight_plan(client, None, 0, 45 * 86400)

        # 45 days is two windows; each carries the full metric set so the window's rows
        # can be emitted in date order.
        assert [[metric for metric, _ in window] for window in plan] == [
            list(ACCOUNT_INSIGHT_METRICS),
            list(ACCOUNT_INSIGHT_METRICS),
        ]
        urls = [url for window in plan for _, url in window]
        assert all("metric_type=time_series" in url and "period=day" in url for url in urls)
        assert [parse_qs(urlparse(url).query)["since"][0] for url in urls[:2]] == ["0", "0"]
        assert parse_qs(urlparse(urls[-1]).query)["since"][0] == str(30 * 86400)

    def test_a_window_is_emitted_as_one_batch_in_date_order(self) -> None:
        def _series(name: str, values: list[tuple[int, str]]) -> dict[str, Any]:
            return {
                "data": [
                    {
                        "name": name,
                        "period": "day",
                        "time_series": [{"value": value, "end_time": end_time} for value, end_time in values],
                    }
                ]
            }

        session = FakeSession(
            [
                ("metric=reach", FakeResponse(200, _series("reach", [(5, "2024-03-02T07:00:00+0000")]))),
                ("metric=views", FakeResponse(200, _series("views", [(9, "2024-03-01T07:00:00+0000")]))),
            ]
        )

        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            batches = list(
                get_rows(
                    access_token="tok",
                    login_type="instagram",
                    api_version="v23.0",
                    endpoint="account_insights",
                    logger=LOGGER,
                    resumable_source_manager=FakeResumableSourceManager(),
                    should_use_incremental_field=True,
                    db_incremental_field_last_value="2024-03-01",
                )
            )

        assert len(batches) == 1
        assert [row["date"] for row in batches[0]] == [
            "2024-03-01T07:00:00+00:00",
            "2024-03-02T07:00:00+00:00",
        ]

    def test_daily_points_become_one_row_per_metric_and_day(self) -> None:
        session = FakeSession(
            [
                (
                    "metric=reach",
                    FakeResponse(
                        200,
                        {
                            "data": [
                                {
                                    "name": "reach",
                                    "period": "day",
                                    "time_series": [
                                        {"value": 5, "end_time": "2024-03-01T07:00:00+0000"},
                                        {"value": 7, "end_time": "2024-03-02T07:00:00+0000"},
                                    ],
                                }
                            ]
                        },
                    ),
                )
            ]
        )

        rows = _collect(
            "account_insights",
            session,
            instagram_account_id="17841",
            should_use_incremental_field=True,
            db_incremental_field_last_value="2024-03-01",
        )

        reach_rows = [row for row in rows if row["metric"] == "reach"]
        assert [(row["date"], row["value"]) for row in reach_rows] == [
            ("2024-03-01T07:00:00+00:00", 5),
            ("2024-03-02T07:00:00+00:00", 7),
        ]
        assert {row["instagram_account_id"] for row in reach_rows} == {"17841"}

    def test_a_metric_meta_has_retired_is_dropped_without_failing_the_table(self) -> None:
        session = FakeSession(
            [
                ("metric=reach", FakeResponse(400, {"error": {"code": 100, "message": "metric deprecated"}})),
                (
                    "metric=views",
                    FakeResponse(
                        200,
                        {
                            "data": [
                                {
                                    "name": "views",
                                    "period": "day",
                                    "time_series": [{"value": 9, "end_time": "2024-03-01T07:00:00+0000"}],
                                }
                            ]
                        },
                    ),
                ),
            ]
        )

        rows = _collect(
            "account_insights",
            session,
            should_use_incremental_field=True,
            db_incremental_field_last_value="2024-03-01",
        )

        assert [row["metric"] for row in rows] == ["views"]

    def test_each_finished_window_checkpoints_the_start_of_the_next(self) -> None:
        session = FakeSession()
        manager = FakeResumableSourceManager()

        _collect("account_insights", session, manager, start_date="2024-01-01")

        assert manager.saved
        # A checkpoint always names the first metric of the next window, and windows
        # advance forward in time.
        assert all("metric=reach" in state.next_url for state in manager.saved)
        checkpoints = [int(parse_qs(urlparse(state.next_url).query)["since"][0]) for state in manager.saved]
        assert checkpoints == sorted(checkpoints)

    def test_a_checkpoint_from_an_older_window_restarts_the_plan(self) -> None:
        session = FakeSession()
        manager = FakeResumableSourceManager(InstagramResumeConfig(next_url="https://graph.instagram.com/stale"))

        _collect(
            "account_insights",
            session,
            manager,
            should_use_incremental_field=True,
            db_incremental_field_last_value="2024-03-01",
        )

        assert session.requested_urls
        assert "metric=reach" in session.requested_urls[0]


class TestValidateCredentials:
    def test_a_working_token_validates(self) -> None:
        session = FakeSession([("me", FakeResponse(200, {"id": "17841", "username": "posthog"}))])
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            assert validate_credentials("tok", "instagram", "v23.0", LOGGER) == (True, None)

    def test_facebook_login_without_an_account_id_is_rejected_before_any_request(self) -> None:
        session = FakeSession()
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            is_valid, message = validate_credentials("tok", "facebook", "v23.0", LOGGER)

        assert is_valid is False
        assert message is not None and "Instagram account ID" in message
        assert session.requested_urls == []

    @pytest.mark.parametrize(
        "status_code,code,expected_fragment",
        [
            (400, 190, "invalid or has expired"),
            (401, 0, "invalid or has expired"),
            (403, 0, "missing the permissions"),
            (400, 10, "missing the permissions"),
        ],
    )
    def test_auth_and_scope_failures_get_actionable_messages(
        self, status_code: int, code: int, expected_fragment: str
    ) -> None:
        session = FakeSession([("me", FakeResponse(status_code, {"error": {"code": code, "message": "nope"}}))])
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            is_valid, message = validate_credentials("tok", "instagram", "v23.0", LOGGER)

        assert is_valid is False
        assert message is not None and expected_fragment in message

    def test_a_node_without_an_id_is_not_a_professional_account(self) -> None:
        session = FakeSession([("me", FakeResponse(200, {}))])
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            is_valid, message = validate_credentials("tok", "instagram", "v23.0", LOGGER)

        assert is_valid is False
        assert message is not None and "professional Instagram account" in message


class TestInstagramSourceResponse:
    @pytest.mark.parametrize("endpoint", list(INSTAGRAM_ENDPOINTS))
    def test_every_endpoint_declares_the_keys_and_partitioning_from_its_settings(self, endpoint: str) -> None:
        config = INSTAGRAM_ENDPOINTS[endpoint]

        with mock.patch(f"{MODULE}.make_tracked_session", return_value=FakeSession()):
            response = instagram_source(
                access_token="tok",
                login_type="instagram",
                api_version="v23.0",
                endpoint=endpoint,
                logger=LOGGER,
                resumable_source_manager=FakeResumableSourceManager(),
            )

        assert response.name == endpoint
        assert response.primary_keys == config.primary_keys
        assert response.sort_mode == config.sort_mode
        assert response.partition_keys == ([config.partition_key] if config.partition_key else None)

    def test_items_are_only_fetched_once_the_pipeline_iterates(self) -> None:
        session = FakeSession()
        with mock.patch(f"{MODULE}.make_tracked_session", return_value=session):
            response = instagram_source(
                access_token="tok",
                login_type="instagram",
                api_version="v23.0",
                endpoint="media",
                logger=LOGGER,
                resumable_source_manager=FakeResumableSourceManager(),
            )
            assert session.requested_urls == []
            list(cast("Iterable[Any]", response.items()))

        assert len(session.requested_urls) == 1
