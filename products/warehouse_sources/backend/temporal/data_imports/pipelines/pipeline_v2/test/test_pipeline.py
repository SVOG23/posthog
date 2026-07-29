import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
from parameterized import parameterized

from posthog.temporal.common.shutdown import ShutdownMonitor

from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema
from products.warehouse_sources.backend.temporal.data_imports.pipelines.core.batcher import Batcher
from products.warehouse_sources.backend.temporal.data_imports.pipelines.core.cdp_producer import CDPProducer
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v2.pipeline import PipelineNonDLT
from products.warehouse_sources.backend.temporal.data_imports.sources.common.typings import SourceResponse

_PIPELINE_MODULE = "products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v2.pipeline"


@pytest.mark.asyncio
async def test_run_cleanup_failure_does_not_mask_import_error(monkeypatch):
    # Regression: run()'s finally calls get_delta_table() (object-storage I/O) purely for
    # memory cleanup. When that raised — e.g. a transient object-storage blip — it replaced
    # the in-flight import error, so a connection failure already classified as non-retryable
    # surfaced as the unrelated cleanup error and the job retried to its maximum instead of
    # stopping. The body error must propagate; the cleanup error must be swallowed.
    pipeline = PipelineNonDLT.__new__(PipelineNonDLT)
    pipeline._logger = AsyncMock()
    pipeline._resumable_source_manager = None
    pipeline._cdp_producer = cast(CDPProducer, object())  # unused: the patched clear-chunks ignores it
    pipeline._resource = cast(SourceResponse, object())
    pipeline._delta_table_helper = AsyncMock()
    pipeline._delta_table_helper.get_delta_table.side_effect = OSError("object storage unavailable")

    class ImportError_(Exception):
        pass

    async def _raise_import_error(_cdp_producer):
        raise ImportError_("Can't connect to MySQL server on")

    monkeypatch.setattr(
        "products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v2.pipeline.cdp_producer_clear_chunks",
        _raise_import_error,
    )

    with pytest.raises(ImportError_, match="Can't connect to MySQL server on"):
        await pipeline.run()

    pipeline._logger.aexception.assert_awaited_once_with("Failed to clean up delta table helper")


def _make_pipeline_for_post_run_operations() -> PipelineNonDLT:
    pipeline = PipelineNonDLT.__new__(PipelineNonDLT)
    pipeline._logger = AsyncMock()
    pipeline._delta_table_helper = AsyncMock()
    pipeline._delta_table_helper.get_delta_table.return_value = MagicMock()
    pipeline._delta_table_helper.get_file_uris.return_value = []
    pipeline._job = MagicMock(id=uuid.uuid4(), team_id=1)
    pipeline._table = None
    pipeline._resource_name = "orders"
    pipeline._schema = MagicMock()
    pipeline._schema.initial_sync_complete = True
    # Non-CDC skips the CDC-companion-seeding block, so only validate_schema_and_update_table
    # (mocked below) needs to be reachable.
    pipeline._schema.sync_type = ExternalDataSchema.SyncType.INCREMENTAL
    pipeline._schema.cdc_table_mode = "consolidated"
    pipeline._source = MagicMock()
    pipeline._resource = MagicMock()
    pipeline._internal_schema = MagicMock()
    pipeline._last_incremental_field_value = None
    return pipeline


def _fake_database_sync_to_async_pool(fn):
    async def _inner(*args, **kwargs):
        return fn(*args, **kwargs)

    return _inner


class TestPostRunOperationsCompactionErrorHandling:
    # Regression: `_post_run_operations` used to call `capture_exception` for every compaction
    # failure, including transient S3 rate-limit/connectivity blips that are already non-fatal
    # (the sync completes regardless) — flooding error tracking with noise. A genuine compaction
    # bug must still be captured.
    @parameterized.expand(
        [
            ("transient_s3_slowdown", OSError("Generic S3 error: Please reduce your request rate."), False),
            ("genuine_bug", OSError("Access Denied: not authorized"), True),
        ]
    )
    @pytest.mark.asyncio
    async def test_compaction_error_handling(self, _name: str, error: Exception, expect_capture: bool):
        pipeline = _make_pipeline_for_post_run_operations()
        cast(AsyncMock, pipeline._delta_table_helper.compact_table).side_effect = error

        with (
            patch(f"{_PIPELINE_MODULE}.capture_exception") as mock_capture,
            patch(f"{_PIPELINE_MODULE}.database_sync_to_async_pool", _fake_database_sync_to_async_pool),
            patch(f"{_PIPELINE_MODULE}.prepare_s3_files_for_querying", AsyncMock(return_value="orders__query_1")),
            patch(f"{_PIPELINE_MODULE}.update_last_synced_at", AsyncMock()),
            patch(f"{_PIPELINE_MODULE}.notify_revenue_analytics_that_sync_has_completed", AsyncMock()),
            patch(f"{_PIPELINE_MODULE}.finalize_desc_sort_incremental_value", AsyncMock()),
            patch(f"{_PIPELINE_MODULE}.validate_schema_and_update_table", AsyncMock()),
        ):
            await pipeline._post_run_operations(row_count=10)

        assert mock_capture.called is expect_capture
        if expect_capture:
            cast(AsyncMock, pipeline._logger.aexception).assert_awaited_once()
        else:
            cast(AsyncMock, pipeline._logger.awarning).assert_awaited_once()


class _YieldForShutdown(Exception):
    pass


class _FakeShutdownMonitor:
    def __init__(self, shutting_down: bool):
        self._shutting_down = shutting_down

    def is_worker_shutdown(self) -> bool:
        return self._shutting_down

    def raise_if_is_worker_shutdown(self):
        if self._shutting_down:
            raise _YieldForShutdown()


def _make_shutdown_pipeline(
    *,
    shutting_down: bool,
    resumable: bool,
    should_use_incremental_field: bool,
    sort_mode: str,
    reset_pipeline: bool,
) -> PipelineNonDLT:
    pipeline = PipelineNonDLT.__new__(PipelineNonDLT)
    pipeline._shutdown_monitor = cast(ShutdownMonitor, _FakeShutdownMonitor(shutting_down))
    pipeline._resumable_source_manager = cast(object, object()) if resumable else None  # type: ignore[assignment]
    pipeline._schema = cast(
        ExternalDataSchema,
        SimpleNamespace(should_use_incremental_field=should_use_incremental_field),
    )
    pipeline._resource = cast(SourceResponse, SimpleNamespace(sort_mode=sort_mode))
    pipeline._reset_pipeline = reset_pipeline
    return pipeline


# (resumable, incremental, sort_mode, reset) -> should this sync hand off *immediately* on drain?
# Mirrors should_check_shutdown: resumable OR (ascending incremental and not a reset).
@pytest.mark.parametrize(
    "resumable,should_use_incremental_field,sort_mode,reset_pipeline,expect_bail_now",
    [
        (True, False, "asc", False, True),  # resumable source (e.g. Stripe), even desc/reset
        (True, True, "desc", True, True),  # resumable wins over the desc/reset guards
        (False, True, "asc", False, True),  # ascending incremental persists its watermark per chunk
        (False, True, "desc", False, False),  # desc incremental commits watermark only at the end
        (False, True, "asc", True, False),  # a reset would restart from 0 rows
        (False, False, "asc", False, False),  # non-resumable full refresh
    ],
)
def test_should_bail_now(resumable, should_use_incremental_field, sort_mode, reset_pipeline, expect_bail_now):
    pipeline = _make_shutdown_pipeline(
        shutting_down=True,
        resumable=resumable,
        should_use_incremental_field=should_use_incremental_field,
        sort_mode=sort_mode,
        reset_pipeline=reset_pipeline,
    )
    assert pipeline._should_bail_now() is expect_bail_now


def test_should_bail_now_false_when_worker_healthy():
    pipeline = _make_shutdown_pipeline(
        shutting_down=False, resumable=True, should_use_incremental_field=True, sort_mode="asc", reset_pipeline=False
    )
    assert pipeline._should_bail_now() is False


def _make_consume_pipeline(
    *,
    shutting_down: bool,
    resumable: bool,
    source_tables: list[pa.Table],
    chunk_size: int = 10_000,
) -> tuple[PipelineNonDLT, AsyncMock]:
    pipeline = PipelineNonDLT.__new__(PipelineNonDLT)
    pipeline._shutdown_monitor = cast(ShutdownMonitor, _FakeShutdownMonitor(shutting_down))
    pipeline._resumable_source_manager = cast(object, object()) if resumable else None  # type: ignore[assignment]
    pipeline._schema = cast(ExternalDataSchema, SimpleNamespace(should_use_incremental_field=False, name="test"))
    pipeline._resource = cast(SourceResponse, SimpleNamespace(items=lambda: source_tables, sort_mode="asc"))
    pipeline._reset_pipeline = False
    pipeline._logger = MagicMock()
    pipeline._source = cast(Any, SimpleNamespace(source_type="test"))
    pipeline._job = cast(Any, SimpleNamespace(team_id=1))
    pipeline._batcher = Batcher(MagicMock(), chunk_size=chunk_size)
    process_mock = AsyncMock()
    pipeline._process_pa_table = process_mock  # type: ignore[method-assign]
    return pipeline, process_mock


def _rows_processed(process_mock: AsyncMock) -> list[int]:
    return [v.as_py() for call in process_mock.await_args_list for v in call.kwargs["pa_table"].column("id")]


@pytest.mark.asyncio
async def test_consume_and_load_syncs_everything_when_healthy():
    tables = [pa.table({"id": [1, 2]}), pa.table({"id": [3, 4]}), pa.table({"id": [5]})]
    pipeline, process_mock = _make_consume_pipeline(shutting_down=False, resumable=True, source_tables=tables)

    row_count = await pipeline._consume_and_load(
        pa_memory_pool=pa.default_memory_pool(), should_resume=False, is_first_ever_sync=True
    )

    assert row_count == 5
    assert _rows_processed(process_mock) == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_consume_and_load_resumable_flushes_partial_before_raising():
    # The whole point: when the worker drains, the buffered-but-not-yet-a-full-chunk rows are
    # committed (flushed) *before* the raise, so committed progress catches up to the source's
    # checkpoint and the resumed pod skips nothing. A big chunk size means nothing flushes on its own.
    tables = [pa.table({"id": [1, 2]}), pa.table({"id": [3, 4]})]
    pipeline, process_mock = _make_consume_pipeline(shutting_down=True, resumable=True, source_tables=tables)

    with pytest.raises(_YieldForShutdown):
        await pipeline._consume_and_load(
            pa_memory_pool=pa.default_memory_pool(), should_resume=False, is_first_ever_sync=True
        )

    # The first item's rows were flushed and committed before the hand-off (not discarded).
    assert _rows_processed(process_mock) == [1, 2]


@pytest.mark.asyncio
async def test_consume_and_load_non_resumable_rides_drain_and_completes():
    # A non-resumable source can't resume cheaply, so it never bails on shutdown — it finishes the
    # whole load on the draining pod rather than restarting from zero on a new one.
    tables = [pa.table({"id": [1, 2]}), pa.table({"id": [3, 4]})]
    pipeline, process_mock = _make_consume_pipeline(shutting_down=True, resumable=False, source_tables=tables)

    row_count = await pipeline._consume_and_load(
        pa_memory_pool=pa.default_memory_pool(), should_resume=False, is_first_ever_sync=True
    )

    assert row_count == 4
    assert _rows_processed(process_mock) == [1, 2, 3, 4]
