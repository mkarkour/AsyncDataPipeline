"""Tests for the pipeline module.

Verifies construction, the data flow from sources through transformers to
destinations, the isolation of failures, concurrency bounds, retries,
metrics and re-runnability.
"""

import asyncio
import logging

import pytest

import asyncdatapipeline.pipeline as pipeline_module
from asyncdatapipeline.exceptions import (
    DestinationError,
    SourceError,
    TransformerError,
)
from asyncdatapipeline.pipeline import AsyncDataPipeline
from asyncdatapipeline.retry import RetryPolicy

FAST = RetryPolicy(base_delay=0, max_delay=0, jitter=False)


class Transient(Exception):
    pass


class Sink:
    def __init__(self):
        self.items = []

    async def __call__(self, item):
        self.items.append(item)


def one(value):
    def source():
        return value

    return source


def stream(*values):
    async def source():
        for value in values:
            yield value

    return source


def flaky(fail_times, exc=Transient):
    calls = []

    def fn(item):
        calls.append(item)
        if len(calls) <= fail_times:
            raise exc(f"failure {len(calls)}")
        return item

    fn.calls = calls
    return fn


def reject(bad):
    def fn(item):
        if item == bad:
            raise Transient(f"rejected {item}")
        return item

    return fn


async def run(sources, transformers=None, destinations=None, **kwargs):
    pipeline = AsyncDataPipeline(sources, transformers, destinations, **kwargs)
    await pipeline.run()
    return pipeline


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(pipeline_module, "_DEFAULT_RETRY", FAST)


# ---------------------------------- #
# CHECK CONSTRUCTION
# ---------------------------------- #


def test_empty_sources_raises_value_error():
    with pytest.raises(ValueError, match="at least one source"):
        AsyncDataPipeline([])


@pytest.mark.parametrize("limit", [0, -1])
def test_non_positive_concurrency_limit_raises_value_error(limit):
    with pytest.raises(ValueError, match="max_concurrent_tasks"):
        AsyncDataPipeline([len], max_concurrent_tasks=limit)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"sources": [42]}, r"sources\[0\]"),
        ({"sources": [len], "transformers": [len, "x"]}, r"transformers\[1\]"),
        ({"sources": [len], "destinations": [None]}, r"destinations\[0\]"),
    ],
)
def test_non_callable_stage_raises_type_error(kwargs, match):
    with pytest.raises(TypeError, match=match):
        AsyncDataPipeline(**kwargs)


def test_fresh_pipeline_has_empty_metrics_and_errors():
    pipeline = AsyncDataPipeline([len])
    assert pipeline.metrics.items_collected == 0
    assert pipeline.metrics.started_at is None
    assert pipeline.errors == []


# ---------------------------------- #
# CHECK DATA FLOW
# ---------------------------------- #


@pytest.mark.asyncio
async def test_single_shot_sources_contribute_one_item_each():
    async def coro_source():
        return "async"

    sink = Sink()
    await run([one("sync"), coro_source], destinations=[sink])
    assert sorted(sink.items) == ["async", "sync"]


@pytest.mark.asyncio
async def test_streaming_source_is_drained():
    sink = Sink()
    await run([stream(1, 2, 3)], destinations=[sink])
    assert sorted(sink.items) == [1, 2, 3]


@pytest.mark.asyncio
async def test_awaited_stream_is_drained():
    async def source():
        return stream(1, 2)()

    sink = Sink()
    await run([source], destinations=[sink])
    assert sorted(sink.items) == [1, 2]


@pytest.mark.asyncio
async def test_sync_iterable_is_a_single_item_not_a_stream():
    sink = Sink()
    await run([one([1, 2, 3])], destinations=[sink])
    assert sink.items == [[1, 2, 3]]


@pytest.mark.asyncio
async def test_none_is_a_valid_item():
    sink = Sink()
    await run([one(None)], destinations=[sink])
    assert sink.items == [None]


@pytest.mark.asyncio
async def test_transformers_apply_in_order():
    def add_one(x):
        return x + 1

    async def double(x):
        return x * 2

    sink = Sink()
    await run([one(1)], [add_one, double], [sink])
    assert sink.items == [4]


@pytest.mark.asyncio
async def test_every_destination_receives_every_item():
    a, b = Sink(), Sink()
    await run([stream(1, 2, 3)], destinations=[a, b])
    assert sorted(a.items) == sorted(b.items) == [1, 2, 3]


@pytest.mark.asyncio
async def test_sources_alone_are_a_valid_pipeline():
    pipeline = await run([stream(1, 2)])
    assert pipeline.metrics.items_transformed == 2
    assert pipeline.metrics.dispatches_ok == 0


# ---------------------------------- #
# CHECK FAILURE ISOLATION
# ---------------------------------- #


@pytest.mark.asyncio
async def test_failing_source_does_not_stop_other_sources():
    def broken():
        raise Transient("boom")

    sink = Sink()
    pipeline = await run([broken, stream(1, 2)], destinations=[sink])
    assert sorted(sink.items) == [1, 2]
    assert pipeline.metrics.failures["source"] == 1
    assert isinstance(pipeline.errors[0], SourceError)


@pytest.mark.asyncio
async def test_source_failing_mid_stream_keeps_earlier_items():
    async def source():
        yield 1
        yield 2
        raise Transient("mid-stream")

    sink = Sink()
    pipeline = await run([source], destinations=[sink])
    assert sorted(sink.items) == [1, 2]
    assert pipeline.metrics.items_collected == 2
    assert pipeline.metrics.failures["source"] == 1


@pytest.mark.asyncio
async def test_a_source_is_invoked_once_and_never_retried():
    calls = []

    def broken():
        calls.append(None)
        raise Transient("boom")

    pipeline = await run([broken])
    assert len(calls) == 1
    assert pipeline.errors[0].attempts == 1


@pytest.mark.asyncio
async def test_failing_transformer_drops_only_that_item():
    sink = Sink()
    pipeline = await run([stream(1, 2, 3)], [reject(2)], [sink])
    assert sorted(sink.items) == [1, 3]
    assert pipeline.metrics.items_transformed == 2
    assert pipeline.metrics.failures["transformer"] == 1
    (err,) = pipeline.errors
    assert isinstance(err, TransformerError)
    assert err.item_repr == "2"


@pytest.mark.asyncio
async def test_failing_destination_does_not_block_siblings():
    async def broken(item):
        raise Transient("boom")

    sink = Sink()
    pipeline = await run([stream(1, 2, 3)], destinations=[broken, sink])
    assert sorted(sink.items) == [1, 2, 3]
    assert pipeline.metrics.dispatches_ok == 3
    assert pipeline.metrics.failures["destination"] == 3
    assert all(isinstance(e, DestinationError) for e in pipeline.errors)


@pytest.mark.asyncio
async def test_recorded_errors_chain_the_original_exception():
    boom = Transient("boom")

    def broken():
        raise boom

    pipeline = await run([broken])
    assert pipeline.errors[0].__cause__ is boom


@pytest.mark.asyncio
async def test_retained_errors_are_capped_but_all_are_counted(monkeypatch):
    monkeypatch.setattr(pipeline_module, "_MAX_RETAINED_ERRORS", 3)

    async def broken(item):
        raise Transient("boom")

    pipeline = await run([stream(*range(10))], destinations=[broken])
    assert len(pipeline.errors) == 3
    assert pipeline.metrics.failures["destination"] == 10


@pytest.mark.asyncio
async def test_stage_failures_are_logged_as_errors(caplog):
    caplog.set_level(logging.ERROR, logger="asyncdatapipeline")

    def broken():
        raise Transient("boom")

    await run([broken])
    (record,) = caplog.records
    assert record.getMessage().startswith("source ")
    assert record.getMessage().endswith("failed after 1 attempt")
    assert record.exc_info is not None


# ---------------------------------- #
# CHECK RETRIES
# ---------------------------------- #


@pytest.mark.asyncio
async def test_transformer_is_retried_until_it_succeeds():
    transformer = flaky(2)
    sink = Sink()
    pipeline = await run([one(1)], [transformer], [sink])
    assert sink.items == [1]
    assert len(transformer.calls) == 3
    assert pipeline.errors == []


@pytest.mark.asyncio
async def test_destination_is_retried_until_it_succeeds():
    destination = flaky(1)
    pipeline = await run([one(1)], destinations=[destination])
    assert len(destination.calls) == 2
    assert pipeline.metrics.dispatches_ok == 1
    assert pipeline.errors == []


@pytest.mark.asyncio
async def test_exhausted_retries_record_the_attempt_count():
    transformer = flaky(10)
    pipeline = await run([one(1)], [transformer])
    assert len(transformer.calls) == FAST.max_attempts
    assert pipeline.errors[0].attempts == FAST.max_attempts


@pytest.mark.asyncio
async def test_each_retry_is_logged_as_a_warning(caplog):
    caplog.set_level(logging.WARNING, logger="asyncdatapipeline")
    await run([one(1)], [flaky(2)])
    messages = [
        r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert len(messages) == 2
    assert "attempt 1 failed (Transient: failure 1)" in messages[0]
    assert "attempt 2 failed (Transient: failure 2)" in messages[1]


# ---------------------------------- #
# CHECK CONCURRENCY BOUNDS
# ---------------------------------- #


@pytest.mark.asyncio
async def test_in_flight_items_never_exceed_the_limit():
    active = 0
    peak = 0

    async def slow(item):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1

    await run([stream(*range(10))], destinations=[slow], max_concurrent_tasks=3)
    assert peak == 3


@pytest.mark.asyncio
async def test_a_slow_destination_suspends_the_source():
    yielded = []
    dispatched = asyncio.Event()
    release = asyncio.Event()

    async def source():
        for n in range(10):
            yielded.append(n)
            yield n

    async def destination(item):
        dispatched.set()
        await release.wait()

    pipeline = AsyncDataPipeline(
        [source], destinations=[destination], max_concurrent_tasks=1
    )
    task = asyncio.create_task(pipeline.run())
    await dispatched.wait()

    # One item in flight, one waiting for a slot, the rest not yet produced.
    assert yielded == [0, 1]

    release.set()
    await task
    assert yielded == list(range(10))


@pytest.mark.asyncio
async def test_slots_are_released_when_an_item_fails():
    def broken(item):
        raise Transient("boom")

    pipeline = AsyncDataPipeline([stream(1, 2, 3)], [broken], max_concurrent_tasks=1)
    await asyncio.wait_for(pipeline.run(), timeout=5)
    assert pipeline.metrics.failures["transformer"] == 3


# ---------------------------------- #
# CHECK METRICS
# ---------------------------------- #


@pytest.mark.asyncio
async def test_counters_reflect_the_run():
    a, b = Sink(), Sink()
    pipeline = await run([one(0), stream(1, 2, 3)], [reject(2)], [a, b])
    m = pipeline.metrics
    assert m.items_collected == 4
    assert m.items_transformed == 3
    assert m.dispatches_ok == 6
    assert m.failures == {"source": 0, "transformer": 1, "destination": 0}
    assert m.started_at is not None
    assert m.finished_at is not None
    assert m.duration > 0


@pytest.mark.asyncio
async def test_latency_spans_transformers_and_destinations():
    async def slow(item):
        await asyncio.sleep(0.01)
        return item

    pipeline = await run([one(1)], [slow], [slow])
    assert pipeline.metrics.latency_total > 0.015
    assert pipeline.metrics.mean_latency == pipeline.metrics.latency_total


@pytest.mark.asyncio
async def test_latency_is_recorded_only_for_delivered_items():
    pipeline = await run([stream(1, 2)], [reject(1), reject(2)], [Sink()])
    assert pipeline.metrics.items_transformed == 0
    assert pipeline.metrics.latency_total == 0.0


@pytest.mark.asyncio
async def test_run_lifecycle_is_logged(caplog):
    caplog.set_level(logging.INFO, logger="asyncdatapipeline")
    await run([one(1)])
    messages = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("run started") for m in messages)
    assert any(m.startswith("run finished") for m in messages)


# ---------------------------------- #
# CHECK RE-RUNNABILITY
# ---------------------------------- #


@pytest.mark.asyncio
async def test_a_second_run_starts_from_fresh_state():
    sink = Sink()
    pipeline = AsyncDataPipeline([stream(1, 2)], [reject(2)], [sink])

    await pipeline.run()
    first_metrics = pipeline.metrics
    await pipeline.run()

    assert sorted(sink.items) == [1, 1]
    assert pipeline.metrics is not first_metrics
    assert pipeline.metrics.items_collected == 2
    assert len(pipeline.errors) == 1


@pytest.mark.asyncio
async def test_overlapping_runs_on_one_instance_are_rejected():
    started = asyncio.Event()
    release = asyncio.Event()

    async def source():
        started.set()
        await release.wait()
        return 1

    pipeline = AsyncDataPipeline([source])
    task = asyncio.create_task(pipeline.run())
    await started.wait()

    with pytest.raises(RuntimeError, match="already in progress"):
        await pipeline.run()

    release.set()
    await task
    assert pipeline.metrics.items_collected == 1


@pytest.mark.asyncio
async def test_cancelled_run_leaves_the_pipeline_reusable():
    started = asyncio.Event()
    calls = []

    async def source():
        calls.append(None)
        if len(calls) == 1:
            started.set()
            await asyncio.sleep(3600)
        return "second"

    sink = Sink()
    pipeline = AsyncDataPipeline([source], destinations=[sink])
    task = asyncio.create_task(pipeline.run())
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    await pipeline.run()
    assert sink.items == ["second"]


@pytest.mark.asyncio
async def test_streaming_source_is_closed_when_the_run_is_cancelled():
    closed = False
    dispatched = asyncio.Event()

    async def numbers():
        nonlocal closed
        try:
            for n in range(10):
                yield n
        finally:
            closed = True

    # Held here so that only an explicit aclose(), not garbage collection,
    # can run the generator's cleanup.
    generator = numbers()

    async def destination(item):
        dispatched.set()
        await asyncio.sleep(3600)

    pipeline = AsyncDataPipeline(
        [one(generator)], destinations=[destination], max_concurrent_tasks=1
    )
    task = asyncio.create_task(pipeline.run())
    await dispatched.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert closed
