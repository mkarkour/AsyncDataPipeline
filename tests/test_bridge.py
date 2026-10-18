"""Tests for the bridge module.

Verifies execution dispatch rules across different callable types:
- Sync functions offloaded to thread workers.
- Coroutine functions awaited inline on the event loop.
- Async generator functions called inline without execution.
- Exception propagation and argument forwarding.
"""

import asyncio
import threading

import pytest

from asyncdatapipeline._bridge import bridge


class AsyncCallable:
    async def __call__(self, x):
        return threading.get_ident(), x


class AwaitableAndIterable:
    def __await__(self):
        raise AssertionError("Should have been iterated, not awaited")

    async def __aiter__(self):
        yield "streamed"

# ------------------------------ #
# CHECK EXECUTION DISPATCH RULES
# ------------------------------ #


@pytest.mark.asyncio
async def test_sync_function_runs_off_loop_thread():
    def fn(a, b):
        return threading.get_ident(), a + b

    ident, total = await bridge(fn, 1, 2)
    assert ident != threading.get_ident()
    assert total == 3


# ------------------------------ #
# CHECK RESULT RESOLUTION RULES
# ------------------------------ #


@pytest.mark.asyncio
async def test_async_generator_is_returned_unconsumed():
    async def gen():
        yield 1
        yield 2

    out = await bridge(gen)
    assert [x async for x in out] == [1, 2]


@pytest.mark.asyncio
async def test_awaitable_returned_from_a_thread_is_awaited():
    async def inner():
        return "resolved"

    def fn():
        return inner()

    assert await bridge(fn) == "resolved"


# ------------------------------ #
# CHECK TYPES RULES
# ------------------------------ #

@pytest.mark.asyncio
async def test_object_that_is_both_is_treated_as_a_stream():
    def fn():
        return AwaitableAndIterable()

    out = await bridge(fn)
    assert [x async for x in out] == ["streamed"]


@pytest.mark.asyncio
async def test_returned_list_is_a_single_value_not_a_stream():
    def fn():
        return [1, 2, 3]

    assert await bridge(fn) == [1, 2, 3]


# ------------------------------ #
# CHECK PROPAGATION RULES
# ------------------------------ #

@pytest.mark.asyncio
async def test_cancellation_propagates():
    started = asyncio.Event()

    async def fn():
        started.set()
        await asyncio.sleep(3600)

    task = asyncio.create_task(bridge(fn))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
