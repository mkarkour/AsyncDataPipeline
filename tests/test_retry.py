"""Tests for the retry module.

Verifies policy validation, the delay schedule, and that call_with_retry
retries only what the policy allows and reports how the call ended.
"""


import asyncio

import pytest

from asyncdatapipeline.retry import RetryPolicy, call_with_retry

FAST = RetryPolicy(base_delay=0, max_delay=0, jitter=False)


class Transient(Exception):
    pass


class Fatal(Exception):
    pass


def flaky(fail_times, exc=Transient):
    calls = []

    def fn():
        calls.append(None)
        if len(calls) <= fail_times:
            raise exc(f"failure {len(calls)}")
        return "ok"

    fn.calls = calls
    return fn

# ---------------------------------- #
# CHECK POLICY
# ---------------------------------- #


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"base_delay": -1},
        {"multiplier": 0.5},
        {"retry_on" : ()},
    ],
)
def test_invalid_policy_raises_value_error(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


# ---------------------------------- #
# CHECK RETRY CONTRACT
# ---------------------------------- #

@pytest.mark.asyncio
async def test_success_retruns_the_result_and_the_attempt_count():
    fn = flaky(2)
    assert await call_with_retry(FAST, fn) == ("ok", 3)


@pytest.mark.asyncio
async def test_exhausted_attempts_reraise_the_original_exception():
    fn = flaky(10)
    with pytest.raises(Transient, match="failure 3"):
        await call_with_retry(FAST, fn)
    assert len(fn.calls) == FAST.max_attempts


@pytest.mark.asyncio
async def test_non_retryable_exception_fails_immediately():
    policy = RetryPolicy(base_delay=0, jitter=False, retry_on=(Transient,))
    fn = flaky(10, exc=Fatal)
    with pytest.raises(Fatal):
        await call_with_retry(policy, fn)
    assert len(fn.calls) == 1


@pytest.mark.asyncio
async def test_none_policy_means_a_single_attempt():
    fn = flaky(1)
    with pytest.raises(Transient):
        await call_with_retry(None, fn)
    assert len(fn.calls) == 1

# ---------------------------------- #
# CHECK DELAY SCHEDULE
# ---------------------------------- #

def test_delay_grows_exponentially_and_is_capped():
    policy = RetryPolicy(base_delay=1, multiplier=2, max_delay=5, jitter=False)
    schedule = [policy.delay_for(n) for n in (1, 2, 3, 4)]
    assert schedule == pytest.approx([1.0, 2.0, 4.0, 5.0])


# ---------------------------------- #
# CHECK CANCELLATION SAFETY
# ---------------------------------- #

@pytest.mark.asyncio
async def test_cancellation_is_not_retried():
    started = asyncio.Event()
    calls = []

    async def fn():
        calls.append(None)
        started.set()
        await asyncio.sleep(3600)

    task = asyncio.create_task(call_with_retry(FAST, fn))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(calls) == 1
