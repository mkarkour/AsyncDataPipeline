"""Retry policy and retrying execution helper for pipeline stages.

Encapsulates retry configuration (attempts, backoff, transient exceptions) via
``RetryPolicy`` and executes callables via ``call_with_retry``.

Transformers and destinations are retried according to policy; sources are
never retried to avoid re-emitting already produced items.
"""

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ._bridge import bridge

__all__ = [
    "RetryPolicy",
    "call_with_retry",
]

type RetryCallback = Callable[[int, Exception, float], None]
"""Callback signature for the `on_retry` hook.

Invoked after a failed attempt with the 1-based attempt count, the caught
exception, and the backoff delay in seconds before the next retry.
"""


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Immutable policy for exponential backoff and retry behavior.

    Computes backoff delays as ``min(base_delay * multiplier^(attempt - 1),
    max_delay)``. Enables equal jitter across the upper half of the delay
    window to stagger concurrent retries without zero-wait attempts.

    Attributes:
        max_attempts: Total invocations allowed, including the initial attempt.
        base_delay: Wait time in seconds after the first failure.
        max_delay: Upper bound in seconds for any single backoff delay.
        multiplier: Exponential growth factor applied after each failed
                    attempt.
        jitter: Whether to randomize delays across the upper backoff window.
        retry_on: Tuple of exception types that trigger a retry. Unmatched
                  exceptions fail immediately. Non-`Exception` subclasses
                  (such as ``CancelledError``) are rejected during
                  initialization to prevent swallowing task cancellations.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 5.0
    multiplier: float = 2.0
    jitter: bool = True
    retry_on: tuple[type[Exception], ...] = (Exception,)

    def __post_init__(self) -> None:
        """Reject configurations that could never behave sensibly.

        Raises:
            ValueError: If attempt limits, delays, multipliers, or exception
                        tuples fall outside valid operational bounds.
            TypeError: If any entry in ``retry_on`` is not a subclass of
                       ``Exception``.
        """
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be non-negative")

        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")

        if not self.retry_on:
            raise ValueError("retry_on must name at least one exception type")

        for exc_type in self.retry_on:
            if not (
                isinstance(exc_type, type) and
                issubclass(exc_type, Exception)
            ):
                raise TypeError(
                    f"retry_on entries must subclass Exception: {exc_type!r}")

    def delay_for(self, attempt: int) -> float:
        """Return the delay, in seconds, to wait after a failed ``attempt``.

        Args:
            attempt: The 1-based number of the attempt that just failed.

        Returns:
            A non-negative number of seconds.

        Raises:
            ValueError: If ``attempt``is below 1.
        """
        if attempt < 1:
            raise ValueError("attempt numbers start at 1")

        growth = self.multiplier ** (attempt - 1)
        capped = min(self.base_delay * growth, self.max_delay)

        if not self.jitter:
            return capped

        return random.uniform(capped / 2, capped)  # noqa: S311


async def call_with_retry(
    policy: RetryPolicy | None,
    fn: Callable[..., Any],
    /,
    *args: object,
    on_retry: RetryCallback | None = None
) -> tuple[Any, int]:
    """Invoke a callable through bridge with optional retry logic.

    Args:
        policy: The retry policy to apply, or None for a single attempt.
        fn: The callable to invoke.
        *args: Positional arguments forwarded to ``fn``.
        on_retry: Optional hook invoked after a retryable failure before
                  sleeping.

    Returns:
        A tuple of ``(result, attempt_count)`` containing the successful call's
        return value and total invocations executed.

    Raises:
        Exception: The exception raised by the final attempt, unchanged. The
                   caller wraps it in a ``StageError``.
        asyncio.CancelledError: Propagated immediately; never retried, because
                                it does not subclass ``Exception``.
    """
    max_attempts = policy.max_attempts if policy is not None else 1

    for attempt in range(1, max_attempts + 1):
        try:
            return await bridge(fn, *args), attempt
        except Exception as exc:
            last_attempt = attempt == max_attempts

            if (
                policy is None or last_attempt or
                not isinstance(exc, policy.retry_on)
            ):
                raise

            delay = policy.delay_for(attempt)
            if on_retry is not None:
                on_retry(attempt, exc, delay)

            await asyncio.sleep(delay)

    raise AssertionError("unreachable: loop always returns or raises")
