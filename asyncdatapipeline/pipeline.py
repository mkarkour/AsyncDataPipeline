"""Asynchronous data processing pipeline module.

Provides a concurrent pipeline framework to collect items from asynchronous or
synchronous sources, apply transformations sequentially, and dispatch results
to multiple destinations with configurable concurrency and retry limits.
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Iterable,
    Sequence,
)
from dataclasses import dataclass
from typing import Any, Final

from ._bridge import bridge
from .exceptions import (
    DestinationError,
    SourceError,
    StageError,
    TransformerError,
    _callable_name,
)
from .metrics import PipelineMetrics
from .retry import RetryCallback, RetryPolicy, call_with_retry
from .types import Destination, Source, Transformer

__all__ = ["AsyncDataPipeline"]

_LOGGER: Final = logging.getLogger("asyncdatapipeline")
_DEFAULT_RETRY: Final = RetryPolicy()
_MAX_RETAINED_ERRORS: Final = 100
"""Upper bound on ``StageError`` instances kept on the pipeline after a run.

Failures beyond this cap are still counted and logged, but not retained, so a
run that fails on every item cannot grow memory without bound.
"""


@dataclass(slots=True)
class _RunState:
    """Isolated execution state for a single pipeline run.

    Attributes:
        semaphore: Concurrency limiter for worker tasks.
        task_group: Task group managing concurrent worker lifetimes.
        metrics: Collector for run-level counters and execution rates.
        errors: Failures captured during the run.
    """

    semaphore: asyncio.Semaphore
    task_group: asyncio.TaskGroup
    metrics: PipelineMetrics
    errors: list[StageError]


def _check_callables(name: str, items: Iterable[object]) -> None:
    """Validate that all elements in an iterable are callable.

    Args:
        name: Name of the pipeline stage being validated.
        items: Sequence of callables to check.

    Raises:
        TypeError: If any item in the sequence is not callable.
    """
    for idx, item in enumerate(items):
        if not callable(item):
            msg = f"{name}[{idx}] is not callable: {item!r}"
            raise TypeError(msg)


def _closing(
    stream: AsyncIterable[object],
) -> contextlib.AbstractAsyncContextManager[object]:
    """Wrap an async iterable in an async cleanup context manager if necessary.

    Args:
        stream: The asynchronous iterable to manage.

    Returns:
        An async context manager that automatically closes async generators on
        exit.
    """
    if isinstance(stream, AsyncGenerator):
        return contextlib.aclosing(stream)

    return contextlib.nullcontext()


class AsyncDataPipeline[T]:
    """Collect from sources, transform, and dispatch to every destination.

    The type parameter is the item type flowing through the pipeline: sources
    produce ``T``, transformers preserve ``T``, destinations accept ``T``.

    Attributes:
        metrics: Counters for the most recent run.
        errors: Failures retained from the most recent run, capped.
    """

    def __init__(
        self,
        sources: Sequence[Source[T]],
        transformers: Sequence[Transformer[T]] | None = None,
        destinations: Sequence[Destination[T]] | None = None,
        max_concurrent_tasks: int = 10,
    ) -> None:
        """Validate and store the pipeline configuration.

        Args:
            sources: Callables invoked once each per run. Each returns one
                item, an awaitable, or an async iterable to be drained.
            transformers: Callables applied in order to every item.
            destinations: Callables receiving every transformed item.
            max_concurrent_tasks: How many items may be in flight at once.

        Raises:
            ValueError: If ``sources`` is empty, or ``max_concurrent_tasks``
                is below 1.
            TypeError: If any source, transformer or destination is not
                callable.
        """
        if not sources:
            raise ValueError("at least one source is required")
        if max_concurrent_tasks < 1:
            raise ValueError("max_concurrent_tasks must be at least 1")

        _check_callables("sources", sources)
        _check_callables("transformers", transformers or ())
        _check_callables("destinations", destinations or ())

        self._sources: tuple[Source[T], ...] = tuple(sources)
        self._transformers: tuple[Transformer[T], ...] = tuple(transformers or ())
        self._destinations: tuple[Destination[T], ...] = tuple(destinations or ())
        self._max_concurrent_tasks = max_concurrent_tasks
        self._retry_policy = _DEFAULT_RETRY
        self._running = False

        self.metrics = PipelineMetrics()
        self.errors: list[StageError] = []

    async def run(self) -> None:
        """Execute one pipeline run and return when every item is dispatched.

        Each source is invoked exactly once. Single-shot sources contribute
        one item; streaming sources are drained until exhausted. Stage
        failures are recorded rather than raised.

        Raises:
            RuntimeError: If a run is already in progress on this instance.
            asyncio.CancelledError: If the run is cancelled from outside.
        """
        if self._running:
            raise RuntimeError("a run is already in progress on this pipeline")

        self._running = True
        self.metrics = PipelineMetrics()
        self.errors = []

        _LOGGER.info(
            "run started: %d source(s), %d transformer(s), %d destination(s), "
            "max_concurrent_tasks=%d",
            len(self._sources),
            len(self._transformers),
            len(self._destinations),
            self._max_concurrent_tasks,
        )
        self.metrics.start()

        try:
            async with asyncio.TaskGroup() as task_group:
                state = _RunState(
                    semaphore=asyncio.Semaphore(self._max_concurrent_tasks),
                    task_group=task_group,
                    metrics=self.metrics,
                    errors=self.errors,
                )
                for source in self._sources:
                    task_group.create_task(
                        self._collect(state, source),
                        name=f"collect:{_callable_name(source)}",
                    )
        finally:
            self.metrics.finish()
            self._running = False
            _LOGGER.info(
                "run finished in %.3fs: %d collected, %d transformed, "
                "%d dispatched, %d failure(s)",
                self.metrics.duration,
                self.metrics.items_collected,
                self.metrics.items_transformed,
                self.metrics.dispatches_ok,
                self.metrics.failures_total,
            )

    async def _collect(self, state: _RunState, source: Source[T]) -> None:
        """Invoke one source and schedule every item it contributes.

        A source is never retried: re-invoking one that failed part-way
        through a stream would re-emit the items it already produced. Items
        produced before the failure keep flowing through the pipeline.

        Args:
            state: State for the current run.
            source: The source callable to invoke.
        """
        produced = 0
        try:
            output = await bridge(source)
            if isinstance(output, AsyncIterable):
                async with _closing(output):
                    async for item in output:
                        await self._spawn(state, item)
                        produced += 1
            else:
                await self._spawn(state, output)
                produced = 1
        except Exception as exc:
            state.metrics.record_failure("source")
            self._record(state, SourceError(source), exc)

        _LOGGER.debug(
            "source %s contributed %d item(s)",
            _callable_name(source),
            produced
        )

    async def _spawn(self, state: _RunState, item: T) -> None:
        """Take a concurrency slot, then schedule the item for processing.

        Acquiring the semaphore here rather than inside the worker is what
        applies backpressure: a source that outruns the destinations is
        suspended on ``acquire`` instead of queueing items in memory.

        Args:
            state: State for the current run.
            item: The item to process.
        """
        await state.semaphore.acquire()
        state.metrics.items_collected += 1
        state.task_group.create_task(self._process(state, item), name="process")

    async def _process(self, state: _RunState, item: T) -> None:
        """Run one item through the transformer chain, then dispatch it.

        A transformer that fails every attempt drops the item: it reaches no
        destination. This method never lets an exception escape, so one bad
        item cannot cancel the task group and end the run.

        Args:
            state: State for the current run.
            item: The item to transform and dispatch.
        """
        started = time.perf_counter()
        try:
            for transformer in self._transformers:
                try:
                    item, _ = await call_with_retry(
                        self._retry_policy,
                        transformer,
                        item,
                        on_retry=self._log_retry(transformer),
                    )
                except Exception as exc:
                    state.metrics.record_failure("transformer")
                    error = TransformerError(
                        transformer,
                        item=item,
                        attempts=self._attempts_for(exc)
                    )
                    self._record(state, error, exc)
                    return

            state.metrics.items_transformed += 1
            await asyncio.gather(
                *(
                    self._dispatch(state, destination, item)
                    for destination in self._destinations
                )
            )
            state.metrics.record_latency(time.perf_counter() - started)
        finally:
            state.semaphore.release()

    async def _dispatch(
        self, state: _RunState, destination: Destination[T], item: T
    ) -> None:
        """Deliver one item to one destination.

        Never raises. Destinations are independent, so a failure here must not
        cancel the sibling dispatches gathered alongside it.

        Args:
            state: State for the current run.
            destination: The destination callable to invoke.
            item: The transformed item to deliver.
        """
        try:
            await call_with_retry(
                self._retry_policy,
                destination,
                item,
                on_retry=self._log_retry(destination),
            )
        except Exception as exc:
            state.metrics.record_failure("destination")
            error = DestinationError(
                destination, item=item, attempts=self._attempts_for(exc)
            )
            self._record(state, error, exc)
        else:
            state.metrics.dispatches_ok += 1

    def _attempts_for(self, _exc: Exception) -> int:
        """Return how many attempts the retry policy allowed.

        ``call_with_retry`` reports its attempt count only on success. A
        failure means the policy ran to its limit, unless the exception was
        not retryable, which the message records anyway.

        Args:
            _exc: The exception that ended the call.

        Returns:
            The configured maximum number of attempts.
        """
        return self._retry_policy.max_attempts

    def _record(
        self, state: _RunState, error: StageError, cause: BaseException
    ) -> None:
        """Log a stage failure and retain it up to the cap.

        Args:
            state: State for the current run.
            error: The stage error describing the failure.
            cause: The original exception, chained onto ``error``.
        """
        error.__cause__ = cause
        if len(state.errors) < _MAX_RETAINED_ERRORS:
            state.errors.append(error)
        _LOGGER.error("%s", error, exc_info=cause)

    def _log_retry(self, fn: Callable[..., Any]) -> RetryCallback:
        """Build the hook that logs a failed attempt before the next one.

        Args:
            fn: The callable being retried.

        Returns:
            A callback taking the attempt number, the exception and the delay.
        """
        name = _callable_name(fn)

        def on_retry(attempt: int, exc: Exception, delay: float) -> None:
            _LOGGER.warning(
                "%s attempt %d failed (%s: %s); retrying in %.2fs",
                name,
                attempt,
                type(exc).__name__,
                exc,
                delay,
            )

        return on_retry
