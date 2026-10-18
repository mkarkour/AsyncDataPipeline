"""Bridge between user callables and the event loop.

Centralizes the execution rule for all pipeline callables (sources,
transformers, destinations) behind a uniform awaitable interface:

- Coroutine functions (``async def``) -> Executed inline on the event loop.
- Async generators (``async def`` with ``yield``) -> Called inline; generator
  returned as-is.
- Synchronous callables (``def``) -> Executed in worker threads to prevent
  loop blocking.
"""

import asyncio
import functools
import inspect
from collections.abc import AsyncIterable, Callable
from typing import Any

__all__ = ["bridge"]


def _target(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Return the object whose signature flags describe the callable.

    For regular functions, methods, and ``functools.partial`` objects,
    the callable itself is inspected. For callable instances, the
    ``__call__`` method of their type is inspected instead.

    Args:
        fn: Callable whose signature flags are used to determine whether
            it is synchronous or asynchronous.

    Returns:
        The object whose signature flags describe the behavior of ``fn``.
    """
    if inspect.isroutine(fn) or isinstance(fn, functools.partial):
        return fn
    return type(fn).__call__


async def bridge(fn: Callable[..., Any], /, *args: object) -> Any:  # noqa: ANN401
    """Invoke a user callable without blocking the event loop.

    Execution dispatch rules:

    - Async generator functions are called inline and returned unconsumed.
    - Coroutine functions are awaited inline on the event loop.
    - Everything else runs in a worker thread via ``asyncio.to_thread``.
      If it returns an awaitable, it is awaited back on the event loop.

    Args:
        fn: Callable to invoke. Positional-only to avoid keyword collisions.
        *args: Positional arguments forwarded to ``fn``.

    Returns:
        An async generator, or the result with any awaitable resolved.

    Notes:
        ``AsyncIterable`` is checked before `Awaitable``, so an object
        that is both is treated as a stream.
    """
    target = _target(fn)

    if inspect.isasyncgenfunction(target):
        return fn(*args)

    if inspect.iscoroutinefunction(target):
        return await fn(*args)

    result = await asyncio.to_thread(fn, *args)

    if isinstance(result, AsyncIterable):
        return result

    if inspect.isawaitable(result):
        result = await result

    return result
