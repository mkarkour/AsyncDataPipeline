"""Public type aliases defining the pipeline's callable contracts.

The aliases describe what sources, transformers, and destinations may
accept and return. Synchronous and asynchronous callables are supported;
the pipeline is responsible for executing synchronous callables without
blocking the event loop.
"""

from collections.abc import AsyncIterable, Awaitable, Callable

__all__ = [
    "Destination",
    "Source",
    "SourceOutput",
    "Transformer",
]

type SourceOutput[T] = T | AsyncIterable[T] | Awaitable[T | AsyncIterable[T]]
"""Possible return types for a source callable.

A source returns either a single value or a stream of values, produced
either synchronously or asynchronously:

- ``T``: Single item returned synchronously.
  (e.g., ``def src(): return payload``)
- ``Awaitable[T]``: Single item resolved via a coroutine.
  (e.g., ``async def src(): return payload``)
- ``AsyncIterable[T]``: Stream of items yielded by an async generator.
  (e.g., ``async def src(): yield chunk``)
- ``Awaitable[AsyncIterable[T]]``: Stream of items resolved via a coroutine
  before iteration. (e.g., ``async def src(): return await connect()``)

Notes:

* Synchronous iterables are treated as a single item, not as a stream.
* If an object is both ``Awaitable`` and ``AsyncIterable``, it is processed
  as an ``AsyncIterable``.
"""

type Source[T] = Callable[[], SourceOutput[T]]
"""Callable contract for a pipeline source.

A source takes no arguments and is invoked exactly once per pipeline execution.
Depending on its return type, it contributes items to the pipeline as follows:

- Single-shot sources (value or coroutine): Contribute a single item.
- Streaming sources (async iterable or coroutine of an async iterable):
  Drained item-by-item until exhausted.

Sources are never re-invoked and are not polled; a source that has
returned or been exhausted contributes nothing further.
"""

type Transformer[T] = Callable[[T], T | Awaitable[T]]
"""Callable contract for a pipeline transformer.

A transformer receives a single item of type ``T`` and returns a transformed
item of the same type, synchronously or asynchronously. One item enters, one
item leaves: transformers never fan out.

Transformers are applied sequentially, in list order, to every item flowing
through the pipeline.

The single type parameter means every transformer in a chain shares the same
item type. A chain that changes type along the way
(``str -> User -> Record -> bytes``) executes correctly but is not verified
statically: a list cannot express a heterogeneous sequence of mappings.
Expressing it would require a chaining API that re-parameterizes the type at
each step, which the prescribed constructor signature rules out.
"""

type Destination[T] = Callable[[T], Awaitable[None] | None]
"""Callable contract for a pipeline destination.

A destination receives one transformed item and returns ``None`` or an
awaitable resolving to ``None``.

Every destination is invoked for every transformed item. Destinations run
concurrently, and their failures are isolated: a failure in one does not
prevent the others from receiving the same item.
"""
