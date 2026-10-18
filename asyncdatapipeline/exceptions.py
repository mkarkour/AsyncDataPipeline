"""Exception hierarchy for pipeline stage failures.

Wraps errors occurring within user callables inside ``StageError`` subclasses
to capture execution context, including stage name, callable identifier,
attempt count, and a bounded string snapshot of the payload.

Pipeline errors are recorded and logged during execution rather than raised by
``AsyncDataPipeline.run()``, preventing total pipeline failure on partial
errors. To ensure memory safety, error instances never store raw payload
objects; they retain only bounded string snapshots. The underlying root cause
remains fully traceable via standard exception chaining on ``__cause__``.
"""

import functools
import reprlib
from collections.abc import Callable
from typing import Any, ClassVar, Final

__all__ = [
    "StageError",
    "SourceError",
    "TransformerError",
    "DestinationError",
]


_ITEM_REPR: Final = reprlib.Repr(maxstring=80, maxother=80, maxlevel=2)
"""Safe, bounded repr formatter for payload snapshots.

Truncates strings/objects during evaluation to prevent memory bloat, and
safely catches exceptions raised by broken __repr__ implementations.
"""

_MISSING: Final = object()
"""Sentinel value used to distinguish a source error from an item that is None.
"""


def _callable_name(fn: Callable[..., Any]) -> str:
    """Return a human-readable identifier for any pipeline callable.

    Args:
        fn: The callable to identify.

    Returns:
        A concise identifier string suitable for log lines and error messages.
    """
    if isinstance(fn, functools.partial):
        return f"partial({_callable_name(fn.func)})"

    name = getattr(fn, "__qualname__", None)

    if isinstance(name, str):
        return name

    return type(fn).__qualname__


class StageError(Exception):
    """Base class for a failure in one pipeline stage.

    Not raised directly; one of the three subclasses identifies the stage.
    Chain the original exception with ``raise ... from exc`` so that it is
    available as ``__cause__``.

    Attributes:
        callable_name: Readable name of the callable that failed.
        attempts: Number of invocations made before giving up. ``1`` when no
                  retry policy applied.
        item_repr: Bounded ``repr`` of the item being processed, or ``None``
                   when the failure is not tied to an item (source failures).
    """

    stage: ClassVar[str] = "stage"
    """Name of the pipeline stage where the error occurred."""

    def __init__(
        self,
        fn: Callable[..., Any],
        /,
        *,
        item: object = _MISSING,
        attempts: int = 1,
    ) -> None:
        """Record the failing callable, the attempt count and an item snapshot.

        Args:
            fn: The callable that failed.
            item: The item being processed, if any. Only its bounded
                ``repr`` is retained.
            attempts: Number of invocations made before giving up.
        """
        self.callable_name = _callable_name(fn)
        self.attempts = attempts
        self.item_repr = None if item is _MISSING else _ITEM_REPR.repr(item)
        super().__init__(self._message())

    def _message(self) -> str:
        """Build the human-readable exception message stored in args.

        Returns:
            A formatted string describing the failing stage, callable name,
            attempt count, and optional item snapshot.
        """
        plural = "" if self.attempts == 1 else "s"
        text = (
            f"{self.stage} {self.callable_name!r} failed "
            f"after {self.attempts} attempt{plural}"
        )
        if self.item_repr is not None:
            text += f" on item {self.item_repr}"
        return text


class SourceError(StageError):
    """Failure in a source callable.

    Raised when a source callable fails to produce data. The original exception
    is available as ``__cause__``.
    """

    stage: ClassVar[str] = "source"


class TransformerError(StageError):
    """Failure in a transformer callable.

    Raised when a transformer callable fails to process an item. The original
    exception is available as ``__cause__``.
    """

    stage: ClassVar[str] = "transformer"


class DestinationError(StageError):
    """Failure in a destination callable.

    Raised when a destination callable fails to send an item. The original
    exception is available as ``__cause__``.
    """

    stage: ClassVar[str] = "destination"
