"""Tests for the exceptions module.

Verifies stage error wrapping, message formatting, and memory safety rules:
- Stage identification across Source, Transformer, and Destination errors.
- Exception chaining via ``__cause__``.
- Payload safety: retains a bounded string snapshot, never the raw item.
"""

import functools
import gc
import weakref

import pytest

from asyncdatapipeline.exceptions import (
    DestinationError,
    SourceError,
    TransformerError,
)


def fn():
    pass


class Callable:
    def __call__(self):
        pass


class Payload:
    pass


class BrokenRepr:
    def __repr__(self):
        raise RuntimeError("broken repr")

# ------------------------------ #
# CHECK MESSAGE FORMAT
# ------------------------------ #


def test_message_without_item_and_single_attempt():
    assert str(SourceError(fn)) == "source 'fn' failed after 1 attempt"


def test_message_with_item_and_retries():
    err = TransformerError(fn, item={"k": 1}, attempts=3)
    assert str(err) == "transformer 'fn' failed after 3 attempts on item {'k': 1}"


def test_none_item_is_distinguished_from_no_item():
    assert SourceError(fn).item_repr is None
    assert TransformerError(fn, item=None).item_repr == "None"


@pytest.mark.parametrize(
    ("callable_obj", "expected"),
    [
        (fn, "fn"),
        (Callable(), "Callable"),
        (functools.partial(fn), "partial(fn)"),
        (lambda x: x, "<lambda>"),
        (len, "len"),
    ],
)
def test_callable_name(callable_obj, expected):
    assert DestinationError(callable_obj).callable_name == expected

# ------------------------------ #
# CHECK MEMORY CONTRACT
# ------------------------------ #


def test_item_repr_is_bounded():
    err = TransformerError(fn, item="x" * 100_000)
    assert len(err.item_repr) < 100
    assert len(str(err)) < 200


def test_item_is_not_retained():
    item = Payload()
    ref = weakref.ref(item)
    err = DestinationError(fn, item=item)
    del item
    gc.collect()
    assert ref() is None
    assert err.item_repr is not None


def test_broken_repr_does_not_break_error_construction():
    err = TransformerError(fn, item=BrokenRepr())
    assert "BrokenRepr" in err.item_repr


# ------------------------------ #
# CHECK CAUSE CHAINING
# ------------------------------ #

def test_cause_is_available_when_chained():
    original = ValueError("original")
    try:
        raise TransformerError(fn, item=1) from original
    except TransformerError as err:
        assert err.__cause__ is original
        assert isinstance(err.__cause__, ValueError)
