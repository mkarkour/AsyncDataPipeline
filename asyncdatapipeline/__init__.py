"""Asynchronous fan-in / transform / fan-out data pipeline library.

An ``AsyncDataPipeline`` collects items from several sources concurrently,
applies a chain of transformers to each item in order, and dispatches every
result to several destinations at once. Callables may be synchronous or
asynchronous; synchronous ones run in worker threads so the event loop is
never blocked.
"""
import logging

from .exceptions import (
    DestinationError,
    SourceError,
    StageError,
    TransformerError,
)
from .metrics import PipelineMetrics, StageName
from .pipeline import AsyncDataPipeline
from .retry import RetryCallback, RetryPolicy, call_with_retry
from .types import Destination, Source, SourceOutput, Transformer

__version__ = "0.1.0"

__all__ = [
    # Pipeline
    "AsyncDataPipeline",

    # Callable contracts
    "Destination",
    "Source",
    "SourceOutput",
    "Transformer",

    # Errors
    "DestinationError",
    "SourceError",
    "StageError",
    "TransformerError",

    # Metrics
    "PipelineMetrics",
    "StageName",

    # Retry
    "RetryCallback",
    "RetryPolicy",
    "call_with_retry",
]

logging.getLogger(__name__).addHandler(logging.NullHandler())
