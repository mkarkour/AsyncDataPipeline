"""Run-scoped metrics and execution timing for pipeline runs.

Tracks real-time counters and timings during pipeline execution via
``pipeline.metrics``. All updates run on the event loop thread,
ensuring lock-free thread safety.
"""

import time
from dataclasses import dataclass, field
from typing import Literal

__all__ = ["PipelineMetrics"]

type StageName = Literal["source", "transformer", "destination"]


def _zero_per_stage() -> dict[StageName, int]:
    """Create a failure counter initialized with zero for every stage.

    Returns:
        A dictionary initialized with zero failures for 'source',
        'transformer', and 'destination' stages.
    """
    return {"source": 0, "transformer": 0, "destination": 0}


@dataclass(slots=True)
class PipelineMetrics:
    """Counters and timings for a single pipeline execution.

    Attributes:
        items_collected: Total items produced by all sources.
        items_transformed: Items that completed all transformers and reached
                           destinations.
        dispatches_ok: Successful destination invocations (e.g., 1 item to 3
                       destinations counts as 3).
        failures: Failure count per stage name.
        latency_total: Total accumulated processing time in seconds across all
                       items.
        started_at: Monotonic timestamp (`time.perf_counter`) when execution
                    began, or None.
        finished_at: Monotonic timestamp (`time.perf_counter`) when execution
                     ended, or None.
    """

    items_collected: int = 0
    items_transformed: int = 0
    dispatches_ok: int = 0
    failures: dict[StageName, int] = field(default_factory=_zero_per_stage)
    latency_total: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None

    def start(self) -> None:
        """Mark the start of the pipeline execution."""
        self.started_at = time.perf_counter()

    def finish(self) -> None:
        """Mark the completion of the pipeline execution."""
        self.finished_at = time.perf_counter()

    def record_failure(self, stage: StageName) -> None:
        """Increment the failure counter for a specific stage.

        Args:
            stage: The stage name where the failure occurred ('source',
                   'transformer', or 'destination').
        """
        self.failures[stage] += 1

    def record_latency(self, seconds: float) -> None:
        """Add an item's end-to-end processing duration.

        Args:
            seconds: Time spent from entering transformers to completion by
                     all destinations.
        """
        self.latency_total += seconds

    @property
    def failures_total(self) -> int:
        """Calculate the total failure count across all stages.

        Returns:
            Sum of all stage failures.
        """
        return sum(self.failures.values())

    @property
    def duration(self) -> float:
        """Calculate elapsed seconds since the run started.

        Returns real-time duration during execution, total duration once
        finished, or 0.0 if not started.

        Returns:
            Elapsed time in seconds.
        """
        if self.started_at is None:
            return 0.0

        end = (
            self.finished_at
            if self.finished_at is not None
            else time.perf_counter()
        )

        return end - self.started_at

    @property
    def throughput(self) -> float:
        """Calculate transformed items processed per second.

        Returns:
            Number of items transformed per second, or 0.0 if duration is zero.
        """
        duration = self.duration
        return self.items_transformed / duration if duration > 0 else 0.0

    @property
    def mean_latency(self) -> float:
        """Calculate average end-to-end processing duration per item.

        Returns:
            Average time in seconds per transformed item, or 0.0 if none
            processed.
        """
        n = self.items_transformed
        return self.latency_total / n if n > 0 else 0.0
