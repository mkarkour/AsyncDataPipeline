"""Tests for the metrics module.

Verifies counter defaults, per-run isolation, and that the derived
properties are safe to read at any point of a run.
"""

import time

import pytest

from asyncdatapipeline.metrics import PipelineMetrics

# ---------------------------------- #
# CHECK DEFAULTS VALUES AND ISOLATION
# ---------------------------------- #


def test_fresh_instance_is_all_zero() -> None:
    m = PipelineMetrics()
    assert m.items_collected == 0
    assert m.items_transformed == 0
    assert m.dispatches_ok == 0
    assert m.failures == {"source": 0, "transformer": 0, "destination": 0}
    assert m.failures_total == 0
    assert m.latency_total == 0.0
    assert m.started_at is None
    assert m.finished_at is None


def test_failure_counters_are_not_shared_between_instances() -> None:
    a, b = PipelineMetrics(), PipelineMetrics()
    a.record_failure("source")
    assert b.failures["source"] == 0


# ---------------------------------- #
# CHECK COUNTERS
# ---------------------------------- #

def test_duration_is_zero_before_start() -> None:
    assert PipelineMetrics().duration == 0.0


def test_duration_is_live_while_running() -> None:
    m = PipelineMetrics()
    m.start()
    first = m.duration
    time.sleep(0.01)
    assert m.duration > first


def test_duration_is_frozen_after_finish() -> None:
    m = PipelineMetrics(started_at=10.0, finished_at=12.5)
    assert m.duration == 2.5
    time.sleep(0.01)
    assert m.duration == 2.5


# ---------------------------------- #
# CHECK DERIVED RATES
# ---------------------------------- #

@pytest.mark.parametrize(
    ("items_transformed", "started_at", "finished_at", "expected"),
    [
        (0, None, None, 0.0),
        (10, 0.0, 4.0, 2.5),
        (100, 10.0, 20.0, 10.0),
        (50, 5.0, 10.0, 10.0),
    ],
)
def test_throughput(
    items_transformed: int,
    started_at: float | None,
    finished_at: float | None,
    expected: float,
) -> None:
    m = PipelineMetrics(
        items_transformed=items_transformed,
        started_at=started_at,
        finished_at=finished_at,
    )

    assert m.throughput == expected


@pytest.mark.parametrize(
    ("items_transformed", "latency_total", "expected"),
    [
        (0, 3.0, 0.0),
        (4, 2.0, 0.5),
        (10, 10.0, 1.0),
        (100, 25.0, 0.25),
    ],
)
def test_mean_latency(
    items_transformed: int,
    latency_total: float,
    expected: float,
) -> None:
    m = PipelineMetrics(
        items_transformed=items_transformed,
        latency_total=latency_total,
    )

    assert m.mean_latency == expected
