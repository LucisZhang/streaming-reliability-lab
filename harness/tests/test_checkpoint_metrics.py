from __future__ import annotations

import pytest

from harness.checkpoint_metrics import (
    PromMetric,
    extract_reporter_snapshot,
    parse_prometheus_metrics,
    scenario_for_profile,
    summarize_samples,
)


def test_scenario_for_profile_scales_load() -> None:
    small = scenario_for_profile("small")
    default = scenario_for_profile("default")

    assert small.spike_events < default.spike_events
    assert small.checkpoint_interval_ms == default.checkpoint_interval_ms
    assert small.alignment_probe_sleep_ms > small.backpressure_sleep_ms


def test_scenario_for_profile_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="RESOURCE_PROFILE"):
        scenario_for_profile("huge")


def test_parse_prometheus_metrics_extracts_labels_and_values() -> None:
    metrics = parse_prometheus_metrics(
        """
        # HELP ignored ignored
        flink_jobmanager_job_lastCheckpointDuration{job_id="abc",job_name="cdc"} 42
        flink_taskmanager_job_task_backPressuredTimeMsPerSecond{job_id="abc"} 250.5
        """
    )

    assert metrics == [
        PromMetric(
            name="flink_jobmanager_job_lastCheckpointDuration",
            labels={"job_id": "abc", "job_name": "cdc"},
            value=42.0,
        ),
        PromMetric(
            name="flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
            labels={"job_id": "abc"},
            value=250.5,
        ),
    ]


def test_extract_reporter_snapshot_filters_by_job_id() -> None:
    metrics = [
        PromMetric("flink_jobmanager_job_lastCheckpointDuration", {"job_id": "other"}, 900.0),
        PromMetric("flink_jobmanager_job_lastCheckpointDuration", {"job_id": "abc"}, 110.0),
        PromMetric("flink_jobmanager_job_numberOfFailedCheckpoints", {"job_id": "abc"}, 0.0),
        PromMetric(
            "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
            {"job_id": "abc", "task_name": "slow"},
            0.0,
        ),
        PromMetric("flink_taskmanager_job_task_busyTimeMsPerSecond", {"job_id": "abc"}, 620.0),
        PromMetric(
            "flink_taskmanager_job_task_checkpointAlignmentTime",
            {"job_id": "abc"},
            2_500_000.0,
        ),
    ]

    snapshot = extract_reporter_snapshot(metrics, job_id="abc")

    assert snapshot.checkpoint_duration_ms == 110.0
    assert snapshot.checkpoint_alignment_time_ms == 2.5
    assert snapshot.checkpoint_failed_count == 0.0
    assert snapshot.backpressure_indicator == 0.62
    assert snapshot.busy_time_ms_per_second == 620.0
    assert snapshot.metric_names["checkpoint_duration_ms"].endswith("lastCheckpointDuration")


def test_summarize_samples_requires_rising_metrics_and_recovered_lag() -> None:
    scenario = scenario_for_profile("small")
    samples = [
        {
            "phase": "baseline",
            "checkpoint": {
                "duration_ms": 20,
                "alignment_time_ms": 1,
                "failed_count": 0,
            },
            "backpressure": {"indicator": 0.0},
            "iceberg_commit_lag": {"lag_events": 0},
        },
        {
            "phase": "backpressure",
            "checkpoint": {
                "duration_ms": 120,
                "alignment_time_ms": 18,
                "failed_count": 0,
            },
            "backpressure": {"indicator": 0.3},
            "iceberg_commit_lag": {"lag_events": 50},
        },
        {
            "phase": "recovery",
            "checkpoint": {
                "duration_ms": 55,
                "alignment_time_ms": 2,
                "failed_count": 0,
            },
            "backpressure": {"indicator": 0.0},
            "iceberg_commit_lag": {"lag_events": 0},
        },
    ]

    summary = summarize_samples(samples, scenario=scenario)

    assert summary["passed"] is True
    assert summary["under_backpressure"]["max_checkpoint_duration_ms"] == 120
    assert summary["under_backpressure"]["max_alignment_time_ms"] == 18
