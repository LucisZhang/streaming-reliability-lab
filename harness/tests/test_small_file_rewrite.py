from __future__ import annotations

import pytest

from harness.small_file_rewrite import compare_metrics, scenario_for_profile


def test_scenario_for_profile_scales_workload() -> None:
    small = scenario_for_profile("small")
    default = scenario_for_profile("default")

    assert small.total_rows < default.total_rows
    assert small.checkpoint_interval_ms == default.checkpoint_interval_ms
    assert small.small_write_target_file_size_bytes == 16 * 1024


def test_scenario_for_profile_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="RESOURCE_PROFILE"):
        scenario_for_profile("huge")


def test_compare_metrics_requires_all_directional_improvements() -> None:
    before = {
        "data_file_count": 24,
        "manifest_count": 24,
        "median_file_size_bytes": 4_096,
        "planning_latency_ms": 80.0,
    }
    after = {
        "data_file_count": 2,
        "manifest_count": 1,
        "median_file_size_bytes": 49_152,
        "planning_latency_ms": 12.5,
    }

    assert compare_metrics(before, after) == {
        "data_file_count_decreased": True,
        "manifest_count_decreased": True,
        "median_file_size_increased": True,
        "planning_latency_decreased": True,
    }


def test_compare_metrics_flags_latency_regression() -> None:
    before = {
        "data_file_count": 24,
        "manifest_count": 24,
        "median_file_size_bytes": 4_096,
        "planning_latency_ms": 10.0,
    }
    after = {
        "data_file_count": 2,
        "manifest_count": 1,
        "median_file_size_bytes": 49_152,
        "planning_latency_ms": 12.5,
    }

    assert compare_metrics(before, after)["planning_latency_decreased"] is False
