from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from harness.config import REPO_ROOT, Settings, load_env_file, load_settings
from harness.eo_verify import (
    CHANGELOG_TABLE,
    _cancel_existing_jobs,
    _compose_up_services,
    _mysql,
    _require_core_services,
    _wait_for_completed_checkpoint,
    _wait_for_flink_cluster,
    _wait_for_job,
)
from harness.flink import (
    BATCH_SQL_CLASS,
    FlinkCommandError,
    cancel_job,
    flink_rest_json,
    reset_iceberg_tables,
    run_java_class,
    run_jobmanager,
    submit_job,
)
from harness.provenance import utc_now, write_result
from harness.sql import clean_sql_output, run_mysql_script

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "checkpoint_metrics.json"
LOG_PATH = REPO_ROOT / "showcase" / "logs" / "phase-2.3-checkpoint-metrics.log"
CHART_PATH = REPO_ROOT / "showcase" / "media" / "phase-2.3-checkpoint-metrics.svg"

BASE_EVENT_ID = 230_000
BASE_TS = datetime(2026, 3, 23, tzinfo=UTC)
STATUSES = ("created", "paid", "packed", "shipped", "delivered")


@dataclass(frozen=True)
class Scenario:
    resource_profile: str
    spike_events: int
    insert_batch_size: int
    checkpoint_interval_ms: int
    backpressure_sleep_ms: int
    alignment_probe_sleep_ms: int
    baseline_samples: int
    poll_interval_seconds: float
    max_samples_after_spike: int
    timeout_seconds: int
    min_backpressure_indicator: float


@dataclass(frozen=True)
class PromMetric:
    name: str
    labels: dict[str, str]
    value: float


@dataclass(frozen=True)
class ReporterSnapshot:
    checkpoint_duration_ms: float | None
    checkpoint_alignment_time_ms: float | None
    checkpoint_alignment_buffered_bytes: float | None
    checkpoint_failed_count: float | None
    backpressured_time_ms_per_second: float | None
    is_backpressured: float | None
    busy_time_ms_per_second: float | None
    metric_names: dict[str, str]

    @property
    def backpressure_indicator(self) -> float | None:
        if self.backpressured_time_ms_per_second is not None:
            explicit = min(max(self.backpressured_time_ms_per_second / 1000.0, 0.0), 1.0)
            if explicit > 0.0:
                return explicit
        if self.is_backpressured is not None and self.is_backpressured > 0.0:
            return min(max(self.is_backpressured, 0.0), 1.0)
        if self.busy_time_ms_per_second is not None:
            return min(max(self.busy_time_ms_per_second / 1000.0, 0.0), 1.0)
        if self.backpressured_time_ms_per_second is not None or self.is_backpressured is not None:
            return 0.0
        return None


def scenario_for_profile(profile: str) -> Scenario:
    if profile == "default":
        return Scenario(
            resource_profile=profile,
            spike_events=600,
            insert_batch_size=150,
            checkpoint_interval_ms=1_000,
            backpressure_sleep_ms=35,
            alignment_probe_sleep_ms=95,
            baseline_samples=3,
            poll_interval_seconds=3.0,
            max_samples_after_spike=30,
            timeout_seconds=540,
            min_backpressure_indicator=0.05,
        )
    if profile != "small":
        raise ValueError("RESOURCE_PROFILE must be small or default")
    return Scenario(
        resource_profile=profile,
        spike_events=320,
        insert_batch_size=80,
        checkpoint_interval_ms=1_000,
        backpressure_sleep_ms=40,
        alignment_probe_sleep_ms=120,
        baseline_samples=3,
        poll_interval_seconds=3.0,
        max_samples_after_spike=24,
        timeout_seconds=420,
        min_backpressure_indicator=0.05,
    )


def run_checkpoint_metrics(
    *,
    scenario: Scenario,
    command: str = "make ckpt-metrics",
) -> dict[str, Any]:
    settings = load_settings()
    started_at = utc_now()
    log_lines: list[str] = []
    samples: list[dict[str, Any]] = []
    checkpoint_records: dict[int, dict[str, Any]] = {}
    job_id: str | None = None

    def log(message: str) -> None:
        line = f"{utc_now()} {message}"
        log_lines.append(line)
        print(message, flush=True)

    try:
        log("checking core services")
        if not _core_services_running(settings):
            _compose_up_services(["taskmanager"], settings=settings)
        _wait_for_flink_cluster(settings=settings, timeout_seconds=120)
        _require_core_services(settings)

        log("resetting running jobs, MySQL source table, and Iceberg tables")
        _cancel_existing_jobs(settings, log=log, ignore_errors=True)
        _mysql("TRUNCATE TABLE orders;", settings)
        reset_iceberg_tables(settings=settings)

        job_id = submit_job(
            settings=settings,
            checkpoint_interval_ms=scenario.checkpoint_interval_ms,
            extra_job_args=[
                "--backpressure-sleep-ms",
                str(scenario.backpressure_sleep_ms),
                "--alignment-probe-sleep-ms",
                str(scenario.alignment_probe_sleep_ms),
            ],
        )
        log(
            f"submitted CDC job {job_id} with sleep gates "
            f"main={scenario.backpressure_sleep_ms}ms "
            f"alignment_probe={scenario.alignment_probe_sleep_ms}ms"
        )
        _wait_for_job(job_id, settings=settings, timeout_seconds=scenario.timeout_seconds)
        baseline_checkpoint = _wait_for_completed_checkpoint(
            job_id, settings=settings, timeout_seconds=scenario.timeout_seconds
        )
        log(f"baseline checkpoint {baseline_checkpoint['id']} completed before input spike")
        time.sleep(scenario.poll_interval_seconds)

        started_monotonic = time.monotonic()
        for index in range(scenario.baseline_samples):
            sample = collect_sample(
                job_id,
                settings=settings,
                phase="baseline",
                sample_index=len(samples),
                started_monotonic=started_monotonic,
                checkpoint_records=checkpoint_records,
            )
            samples.append(sample)
            log_sample(sample, log=log)
            if index + 1 < scenario.baseline_samples:
                time.sleep(scenario.poll_interval_seconds)

        _insert_events(
            settings, BASE_EVENT_ID + 1, scenario.spike_events, scenario.insert_batch_size
        )
        log(
            "inserted input spike "
            f"events={scenario.spike_events} "
            f"event_id_range={BASE_EVENT_ID + 1}-{BASE_EVENT_ID + scenario.spike_events}"
        )

        recovered_samples = 0
        deadline = time.monotonic() + scenario.timeout_seconds
        while time.monotonic() < deadline and len(samples) < (
            scenario.baseline_samples + scenario.max_samples_after_spike
        ):
            sample = collect_sample(
                job_id,
                settings=settings,
                phase="load",
                sample_index=len(samples),
                started_monotonic=started_monotonic,
                checkpoint_records=checkpoint_records,
            )
            lag = int(cast(dict[str, Any], sample["iceberg_commit_lag"])["lag_events"])
            sample["phase"] = "backpressure" if lag > 0 else "recovery"
            samples.append(sample)
            log_sample(sample, log=log)

            if lag == 0:
                recovered_samples += 1
            else:
                recovered_samples = 0
            summary = summarize_samples(samples, scenario=scenario)
            if (
                recovered_samples >= 3
                and bool(summary["checks"]["checkpoint_duration_rose"])
                and bool(summary["checks"]["alignment_time_rose"])
                and bool(summary["checks"]["backpressure_observed"])
            ):
                break
            time.sleep(scenario.poll_interval_seconds)

        summary = summarize_samples(samples, scenario=scenario)
        payload: dict[str, Any] = {
            "phase": "2.3",
            "artifact": "Checkpoint and backpressure metrics under induced load",
            "resource_profile": scenario.resource_profile,
            "reader": "Flink Prometheus metrics reporter plus Flink SQL batch for Iceberg lag",
            "scenario": scenario_payload(scenario),
            "metric_names": metric_names_payload(),
            "induction": {
                "mechanism": (
                    "A deterministic MySQL input spike is processed by test-only Flink sleep "
                    "operators in the CDC job; metrics are scraped from the real Prometheus "
                    "reporter endpoints on JobManager and TaskManager."
                ),
                "job_args": {
                    "backpressure_sleep_ms": scenario.backpressure_sleep_ms,
                    "alignment_probe_sleep_ms": scenario.alignment_probe_sleep_ms,
                },
            },
            "job_id": job_id,
            "time_series": samples,
            "checkpoint_records": sorted(checkpoint_records.values(), key=lambda item: item["id"]),
            "summary": summary,
            "chart": str(CHART_PATH.relative_to(REPO_ROOT)),
        }
        _write_chart(payload)
        _write_log(log_lines)
        write_result(
            RESULT_PATH,
            payload=payload,
            command=command,
            logs=str(LOG_PATH.relative_to(REPO_ROOT)),
            started_at=started_at,
            finished_at=utc_now(),
        )
        failed = [name for name, passed in summary["checks"].items() if not passed]
        if failed:
            raise AssertionError(f"checkpoint metric checks failed: {', '.join(failed)}")
        return payload
    finally:
        if job_id is not None:
            with suppress(Exception):
                cancel_job(job_id, settings=settings)
        _write_log(log_lines)


def collect_sample(
    job_id: str,
    *,
    settings: Settings,
    phase: str,
    sample_index: int,
    started_monotonic: float,
    checkpoint_records: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    reporter = extract_reporter_snapshot(scrape_reporter_metrics(settings), job_id=job_id)
    checkpoint = checkpoint_snapshot(
        job_id, settings=settings, checkpoint_records=checkpoint_records
    )
    lag = iceberg_commit_lag(settings)
    duration_ms = _max_number(
        reporter.checkpoint_duration_ms,
        checkpoint.get("duration_ms"),
        checkpoint.get("recent_max_duration_ms"),
    )
    alignment_time_ms = _max_number(
        reporter.checkpoint_alignment_time_ms, checkpoint.get("alignment_time_ms")
    )
    alignment_buffered_bytes = _first_number(
        reporter.checkpoint_alignment_buffered_bytes,
        checkpoint.get("alignment_buffered_bytes"),
    )
    failed_count = _first_number(
        reporter.checkpoint_failed_count,
        checkpoint.get("failed_count"),
    )
    return {
        "sample_index": sample_index,
        "phase": phase,
        "collected_at": utc_now(),
        "elapsed_ms": int((time.monotonic() - started_monotonic) * 1000),
        "checkpoint": {
            "latest_completed_id": checkpoint.get("latest_completed_id"),
            "duration_ms": duration_ms,
            "alignment_time_ms": alignment_time_ms,
            "alignment_buffered_bytes": alignment_buffered_bytes,
            "start_delay_ms": checkpoint.get("start_delay_ms"),
            "failed_count": failed_count,
            "reporter_metric_names": reporter.metric_names,
        },
        "backpressure": {
            "indicator": reporter.backpressure_indicator,
            "backpressured_time_ms_per_second": reporter.backpressured_time_ms_per_second,
            "is_backpressured": reporter.is_backpressured,
            "busy_time_ms_per_second": reporter.busy_time_ms_per_second,
            "metric_names": {
                key: value
                for key, value in reporter.metric_names.items()
                if key.startswith("backpressure")
            },
        },
        "iceberg_commit_lag": lag,
    }


def log_sample(sample: dict[str, Any], *, log: Any) -> None:
    checkpoint = cast(dict[str, Any], sample["checkpoint"])
    backpressure = cast(dict[str, Any], sample["backpressure"])
    lag = cast(dict[str, Any], sample["iceberg_commit_lag"])
    log(
        "sample "
        f"{sample['sample_index']} phase={sample['phase']} "
        f"checkpoint={checkpoint.get('latest_completed_id')} "
        f"duration_ms={checkpoint.get('duration_ms')} "
        f"alignment_ms={checkpoint.get('alignment_time_ms')} "
        f"backpressure={backpressure.get('indicator')} "
        f"lag_events={lag['lag_events']}"
    )


def parse_prometheus_metrics(text: str) -> list[PromMetric]:
    metrics: list[PromMetric] = []
    pattern = re.compile(
        r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+"
        r"(?P<value>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)"
    )
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = pattern.match(stripped)
        if not match:
            continue
        metrics.append(
            PromMetric(
                name=match.group("name"),
                labels=_parse_prometheus_labels(match.group("labels") or ""),
                value=float(match.group("value")),
            )
        )
    return metrics


def scrape_reporter_metrics(settings: Settings) -> list[PromMetric]:
    errors: list[str] = []
    for attempt in range(3):
        metrics: list[PromMetric] = []
        attempt_errors: list[str] = []
        for service in ("jobmanager", "taskmanager"):
            proc = run_jobmanager(
                ["curl", "-fsS", f"http://{service}:9249/metrics"],
                settings=settings,
                capture=True,
                timeout_seconds=20,
            )
            if proc.returncode == 0:
                metrics.extend(parse_prometheus_metrics(proc.stdout))
            else:
                attempt_errors.append(f"{service}: {proc.stderr.strip() or proc.stdout.strip()}")
        if metrics:
            return metrics
        errors = attempt_errors
        if attempt < 2:
            time.sleep(1.0)
    raise FlinkCommandError("Prometheus metrics reporter scrape failed: " + "; ".join(errors))


def extract_reporter_snapshot(metrics: Sequence[PromMetric], *, job_id: str) -> ReporterSnapshot:
    duration = _max_metric(metrics, ("lastCheckpointDuration",), job_id=job_id)
    alignment_time = _max_metric(
        metrics,
        (
            "lastCheckpointAlignmentDuration",
            "checkpointAlignmentTime",
            "alignmentDuration",
        ),
        job_id=job_id,
    )
    alignment_buffered = _max_metric(metrics, ("lastCheckpointAlignmentBuffered",), job_id=job_id)
    failed = _max_metric(metrics, ("numberOfFailedCheckpoints",), job_id=job_id)
    backpressured = _max_metric(metrics, ("backPressuredTimeMsPerSecond",), job_id=job_id)
    is_backpressured = _max_metric(metrics, ("isBackPressured",), job_id=job_id)
    busy = _max_metric(metrics, ("busyTimeMsPerSecond",), job_id=job_id)
    alignment_time_ms = _normalize_alignment_time(alignment_time[0], alignment_time[1])
    return ReporterSnapshot(
        checkpoint_duration_ms=duration[0],
        checkpoint_alignment_time_ms=alignment_time_ms,
        checkpoint_alignment_buffered_bytes=alignment_buffered[0],
        checkpoint_failed_count=failed[0],
        backpressured_time_ms_per_second=backpressured[0],
        is_backpressured=is_backpressured[0],
        busy_time_ms_per_second=busy[0],
        metric_names={
            "checkpoint_duration_ms": duration[1],
            "checkpoint_alignment_time_ms": alignment_time[1],
            "checkpoint_alignment_buffered_bytes": alignment_buffered[1],
            "checkpoint_failed_count": failed[1],
            "backpressure_time_ms_per_second": backpressured[1],
            "backpressure_is_backpressured": is_backpressured[1],
            "backpressure_busy_time_ms_per_second": busy[1],
        },
    )


def checkpoint_snapshot(
    job_id: str,
    *,
    settings: Settings,
    checkpoint_records: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    payload = flink_rest_json(f"/jobs/{job_id}/checkpoints", settings=settings)
    known_checkpoint_ids = set(checkpoint_records)
    for completed in _completed_checkpoint_payloads(payload):
        completed_id = int(cast(int | str, completed["id"]))
        details = _checkpoint_details(job_id, completed_id, settings=settings)
        checkpoint_records[completed_id] = checkpoint_record(
            completed_id, latest=completed, details=details
        )
    latest = _latest_completed_checkpoint_payload(payload)
    counts = payload.get("counts")
    failed_count = None
    if isinstance(counts, dict):
        failed_count = _as_float(counts.get("failed"))
    if latest is None:
        return {"failed_count": failed_count}

    checkpoint_id = int(cast(int | str, latest["id"]))
    record = checkpoint_records[checkpoint_id]
    recent_records = [
        item for key, item in checkpoint_records.items() if key not in known_checkpoint_ids
    ]
    return {
        "latest_completed_id": checkpoint_id,
        "duration_ms": record["duration_ms"],
        "recent_max_duration_ms": _max_record_number(recent_records, "duration_ms"),
        "alignment_time_ms": record["alignment_time_ms"],
        "alignment_buffered_bytes": record["alignment_buffered_bytes"],
        "start_delay_ms": record["start_delay_ms"],
        "failed_count": failed_count,
    }


def checkpoint_record(
    checkpoint_id: int,
    *,
    latest: dict[str, Any],
    details: dict[str, Any] | None,
) -> dict[str, Any]:
    source = details if details else latest
    duration_ms = _first_number(
        _as_float(source.get("end_to_end_duration")),
        _as_float(latest.get("end_to_end_duration")),
    )
    alignment_time_ms = _max_path_number(source, include=("alignment",), suffixes=("duration",))
    start_delay_ms = _max_path_number(source, include=("start", "delay"), suffixes=())
    alignment_buffered = _first_number(
        _max_path_number(source, include=("alignment",), suffixes=("buffered",)),
        _as_float(latest.get("alignment_buffered")),
    )
    return {
        "id": checkpoint_id,
        "status": latest.get("status"),
        "trigger_timestamp": latest.get("trigger_timestamp"),
        "latest_ack_timestamp": latest.get("latest_ack_timestamp"),
        "duration_ms": duration_ms,
        "alignment_time_ms": alignment_time_ms,
        "alignment_buffered_bytes": alignment_buffered,
        "start_delay_ms": start_delay_ms,
        "checkpointed_size_bytes": _as_float(latest.get("checkpointed_size")),
        "processed_data_bytes": _as_float(latest.get("processed_data")),
        "persisted_data_bytes": _as_float(latest.get("persisted_data")),
    }


def iceberg_commit_lag(settings: Settings) -> dict[str, int]:
    source_max, source_count = _source_progress(settings)
    iceberg_max, iceberg_count = _iceberg_progress(settings)
    return {
        "source_max_event_id": source_max,
        "iceberg_max_event_id": iceberg_max,
        "max_event_id_gap": max(0, source_max - iceberg_max),
        "source_rows": source_count,
        "iceberg_changelog_rows": iceberg_count,
        "lag_events": max(0, source_count - iceberg_count),
    }


def summarize_samples(samples: Sequence[dict[str, Any]], *, scenario: Scenario) -> dict[str, Any]:
    baseline = [sample for sample in samples if sample.get("phase") == "baseline"]
    load = [sample for sample in samples if sample.get("phase") != "baseline"]
    duration_baseline = _max_sample_number(baseline, "checkpoint", "duration_ms")
    duration_load = _max_sample_number(load, "checkpoint", "duration_ms")
    alignment_baseline = _max_sample_number(baseline, "checkpoint", "alignment_time_ms")
    alignment_load = _max_sample_number(load, "checkpoint", "alignment_time_ms")
    backpressure_peak = _max_sample_number(load, "backpressure", "indicator")
    lag_peak = _max_sample_number(load, "iceberg_commit_lag", "lag_events")
    final_lag = _last_sample_number(samples, "iceberg_commit_lag", "lag_events")
    failed_count_peak = _max_sample_number(samples, "checkpoint", "failed_count")

    checks = {
        "checkpoint_duration_rose": _rose(duration_baseline, duration_load),
        "alignment_time_rose": _rose(alignment_baseline, alignment_load),
        "checkpoint_failure_count_recorded": failed_count_peak is not None,
        "backpressure_observed": (backpressure_peak or 0.0) >= scenario.min_backpressure_indicator,
        "iceberg_commit_lag_observed": (lag_peak or 0.0) > 0.0,
        "lag_recovered": final_lag == 0.0,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "baseline": {
            "max_checkpoint_duration_ms": duration_baseline,
            "max_alignment_time_ms": alignment_baseline,
        },
        "under_backpressure": {
            "max_checkpoint_duration_ms": duration_load,
            "max_alignment_time_ms": alignment_load,
            "max_backpressure_indicator": backpressure_peak,
            "max_iceberg_commit_lag_events": lag_peak,
        },
        "final": {
            "iceberg_commit_lag_events": final_lag,
            "checkpoint_failure_count": failed_count_peak,
        },
    }


def scenario_payload(scenario: Scenario) -> dict[str, Any]:
    return {
        "spike_events": scenario.spike_events,
        "insert_batch_size": scenario.insert_batch_size,
        "checkpoint_interval_ms": scenario.checkpoint_interval_ms,
        "backpressure_sleep_ms": scenario.backpressure_sleep_ms,
        "alignment_probe_sleep_ms": scenario.alignment_probe_sleep_ms,
        "baseline_samples": scenario.baseline_samples,
        "poll_interval_seconds": scenario.poll_interval_seconds,
        "max_samples_after_spike": scenario.max_samples_after_spike,
        "timeout_seconds": scenario.timeout_seconds,
        "min_backpressure_indicator": scenario.min_backpressure_indicator,
    }


def metric_names_payload() -> dict[str, Any]:
    return {
        "checkpoint_duration_ms": {
            "prometheus_pattern": "*lastCheckpointDuration",
            "rest_field": "checkpoints.latest.completed.end_to_end_duration",
        },
        "checkpoint_alignment_time_ms": {
            "prometheus_patterns": [
                "*lastCheckpointAlignmentDuration",
                "*checkpointAlignmentTime / 1000000",
                "*alignmentDuration",
            ],
            "rest_field": "checkpoints.details.tasks.*.subtasks.*.alignment.duration",
        },
        "checkpoint_failure_count": {
            "prometheus_pattern": "*numberOfFailedCheckpoints",
            "rest_field": "checkpoints.counts.failed",
        },
        "backpressure_indicator": {
            "prometheus_patterns": [
                "*backPressuredTimeMsPerSecond / 1000",
                "*isBackPressured",
                "*busyTimeMsPerSecond / 1000 when explicit backpressured time is zero",
            ]
        },
        "iceberg_commit_lag_events": {
            "source": (
                "COUNT(*) from MySQL orders minus COUNT(*) from Iceberg changelog; "
                "max_event_id_gap is also recorded"
            ),
            "iceberg_reader": "Flink SQL batch",
        },
    }


def _core_services_running(settings: Settings) -> bool:
    try:
        _require_core_services(settings)
    except FlinkCommandError:
        return False
    return True


def _parse_prometheus_labels(raw: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    if not raw:
        return labels
    for part in raw.split(","):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        labels[key.strip()] = value.strip().strip('"').replace(r"\"", '"').replace(r"\\", "\\")
    return labels


def _max_metric(
    metrics: Sequence[PromMetric],
    suffixes: Sequence[str],
    *,
    job_id: str,
) -> tuple[float | None, str]:
    suffix_keys = tuple(_metric_key(suffix) for suffix in suffixes)
    values: list[tuple[float, str]] = []
    for metric in metrics:
        if not any(_metric_key(metric.name).endswith(suffix) for suffix in suffix_keys):
            continue
        metric_job_id = metric.labels.get("job_id") or metric.labels.get("job")
        if metric_job_id and metric_job_id != job_id:
            continue
        values.append((metric.value, metric.name))
    if not values:
        return None, ""
    value, name = max(values, key=lambda item: item[0])
    return value, name


def _metric_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _normalize_alignment_time(value: float | None, metric_name: str) -> float | None:
    if value is None:
        return None
    if _metric_key(metric_name).endswith("checkpointalignmenttime"):
        return value / 1_000_000.0
    return value


def _latest_completed_checkpoint_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    completed_items = _completed_checkpoint_payloads(payload)
    if completed_items:
        return max(completed_items, key=lambda item: int(item["id"]))
    return None


def _completed_checkpoint_payloads(payload: dict[str, Any]) -> list[dict[str, Any]]:
    completed_items: list[dict[str, Any]] = []
    latest = payload.get("latest")
    if isinstance(latest, dict):
        completed = latest.get("completed")
        if isinstance(completed, dict) and completed.get("id") is not None:
            completed_items.append(completed)
    history = payload.get("history")
    if not isinstance(history, list):
        return completed_items
    seen = {int(cast(int | str, item["id"])) for item in completed_items}
    for item in history:
        if (
            isinstance(item, dict)
            and item.get("status") == "COMPLETED"
            and item.get("id") is not None
        ):
            checkpoint_id = int(cast(int | str, item["id"]))
            if checkpoint_id not in seen:
                completed_items.append(item)
                seen.add(checkpoint_id)
    return completed_items


def _checkpoint_details(
    job_id: str,
    checkpoint_id: int,
    *,
    settings: Settings,
) -> dict[str, Any] | None:
    with suppress(Exception):
        return flink_rest_json(
            f"/jobs/{job_id}/checkpoints/details/{checkpoint_id}", settings=settings
        )
    return None


def _numeric_paths(value: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], float]]:
    if isinstance(value, dict):
        result: list[tuple[tuple[str, ...], float]] = []
        for key, child in value.items():
            result.extend(_numeric_paths(child, (*path, str(key))))
        return result
    if isinstance(value, list):
        result = []
        for index, child in enumerate(value):
            result.extend(_numeric_paths(child, (*path, str(index))))
        return result
    number = _as_float(value)
    if number is None:
        return []
    return [(path, number)]


def _max_path_number(
    payload: dict[str, Any],
    *,
    include: Sequence[str],
    suffixes: Sequence[str],
) -> float | None:
    matches: list[float] = []
    include_keys = tuple(_metric_key(item) for item in include)
    suffix_keys = tuple(_metric_key(item) for item in suffixes)
    for path, value in _numeric_paths(payload):
        path_key = _metric_key(".".join(path))
        last_key = _metric_key(path[-1]) if path else ""
        if include_keys and not all(item in path_key for item in include_keys):
            continue
        if suffix_keys and not any(last_key.endswith(item) for item in suffix_keys):
            continue
        matches.append(value)
    if not matches:
        return None
    return max(matches)


def _source_progress(settings: Settings) -> tuple[int, int]:
    proc = run_mysql_script(
        "SELECT COALESCE(MAX(event_id), 0), COUNT(*) FROM orders;",
        settings=settings,
        capture=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    fields = _first_tsv_fields(proc.stdout, expected=2)
    return int(fields[0]), int(fields[1])


def _iceberg_progress(settings: Settings) -> tuple[int, int]:
    query = f"SELECT COALESCE(MAX(event_id), 0), COUNT(*) FROM {CHANGELOG_TABLE}"
    proc = run_java_class(BATCH_SQL_CLASS, ["--query", query], settings=settings)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    fields = _first_tsv_fields(clean_sql_output(proc.stdout), expected=2)
    return int(fields[0]), int(fields[1])


def _first_tsv_fields(output: str, *, expected: int) -> list[str]:
    for line in output.splitlines():
        if line.strip():
            fields = line.split("\t")
            if len(fields) != expected:
                raise ValueError(f"expected {expected} TSV fields, got {len(fields)}: {fields}")
            return fields
    raise ValueError("query returned no rows")


def _insert_events(settings: Settings, first_event_id: int, rows: int, batch_size: int) -> None:
    remaining = rows
    next_event_id = first_event_id
    while remaining > 0:
        current_batch = min(batch_size, remaining)
        values = ",\n".join(
            _event_values(next_event_id + offset) for offset in range(current_batch)
        )
        sql = (
            "INSERT INTO orders "
            "(order_id, business_key, event_id, customer_id, status, amount_cents, "
            "updated_at, seed) "
            f"VALUES\n{values};"
        )
        _mysql(sql, settings)
        next_event_id += current_batch
        remaining -= current_batch


def _event_values(event_id: int) -> str:
    sequence = event_id - BASE_EVENT_ID
    timestamp = (BASE_TS + timedelta(seconds=sequence)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    status = STATUSES[sequence % len(STATUSES)]
    return (
        f"({event_id},"
        f"{_sql_string(f'phase-2-3-order-{event_id:012d}')},"
        f"{event_id},"
        f"{23_000 + sequence % 1_000},"
        f"{_sql_string(status)},"
        f"{30_000 + sequence},"
        f"{_sql_string(timestamp)},"
        "223)"
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _first_number(*values: object) -> float | None:
    for value in values:
        number = _as_float(value)
        if number is not None:
            return number
    return None


def _max_number(*values: object) -> float | None:
    numbers = [number for number in (_as_float(value) for value in values) if number is not None]
    if not numbers:
        return None
    return max(numbers)


def _max_record_number(records: Sequence[dict[str, Any]], key: str) -> float | None:
    values = [
        number
        for number in (_as_float(record.get(key)) for record in records)
        if number is not None
    ]
    if not values:
        return None
    return max(values)


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        with suppress(ValueError):
            return float(value)
    return None


def _sample_number(sample: dict[str, Any], section: str, key: str) -> float | None:
    section_value = sample.get(section)
    if not isinstance(section_value, dict):
        return None
    return _as_float(section_value.get(key))


def _max_sample_number(samples: Sequence[dict[str, Any]], section: str, key: str) -> float | None:
    values = [
        value
        for value in (_sample_number(sample, section, key) for sample in samples)
        if value is not None
    ]
    if not values:
        return None
    return max(values)


def _last_sample_number(samples: Sequence[dict[str, Any]], section: str, key: str) -> float | None:
    for sample in reversed(samples):
        value = _sample_number(sample, section, key)
        if value is not None:
            return value
    return None


def _rose(baseline: float | None, load: float | None) -> bool:
    if baseline is None or load is None:
        return False
    return load > baseline


def _write_log(lines: Sequence[str]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_chart(payload: dict[str, Any]) -> None:
    samples = cast(list[dict[str, Any]], payload["time_series"])
    width = 980
    height = 540
    left = 82
    top = 82
    plot_width = 800
    plot_height = 340
    series = [
        ("Checkpoint duration", "checkpoint", "duration_ms", "#1f6f68"),
        ("Alignment time", "checkpoint", "alignment_time_ms", "#b2532f"),
        ("Commit lag", "iceberg_commit_lag", "lag_events", "#314f9f"),
    ]
    max_value = max(
        [
            _sample_number(sample, section, key) or 0.0
            for sample in samples
            for _, section, key, _ in series
        ]
        + [1.0]
    )
    lines = []
    legend = []
    for index, (label, section, key, color) in enumerate(series):
        points = _chart_points(
            samples,
            section=section,
            key=key,
            max_value=max_value,
            left=left,
            top=top,
            width=plot_width,
            height=plot_height,
        )
        if points:
            lines.append(
                f'<polyline fill="none" stroke="{color}" stroke-width="3" '
                f'points="{_xml_escape(points)}"/>'
            )
        y = 452 + index * 24
        legend.append(f'<rect x="82" y="{y - 12}" width="18" height="4" fill="{color}"/>')
        legend.append(f'<text x="112" y="{y - 6}" class="legend">{_xml_escape(label)}</text>')

    samples_count = len(samples)
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg"
  width="{width}" height="{height}" viewBox="0 0 {width} {height}"
  role="img" aria-labelledby="title desc">
  <title id="title">Checkpoint and backpressure metrics under induced load</title>
  <desc id="desc">Time series for checkpoint duration, alignment time,
    and Iceberg commit lag.</desc>
  <style>
    .bg {{ fill: #f7f8f6; }}
    .title {{ fill: #111815; font: 700 25px system-ui, sans-serif; }}
    .subtitle {{ fill: #58665f; font: 15px system-ui, sans-serif; }}
    .axis {{ stroke: #bcc8c0; stroke-width: 1; }}
    .grid {{ stroke: #dfe5e0; stroke-width: 1; }}
    .tick {{ fill: #5f6d65; font: 12px ui-monospace, monospace; }}
    .legend {{ fill: #26332d; font: 700 13px system-ui, sans-serif; }}
  </style>
  <rect class="bg" x="0" y="0" width="{width}" height="{height}"/>
  <text x="40" y="42" class="title">Phase 2.3 checkpoint metrics</text>
  <text x="40" y="66" class="subtitle">Real Flink reporter scrape; {samples_count} samples</text>
  <line class="axis" x1="{left}" y1="{top + plot_height}"
    x2="{left + plot_width}" y2="{top + plot_height}"/>
  <line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}"/>
  <line class="grid" x1="{left}" y1="{top + plot_height / 2:.1f}"
    x2="{left + plot_width}" y2="{top + plot_height / 2:.1f}"/>
  <text x="40" y="{top + 4}" class="tick">{max_value:g}</text>
  <text x="54" y="{top + plot_height + 4}" class="tick">0</text>
  {''.join(lines)}
  {''.join(legend)}
</svg>
"""
    CHART_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHART_PATH.write_text(svg, encoding="utf-8")


def _chart_points(
    samples: Sequence[dict[str, Any]],
    *,
    section: str,
    key: str,
    max_value: float,
    left: int,
    top: int,
    width: int,
    height: int,
) -> str:
    if not samples:
        return ""
    points: list[str] = []
    denominator = max(1, len(samples) - 1)
    for index, sample in enumerate(samples):
        value = _sample_number(sample, section, key)
        if value is None:
            continue
        x = left + width * index / denominator
        y = top + height - (height * value / max_value)
        points.append(f"{x:.1f},{y:.1f}")
    return " ".join(points)


def _xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _resource_profile() -> str:
    env_values = load_env_file()
    return os.environ.get("RESOURCE_PROFILE", env_values.get("RESOURCE_PROFILE", "small"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Phase 2.3 checkpoint/backpressure metric capture."
    )
    parser.add_argument("--resource-profile", choices=("small", "default"))
    parser.add_argument("--spike-events", type=int)
    parser.add_argument("--timeout-seconds", type=int)
    parser.add_argument("--max-samples-after-spike", type=int)
    parser.add_argument("--poll-interval-seconds", type=float)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    profile = args.resource_profile or _resource_profile()
    scenario = scenario_for_profile(profile)
    if (
        args.spike_events is not None
        or args.timeout_seconds is not None
        or args.max_samples_after_spike is not None
        or args.poll_interval_seconds is not None
    ):
        scenario = Scenario(
            resource_profile=scenario.resource_profile,
            spike_events=args.spike_events or scenario.spike_events,
            insert_batch_size=scenario.insert_batch_size,
            checkpoint_interval_ms=scenario.checkpoint_interval_ms,
            backpressure_sleep_ms=scenario.backpressure_sleep_ms,
            alignment_probe_sleep_ms=scenario.alignment_probe_sleep_ms,
            baseline_samples=scenario.baseline_samples,
            poll_interval_seconds=args.poll_interval_seconds or scenario.poll_interval_seconds,
            max_samples_after_spike=(
                args.max_samples_after_spike or scenario.max_samples_after_spike
            ),
            timeout_seconds=args.timeout_seconds or scenario.timeout_seconds,
            min_backpressure_indicator=scenario.min_backpressure_indicator,
        )
    try:
        payload = run_checkpoint_metrics(scenario=scenario)
    except Exception as exc:
        print(f"ckpt-metrics failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
