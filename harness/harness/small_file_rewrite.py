from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from harness.config import REPO_ROOT, Settings, load_env_file, load_settings
from harness.eo_verify import (
    CURRENT_TABLE,
    _cancel_existing_jobs,
    _compose_up_services,
    _mysql,
    _require_core_services,
    _wait_for_completed_checkpoint,
    _wait_for_flink_cluster,
    _wait_for_iceberg_count,
    _wait_for_job,
)
from harness.flink import (
    ADMIN_CLASS,
    FlinkCommandError,
    cancel_job,
    flink_rest_json,
    latest_completed_checkpoint,
    reset_iceberg_tables,
    run_java_class,
    submit_job,
)
from harness.provenance import utc_now, write_result

RESULT_PATH = REPO_ROOT / "showcase" / "results" / "iceberg_small_file_rewrite.json"
LOG_PATH = REPO_ROOT / "showcase" / "logs" / "phase-2.2-small-file-rewrite.log"
CHART_PATH = REPO_ROOT / "showcase" / "media" / "phase-2.2-small-file-rewrite.svg"

BASE_EVENT_ID = 220_000
BASE_TS = datetime(2026, 2, 22, tzinfo=UTC)
STATUSES = ("created", "paid", "packed", "shipped", "delivered")

Metrics = dict[str, Any]


@dataclass(frozen=True)
class Scenario:
    resource_profile: str
    batches: int
    rows_per_batch: int
    checkpoint_interval_ms: int
    small_write_target_file_size_bytes: int
    rewrite_target_file_size_bytes: int
    manifest_merge_min_count: int
    planning_repetitions: int
    timeout_seconds: int

    @property
    def total_rows(self) -> int:
        return self.batches * self.rows_per_batch


def scenario_for_profile(profile: str) -> Scenario:
    if profile == "default":
        return Scenario(
            resource_profile=profile,
            batches=40,
            rows_per_batch=40,
            checkpoint_interval_ms=500,
            small_write_target_file_size_bytes=16 * 1024,
            rewrite_target_file_size_bytes=128 * 1024 * 1024,
            manifest_merge_min_count=10_000,
            planning_repetitions=7,
            timeout_seconds=600,
        )
    if profile != "small":
        raise ValueError("RESOURCE_PROFILE must be small or default")
    return Scenario(
        resource_profile=profile,
        batches=24,
        rows_per_batch=24,
        checkpoint_interval_ms=500,
        small_write_target_file_size_bytes=16 * 1024,
        rewrite_target_file_size_bytes=128 * 1024 * 1024,
        manifest_merge_min_count=10_000,
        planning_repetitions=7,
        timeout_seconds=420,
    )


def run_small_file_rewrite(
    *,
    scenario: Scenario,
    command: str = "make small-file-rewrite",
) -> dict[str, Any]:
    settings = load_settings()
    started_at = utc_now()
    log_lines: list[str] = []
    job_id: str | None = None

    def log(message: str) -> None:
        line = f"{utc_now()} {message}"
        log_lines.append(line)
        print(message, flush=True)

    try:
        log("checking core services")
        _compose_up_services(["taskmanager"], settings=settings)
        _wait_for_flink_cluster(settings=settings, timeout_seconds=120)
        _require_core_services(settings)

        log("resetting running jobs, MySQL source table, and Iceberg tables")
        _cancel_existing_jobs(settings, log=log, ignore_errors=True)
        _mysql("TRUNCATE TABLE orders;", settings)
        reset_iceberg_tables(settings=settings)

        extra_job_args = [
            "--write-target-file-size-bytes",
            str(scenario.small_write_target_file_size_bytes),
            "--write-parquet-row-group-size-bytes",
            str(scenario.small_write_target_file_size_bytes),
            "--commit-manifest-min-count-to-merge",
            str(scenario.manifest_merge_min_count),
        ]
        job_id = submit_job(
            settings=settings,
            checkpoint_interval_ms=scenario.checkpoint_interval_ms,
            extra_job_args=extra_job_args,
        )
        log(f"submitted CDC job {job_id} with short checkpoints for small-file generation")
        _wait_for_job(job_id, settings=settings, timeout_seconds=scenario.timeout_seconds)
        checkpoint = _wait_for_completed_checkpoint(
            job_id, settings=settings, timeout_seconds=scenario.timeout_seconds
        )
        last_checkpoint_id = int(checkpoint["id"])
        log(f"baseline checkpoint {last_checkpoint_id} completed before inserts")

        inserted_rows = 0
        for batch_index in range(scenario.batches):
            first_event_id = BASE_EVENT_ID + batch_index * scenario.rows_per_batch + 1
            _insert_batch(settings, first_event_id, scenario.rows_per_batch)
            inserted_rows += scenario.rows_per_batch
            checkpoint = _wait_for_checkpoint_after(
                job_id,
                last_checkpoint_id,
                settings=settings,
                timeout_seconds=scenario.timeout_seconds,
            )
            last_checkpoint_id = int(checkpoint["id"])
            if (batch_index + 1) % 4 == 0 or batch_index + 1 == scenario.batches:
                log(
                    "small-file batch "
                    f"{batch_index + 1}/{scenario.batches}: inserted_rows={inserted_rows} "
                    f"checkpoint={last_checkpoint_id}"
                )

        _wait_for_iceberg_count(
            scenario.total_rows, settings=settings, timeout_seconds=scenario.timeout_seconds
        )
        _wait_for_checkpoint_after(
            job_id,
            last_checkpoint_id,
            settings=settings,
            timeout_seconds=scenario.timeout_seconds,
        )
        cancel_job(job_id, settings=settings)
        log(f"cancelled CDC job {job_id} after small files were committed")
        job_id = None

        before = _admin_json(
            [
                "small-file-metrics",
                "--table",
                CURRENT_TABLE,
                "--planning-repetitions",
                str(scenario.planning_repetitions),
            ],
            settings=settings,
        )
        log(
            "before rewrite: "
            f"data_files={before['data_file_count']} "
            f"manifests={before['manifest_count']} "
            f"median_file_size={before['median_file_size_bytes']} "
            f"planning_ms={before['planning_latency_ms']}"
        )

        rewrite_data_files = _admin_json(
            [
                "rewrite-data-files",
                "--table",
                CURRENT_TABLE,
                "--target-file-size-bytes",
                str(scenario.rewrite_target_file_size_bytes),
                "--max-parallelism",
                "2",
            ],
            settings=settings,
        )
        log(
            "rewrite_data_files: "
            f"rewritten={rewrite_data_files['rewritten_data_files_count']} "
            f"added={rewrite_data_files['added_data_files_count']}"
        )

        after_data_rewrite = _admin_json(
            [
                "small-file-metrics",
                "--table",
                CURRENT_TABLE,
                "--planning-repetitions",
                str(scenario.planning_repetitions),
            ],
            settings=settings,
        )
        manifest_rewrite = _admin_json(
            ["rewrite-manifests", "--table", CURRENT_TABLE], settings=settings
        )
        log(
            "rewrite_manifests: "
            f"ran={manifest_rewrite['ran']} "
            f"live_before={manifest_rewrite['live_manifest_count_before']} "
            f"live_after={manifest_rewrite['live_manifest_count_after']}"
        )

        after = _admin_json(
            [
                "small-file-metrics",
                "--table",
                CURRENT_TABLE,
                "--planning-repetitions",
                str(scenario.planning_repetitions),
            ],
            settings=settings,
        )
        log(
            "after rewrite: "
            f"data_files={after['data_file_count']} "
            f"manifests={after['manifest_count']} "
            f"median_file_size={after['median_file_size_bytes']} "
            f"planning_ms={after['planning_latency_ms']}"
        )

        checks = compare_metrics(before, after)
        payload: dict[str, object] = {
            "phase": "2.2",
            "artifact": "Iceberg small-file rewrite_data_files evidence",
            "table": CURRENT_TABLE,
            "maintenance_scope": (
                "Iceberg small-file management on orders_current; "
                "StarRocks ingestion and Primary Key table maintenance are not involved."
            ),
            "resource_profile": scenario.resource_profile,
            "reader": (
                "Iceberg Java metadata scan for files/manifests; " "Flink SQL batch for row count"
            ),
            "scenario": scenario_payload(scenario),
            "before": before,
            "rewrite_data_files": rewrite_data_files,
            "after_data_rewrite": after_data_rewrite,
            "rewrite_manifests": manifest_rewrite,
            "after": after,
            "deltas": deltas(before, after),
            "checks": checks,
            "summary": {
                "passed": all(checks.values()),
                "data_file_count_decreased": checks["data_file_count_decreased"],
                "manifest_count_decreased": checks["manifest_count_decreased"],
                "median_file_size_increased": checks["median_file_size_increased"],
                "planning_latency_decreased": checks["planning_latency_decreased"],
            },
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
        if not all(checks.values()):
            failed = ", ".join(name for name, passed in checks.items() if not passed)
            raise AssertionError(f"small-file rewrite checks failed: {failed}")
        return payload
    finally:
        if job_id is not None:
            with suppress(Exception):
                cancel_job(job_id, settings=settings)
        _write_log(log_lines)


def compare_metrics(before: Metrics, after: Metrics) -> dict[str, bool]:
    return {
        "data_file_count_decreased": int(after["data_file_count"]) < int(before["data_file_count"]),
        "manifest_count_decreased": int(after["manifest_count"]) < int(before["manifest_count"]),
        "median_file_size_increased": float(after["median_file_size_bytes"])
        > float(before["median_file_size_bytes"]),
        "planning_latency_decreased": float(after["planning_latency_ms"])
        < float(before["planning_latency_ms"]),
    }


def deltas(before: Metrics, after: Metrics) -> dict[str, float]:
    return {
        "data_file_count": float(after["data_file_count"]) - float(before["data_file_count"]),
        "manifest_count": float(after["manifest_count"]) - float(before["manifest_count"]),
        "median_file_size_bytes": float(after["median_file_size_bytes"])
        - float(before["median_file_size_bytes"]),
        "planning_latency_ms": float(after["planning_latency_ms"])
        - float(before["planning_latency_ms"]),
    }


def scenario_payload(scenario: Scenario) -> dict[str, object]:
    return {
        "batches": scenario.batches,
        "rows_per_batch": scenario.rows_per_batch,
        "total_rows": scenario.total_rows,
        "checkpoint_interval_ms": scenario.checkpoint_interval_ms,
        "small_write_target_file_size_bytes": scenario.small_write_target_file_size_bytes,
        "rewrite_target_file_size_bytes": scenario.rewrite_target_file_size_bytes,
        "manifest_merge_min_count": scenario.manifest_merge_min_count,
        "planning_repetitions": scenario.planning_repetitions,
        "timeout_seconds": scenario.timeout_seconds,
    }


def _wait_for_checkpoint_after(
    job_id: str,
    previous_checkpoint_id: int,
    *,
    settings: Settings,
    timeout_seconds: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_checkpoint: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        state = _job_state(job_id, settings=settings)
        if state in {"FAILED", "CANCELED", "CANCELLING", "FAILING", "SUSPENDED"}:
            raise RuntimeError(f"Flink job {job_id} entered state {state}")
        checkpoint = latest_completed_checkpoint(job_id, settings=settings)
        if checkpoint is not None:
            last_checkpoint = checkpoint
            if int(checkpoint["id"]) > previous_checkpoint_id:
                return checkpoint
        time.sleep(1.0)
    last_id = last_checkpoint["id"] if last_checkpoint else "none"
    raise TimeoutError(
        f"timed out waiting for checkpoint after {previous_checkpoint_id}; latest={last_id}"
    )


def _job_state(job_id: str, *, settings: Settings) -> str:
    payload = flink_rest_json(f"/jobs/{job_id}", settings=settings)
    return str(payload.get("state", "UNKNOWN"))


def _insert_batch(settings: Settings, first_event_id: int, rows: int) -> None:
    values = ",\n".join(_event_values(first_event_id + offset) for offset in range(rows))
    sql = (
        "INSERT INTO orders "
        "(order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed) "
        f"VALUES\n{values};"
    )
    _mysql(sql, settings)


def _event_values(event_id: int) -> str:
    sequence = event_id - BASE_EVENT_ID
    timestamp = (BASE_TS + timedelta(seconds=sequence)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    status = STATUSES[sequence % len(STATUSES)]
    return (
        f"({event_id},"
        f"{_sql_string(f'phase-2-2-order-{event_id:012d}')},"
        f"{event_id},"
        f"{10_000 + sequence % 1_000},"
        f"{_sql_string(status)},"
        f"{25_000 + sequence},"
        f"{_sql_string(timestamp)},"
        "222)"
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _admin_json(args: list[str], *, settings: Settings) -> dict[str, Any]:
    proc = run_java_class(ADMIN_CLASS, args, settings=settings)
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())
    return _last_json(proc.stdout)


def _last_json(output: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            return cast(dict[str, Any], json.loads(stripped))
    start = output.rfind("{")
    end = output.rfind("}")
    if start >= 0 and end > start:
        return cast(dict[str, Any], json.loads(output[start : end + 1]))
    raise ValueError(f"no JSON object found in admin output: {output}")


def _write_log(lines: Sequence[str]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_chart(payload: dict[str, Any]) -> None:
    before = cast(Metrics, payload["before"])
    after = cast(Metrics, payload["after"])
    metrics = [
        ("Data files", float(before["data_file_count"]), float(after["data_file_count"])),
        ("Manifests", float(before["manifest_count"]), float(after["manifest_count"])),
        (
            "Median file bytes",
            float(before["median_file_size_bytes"]),
            float(after["median_file_size_bytes"]),
        ),
        (
            "Planning ms",
            float(before["planning_latency_ms"]),
            float(after["planning_latency_ms"]),
        ),
    ]
    width = 940
    height = 520
    plot_left = 190
    plot_top = 90
    row_gap = 86
    max_ratio_width = 520
    rows: list[str] = []
    for index, (label, before_value, after_value) in enumerate(metrics):
        y = plot_top + index * row_gap
        max_value = max(before_value, after_value, 1.0)
        before_width = max(4.0, before_value / max_value * max_ratio_width)
        after_width = max(4.0, after_value / max_value * max_ratio_width)
        value_x = plot_left + max_ratio_width + 18
        rows.append(
            f'<text x="32" y="{y + 19}" class="label">{_xml_escape(label)}</text>'
            f'{_svg_rect(plot_left, y, before_width, 26, "before")}'
            f'{_svg_rect(plot_left, y + 34, after_width, 26, "after")}'
            f'<text x="{value_x}" y="{y + 19}" class="value">'
            f"before {before_value:g}</text>"
            f'<text x="{value_x}" y="{y + 53}" class="value">'
            f"after {after_value:g}</text>"
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg"
  width="{width}" height="{height}" viewBox="0 0 {width} {height}"
  role="img" aria-labelledby="title desc">
  <title id="title">Iceberg small-file rewrite before and after metrics</title>
  <desc id="desc">Before and after Iceberg file and planning metrics.</desc>
  <style>
    .bg {{ fill: #f7f8f6; }}
    .title {{ fill: #111815; font: 700 26px system-ui, sans-serif; }}
    .subtitle {{ fill: #58665f; font: 15px system-ui, sans-serif; }}
    .label {{ fill: #1b2520; font: 700 15px system-ui, sans-serif; }}
    .value {{ fill: #4e5b55; font: 13px ui-monospace, monospace; }}
    .before {{ fill: #a75238; }}
    .after {{ fill: #267a4a; }}
    .legend {{ fill: #334039; font: 700 13px system-ui, sans-serif; }}
  </style>
  <rect class="bg" x="0" y="0" width="{width}" height="{height}" rx="0"/>
  <text x="32" y="42" class="title">Iceberg small-file rewrite_data_files</text>
  <text x="32" y="68" class="subtitle">cdc_lab.orders_current metadata</text>
  <rect x="32" y="455" width="22" height="14" class="before"/>
  <text x="62" y="467" class="legend">Before rewrite</text>
  <rect x="205" y="455" width="22" height="14" class="after"/>
  <text x="235" y="467" class="legend">After rewrite</text>
  {''.join(rows)}
</svg>
"""
    CHART_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHART_PATH.write_text(svg, encoding="utf-8")


def _svg_rect(x: int, y: int, width: float, height: int, class_name: str) -> str:
    return f'<rect x="{x}" y="{y}" width="{width:.1f}" ' f'height="{height}" class="{class_name}"/>'


def _xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _resource_profile() -> str:
    env_values = load_env_file()
    return os.environ.get("RESOURCE_PROFILE", env_values.get("RESOURCE_PROFILE", "small"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 2.2 Iceberg small-file maintenance.")
    parser.add_argument("--resource-profile", choices=("small", "default"))
    parser.add_argument("--batches", type=int)
    parser.add_argument("--rows-per-batch", type=int)
    parser.add_argument("--timeout-seconds", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    profile = args.resource_profile or _resource_profile()
    scenario = scenario_for_profile(profile)
    if (
        args.batches is not None
        or args.rows_per_batch is not None
        or args.timeout_seconds is not None
    ):
        scenario = Scenario(
            resource_profile=scenario.resource_profile,
            batches=args.batches or scenario.batches,
            rows_per_batch=args.rows_per_batch or scenario.rows_per_batch,
            checkpoint_interval_ms=scenario.checkpoint_interval_ms,
            small_write_target_file_size_bytes=scenario.small_write_target_file_size_bytes,
            rewrite_target_file_size_bytes=scenario.rewrite_target_file_size_bytes,
            manifest_merge_min_count=scenario.manifest_merge_min_count,
            planning_repetitions=scenario.planning_repetitions,
            timeout_seconds=args.timeout_seconds or scenario.timeout_seconds,
        )
    try:
        payload = run_small_file_rewrite(scenario=scenario)
    except Exception as exc:
        print(f"small-file-rewrite failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
