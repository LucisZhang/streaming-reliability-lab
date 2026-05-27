from __future__ import annotations

import argparse
import json
import subprocess
import time
from collections.abc import Callable, Sequence
from typing import Any

from harness.config import REPO_ROOT, Settings, load_settings
from harness.flink import (
    BATCH_SQL_CLASS,
    FlinkCommandError,
    cancel_job,
    reset_iceberg_tables,
    run_java_class,
    running_job_ids,
    savepoint,
    submit_job,
)
from harness.iceberg_meta import inspect_iceberg_metadata
from harness.provenance import utc_now, write_result
from harness.sql import clean_sql_output, run_mysql_script

CURRENT_TABLE = "cdc_lab.orders_current"
CHANGELOG_TABLE = "cdc_lab.orders_changelog"
LOG_PATH = REPO_ROOT / "showcase" / "logs" / "phase-1.2-cdc-smoke.log"
RESULT_PATH = REPO_ROOT / "showcase" / "results" / "phase-1.2-cdc-smoke.json"


def run_smoke(*, timeout_seconds: int = 180) -> dict[str, Any]:
    settings = load_settings()
    started_at = utc_now()
    log_lines: list[str] = []

    def log(message: str) -> None:
        line = f"{utc_now()} {message}"
        log_lines.append(line)
        print(message)

    try:
        log("checking core services")
        _require_core_services(settings)

        log("cancelling any existing Flink CDC job")
        for job_id in running_job_ids(settings=settings):
            cancel_job(job_id, settings=settings)

        log("resetting MySQL source and Iceberg target tables")
        _mysql("TRUNCATE TABLE orders;", settings)
        reset_iceberg_tables(settings=settings)

        log("submitting CDC job")
        job_id = submit_job(settings=settings, checkpoint_interval_ms=5_000)
        _wait_for_job(job_id, settings=settings, timeout_seconds=timeout_seconds)

        log("inserting baseline rows before controlled restart")
        _mysql(
            """
            INSERT INTO orders
              (
                order_id, business_key, event_id, customer_id,
                status, amount_cents, updated_at, seed
              )
            VALUES
              (
                1201, 'phase-1-2-delete', 1201, 501,
                'created', 11100, '2026-01-02 00:00:01.000', 12
              ),
              (
                1202, 'phase-1-2-update', 1202, 502,
                'created', 22200, '2026-01-02 00:00:02.000', 12
              );
            """,
            settings,
        )
        _wait_for_iceberg_count(2, settings=settings, timeout_seconds=timeout_seconds)

        log("creating savepoint and restarting from it")
        savepoint_path = savepoint(job_id, settings=settings)
        cancel_job(job_id, settings=settings)
        restored_job_id = submit_job(
            settings=settings, savepoint=savepoint_path, checkpoint_interval_ms=5_000
        )
        _wait_for_job(restored_job_id, settings=settings, timeout_seconds=timeout_seconds)

        log("driving update events after restart")
        _mysql(
            """
            UPDATE orders
               SET event_id = 2201,
                   status = 'paid',
                   amount_cents = 33300,
                   updated_at = '2026-01-02 00:01:01.000'
             WHERE order_id = 1201;

            UPDATE orders
               SET event_id = 2202,
                   status = 'shipped',
                   amount_cents = 44400,
                   updated_at = '2026-01-02 00:01:02.000'
             WHERE order_id = 1202;
            """,
            settings,
        )
        _wait_for_updated_state(settings=settings, timeout_seconds=timeout_seconds)

        log("driving delete event after the update snapshot is visible")
        _mysql(
            """
            DELETE FROM orders WHERE order_id = 1201;
            """,
            settings,
        )

        log("waiting for final source and Iceberg snapshots to match")
        final_rows = _wait_for_final_state(settings=settings, timeout_seconds=timeout_seconds)
        mysql_rows = _mysql_rows(settings)
        iceberg_rows = _iceberg_rows(settings)
        diff = _diff_rows(mysql_rows, iceberg_rows)
        changelog_count = _wait_for_changelog_count(
            7, settings=settings, timeout_seconds=timeout_seconds
        )
        metadata = inspect_iceberg_metadata(CURRENT_TABLE, settings=settings)

        deleted_key_absent = all(row["order_id"] != "1201" for row in iceberg_rows)
        updated = next((row for row in iceberg_rows if row["order_id"] == "1202"), None)
        updated_key_current = updated is not None and updated["status"] == "shipped"

        if diff["missing_in_iceberg"] or diff["unexpected_in_iceberg"]:
            raise AssertionError(f"source/Iceberg diff was not empty: {diff}")
        if not deleted_key_absent:
            raise AssertionError("deleted key 1201 was still present in orders_current")
        if not updated_key_current:
            raise AssertionError("updated key 1202 did not show the current value")

        payload: dict[str, Any] = {
            "phase": "1.2",
            "reader": "Flink SQL batch",
            "job_id": restored_job_id,
            "savepoint": savepoint_path,
            "source_iceberg_diff_count": 0,
            "deleted_key_absent": deleted_key_absent,
            "updated_key_current": updated_key_current,
            "orders_changelog_change_count": changelog_count,
            "final_rows": final_rows,
            "mysql_rows": mysql_rows,
            "iceberg_rows": iceberg_rows,
            "delete_file_smoke": {
                "delete_files": metadata.get("delete_files", 0),
                "equality_delete_files": metadata.get("equality_delete_files", 0),
                "metadata_reader": metadata.get("metadata_reader"),
                "summary": metadata.get("summary", {}),
            },
        }
        finished_at = utc_now()
        _write_log(log_lines)
        write_result(
            RESULT_PATH,
            payload=payload,
            command="make test-cdc",
            logs=str(LOG_PATH.relative_to(REPO_ROOT)),
            started_at=started_at,
            finished_at=finished_at,
        )
        return payload
    except Exception:
        _write_log(log_lines)
        raise


def _require_core_services(settings: Settings) -> None:
    proc = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(settings.env_file),
            "-f",
            str(settings.compose_file),
            "ps",
            "--status",
            "running",
            "--services",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip())
    services = set(proc.stdout.split())
    required = {"mysql", "minio", "jobmanager", "taskmanager"}
    missing = sorted(required - services)
    if missing:
        raise FlinkCommandError(f"core services are not running: {', '.join(missing)}")


def _mysql(script: str, settings: Settings) -> None:
    proc = run_mysql_script(script, settings=settings, capture=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip())


def _wait_for_job(job_id: str, *, settings: Settings, timeout_seconds: int) -> None:
    _wait_until(
        lambda: job_id in running_job_ids(settings=settings),
        description=f"job {job_id} running",
        timeout_seconds=timeout_seconds,
    )


def _wait_for_iceberg_count(count: int, *, settings: Settings, timeout_seconds: int) -> None:
    _wait_until(
        lambda: _iceberg_scalar(f"SELECT COUNT(*) FROM {CURRENT_TABLE}", settings) == str(count),
        description=f"{CURRENT_TABLE} count {count}",
        timeout_seconds=timeout_seconds,
    )


def _wait_for_final_state(*, settings: Settings, timeout_seconds: int) -> list[dict[str, str]]:
    def ready() -> bool:
        rows = _iceberg_rows(settings)
        return len(rows) == 1 and rows[0]["order_id"] == "1202" and rows[0]["status"] == "shipped"

    _wait_until(ready, description="final current table state", timeout_seconds=timeout_seconds)
    return _iceberg_rows(settings)


def _wait_for_updated_state(*, settings: Settings, timeout_seconds: int) -> list[dict[str, str]]:
    def ready() -> bool:
        rows = _iceberg_rows(settings)
        statuses = {row["order_id"]: row["status"] for row in rows}
        return len(rows) == 2 and statuses == {"1201": "paid", "1202": "shipped"}

    _wait_until(ready, description="post-restart update state", timeout_seconds=timeout_seconds)
    return _iceberg_rows(settings)


def _wait_for_changelog_count(count: int, *, settings: Settings, timeout_seconds: int) -> int:
    _wait_until(
        lambda: int(_iceberg_scalar(f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings)) >= count,
        description=f"{CHANGELOG_TABLE} at least {count} rows",
        timeout_seconds=timeout_seconds,
    )
    return int(_iceberg_scalar(f"SELECT COUNT(*) FROM {CHANGELOG_TABLE}", settings))


def _mysql_rows(settings: Settings) -> list[dict[str, str]]:
    proc = run_mysql_script(
        """
        SELECT order_id, business_key, event_id, customer_id, status, amount_cents,
               DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f'), seed
          FROM orders
         ORDER BY order_id;
        """,
        settings=settings,
        capture=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip())
    return [_row_dict(line.split("\t")) for line in proc.stdout.splitlines() if line.strip()]


def _iceberg_rows(settings: Settings) -> list[dict[str, str]]:
    output = _iceberg_query(
        f"""
        SELECT order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed
          FROM {CURRENT_TABLE}
         ORDER BY order_id
        """,
        settings,
    )
    return [_row_dict(line.split("\t")) for line in output.splitlines() if line.strip()]


def _iceberg_scalar(query: str, settings: Settings) -> str:
    output = _iceberg_query(query, settings)
    lines = [line for line in output.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"query returned no rows: {query}")
    return lines[-1].split("\t")[0]


def _iceberg_query(query: str, settings: Settings) -> str:
    proc = run_java_class(BATCH_SQL_CLASS, ["--query", query], settings=settings)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return clean_sql_output(proc.stdout)


def _row_dict(fields: list[str]) -> dict[str, str]:
    if len(fields) != 8:
        raise ValueError(f"expected 8 fields, got {len(fields)}: {fields}")
    return {
        "order_id": fields[0],
        "business_key": fields[1],
        "event_id": fields[2],
        "customer_id": fields[3],
        "status": fields[4],
        "amount_cents": fields[5],
        "updated_at": _normalize_timestamp(fields[6]),
        "seed": fields[7],
    }


def _normalize_timestamp(value: str) -> str:
    if "." not in value:
        return f"{value}.000"
    head, fraction = value.split(".", 1)
    return f"{head}.{fraction[:3].ljust(3, '0')}"


def _diff_rows(
    mysql_rows: list[dict[str, str]], iceberg_rows: list[dict[str, str]]
) -> dict[str, list[dict[str, str]]]:
    mysql_set = {json.dumps(row, sort_keys=True) for row in mysql_rows}
    iceberg_set = {json.dumps(row, sort_keys=True) for row in iceberg_rows}
    return {
        "missing_in_iceberg": [json.loads(row) for row in sorted(mysql_set - iceberg_set)],
        "unexpected_in_iceberg": [json.loads(row) for row in sorted(iceberg_set - mysql_set)],
    }


def _wait_until(
    predicate: Callable[[], bool], *, description: str, timeout_seconds: int, interval: float = 2.0
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:
            last_error = exc
        time.sleep(interval)
    if last_error is not None:
        raise TimeoutError(f"timed out waiting for {description}; last error: {last_error}")
    raise TimeoutError(f"timed out waiting for {description}")


def _write_log(lines: list[str]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Phase 1.2 CDC correctness smoke.")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_smoke(timeout_seconds=args.timeout_seconds)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
