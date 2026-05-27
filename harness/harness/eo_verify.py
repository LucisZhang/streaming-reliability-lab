from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from typing import Any, cast

from harness.config import REPO_ROOT, Settings, load_settings
from harness.flink import (
    BATCH_SQL_CLASS,
    FlinkCommandError,
    cancel_job,
    compose_base,
    flink_rest_json,
    latest_completed_checkpoint,
    reset_iceberg_tables,
    run_java_class,
    running_job_ids,
    savepoint,
    submit_job,
)
from harness.provenance import utc_now, write_result
from harness.sql import clean_sql_output, run_mysql_script

CURRENT_TABLE = "cdc_lab.orders_current"
CHANGELOG_TABLE = "cdc_lab.orders_changelog"
RESULT_PATH = REPO_ROOT / "showcase" / "results" / "eo_reconciliation.json"
LOG_PATH = REPO_ROOT / "showcase" / "logs" / "phase-2.1-eo-verify.log"
SUPPORTED_FAILURES = (
    "task-crash",
    "checkpoint-restore",
    "jobmanager-restart",
    "savepoint-restore",
    "sink-commit-fault",
)

Row = dict[str, str]


def parse_failure_classes(raw: str) -> list[str]:
    requested = [item.strip() for item in raw.split(",") if item.strip()]
    if not requested:
        raise ValueError("--failure must name at least one failure class")
    if requested == ["all"]:
        return list(SUPPORTED_FAILURES)

    unknown = sorted(set(requested) - set(SUPPORTED_FAILURES))
    if unknown:
        supported = ", ".join([*SUPPORTED_FAILURES, "all"])
        raise ValueError(f"unsupported failure class(es): {', '.join(unknown)}; use {supported}")
    return requested


def run_eo_verify(
    *,
    failure_classes: Sequence[str],
    timeout_seconds: int = 360,
) -> dict[str, Any]:
    settings = load_settings()
    started_at = utc_now()
    log_lines: list[str] = []
    results: list[dict[str, Any]] = []
    errors: list[str] = []

    def log(message: str) -> None:
        line = f"{utc_now()} {message}"
        log_lines.append(line)
        print(message, flush=True)

    try:
        log("checking core services")
        _require_core_services(settings)

        for failure_class in failure_classes:
            log(f"{failure_class}: starting deterministic scenario")
            try:
                if failure_class == "task-crash":
                    result = _run_task_crash(settings, timeout_seconds=timeout_seconds, log=log)
                elif failure_class == "checkpoint-restore":
                    result = _run_checkpoint_restore(
                        settings, timeout_seconds=timeout_seconds, log=log
                    )
                elif failure_class == "jobmanager-restart":
                    result = _run_jobmanager_restart(
                        settings, timeout_seconds=timeout_seconds, log=log
                    )
                elif failure_class == "savepoint-restore":
                    result = _run_savepoint_restore(
                        settings, timeout_seconds=timeout_seconds, log=log
                    )
                elif failure_class == "sink-commit-fault":
                    result = _run_sink_commit_fault(
                        settings, timeout_seconds=timeout_seconds, log=log
                    )
                else:
                    raise AssertionError(f"unhandled failure class {failure_class}")
                results.append(result)
                log(
                    f"{failure_class}: snapshot_diff_count={result['snapshot_diff_count']} "
                    f"event_id_audit_consistent={result['event_id_audit']['consistent']}"
                )
            except Exception as exc:
                error = f"{failure_class}: {exc}"
                errors.append(error)
                results.append(
                    {
                        "failure_class": failure_class,
                        "passed": False,
                        "error": str(exc),
                    }
                )
                log(f"{failure_class}: failed: {exc}")
            finally:
                _cancel_existing_jobs(settings, log=log, ignore_errors=True)

        payload = _payload(results=results, errors=errors)
        finished_at = utc_now()
        _write_log(log_lines)
        write_result(
            RESULT_PATH,
            payload=payload,
            command=f'make eo-verify ARGS="--failure {",".join(failure_classes)}"',
            logs=str(LOG_PATH.relative_to(REPO_ROOT)),
            started_at=started_at,
            finished_at=finished_at,
        )

        if errors:
            raise AssertionError("; ".join(errors))
        return payload
    except Exception as exc:
        if not results:
            results.append({"failure_class": "setup", "passed": False, "error": str(exc)})
        payload = _payload(results=results, errors=errors or [str(exc)])
        finished_at = utc_now()
        _write_log(log_lines)
        write_result(
            RESULT_PATH,
            payload=payload,
            command=f'make eo-verify ARGS="--failure {",".join(failure_classes)}"',
            logs=str(LOG_PATH.relative_to(REPO_ROOT)),
            started_at=started_at,
            finished_at=finished_at,
        )
        raise


def _payload(*, results: list[dict[str, Any]], errors: list[str]) -> dict[str, Any]:
    passed_results = [result for result in results if result.get("passed") is True]
    all_snapshot_diffs_zero = len(passed_results) == len(results) and all(
        result.get("snapshot_diff_count") == 0 for result in passed_results
    )
    all_event_id_audits_consistent = len(passed_results) == len(results) and all(
        bool(result.get("event_id_audit", {}).get("consistent")) for result in passed_results
    )
    return {
        "phase": "2.1",
        "reader": "Flink SQL batch",
        "claim_boundary": "MySQL CDC -> Flink -> Iceberg",
        "results": results,
        "summary": {
            "passed": not errors,
            "failure_classes": [str(result.get("failure_class")) for result in results],
            "all_snapshot_diffs_zero": all_snapshot_diffs_zero,
            "all_event_id_audits_consistent": all_event_id_audits_consistent,
            "errors": errors,
        },
    }


def _run_task_crash(
    settings: Settings,
    *,
    timeout_seconds: int,
    log: Callable[[str], None],
) -> dict[str, Any]:
    _reset_for_scenario(settings, log=log)

    marker_path = f"/tmp/p1-phase-1-3-task-crash-{uuid.uuid4().hex}.marker"
    job_id = submit_job(
        settings=settings,
        checkpoint_interval_ms=2_000,
        extra_job_args=[
            "--task-crash-event-id",
            "1303",
            "--task-crash-marker-path",
            marker_path,
        ],
    )
    log(f"task-crash: submitted job {job_id} with one-shot crash marker {marker_path}")
    _wait_for_job(job_id, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (1301, 'phase-1-3-task-crash-delete', 1301, 601,
           'created', 10100, '2026-01-03 00:00:01.000', 13),
          (1302, 'phase-1-3-task-crash-update', 1302, 602,
           'created', 20200, '2026-01-03 00:00:02.000', 13);
        """,
        settings,
    )
    _wait_for_iceberg_count(2, settings=settings, timeout_seconds=timeout_seconds)
    pre_crash_checkpoint = _wait_for_completed_checkpoint(
        job_id, settings=settings, timeout_seconds=timeout_seconds
    )
    log(
        "task-crash: baseline committed before crash at checkpoint " f"{pre_crash_checkpoint['id']}"
    )

    _mysql(
        """
        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (1303, 'phase-1-3-task-crash-trigger', 1303, 603,
           'paid', 30300, '2026-01-03 00:00:03.000', 13),
          (1304, 'phase-1-3-task-crash-trailing', 1304, 604,
           'packed', 40400, '2026-01-03 00:00:04.000', 13);
        """,
        settings,
    )
    _wait_until(
        lambda: _container_path_exists("taskmanager", marker_path, settings=settings),
        description="task crash marker",
        timeout_seconds=timeout_seconds,
    )
    log("task-crash: controlled task exception observed; waiting for automatic recovery")
    _wait_for_job(job_id, settings=settings, timeout_seconds=timeout_seconds)
    _wait_for_iceberg_count(4, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        UPDATE orders
           SET event_id = 2301,
               status = 'paid',
               amount_cents = 11111,
               updated_at = '2026-01-03 00:01:01.000'
         WHERE order_id = 1301;

        UPDATE orders
           SET event_id = 2302,
               status = 'shipped',
               amount_cents = 22222,
               updated_at = '2026-01-03 00:01:02.000'
         WHERE order_id = 1302;

        """,
        settings,
    )
    _wait_for_iceberg_statuses(
        {"1301": "paid", "1302": "shipped", "1303": "paid", "1304": "packed"},
        settings=settings,
        timeout_seconds=timeout_seconds,
    )
    _mysql("DELETE FROM orders WHERE order_id = 1301;", settings)

    reconciliation = _wait_for_reconciliation(
        settings=settings,
        expected_current_event_ids=[1303, 1304, 2302],
        expected_changelog_event_ids=[1301, 1302, 1303, 1304, 2301, 2302],
        expected_changelog_rows=9,
        timeout_seconds=timeout_seconds,
    )
    return {
        "failure_class": "task-crash",
        "passed": True,
        "trigger": "one-shot Flink operator exception on event_id=1303",
        "job_id": job_id,
        "recovery": {
            "mode": "automatic task restart",
            "marker_path": marker_path,
            "pre_crash_checkpoint": pre_crash_checkpoint,
        },
        **reconciliation,
    }


def _run_checkpoint_restore(
    settings: Settings,
    *,
    timeout_seconds: int,
    log: Callable[[str], None],
) -> dict[str, Any]:
    _reset_for_scenario(settings, log=log)

    job_id = submit_job(settings=settings, checkpoint_interval_ms=2_000)
    log(f"checkpoint-restore: submitted job {job_id}")
    _wait_for_job(job_id, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (3301, 'phase-1-3-checkpoint-delete', 3301, 701,
           'created', 11100, '2026-01-04 00:00:01.000', 33),
          (3302, 'phase-1-3-checkpoint-update', 3302, 702,
           'created', 22200, '2026-01-04 00:00:02.000', 33),
          (3303, 'phase-1-3-checkpoint-carry', 3303, 703,
           'packed', 33300, '2026-01-04 00:00:03.000', 33);
        """,
        settings,
    )
    _wait_for_iceberg_count(3, settings=settings, timeout_seconds=timeout_seconds)
    checkpoint_before_stop = _wait_for_completed_checkpoint(
        job_id, settings=settings, timeout_seconds=timeout_seconds
    )
    log(
        "checkpoint-restore: stopping job "
        f"{job_id} after checkpoint {checkpoint_before_stop['id']}"
    )
    cancel_job(job_id, settings=settings)
    checkpoint = _wait_for_retained_checkpoint(
        job_id, settings=settings, timeout_seconds=timeout_seconds
    )
    checkpoint_path = str(checkpoint["external_path"])

    restored_job_id = submit_job(
        settings=settings,
        savepoint=checkpoint_path,
        checkpoint_interval_ms=2_000,
    )
    log(f"checkpoint-restore: restored job {restored_job_id} from {checkpoint_path}")
    _wait_for_job(restored_job_id, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        UPDATE orders
           SET event_id = 4301,
               status = 'paid',
               amount_cents = 44400,
               updated_at = '2026-01-04 00:01:01.000'
         WHERE order_id = 3301;

        UPDATE orders
           SET event_id = 4302,
               status = 'delivered',
               amount_cents = 55500,
               updated_at = '2026-01-04 00:01:02.000'
         WHERE order_id = 3302;

        """,
        settings,
    )
    _wait_for_iceberg_statuses(
        {"3301": "paid", "3302": "delivered", "3303": "packed"},
        settings=settings,
        timeout_seconds=timeout_seconds,
    )
    _mysql(
        """
        DELETE FROM orders WHERE order_id = 3301;

        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (3304, 'phase-1-3-checkpoint-post-restore', 3304, 704,
           'paid', 66600, '2026-01-04 00:01:04.000', 33);
        """,
        settings,
    )

    reconciliation = _wait_for_reconciliation(
        settings=settings,
        expected_current_event_ids=[3303, 3304, 4302],
        expected_changelog_event_ids=[3301, 3302, 3303, 3304, 4301, 4302],
        expected_changelog_rows=9,
        timeout_seconds=timeout_seconds,
    )
    return {
        "failure_class": "checkpoint-restore",
        "passed": True,
        "trigger": "cancel running job and restore from latest completed checkpoint",
        "job_id": job_id,
        "restored_job_id": restored_job_id,
        "recovery": {
            "mode": "restore from externalized checkpoint",
            "checkpoint_before_stop": checkpoint_before_stop,
            "checkpoint": checkpoint,
        },
        **reconciliation,
    }


def _run_jobmanager_restart(
    settings: Settings,
    *,
    timeout_seconds: int,
    log: Callable[[str], None],
) -> dict[str, Any]:
    _reset_for_scenario(settings, log=log)

    job_id = submit_job(settings=settings, checkpoint_interval_ms=2_000)
    log(f"jobmanager-restart: submitted job {job_id}")
    _wait_for_job(job_id, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (5301, 'phase-2-1-jm-delete', 5301, 801,
           'created', 12100, '2026-01-05 00:00:01.000', 53),
          (5302, 'phase-2-1-jm-update', 5302, 802,
           'created', 24200, '2026-01-05 00:00:02.000', 53),
          (5303, 'phase-2-1-jm-carry', 5303, 803,
           'packed', 36300, '2026-01-05 00:00:03.000', 53);
        """,
        settings,
    )
    _wait_for_iceberg_count(3, settings=settings, timeout_seconds=timeout_seconds)
    checkpoint_before_restart = _wait_for_completed_checkpoint(
        job_id, settings=settings, timeout_seconds=timeout_seconds
    )
    checkpoint_path = _normalize_state_path(str(checkpoint_before_restart["external_path"]))
    log(
        "jobmanager-restart: restarting JobManager after checkpoint "
        f"{checkpoint_before_restart['id']}"
    )

    _restart_compose_service("jobmanager", settings=settings)
    _compose_up_services(["taskmanager"], settings=settings)
    log("jobmanager-restart: ensured TaskManager is running after JobManager restart")
    _wait_for_flink_cluster(settings=settings, timeout_seconds=timeout_seconds)

    jobs_after_restart = running_job_ids(settings=settings)
    if job_id in jobs_after_restart:
        restored_job_id = job_id
        recovery_mode = "session job remained running after JobManager container restart"
    else:
        restored_job_id = submit_job(
            settings=settings,
            savepoint=checkpoint_path,
            checkpoint_interval_ms=2_000,
        )
        recovery_mode = "restore from latest checkpoint after JobManager container restart"
    log(f"jobmanager-restart: active job after recovery {restored_job_id}")
    _wait_for_job(restored_job_id, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        UPDATE orders
           SET event_id = 6301,
               status = 'paid',
               amount_cents = 48400,
               updated_at = '2026-01-05 00:01:01.000'
         WHERE order_id = 5301;

        UPDATE orders
           SET event_id = 6302,
               status = 'delivered',
               amount_cents = 60500,
               updated_at = '2026-01-05 00:01:02.000'
         WHERE order_id = 5302;

        """,
        settings,
    )
    _wait_for_iceberg_statuses(
        {"5301": "paid", "5302": "delivered", "5303": "packed"},
        settings=settings,
        timeout_seconds=timeout_seconds,
    )
    _mysql(
        """
        DELETE FROM orders WHERE order_id = 5301;

        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (5304, 'phase-2-1-jm-post-restart', 5304, 804,
           'paid', 72600, '2026-01-05 00:01:04.000', 53);
        """,
        settings,
    )

    reconciliation = _wait_for_reconciliation(
        settings=settings,
        expected_current_event_ids=[5303, 5304, 6302],
        expected_changelog_event_ids=[5301, 5302, 5303, 5304, 6301, 6302],
        expected_changelog_rows=9,
        timeout_seconds=timeout_seconds,
    )
    return {
        "failure_class": "jobmanager-restart",
        "passed": True,
        "trigger": "restart the Flink JobManager container after a completed checkpoint",
        "job_id": job_id,
        "restored_job_id": restored_job_id,
        "recovery": {
            "mode": recovery_mode,
            "checkpoint_before_restart": checkpoint_before_restart,
            "checkpoint_path_used": checkpoint_path,
            "taskmanager_recovery": "docker compose up -d taskmanager after JobManager restart",
        },
        **reconciliation,
    }


def _run_savepoint_restore(
    settings: Settings,
    *,
    timeout_seconds: int,
    log: Callable[[str], None],
) -> dict[str, Any]:
    _reset_for_scenario(settings, log=log)

    job_id = submit_job(settings=settings, checkpoint_interval_ms=2_000)
    log(f"savepoint-restore: submitted job {job_id}")
    _wait_for_job(job_id, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (7301, 'phase-2-1-savepoint-delete', 7301, 901,
           'created', 13100, '2026-01-06 00:00:01.000', 73),
          (7302, 'phase-2-1-savepoint-update', 7302, 902,
           'created', 26200, '2026-01-06 00:00:02.000', 73),
          (7303, 'phase-2-1-savepoint-carry', 7303, 903,
           'packed', 39300, '2026-01-06 00:00:03.000', 73);
        """,
        settings,
    )
    _wait_for_iceberg_count(3, settings=settings, timeout_seconds=timeout_seconds)
    checkpoint_before_savepoint = _wait_for_completed_checkpoint(
        job_id, settings=settings, timeout_seconds=timeout_seconds
    )
    savepoint_path = savepoint(job_id, settings=settings, timeout_seconds=timeout_seconds)
    log(f"savepoint-restore: created savepoint {savepoint_path}")
    cancel_job(job_id, settings=settings)

    restored_job_id = submit_job(
        settings=settings,
        savepoint=savepoint_path,
        checkpoint_interval_ms=2_000,
    )
    log(f"savepoint-restore: restored job {restored_job_id} from {savepoint_path}")
    _wait_for_job(restored_job_id, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        UPDATE orders
           SET event_id = 8301,
               status = 'paid',
               amount_cents = 52400,
               updated_at = '2026-01-06 00:01:01.000'
         WHERE order_id = 7301;

        UPDATE orders
           SET event_id = 8302,
               status = 'delivered',
               amount_cents = 65500,
               updated_at = '2026-01-06 00:01:02.000'
         WHERE order_id = 7302;

        """,
        settings,
    )
    _wait_for_iceberg_statuses(
        {"7301": "paid", "7302": "delivered", "7303": "packed"},
        settings=settings,
        timeout_seconds=timeout_seconds,
    )
    _mysql(
        """
        DELETE FROM orders WHERE order_id = 7301;

        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (7304, 'phase-2-1-savepoint-post-restore', 7304, 904,
           'paid', 78600, '2026-01-06 00:01:04.000', 73);
        """,
        settings,
    )

    reconciliation = _wait_for_reconciliation(
        settings=settings,
        expected_current_event_ids=[7303, 7304, 8302],
        expected_changelog_event_ids=[7301, 7302, 7303, 7304, 8301, 8302],
        expected_changelog_rows=9,
        timeout_seconds=timeout_seconds,
    )
    return {
        "failure_class": "savepoint-restore",
        "passed": True,
        "trigger": "create a Flink savepoint, cancel the job, and restore from that savepoint",
        "job_id": job_id,
        "restored_job_id": restored_job_id,
        "recovery": {
            "mode": "restore from explicit Flink savepoint",
            "checkpoint_before_savepoint": checkpoint_before_savepoint,
            "savepoint": savepoint_path,
        },
        **reconciliation,
    }


def _run_sink_commit_fault(
    settings: Settings,
    *,
    timeout_seconds: int,
    log: Callable[[str], None],
) -> dict[str, Any]:
    _reset_for_scenario(settings, log=log)

    marker_path = f"/tmp/p1-phase-2-1-sink-commit-{uuid.uuid4().hex}.marker"
    job_id = submit_job(
        settings=settings,
        checkpoint_interval_ms=2_000,
        extra_job_args=[
            "--checkpoint-complete-fault-event-id",
            "9303",
            "--checkpoint-complete-fault-marker-path",
            marker_path,
        ],
    )
    log(
        "sink-commit-fault: submitted job "
        f"{job_id} with checkpoint-complete marker {marker_path}"
    )
    _wait_for_job(job_id, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (9301, 'phase-2-1-commit-delete', 9301, 1001,
           'created', 14100, '2026-01-07 00:00:01.000', 93),
          (9302, 'phase-2-1-commit-update', 9302, 1002,
           'created', 28200, '2026-01-07 00:00:02.000', 93);
        """,
        settings,
    )
    _wait_for_iceberg_count(2, settings=settings, timeout_seconds=timeout_seconds)
    pre_fault_checkpoint = _wait_for_completed_checkpoint(
        job_id, settings=settings, timeout_seconds=timeout_seconds
    )
    log(
        "sink-commit-fault: baseline committed before injected fault at checkpoint "
        f"{pre_fault_checkpoint['id']}"
    )

    _mysql(
        """
        INSERT INTO orders
          (order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed)
        VALUES
          (9303, 'phase-2-1-commit-trigger', 9303, 1003,
           'paid', 42300, '2026-01-07 00:00:03.000', 93),
          (9304, 'phase-2-1-commit-trailing', 9304, 1004,
           'packed', 56400, '2026-01-07 00:00:04.000', 93);
        """,
        settings,
    )
    _wait_until(
        lambda: _container_path_exists("taskmanager", marker_path, settings=settings),
        description="sink commit fault marker",
        timeout_seconds=timeout_seconds,
    )
    marker_text = _container_file_text("taskmanager", marker_path, settings=settings)
    log("sink-commit-fault: checkpoint-complete exception observed; waiting for recovery")
    _wait_for_job(job_id, settings=settings, timeout_seconds=timeout_seconds)
    _wait_for_iceberg_count(4, settings=settings, timeout_seconds=timeout_seconds)

    _mysql(
        """
        UPDATE orders
           SET event_id = 10301,
               status = 'paid',
               amount_cents = 56500,
               updated_at = '2026-01-07 00:01:01.000'
         WHERE order_id = 9301;

        UPDATE orders
           SET event_id = 10302,
               status = 'delivered',
               amount_cents = 70600,
               updated_at = '2026-01-07 00:01:02.000'
         WHERE order_id = 9302;

        """,
        settings,
    )
    _wait_for_iceberg_statuses(
        {"9301": "paid", "9302": "delivered", "9303": "paid", "9304": "packed"},
        settings=settings,
        timeout_seconds=timeout_seconds,
    )
    _mysql("DELETE FROM orders WHERE order_id = 9301;", settings)

    reconciliation = _wait_for_reconciliation(
        settings=settings,
        expected_current_event_ids=[9303, 9304, 10302],
        expected_changelog_event_ids=[9301, 9302, 9303, 9304, 10301, 10302],
        expected_changelog_rows=9,
        timeout_seconds=timeout_seconds,
    )
    return {
        "failure_class": "sink-commit-fault",
        "passed": True,
        "trigger": "test-only checkpoint-complete fault after event_id=9303",
        "job_id": job_id,
        "fault_injection": {
            "mechanism": (
                "The job flag --checkpoint-complete-fault-event-id inserts a test-only "
                "operator immediately upstream of the Iceberg sinks. After the trigger "
                "event is observed, the operator throws once from "
                "CheckpointListener.notifyCheckpointComplete; Iceberg sink commits are "
                "driven by the same checkpoint-complete phase."
            ),
            "trigger_event_id": 9303,
            "marker_path": marker_path,
            "marker_text": marker_text,
        },
        "recovery": {
            "mode": "automatic task restart after checkpoint-complete callback failure",
            "pre_fault_checkpoint": pre_fault_checkpoint,
        },
        **reconciliation,
    }


def _reset_for_scenario(settings: Settings, *, log: Callable[[str], None]) -> None:
    log("resetting running jobs, MySQL source table, and Iceberg target tables")
    _cancel_existing_jobs(settings, log=log, ignore_errors=False)
    _compose_up_services(["taskmanager"], settings=settings)
    _wait_for_flink_cluster(settings=settings, timeout_seconds=120)
    _mysql("TRUNCATE TABLE orders;", settings)
    reset_iceberg_tables(settings=settings)


def _wait_for_reconciliation(
    *,
    settings: Settings,
    expected_current_event_ids: Sequence[int],
    expected_changelog_event_ids: Sequence[int],
    expected_changelog_rows: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    def ready() -> bool:
        snapshot = _reconciliation_snapshot(
            settings=settings,
            expected_current_event_ids=expected_current_event_ids,
            expected_changelog_event_ids=expected_changelog_event_ids,
            expected_changelog_rows=expected_changelog_rows,
        )
        return snapshot["snapshot_diff_count"] == 0 and bool(
            snapshot["event_id_audit"]["consistent"]
        )

    _wait_until(
        ready,
        description="source snapshot, Iceberg snapshot, and event-id audit to match",
        timeout_seconds=timeout_seconds,
        interval=3.0,
    )
    return _reconciliation_snapshot(
        settings=settings,
        expected_current_event_ids=expected_current_event_ids,
        expected_changelog_event_ids=expected_changelog_event_ids,
        expected_changelog_rows=expected_changelog_rows,
    )


def _reconciliation_snapshot(
    *,
    settings: Settings,
    expected_current_event_ids: Sequence[int],
    expected_changelog_event_ids: Sequence[int],
    expected_changelog_rows: int,
) -> dict[str, Any]:
    mysql_rows = _mysql_rows(settings)
    iceberg_rows = _iceberg_rows(settings)
    diff = _diff_rows(mysql_rows, iceberg_rows)
    changelog_event_ids = _changelog_event_ids(settings)
    event_id_audit = _event_id_audit(
        mysql_rows=mysql_rows,
        iceberg_rows=iceberg_rows,
        changelog_event_ids=changelog_event_ids,
        expected_current_event_ids=expected_current_event_ids,
        expected_changelog_event_ids=expected_changelog_event_ids,
        expected_changelog_rows=expected_changelog_rows,
    )
    return {
        "snapshot_diff_count": _diff_count(diff),
        "source_iceberg_diff_count": _diff_count(diff),
        "snapshot_diff": diff,
        "source_snapshot_row_count": len(mysql_rows),
        "iceberg_snapshot_row_count": len(iceberg_rows),
        "mysql_rows": mysql_rows,
        "iceberg_rows": iceberg_rows,
        "event_id_audit": event_id_audit,
    }


def _event_id_audit(
    *,
    mysql_rows: Sequence[Row],
    iceberg_rows: Sequence[Row],
    changelog_event_ids: Sequence[int],
    expected_current_event_ids: Sequence[int],
    expected_changelog_event_ids: Sequence[int],
    expected_changelog_rows: int,
) -> dict[str, Any]:
    source_current = _sorted_unique_ints([row["event_id"] for row in mysql_rows])
    iceberg_current = _sorted_unique_ints([row["event_id"] for row in iceberg_rows])
    expected_current = sorted(set(expected_current_event_ids))
    changelog_distinct = sorted(set(changelog_event_ids))
    expected_changelog = sorted(set(expected_changelog_event_ids))
    changelog_row_count = len(changelog_event_ids)

    current_sets_match = source_current == iceberg_current == expected_current
    changelog_set_match = changelog_distinct == expected_changelog
    changelog_row_count_match = changelog_row_count == expected_changelog_rows
    return {
        "source_current_event_ids": source_current,
        "iceberg_current_event_ids": iceberg_current,
        "expected_current_event_ids": expected_current,
        "current_sets_match": current_sets_match,
        "iceberg_changelog_distinct_event_ids": changelog_distinct,
        "expected_changelog_event_ids": expected_changelog,
        "changelog_set_match": changelog_set_match,
        "iceberg_changelog_row_count": changelog_row_count,
        "expected_changelog_row_count": expected_changelog_rows,
        "changelog_row_count_match": changelog_row_count_match,
        "consistent": current_sets_match and changelog_set_match and changelog_row_count_match,
    }


def _sorted_unique_ints(values: Sequence[str]) -> list[int]:
    return sorted({int(value) for value in values})


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


def _restart_compose_service(service: str, *, settings: Settings) -> None:
    proc = subprocess.run(
        [*compose_base(settings), "restart", service],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())


def _compose_up_services(services: Sequence[str], *, settings: Settings) -> None:
    proc = subprocess.run(
        [*compose_base(settings), "--profile", "core", "up", "-d", *services],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise FlinkCommandError(proc.stderr.strip() or proc.stdout.strip())


def _wait_for_flink_cluster(*, settings: Settings, timeout_seconds: int) -> None:
    _wait_until(
        lambda: _flink_cluster_ready(settings),
        description="Flink JobManager REST API and a registered TaskManager",
        timeout_seconds=timeout_seconds,
        interval=2.0,
    )


def _flink_cluster_ready(settings: Settings) -> bool:
    payload = flink_rest_json("/taskmanagers", settings=settings)
    taskmanagers = payload.get("taskmanagers")
    return isinstance(taskmanagers, list) and len(taskmanagers) > 0


def _cancel_existing_jobs(
    settings: Settings,
    *,
    log: Callable[[str], None],
    ignore_errors: bool,
) -> None:
    try:
        job_ids = running_job_ids(settings=settings)
        for job_id in job_ids:
            log(f"cancelling running Flink job {job_id}")
            cancel_job(job_id, settings=settings)
    except Exception:
        if ignore_errors:
            return
        raise


def _wait_for_completed_checkpoint(
    job_id: str,
    *,
    settings: Settings,
    timeout_seconds: int,
) -> dict[str, Any]:
    checkpoint: dict[str, Any] | None = None

    def ready() -> bool:
        nonlocal checkpoint
        checkpoint = latest_completed_checkpoint(job_id, settings=settings)
        return checkpoint is not None

    _wait_until(
        ready,
        description=f"completed checkpoint for job {job_id}",
        timeout_seconds=timeout_seconds,
    )
    if checkpoint is None:
        raise TimeoutError(f"timed out waiting for completed checkpoint for job {job_id}")
    return checkpoint


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
        interval=3.0,
    )


def _wait_for_iceberg_statuses(
    expected: dict[str, str], *, settings: Settings, timeout_seconds: int
) -> None:
    def ready() -> bool:
        statuses = {row["order_id"]: row["status"] for row in _iceberg_rows(settings)}
        return statuses == expected

    _wait_until(
        ready,
        description=f"{CURRENT_TABLE} statuses {expected}",
        timeout_seconds=timeout_seconds,
        interval=3.0,
    )


def _wait_for_retained_checkpoint(
    job_id: str,
    *,
    settings: Settings,
    timeout_seconds: int,
) -> dict[str, Any]:
    checkpoint: dict[str, Any] | None = None

    def ready() -> bool:
        nonlocal checkpoint
        checkpoint = _latest_retained_checkpoint(job_id, settings=settings)
        return checkpoint is not None

    _wait_until(
        ready,
        description=f"retained checkpoint for job {job_id}",
        timeout_seconds=timeout_seconds,
    )
    if checkpoint is None:
        raise TimeoutError(f"timed out waiting for retained checkpoint for job {job_id}")
    return checkpoint


def _latest_retained_checkpoint(job_id: str, *, settings: Settings) -> dict[str, Any] | None:
    checkpoint_root = f"/opt/flink/checkpoints/{job_id}"
    proc = subprocess.run(
        [
            *compose_base(settings),
            "exec",
            "-T",
            "jobmanager",
            "find",
            checkpoint_root,
            "-maxdepth",
            "1",
            "-type",
            "d",
            "-name",
            "chk-*",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None

    latest_id = -1
    latest_path = ""
    for line in proc.stdout.splitlines():
        path = line.strip()
        name = path.rsplit("/", 1)[-1]
        if not name.startswith("chk-") or not name[4:].isdigit():
            continue
        checkpoint_id = int(name[4:])
        if checkpoint_id > latest_id:
            latest_id = checkpoint_id
            latest_path = path

    if latest_id < 0:
        return None
    return {
        "id": latest_id,
        "status": "RETAINED",
        "external_path": f"file://{latest_path}",
    }


def _normalize_state_path(path: str) -> str:
    if path.startswith("file:/") and not path.startswith("file:///"):
        return "file://" + path.removeprefix("file:")
    return path


def _mysql(script: str, settings: Settings) -> None:
    proc = run_mysql_script(script, settings=settings, capture=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())


def _mysql_rows(settings: Settings) -> list[Row]:
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
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return [_row_dict(line.split("\t")) for line in proc.stdout.splitlines() if line.strip()]


def _iceberg_rows(settings: Settings) -> list[Row]:
    output = _iceberg_query(
        f"""
        SELECT order_id, business_key, event_id, customer_id, status, amount_cents, updated_at, seed
          FROM {CURRENT_TABLE}
         ORDER BY order_id
        """,
        settings,
    )
    return [_row_dict(line.split("\t")) for line in output.splitlines() if line.strip()]


def _changelog_event_ids(settings: Settings) -> list[int]:
    output = _iceberg_query(
        f"""
        SELECT event_id
          FROM {CHANGELOG_TABLE}
         ORDER BY event_id
        """,
        settings,
    )
    return [int(line.split("\t")[0]) for line in output.splitlines() if line.strip()]


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


def _row_dict(fields: list[str]) -> Row:
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


def _diff_rows(mysql_rows: list[Row], iceberg_rows: list[Row]) -> dict[str, list[Row]]:
    mysql_set = {json.dumps(row, sort_keys=True) for row in mysql_rows}
    iceberg_set = {json.dumps(row, sort_keys=True) for row in iceberg_rows}
    return {
        "missing_in_iceberg": [
            cast(Row, json.loads(row)) for row in sorted(mysql_set - iceberg_set)
        ],
        "unexpected_in_iceberg": [
            cast(Row, json.loads(row)) for row in sorted(iceberg_set - mysql_set)
        ],
    }


def _diff_count(diff: dict[str, list[Row]]) -> int:
    return len(diff["missing_in_iceberg"]) + len(diff["unexpected_in_iceberg"])


def _container_path_exists(container: str, path: str, *, settings: Settings) -> bool:
    proc = subprocess.run(
        [*compose_base(settings), "exec", "-T", container, "test", "-f", path],
        check=False,
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0


def _container_file_text(container: str, path: str, *, settings: Settings) -> str:
    proc = subprocess.run(
        [*compose_base(settings), "exec", "-T", container, "cat", path],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return proc.stdout.strip()


def _wait_until(
    predicate: Callable[[], bool],
    *,
    description: str,
    timeout_seconds: int,
    interval: float = 2.0,
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


def _write_log(lines: Sequence[str]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 2.1 failure reconciliation.")
    parser.add_argument("--failure", default="all")
    parser.add_argument("--timeout-seconds", type=int, default=360)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        failure_classes = parse_failure_classes(args.failure)
        payload = run_eo_verify(
            failure_classes=failure_classes,
            timeout_seconds=args.timeout_seconds,
        )
    except Exception as exc:
        print(f"eo-verify failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
