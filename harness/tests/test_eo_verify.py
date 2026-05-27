from __future__ import annotations

import pytest

from harness.eo_verify import parse_failure_classes


def test_parse_failure_classes_expands_all() -> None:
    assert parse_failure_classes("all") == [
        "task-crash",
        "checkpoint-restore",
        "jobmanager-restart",
        "savepoint-restore",
        "sink-commit-fault",
    ]


def test_parse_failure_classes_accepts_comma_list() -> None:
    assert parse_failure_classes("task-crash, checkpoint-restore") == [
        "task-crash",
        "checkpoint-restore",
    ]


def test_parse_failure_classes_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="unsupported failure"):
        parse_failure_classes("task-crash,minio-restart")
