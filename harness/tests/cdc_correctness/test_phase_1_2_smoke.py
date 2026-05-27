from __future__ import annotations

import os

import pytest

from harness.cdc_correctness import run_smoke


@pytest.mark.skipif(
    os.environ.get("CDC_INTEGRATION") != "1",
    reason="set CDC_INTEGRATION=1 to run the Docker/Flink CDC smoke",
)
def test_phase_1_2_cdc_smoke() -> None:
    payload = run_smoke()
    assert payload["source_iceberg_diff_count"] == 0
    assert payload["deleted_key_absent"] is True
    assert payload["updated_key_current"] is True
    assert payload["orders_changelog_change_count"] >= 7
