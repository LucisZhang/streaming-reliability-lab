#!/usr/bin/env python3
"""Fail fast before starting the heavy Docker stack on undersized machines."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


DEFAULT_MIN_FREE_GIB = 25
DEFAULT_DOCKER_TIMEOUT_SECONDS = 10


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        print(f"{name} must be an integer; got {raw!r}", file=sys.stderr)
        raise SystemExit(2)
    if value < 0:
        print(f"{name} must be non-negative; got {value}", file=sys.stderr)
        raise SystemExit(2)
    return value


def gib(bytes_value: int) -> float:
    return bytes_value / (1024**3)


def check_disk(root: Path) -> bool:
    minimum = env_int("P1_HEAVY_MIN_FREE_GIB", DEFAULT_MIN_FREE_GIB)
    usage = shutil.disk_usage(root)
    free_gib = gib(usage.free)
    print(
        f"disk preflight: {free_gib:.1f} GiB free at {root} "
        f"(minimum {minimum} GiB for heavy Docker reproduction)",
        flush=True,
    )
    if free_gib < minimum:
        print(
            "Refusing to start the heavy stack on this machine. "
            "Use `make local-verify` here, or run the full reproduction on a larger workstation.",
            file=sys.stderr,
        )
        print(
            "Override only with an explicit, informed choice: "
            "P1_HEAVY_MIN_FREE_GIB=<lower-number> make <heavy-target>",
            file=sys.stderr,
        )
        return False
    return True


def check_docker() -> bool:
    if os.environ.get("P1_SKIP_DOCKER_CHECK") == "1":
        print("docker preflight: skipped because P1_SKIP_DOCKER_CHECK=1")
        return True

    timeout = env_int("P1_DOCKER_CHECK_TIMEOUT_SECONDS", DEFAULT_DOCKER_TIMEOUT_SECONDS)
    try:
        proc = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError:
        print("docker preflight: docker CLI not found", file=sys.stderr)
        return False
    except subprocess.TimeoutExpired:
        print(
            f"docker preflight: daemon did not respond within {timeout}s; "
            "not starting the heavy stack",
            file=sys.stderr,
        )
        return False

    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        print(f"docker preflight failed: {detail}", file=sys.stderr)
        return False

    print(f"docker preflight: daemon responsive (server {proc.stdout.strip()})")
    return True


def main() -> int:
    root = Path(os.environ.get("P1_REPO_ROOT", Path.cwd())).resolve()
    if not check_disk(root):
        return 2
    return 0 if check_docker() else 2


if __name__ == "__main__":
    raise SystemExit(main())
