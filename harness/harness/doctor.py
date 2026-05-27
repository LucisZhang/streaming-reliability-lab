from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from harness.config import REPO_ROOT

Executable = Path | str


@dataclass(frozen=True)
class ToolCheck:
    name: str
    executable: Executable
    command: list[str]
    parser: Callable[[str], tuple[int, ...] | None]
    expected: tuple[int, ...]
    expected_label: str
    env: dict[str, str] | None = None


def _which_or_path(candidates: list[str]) -> Executable | None:
    for candidate in candidates:
        path = Path(candidate)
        if path.is_file():
            return path
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def _first_version(text: str) -> tuple[int, ...] | None:
    match = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", text)
    if not match:
        return None
    parts = [int(part) for part in match.groups() if part is not None]
    return tuple(parts)


def _java_version(text: str) -> tuple[int, ...] | None:
    match = re.search(r'version "(\d+)(?:\.(\d+))?(?:\.(\d+))?', text)
    if not match:
        return None
    return tuple(int(part) for part in match.groups("0"))


def _node_version(text: str) -> tuple[int, ...] | None:
    match = re.search(r"v?(\d+)\.(\d+)(?:\.(\d+))?", text)
    if not match:
        return None
    return tuple(int(part) for part in match.groups("0"))


def _read_tool_versions() -> dict[str, str]:
    path = REPO_ROOT / ".tool-versions"
    found: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        name, version = stripped.split(maxsplit=1)
        found[name] = version
    return found


def _verify_pin_files() -> list[str]:
    errors: list[str] = []
    tool_versions = _read_tool_versions()
    expected = {
        "java": "temurin-11",
        "maven": "3.9",
        "python": "3.11",
        "nodejs": "20",
    }
    for name, version in expected.items():
        if tool_versions.get(name) != version:
            errors.append(f".tool-versions: expected {name} {version!r}")

    mise = (REPO_ROOT / "mise.toml").read_text(encoding="utf-8")
    for needle in ['java = "temurin-11"', 'maven = "3.9"', 'python = "3.11"', 'node = "20"']:
        if needle not in mise:
            errors.append(f"mise.toml: missing {needle}")
    return errors


def _run_check(check: ToolCheck) -> str:
    env = os.environ.copy()
    if check.env:
        env.update(check.env)
    proc = subprocess.run(
        check.command,
        check=False,
        env=env,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        text=True,
    )
    output = proc.stdout.strip()
    if proc.returncode != 0:
        raise RuntimeError(f"{check.name} command failed: {output}")
    parsed = check.parser(output)
    if parsed is None:
        raise RuntimeError(f"{check.name} version could not be parsed from: {output}")
    if parsed[: len(check.expected)] != check.expected:
        version_label = ".".join(str(part) for part in parsed)
        raise RuntimeError(
            f"{check.name} version mismatch: expected {check.expected_label}, got {version_label}"
        )
    first_line = output.splitlines()[0] if output else ""
    return f"{check.name}: {first_line} [{check.executable}]"


def _java_home(java_executable: Executable) -> str | None:
    path = Path(java_executable)
    if path.name == "java" and path.parent.name == "bin":
        return str(path.parent.parent)
    return None


def build_checks() -> list[ToolCheck]:
    java = _which_or_path(
        [
            os.environ.get("JAVA", ""),
            "/opt/homebrew/opt/openjdk@11/bin/java",
            "/usr/local/opt/openjdk@11/bin/java",
            "java",
        ]
    )
    python = _which_or_path(
        [
            os.environ.get("PYTHON", ""),
            "/opt/homebrew/bin/python3.11",
            "/usr/local/bin/python3.11",
            "python3.11",
            "python3",
        ]
    )
    node = _which_or_path(
        [
            os.environ.get("NODE", ""),
            "/opt/homebrew/opt/node@20/bin/node",
            "/usr/local/opt/node@20/bin/node",
            "node",
        ]
    )
    maven = _which_or_path(
        [
            os.environ.get("MVN", ""),
            "/opt/homebrew/opt/maven/bin/mvn",
            "/usr/local/opt/maven/bin/mvn",
            "mvn",
        ]
    )

    missing = [
        name
        for name, executable in {
            "java 11": java,
            "maven 3.9": maven,
            "python 3.11": python,
            "node 20": node,
        }.items()
        if executable is None
    ]
    if missing:
        raise RuntimeError("missing required tool(s): " + ", ".join(missing))

    java = cast(Executable, java)
    maven = cast(Executable, maven)
    python = cast(Executable, python)
    node = cast(Executable, node)
    java_home = _java_home(java)
    maven_env = {"JAVA_HOME": java_home} if java_home else None
    return [
        ToolCheck("java", java, [str(java), "-version"], _java_version, (11,), "11.x"),
        ToolCheck(
            "maven",
            maven,
            [str(maven), "-version"],
            _first_version,
            (3, 9),
            "3.9.x",
            maven_env,
        ),
        ToolCheck("python", python, [str(python), "--version"], _first_version, (3, 11), "3.11.x"),
        ToolCheck("node", node, [str(node), "--version"], _node_version, (20,), "20.x"),
    ]


def main() -> int:
    errors = _verify_pin_files()
    try:
        checks = build_checks()
        passed = [_run_check(check) for check in checks]
    except RuntimeError as exc:
        errors.append(str(exc))
        passed = []

    if errors:
        print("doctor failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        print(
            "Install/activate the pinned toolchain from mise.toml or provide JAVA/MVN/PYTHON/NODE.",
            file=sys.stderr,
        )
        return 1

    print("doctor ok")
    for line in passed:
        print(f"  - {line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
