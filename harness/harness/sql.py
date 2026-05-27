from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Sequence

from harness.config import Settings, load_settings


def _compose_base(settings: Settings) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        str(settings.env_file),
        "-f",
        str(settings.compose_file),
    ]


def run_mysql_script(
    script: str,
    *,
    settings: Settings | None = None,
    database: str | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    active = settings or load_settings()
    db = database or active.mysql_database
    cmd = [
        *_compose_base(active),
        "exec",
        "-T",
        active.mysql_container,
        "mysql",
        f"-u{active.mysql_user}",
        f"-p{active.mysql_password}",
        "--batch",
        "--raw",
        "--skip-column-names",
        db,
    ]
    return subprocess.run(
        cmd,
        input=script,
        check=False,
        stderr=subprocess.PIPE if capture else None,
        stdout=subprocess.PIPE if capture else None,
        text=True,
    )


def run_mysql_query(query: str, *, settings: Settings | None = None) -> int:
    proc = run_mysql_script(query, settings=settings, capture=True)
    if proc.stdout:
        print(proc.stdout.rstrip())
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr.rstrip(), file=sys.stderr)
        return proc.returncode
    return 0


def mysql_main(args: argparse.Namespace) -> int:
    if not args.query:
        print('mysql SQL wrapper requires --query or make sql-mysql Q="..."', file=sys.stderr)
        return 2
    return run_mysql_query(args.query)


def iceberg_main(args: argparse.Namespace) -> int:
    if args.query is None:
        print(
            "Iceberg data reader: Flink SQL batch via the shared JDBC catalog and MinIO "
            "warehouse. This path is equality-delete-correct and intentionally does not use "
            "pyiceberg for data materialization."
        )
        return 0
    settings = load_settings()
    print(
        "Flink SQL batch reader is fixed for this target, but Phase 1.1 has no Iceberg table "
        "or connector job yet. Phase 1.2 wires query execution through the Flink SQL client.",
        file=sys.stderr,
    )
    print(
        f"Catalog={settings.iceberg_catalog_name} JDBC_DB={settings.iceberg_catalog_database} "
        f"warehouse={settings.iceberg_warehouse}",
        file=sys.stderr,
    )
    return 2


def iceberg_meta_main(args: argparse.Namespace) -> int:
    if args.table is None:
        print(
            "Iceberg metadata reader: pyiceberg metadata-only path. Use this for snapshots, "
            "manifests, file counts, and small-file metrics; never for final-state data "
            "reconciliation on v2 upsert tables."
        )
        return 0
    try:
        import pyiceberg  # noqa: F401
    except ImportError:
        print(
            "pyiceberg is not installed in the active Python environment. Install "
            "harness/requirements.txt before metadata inspection.",
            file=sys.stderr,
        )
        return 2
    print(
        "Metadata inspection is scaffolded in Phase 1.1; table-specific pyiceberg catalog "
        "loading is added when Iceberg tables are introduced.",
        file=sys.stderr,
    )
    return 2


def starrocks_main(args: argparse.Namespace) -> int:
    print("StarRocks SQL wrapper is intentionally stubbed until M3.", file=sys.stderr)
    if args.query:
        print(f"Deferred query: {args.query}", file=sys.stderr)
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SQL wrappers for the reliability lab.")
    subparsers = parser.add_subparsers(dest="engine", required=True)

    mysql = subparsers.add_parser("mysql", description="Run SQL against the MySQL source DB.")
    mysql.add_argument("--query", "-q")
    mysql.set_defaults(func=mysql_main)

    iceberg = subparsers.add_parser(
        "iceberg",
        description=(
            "Run data SQL through Flink SQL batch. This is the equality-delete-correct "
            "Iceberg reader and must never be replaced with pyiceberg."
        ),
    )
    iceberg.add_argument("--query", "-q")
    iceberg.set_defaults(func=iceberg_main)

    iceberg_meta = subparsers.add_parser(
        "iceberg-meta",
        description=(
            "Read Iceberg metadata with pyiceberg only; this command must not materialize "
            "table data for reconciliation."
        ),
    )
    iceberg_meta.add_argument("--table")
    iceberg_meta.add_argument("--warehouse")
    iceberg_meta.set_defaults(func=iceberg_meta_main)

    starrocks = subparsers.add_parser("starrocks", description="StarRocks wrapper stub for M3+.")
    starrocks.add_argument("--query", "-q")
    starrocks.set_defaults(func=starrocks_main)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
