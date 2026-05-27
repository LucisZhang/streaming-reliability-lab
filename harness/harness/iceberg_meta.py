from __future__ import annotations

import json
import subprocess
from typing import Any

from harness.config import Settings, load_settings
from harness.flink import compose_base


def inspect_iceberg_metadata(table: str, *, settings: Settings | None = None) -> dict[str, Any]:
    active = settings or load_settings()
    try:
        return _inspect_with_pyiceberg(table, active)
    except Exception as exc:
        metadata = _inspect_from_metadata_json(table, active)
        metadata["metadata_reader"] = "metadata-json-fallback"
        metadata["pyiceberg_error"] = f"{type(exc).__name__}: {exc}"
        return metadata


def _inspect_with_pyiceberg(table: str, settings: Settings) -> dict[str, Any]:
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        settings.iceberg_catalog_name,
        **{
            "type": "sql",
            "uri": (
                f"mysql+pymysql://{settings.mysql_user}:{settings.mysql_password}"
                f"@{settings.mysql_host}:{settings.mysql_port}/{settings.iceberg_catalog_database}"
            ),
            "warehouse": settings.iceberg_warehouse,
            "s3.endpoint": settings.minio_endpoint,
            "s3.access-key-id": settings.minio_root_user,
            "s3.secret-access-key": settings.minio_root_password,
            "s3.path-style-access": "true",
            "client.region": settings.minio_region,
        },
    )
    iceberg_table = catalog.load_table(table)
    snapshots = list(iceberg_table.snapshots())
    current = iceberg_table.current_snapshot()
    summary = dict(current.summary) if current is not None and current.summary is not None else {}
    return {
        "table": table,
        "metadata_reader": "pyiceberg",
        "snapshot_count": len(snapshots),
        "current_snapshot_id": current.snapshot_id if current is not None else None,
        "summary": summary,
        "delete_files": _summary_int(summary, "total-delete-files")
        + _summary_int(summary, "added-delete-files"),
        "equality_delete_files": _summary_int(summary, "total-equality-deletes")
        + _summary_int(summary, "added-equality-deletes"),
    }


def _inspect_from_metadata_json(table: str, settings: Settings) -> dict[str, Any]:
    from harness.sql import run_mysql_script

    namespace, table_name = _split_table(table, settings)
    query = (
        "SELECT metadata_location FROM iceberg_tables "
        f"WHERE catalog_name = '{settings.iceberg_catalog_name}' "
        f"AND table_namespace = '{namespace}' "
        f"AND table_name = '{table_name}'"
    )
    proc = run_mysql_script(
        query, settings=settings, database=settings.iceberg_catalog_database, capture=True
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip())
    metadata_location = proc.stdout.strip().splitlines()[-1]
    metadata = json.loads(_read_minio_text(metadata_location, settings))
    snapshots = metadata.get("snapshots", [])
    current_snapshot_id = metadata.get("current-snapshot-id")
    current = next(
        (snapshot for snapshot in snapshots if snapshot.get("snapshot-id") == current_snapshot_id),
        snapshots[-1] if snapshots else {},
    )
    summary = current.get("summary", {})
    return {
        "table": f"{namespace}.{table_name}",
        "metadata_location": metadata_location,
        "snapshot_count": len(snapshots),
        "current_snapshot_id": current_snapshot_id,
        "summary": summary,
        "delete_files": _summary_int(summary, "total-delete-files")
        + _summary_int(summary, "added-delete-files"),
        "equality_delete_files": _summary_int(summary, "total-equality-deletes")
        + _summary_int(summary, "added-equality-deletes"),
    }


def _read_minio_text(s3_uri: str, settings: Settings) -> str:
    bucket, key = _split_s3_uri(s3_uri)
    object_uri = f"local/{bucket}/{key}"
    script = (
        "mc alias set local "
        f"{_sh_quote(settings.minio_docker_endpoint)} "
        f"{_sh_quote(settings.minio_root_user)} "
        f"{_sh_quote(settings.minio_root_password)} >/dev/null; "
        f"mc cat {_sh_quote(object_uri)}"
    )
    proc = subprocess.run(
        [
            *compose_base(settings),
            "run",
            "--rm",
            "--entrypoint",
            "/bin/sh",
            "minio-init",
            "-ec",
            script,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip())
    return proc.stdout


def _split_table(table: str, settings: Settings) -> tuple[str, str]:
    parts = table.split(".")
    if len(parts) == 1:
        return settings.mysql_database, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 3:
        return parts[1], parts[2]
    raise ValueError(f"unsupported Iceberg table identifier: {table}")


def _split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"expected s3:// URI, got {uri}")
    path = uri.removeprefix("s3://")
    bucket, key = path.split("/", 1)
    return bucket, key


def _sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _summary_int(summary: dict[str, Any], key: str) -> int:
    value = summary.get(key, 0)
    if value is None:
        return 0
    return int(value)
