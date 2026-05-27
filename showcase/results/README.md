# Results JSON Contract

Every exported result JSON must include:

- `run_id`
- `git_sha`
- `started_at`
- `finished_at`
- `stack_versions`
- `command`
- `logs`

Additional phase-specific fields should be stable, typed, and documented in the phase that introduces them.

`scripts/sync-results-to-dashboard.sh` validates these fields before copying result artifacts
into `dashboard/public/results/`. The sync also writes `dashboard/public/results/index.json`,
which is the static dashboard manifest. The dashboard reads that manifest and the copied JSON
only; it does not call MySQL, Flink, Iceberg, MinIO, StarRocks, or any backend service.

For deployed dashboard builds, the default results location is Vite's static `/results/` path.
Set `BASE_RESULTS_URL` at build time to point the dashboard at a separately hosted immutable
results directory with the same files and `index.json` manifest.

## Synced Results Manifest

`dashboard/public/results/index.json` is generated, not hand-authored:

- `generated_at`: ISO-8601 sync timestamp.
- `source`: source glob used by the sync step.
- `artifacts`: one entry per validated JSON artifact.
- Per artifact: `filename`, `phase`, `run_id`, `git_sha`, `started_at`, `finished_at`,
  `command`, and `logs`.

## Phase 1.2 CDC Smoke

`phase-1.2-cdc-smoke.json` adds:

- `reader`: data reader used for reconciliation; must be `Flink SQL batch`.
- `job_id` and `savepoint`: Flink job and controlled restart provenance.
- `source_iceberg_diff_count`: final source snapshot vs `orders_current` diff count.
- `deleted_key_absent` and `updated_key_current`: delete/update smoke assertions.
- `orders_changelog_change_count`: audit-table CDC row count for the known sequence.
- `mysql_rows`, `iceberg_rows`, and `final_rows`: normalized final-state snapshots.
- `delete_file_smoke`: Iceberg metadata-only evidence for delete and equality-delete files.

## Phase 2.1 Failure Reconciliation

`eo_reconciliation.json` adds:

- `reader`: data reader used for final-state reconciliation; must be `Flink SQL batch`.
- `claim_boundary`: the verified pipeline segment, `MySQL CDC -> Flink -> Iceberg`.
- `results`: one object per induced failure class.
- Per result, `failure_class`, `trigger`, `recovery`, job/checkpoint IDs, `snapshot_diff_count`,
  `snapshot_diff`, source/Iceberg snapshot row counts, normalized `mysql_rows` and
  `iceberg_rows`, and `event_id_audit`.
- `event_id_audit`: final current-table event-id set comparison plus changelog distinct-event-id
  and row-count checks. This is an audit supplement; snapshot reconciliation is the proof.
- `summary`: aggregate pass/fail flags for snapshot diffs and event-id audits.
- Phase 2.1 extends the matrix to five failure classes: `task-crash`,
  `checkpoint-restore`, `jobmanager-restart`, `savepoint-restore`, and
  `sink-commit-fault`.
- `sink-commit-fault.fault_injection`: documents the test-only
  `--checkpoint-complete-fault-event-id` mechanism, marker path, marker text, and trigger event.
