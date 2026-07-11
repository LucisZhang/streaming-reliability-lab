# Local Mac Heavy Reproduction Summary

Run ID: `20260711T034018Z-local-mac`
Branch: `evidence/u6-local-mac-20260711T034018Z`
Baseline commit: `05738dd80038ada6862dcdb8fee1ffc8f8c1e018`

## Environment

- Host: macOS on Apple Silicon, 16 GiB RAM, 10 logical CPUs.
- Docker Desktop daemon was available and responsive.
- Docker Desktop VM reported 10 CPUs and about 7.65 GiB memory.
- Free disk before heavy path: about 69 GiB.

## Commands

1. `make doctor`
2. `make preflight-heavy`
3. `make up-core`
4. `make eo-verify ARGS="--failure task-crash"`
5. `make eo-verify ARGS="--failure all"`
6. `make down`

## Result

The local Mac completed the Phase 2.1 heavy exactly-once reproduction across all five induced failure classes:

- `task-crash`
- `checkpoint-restore`
- `jobmanager-restart`
- `savepoint-restore`
- `sink-commit-fault`

Final summary from `eo_reconciliation-all.json`:

- `passed`: `true`
- `all_snapshot_diffs_zero`: `true`
- `all_event_id_audits_consistent`: `true`
- `errors`: `[]`

## Evidence

Primary files:

- `eo-verify-all.log`
- `eo_reconciliation-all.json`
- `eo-verify-task-crash.log`
- `eo_reconciliation-task-crash.json`
- `compose-ps-after-all.txt`
- `compose-logs-tail-after-all.txt`
- `docker-system-df-final.txt`
- `disk-final.txt`
- `memory-pressure-final.txt`

The harness also updated:

- `showcase/logs/phase-2.1-eo-verify.log`
- `showcase/results/eo_reconciliation.json`

## Cleanup

After evidence capture, `make down -v` removed the p1 containers, network, and volumes. The p1 Docker images and build cache were also removed explicitly. Final Docker state reported zero images, containers, local volumes, and build cache.
