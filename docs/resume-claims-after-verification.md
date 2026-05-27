# Resume Claims After Verification

This file is intentionally gated. Resume-facing claims are added only after the phase that proves them has passed and produced auditable JSON under `showcase/results/`.

| Claim | Gate |
| --- | --- |
| Verified exactly-once final-state reconciliation for `MySQL CDC -> Flink -> Iceberg` across task crash, retained-checkpoint restore, JobManager restart, savepoint restore, and deterministic checkpoint-complete sink-commit fault. | Phase 2.1 passed: `make eo-verify ARGS="--failure all"` produced `showcase/results/eo_reconciliation.json` run `20260527T151754Z-ef73a5a5` with zero snapshot diff across all five classes. |
