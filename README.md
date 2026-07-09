# Reliability Lab — MySQL CDC → Flink → Iceberg

[![ci](https://github.com/LucisZhang/p1-reliability-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/LucisZhang/p1-reliability-lab/actions/workflows/ci.yml)

A single-node **reliability lab** for a real-time data pipeline:
`MySQL CDC → Flink 1.20 → Apache Iceberg v2 (upsert)`.

The point is not wiring components together — it is **reproducible evidence of
correctness under failure**. A Python harness induces real failures in the running
pipeline and proves exactly-once delivery by reconciling the final MySQL source
snapshot against the final Iceberg snapshot, row by row, through a correctness-safe
reader. Every claim is backed by a committed, machine-checkable JSON artifact with
full provenance (`run_id`, `git_sha`, exact command, logs).

## Verified claims

Claims are **gated**: a claim is added to
[`docs/resume-claims-after-verification.md`](docs/resume-claims-after-verification.md)
only after the phase that proves it has passed and produced auditable JSON under
[`showcase/results/`](showcase/results/).

| Claim | Evidence |
| --- | --- |
| Exactly-once final-state reconciliation across **five induced failure classes** — task crash, retained-checkpoint restore, JobManager restart, savepoint restore, and a deterministic checkpoint-complete sink-commit fault — with **zero snapshot diff** in every class. | [`showcase/results/eo_reconciliation.json`](showcase/results/eo_reconciliation.json) (run `20260527T151754Z-ef73a5a5`), incident log in [`RUNBOOK.md`](RUNBOOK.md) |
| CDC correctness smoke: source-vs-Iceberg final-state parity including updates and deletes, changelog audit counts, and equality-delete file metadata evidence. | [`showcase/results/phase-1.2-cdc-smoke.json`](showcase/results/phase-1.2-cdc-smoke.json) |
| Iceberg small-file maintenance: `rewrite_data_files` + manifest rewrite measurably reduced data-file and manifest counts, raised median file size, and lowered `planFiles()` planning latency. | [`showcase/results/iceberg_small_file_rewrite.json`](showcase/results/iceberg_small_file_rewrite.json), chart in [`showcase/media/`](showcase/media/) |
| Checkpoint behavior under load: real Prometheus-reporter metrics show checkpoint duration/alignment rising under a deterministic input spike, backpressure appearing, Iceberg commit lag growing and **recovering to zero**. | [`showcase/results/checkpoint_metrics.json`](showcase/results/checkpoint_metrics.json), chart in [`showcase/media/`](showcase/media/) |

## How the evidence works

- **Correctness-safe reading.** Iceberg v2 upsert tables contain equality deletes;
  pyiceberg is not a correctness reader for them. The lab splits the paths:
  `make sql-iceberg` reads data through **Flink SQL batch**; `make sql-iceberg-meta`
  uses pyiceberg for **metadata only** (files, manifests, snapshots).
- **Results contract.** Every artifact must carry `run_id`, `git_sha`, `started_at`,
  `finished_at`, `stack_versions`, `command`, and `logs`
  ([contract](showcase/results/README.md)); the dashboard sync step validates this
  before an artifact is publishable.
- **Incident log.** [`RUNBOOK.md`](RUNBOOK.md) records each induced failure as an
  incident: trigger, observed symptom, detection/recovery commands, validation,
  artifact links.

## Evidence dashboard (deployable slice)

The heavy pipeline is not a public live demo. The deployable slice is a **static
dashboard** ([`dashboard/`](dashboard/)) built over the exported result JSON — it
renders the artifacts and their provenance and calls no backend.

```bash
make dashboard-build     # validates results contract, then vite build
make dashboard-preview   # serve the built dashboard locally
```

## Quickstart

Pinned toolchain: Java 11 (Temurin), Maven 3.9, Python 3.11, Node 20
(see [`docs/version-matrix.md`](docs/version-matrix.md) and `.tool-versions`).

```bash
make doctor                                   # toolchain / env preflight
make up-core                                  # MySQL + Flink JM/TM + MinIO + Iceberg JDBC catalog
make gen ARGS="--events 10000 --seed 1"       # deterministic source generator
make sql-mysql Q="SELECT COUNT(*) FROM orders"
make eo-verify ARGS="--failure all"           # induce all five failure classes and reconcile
```

Lightweight checks (no Docker): `make test` (harness unit tests), `make lint`
(ruff, black, mypy, Maven verify), `make dashboard-build`.

## CI

GitHub Actions runs the light paths on every push: Python lint + unit tests,
the Flink job Maven build, and the dashboard build with results-contract
validation. The heavy Docker integration (`make eo-verify`, `make test-cdc`)
is intentionally **not** in CI — it runs manually on a single node and its
outputs are committed as auditable artifacts.

## Scope and status

- Verified through **Phase 2.3** (five-failure-class EO reconciliation,
  Iceberg small-file maintenance, checkpoint metrics under load).
- **StarRocks (M3+) has not been started** — the `olap` compose profile,
  serving-table imports, and the compaction benchmark are reserved future work.
- Single-node Docker Compose only; no cloud, no multi-node, no GPU.
