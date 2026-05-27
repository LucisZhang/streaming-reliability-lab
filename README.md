# Reliability Lab

This repo is being built as a local evidence lab for a MySQL CDC to Flink to Iceberg pipeline. The visitor-facing story starts with failure evidence, reconciliation artifacts, and reproducible command transcripts as phases are completed.

Phase 1.1 establishes the pinned toolchain, core Docker services, deterministic source generator, SQL wrapper identities, provenance helper, and runbook skeleton. The heavy pipeline is not presented as a public live demo; the deployable slice will be a static dashboard over exported result JSON.

## Phase 1.1 Quick Check

```bash
make doctor
make up-core
make ps
make gen ARGS="--events 10000 --seed 1"
make sql-mysql Q="SELECT COUNT(*) FROM orders"
make sql-iceberg ARGS="--help"
make sql-iceberg-meta ARGS="--help"
```

