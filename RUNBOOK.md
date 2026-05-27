# RUNBOOK

This runbook is the durable incident record for the reliability lab. Phase 1.1 creates the skeleton only; failure phases append real incidents, observed symptoms, commands, recovery steps, and links to artifacts immediately after verification.

## Operating Envelope

- Single-node Docker Compose.
- `core` profile: MySQL, Flink JobManager/TaskManager, MinIO, and the Iceberg JDBC catalog database schema in MySQL.
- `olap` profile is reserved for StarRocks in M3+.
- Use repo-root `make` targets only.

## Incident Log

Append one section per induced or observed failure.

### Incident Template

- Phase:
- Run ID:
- Trigger:
- User-visible symptom:
- Detection command:
- Recovery command:
- Validation:
- Artifacts:
- Notes for next run:

## Recovery Procedures

### Core Stack Reset

Use only when a phase explicitly allows a clean reset:

```bash
make down
make up-core
make ps
```

### Source Data Regeneration

Generator runs are deterministic by seed. For a clean database, the same `--events` and `--seed` produce the same event-id range and logical stream.

```bash
make gen ARGS="--events 10000 --seed 1"
make sql-mysql Q="SELECT MIN(event_id), MAX(event_id), COUNT(*) FROM orders"
```

## Artifact Rules

- Raw command transcripts belong in `showcase/logs/`.
- Result JSON belongs in `showcase/results/` and must include provenance.
- Screenshots, recordings, and diagrams belong in `showcase/media/`.

