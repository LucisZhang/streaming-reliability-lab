# Local Lite Mode and Workstation Reproduction

This lab has two execution modes. Keep them separate.

## 1. Local lite mode

Use this on a space-constrained laptop. It does not start Docker.

```bash
make local-verify
```

What this checks:

- Python harness unit tests.
- Ruff, black, mypy, and Maven verification for the Flink job.
- Static dashboard build, including the results-contract validation for all committed
  `showcase/results/*.json` artifacts.

What this does not check:

- Live MySQL CDC ingestion.
- Live Flink task recovery.
- Iceberg writes to MinIO.
- The full induced-failure `make eo-verify ARGS="--failure all"` run.

Local lite mode is enough to review and build the portfolio evidence dashboard, but it is not
an on-demand reproduction of the heavy reliability claim.

## 2. Heavy reproduction mode

Use this only on a workstation with enough free disk and Docker capacity.

```bash
make preflight-heavy
make up-core
make eo-verify ARGS="--failure all"
make down
```

The heavy targets fail fast through `make preflight-heavy` before starting Docker. By default,
the preflight requires at least 25 GiB free on the repository volume and a Docker daemon that
responds within 10 seconds.

Environment knobs:

```bash
P1_HEAVY_MIN_FREE_GIB=40 make preflight-heavy
P1_DOCKER_CHECK_TIMEOUT_SECONDS=20 make preflight-heavy
P1_SKIP_DOCKER_CHECK=1 make preflight-heavy
```

Only lower `P1_HEAVY_MIN_FREE_GIB` when you have a specific reason and are prepared to clean up
Docker volumes if the run fills the disk.

## Recommended workstation resources

- At least 16 GiB RAM, with Docker Desktop allowed to use 10-12 GiB.
- At least 40 GiB free disk before starting the full run.
- Java 11, Maven 3.9, Python 3.11, Node 20.
- Docker Desktop running and responsive.
- No other local service on the Flink REST port. If `8081` is occupied, set
  `FLINK_REST_PORT=18081` in the gitignored `.env`.

## Evidence to preserve after a workstation run

After a successful heavy run, keep these files together:

- `showcase/results/eo_reconciliation.json`
- `showcase/logs/phase-2.1-eo-verify.log`
- `RUNBOOK.md`
- `git rev-parse HEAD`
- `git status --short`
- `make preflight-heavy` output
- `docker compose --env-file .env -f infra/docker-compose.yml ps` output
- Host environment notes: OS, CPU/RAM, Docker Desktop memory setting, and available disk before
  and after the run.

The portfolio may claim "reproduced on demand" only when the heavy run passes all five failure
classes on the workstation and the evidence bundle is copied back with the exact commit SHA.
