SHELL := /bin/bash

ROOT := $(CURDIR)
ENV_FILE ?= .env
PYTHON ?= $(if $(wildcard $(ROOT)/.venv/bin/python),$(ROOT)/.venv/bin/python,/opt/homebrew/bin/python3.11)
MAVEN ?= mvn
MAVEN_REPO ?= $(ROOT)/.m2/repository
RESOURCE_PROFILE ?= small
COMPOSE := docker compose --env-file $(ENV_FILE) -f infra/docker-compose.yml
PYTHONPATH := $(ROOT)/harness
export PYTHONPATH

.PHONY: ensure-env doctor up-core up-olap ps down build-flink submit-flink savepoint restore
.PHONY: gen eo-verify small-file-rewrite ckpt-metrics import-starrocks smoke-starrocks-catalog
.PHONY: compaction-bench dq backfill test test-cdc lint sql-mysql sql-iceberg sql-iceberg-meta
.PHONY: sql-starrocks dashboard-build dashboard-preview

ensure-env:
	@if [ ! -f "$(ENV_FILE)" ]; then cp .env.example "$(ENV_FILE)"; fi

doctor:
	$(PYTHON) -m harness.doctor

up-core: ensure-env
	RESOURCE_PROFILE=$(RESOURCE_PROFILE) $(COMPOSE) --profile core up -d --build

up-olap: ensure-env
	RESOURCE_PROFILE=$(RESOURCE_PROFILE) $(COMPOSE) --profile olap up -d --build

ps: ensure-env
	$(COMPOSE) ps

down: ensure-env
	$(COMPOSE) --profile core --profile olap down -v

build-flink:
	$(MAVEN) -q -Dmaven.repo.local=$(MAVEN_REPO) -f flink-jobs/pom.xml clean package

submit-flink: ensure-env
	$(PYTHON) -m harness.flink submit

savepoint:
	@test -n "$(JOB)" || (echo "JOB=<flink-job-id> is required" >&2; exit 2)
	$(COMPOSE) exec -T jobmanager flink savepoint $(JOB)

restore:
	@test -n "$(SP)" || (echo "SP=<savepoint-path> is required" >&2; exit 2)
	$(PYTHON) -m harness.flink submit --savepoint "$(SP)"

gen: ensure-env
	$(PYTHON) -m harness.generator $(ARGS)

eo-verify: ensure-env build-flink
	$(PYTHON) -m harness.eo_verify $(ARGS)

small-file-rewrite: ensure-env build-flink
	$(PYTHON) -m harness.small_file_rewrite $(ARGS)

ckpt-metrics: ensure-env build-flink
	$(PYTHON) -m harness.checkpoint_metrics $(ARGS)

import-starrocks:
	@echo "StarRocks is intentionally out of core and starts in M3."
	@exit 2

smoke-starrocks-catalog:
	@echo "StarRocks catalog smoke test is intentionally out of Phase 1.1/core."
	@exit 2

compaction-bench:
	@echo "StarRocks compaction bench is intentionally out of Phase 1.1/core."
	@exit 2

dq:
	@echo "Data-quality harness is introduced in a later phase."
	@exit 2

backfill:
	@echo "Backfill harness is introduced in a later phase."
	@exit 2

test:
	$(PYTHON) -m pytest -q harness/tests

test-cdc: ensure-env build-flink
	CDC_INTEGRATION=1 $(PYTHON) -m pytest harness/tests/cdc_correctness -v

lint:
	$(PYTHON) -m ruff check harness
	$(PYTHON) -m black --check harness
	$(PYTHON) -m mypy harness
	@if [ -f flink-jobs/pom.xml ]; then \
		$(MAVEN) -q -Dmaven.repo.local=$(MAVEN_REPO) -f flink-jobs/pom.xml verify; \
	else \
		echo "Skipping Maven verify: Flink job is introduced in Phase 1.2."; \
	fi

sql-mysql: ensure-env
	$(PYTHON) -m harness.sql mysql $(if $(Q),--query "$(Q)",$(ARGS))

sql-iceberg: ensure-env
	$(PYTHON) -m harness.sql iceberg $(if $(Q),--query "$(Q)",$(ARGS))

sql-iceberg-meta: ensure-env
	$(PYTHON) -m harness.sql iceberg-meta $(ARGS)

sql-starrocks: ensure-env
	$(PYTHON) -m harness.sql starrocks $(if $(Q),--query "$(Q)",$(ARGS))

dashboard-build:
	scripts/sync-results-to-dashboard.sh
	npm --prefix dashboard ci
	npm --prefix dashboard run build

dashboard-preview:
	npm --prefix dashboard run preview
