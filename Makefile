# One command to try it: make demo. Everything else is a building block for it.
SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export AIRFLOW_UID ?= $(shell id -u)
COMPOSE := docker compose
OPEN ?= open
AF := $(COMPOSE) exec -T airflow airflow

.PHONY: help setup up down clean data test test-unit test-dags test-e2e lint typecheck dbt-build \
        demo report images pdf all

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -/'

$(VENV)/bin/activate:
	python3.12 -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -e ".[dev]"

setup: $(VENV)/bin/activate .env ## create the venv and install dependencies
	$(VENV)/bin/playwright install chromium

.env:
	cp .env.example .env

# The Airflow UI login is written from .env at start-up, so no credentials file lives in git.
docker/airflow/auth/passwords.json: .env
	mkdir -p docker/airflow/auth
	set -a; . ./.env; set +a; printf '{"%s": "%s"}\n' "$${AIRFLOW_UI_USER:-admin}" "$${AIRFLOW_UI_PASSWORD:-admin}" > $@

up: .env docker/airflow/auth/passwords.json ## start Postgres, MinIO and Airflow, wait until Airflow is healthy
	$(COMPOSE) up -d --build
	@echo "waiting for Airflow..."
	@for i in $$(seq 1 90); do \
	  [ "$$($(COMPOSE) ps airflow --format '{{.Health}}')" = "healthy" ] && exit 0; sleep 3; \
	done; echo "Airflow did not become healthy"; $(COMPOSE) logs --tail 40 airflow; exit 1

down: ## stop the stack, keep data
	$(COMPOSE) down

clean: ## stop the stack and delete its volumes and generated data
	$(COMPOSE) down -v
	rm -rf data/generated out dbt/target dbt/logs

data: $(VENV)/bin/activate ## generate the synthetic dataset (seed 42) and put it in the bucket
	$(PY) scripts/generate.py --seed 42
	$(PY) -m pipeline.cli upload

lint: $(VENV)/bin/activate ## ruff
	$(VENV)/bin/ruff check . && $(VENV)/bin/ruff format --check .

typecheck: $(VENV)/bin/activate ## mypy on the pipeline package
	$(VENV)/bin/mypy

test-unit: $(VENV)/bin/activate ## unit tests only, no services needed
	$(VENV)/bin/pytest -m "not integration and not e2e and not dags"

test: $(VENV)/bin/activate .env ## unit and integration tests with the coverage gate (starts Postgres and MinIO)
	$(COMPOSE) up -d postgres minio
	$(VENV)/bin/pytest -m "not e2e and not dags" --cov --cov-report=term-missing --cov-fail-under=85

test-dags: up ## DAG integrity tests, run inside the Airflow image
	$(COMPOSE) exec -T airflow python -m pytest tests/dags -q -p no:cacheprovider

test-e2e: up $(VENV)/bin/activate ## full pipeline through Airflow, dbt, parity against the legacy script
	$(VENV)/bin/pytest -m e2e -q

dbt-build: $(VENV)/bin/activate ## dbt build from the host against the compose Postgres
	cd dbt && ../$(VENV)/bin/dbt build --profiles-dir .

demo: up data ## start everything, load 30 days through Airflow, build the marts, write the report
	-$(AF) dags unpause ingest_vendor_files
	-$(AF) dags unpause transform
	$(AF) backfill create --dag-id ingest_vendor_files --from-date 2026-03-01 --to-date 2026-03-30 --max-active-runs 6
	scripts/wait_for_runs.sh ingest_vendor_files
	$(AF) dags trigger transform
	scripts/wait_for_runs.sh transform
	mkdir -p out
	$(COMPOSE) exec -T airflow cat /tmp/dbt-target/run_results.json > out/dbt_run_results.json
	$(MAKE) report
	@echo
	@echo "Airflow UI:    http://localhost:8080  (login in .env.example)"
	@echo "Health report: out/health_report.html"
	-@$(OPEN) out/health_report.html 2>/dev/null || true
	-@$(OPEN) http://localhost:8080 2>/dev/null || true

report: $(VENV)/bin/activate ## render out/health_report.html from the ops and marts schemas
	$(PY) -m pipeline.cli report --out out/health_report.html --facts out/report_facts.json

images: $(VENV)/bin/activate ## render the three portfolio images (needs a finished make demo)
	$(PY) scripts/make_images.py

pdf: $(VENV)/bin/activate ## build the one-page case study PDF
	$(PY) scripts/make_pdf.py

all: lint typecheck test test-dags test-e2e images pdf ## everything CI runs
