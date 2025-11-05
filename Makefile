.PHONY: up build api migrate install lint test stop

COMPOSE_FILE=infra/compose.yaml

up: build
	@echo "Starting services with docker compose..."
	docker compose -f $(COMPOSE_FILE) up -d

stop:
	docker compose -f $(COMPOSE_FILE) down

build:
	@echo "Building api image"
	docker build -t certus-api:local .

api:
	@echo "Run API locally (requires deps)"
	uvicorn apps.api.main:app --reload --host 0.0.0.0 --port 8000

api-small:
	@echo "Run small API app for CoinGecko/CoinMarketCal"
	uvicorn apps.api_small.main:app --reload --host 0.0.0.0 --port 8001

install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	# developer extras
	pip install alembic asyncpg sqlalchemy sqlmodel

migrate:
	@echo "Apply SQL migrations using DATABASE_URL env var or default"
	@echo "Apply SQL migrations using Alembic (fallback to raw SQL runner)"
	if command -v alembic > /dev/null 2>&1; then \
		alembic -c infra/db/alembic.ini upgrade head; \
	else \
		./scripts/run_migrations.sh; \
	fi

lint:
	ruff check . || true

test:
	pytest -q
.PHONY: fetch indicators scores signals run smoke debug clean

# Paths
DB := data/markets.duckdb

fetch:
	@echo "== Fetch markets =="
	python scripts/fetch_markets.py

indicators:
	@echo "== Indicators =="
	python scripts/calc_indicators.py

scores:
	@echo "== Scores =="
	python scripts/calc_scores.py

signals:
	@echo "== Signals =="
	python scripts/calc_signals.py

run: indicators scores signals
	@echo "== Chain complete =="

smoke:
	@echo "== Smoke check =="
	python3 run_smoke.py

check-apis:
	@echo "== Run API checker =="
	set -a; source .env; set +a; python3 scripts/action_check_apis.py

debug:
	@echo "== Debug trace =="
	bash -x scripts/p29_run.sh 2>&1 | tee logs/trace_$(shell date +%Y%m%d_%H%M%S).log

clean:
	@echo "== Clean outputs =="
	rm -f data/indicators.parquet
	sqlite3 /dev/null "" >/dev/null 2>&1 || true
