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
	bash scripts/p29_run.sh

debug:
	@echo "== Debug trace =="
	bash -x scripts/p29_run.sh 2>&1 | tee logs/trace_$(shell date +%Y%m%d_%H%M%S).log

clean:
	@echo "== Clean outputs =="
	rm -f data/indicators.parquet
	sqlite3 /dev/null "" >/dev/null 2>&1 || true
