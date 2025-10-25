#!/usr/bin/env bash
set -Eeuo pipefail
trap 'echo "❌ FAILED at line $LINENO: $BASH_COMMAND"' ERR

echo "== Certus CI-Light Verification =="

# Basic API & DB checks (honors CERTUS_PAUSED)
python scripts/test_apis.py

# If a DuckDB file is present, try indicators/scores (don’t fail CI if they error)
if [ -f data/markets.duckdb ]; then
  echo "== indicators =="; python scripts/calc_indicators.py || true
  echo "== scores ==";     python scripts/calc_scores.py     || true
else
  echo "⚠ data/markets.duckdb not found; skipping indicators/scores"
fi

echo "== ✅ CI-light verification complete =="
