#!/usr/bin/env bash
set -euo pipefail

echo "== P4.5 Step 1: deps & folders =="

# 0) Ensure folders
mkdir -p scripts certus/{data,analytics,ingest,utils}

# 1) Safe helper to append unique req lines
touch requirements.txt
ensure_req() {
  local pkg="$1"
  grep -qxF "$pkg" requirements.txt || echo "$pkg" >> requirements.txt
}

# 2) Core deps
ensure_req "pandas>=2.2.2"
ensure_req "duckdb>=1.0.0"
ensure_req "python-dotenv>=1.0.1"
ensure_req "httpx>=0.27.2"
ensure_req "pydantic>=2.9.2"
ensure_req "loguru>=0.7.2"

# 3) API clients
ensure_req "finnhub-python>=2.4.19"
ensure_req "alpha_vantage>=2.3.1"

# 4) Install
pip install -r requirements.txt

echo "== Done: Step 1 complete =="
