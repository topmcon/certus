#!/usr/bin/env bash
# Robust nightly smoke
set -euo pipefail

cd "$(dirname "$0")/.."

# Load .env safely (handles URLs, no xargs splitting issues)
set -a
source .env
set +a

echo ">> Running fetch..."
PYTHONPATH="$(pwd)" python scripts/fetch_markets.py

DB_PATH="${DUCKDB_PATH:-data/certus.duckdb}"
echo ">> Verifying DB ($DB_PATH)..."

python - <<PY
import duckdb
db = r"${DB_PATH}"
con = duckdb.connect(db, read_only=True)
print(con.sql("select count(*) n from markets").fetchdf())
print(con.sql("select symbol, price, pct_change_24h from markets order by market_cap desc limit 5").fetchdf())
con.close()
PY

echo "✅ Smoke OK."
