#!/usr/bin/env bash
set -euo pipefail
DB_PATH="${1:-data/markets.duckdb}"
if command -v duckdb >/dev/null 2>&1; then
  for f in db/migrations/*.sql; do
    echo "Running $f"
    duckdb "$DB_PATH" < "$f"
  done
else
  # Fallback to Python runner (uses duckdb Python package)
  scripts/run_migrations.py "$DB_PATH"
fi
