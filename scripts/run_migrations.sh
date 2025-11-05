#!/usr/bin/env bash
set -euo pipefail

# Simple migration runner: applies SQL files in infra/db/migrations in lexical order
# Usage: DATABASE_URL=postgresql://user:pass@host:5432/db ./scripts/run_migrations.sh

MIGRATIONS_DIR="infra/db/migrations"
DATABASE_URL="${DATABASE_URL:-postgresql://certus:certus@localhost:5432/certus}"

echo "Using DATABASE_URL=${DATABASE_URL}"

if ! command -v psql &> /dev/null; then
  echo "psql not found in PATH. Install libpq or use docker to run psql."
  exit 1
fi

for f in "${MIGRATIONS_DIR}"/*.sql; do
  [ -e "$f" ] || continue
  echo "Applying $f"
  psql "$DATABASE_URL" -f "$f"
done

echo "Migrations applied."
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
