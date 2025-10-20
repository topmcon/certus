#!/usr/bin/env bash
set -euo pipefail

echo "== Python =="
python -V

echo "== Pip freeze (top lines) =="
# Avoid pipefail issues by listing packages in Python instead of piping to head
python - <<'PY'
import pkg_resources
pkgs = sorted([(d.project_name, d.version) for d in pkg_resources.working_set])
print("Package                   Version")
print("------------------------- --------------")
for name, ver in pkgs[:25]:
    print(f"{name:<25} {ver}")
PY

echo "== Editable install sanity (skipping if already ok) =="
if python - <<'PY'
import sys
try:
    import certus  # noqa
except Exception:
    sys.exit(1)
PY
then
  echo "certus import OK"
else
  echo "Trying pip install -e ."
  pip install -e .
fi

echo "== Import checks =="
python - <<'PY'
try:
    import certus
    from certus.config import SETTINGS
    print("certus + SETTINGS import OK")
    from certus.data import fetch_markets as fm
    exported = [n for n in dir(fm) if n in ("fetch_markets_df","save_markets","validate_markets","main")]
    print("certus.data.fetch_markets import OK; exports:", exported)
except Exception as e:
    import traceback; traceback.print_exc(); raise SystemExit(1)
PY

echo "== One-shot fetch test (module mode; non-fatal on failure) =="
set +e
python -m certus.data.fetch_markets
FETCH_RC=$?
set -e
if [ $FETCH_RC -ne 0 ]; then
  echo "Fetch returned non-zero (this can happen if API key or client not fully wired)."
fi

echo "== Artifacts check =="
ls -lh data 2>/dev/null || true
[ -f data/markets.parquet ] && echo "Parquet present ✓" || echo "Parquet missing ✗"
[ -f data/markets.duckdb ]  && echo "DuckDB present ✓"   || echo "DuckDB missing ✗"

echo "== DuckDB schema (if present) =="
if [ -f data/markets.duckdb ]; then
  python - <<'PY'
import duckdb
con = duckdb.connect("data/markets.duckdb", read_only=True)
print(con.sql("SHOW TABLES"))
try:
    print(con.sql("DESCRIBE markets"))
    print(con.sql("SELECT COUNT(*) AS n FROM markets"))
except Exception as e:
    print("markets table not found or empty:", e)
PY
fi

echo "== Summary =="
echo "If imports succeeded and at least one artifact exists, the pipeline skeleton is good to resume."
