#!/usr/bin/env bash
set -Eeuo pipefail
trap 'echo "❌ FAILED at line $LINENO: $BASH_COMMAND"' ERR

# Usage:
#   ./scripts/test_backup.sh              # tests newest zip
#   ./scripts/test_backup.sh path/to.zip  # tests a specific zip

ZIP="${1:-}"
if [ -z "$ZIP" ]; then
  ZIP="$(ls -t backups/*.zip | head -n1)"
fi

[ -f "$ZIP" ] || { echo "Zip not found: $ZIP"; exit 1; }

echo "== Checking zip =="
ls -lh "$ZIP"
ENTRIES=$(unzip -l "$ZIP" | tail -n 1 | awk '{print $(NF-1)}')
echo "Entries: ${ENTRIES:-0}"
if [ -z "$ENTRIES" ] || [ "$ENTRIES" = "0" ]; then
  echo "❌ Zip has 0 entries"; exit 2
fi

TMPDIR="$(mktemp -d -t certus_backup_test_XXXXXX)"
echo "== Extracting to: $TMPDIR =="
unzip -q "$ZIP" -d "$TMPDIR"

cd "$TMPDIR"

echo "== Structure peek =="
find . -maxdepth 2 -type d -print | sed 's#^\./##' | head -n 20

echo "== Critical files check =="
need=( "scripts" "certus" ".github/workflows/certus-verify.yml" )
for p in "${need[@]}"; do
  if [ ! -e "$p" ]; then
    echo "❌ Missing: $p"; exit 3
  fi
done
echo "✔ Critical files present"

echo "== Python syntax check (all .py) =="
PYCOUNT=$(find . -type f -name '*.py' | wc -l | awk '{print $1}')
if [ "$PYCOUNT" -gt 0 ]; then
  python - <<'PY'
import sys, py_compile, pathlib
bad=[]
for p in pathlib.Path('.').rglob('*.py'):
    try:
        py_compile.compile(str(p), doraise=True)
    except Exception as e:
        bad.append((str(p), str(e)))
if bad:
    print("Syntax errors:"); [print(" -",p,"->",e) for p,e in bad]; sys.exit(1)
print("✔ No syntax errors")
PY
else
  echo "No Python files found."
fi

echo "== DuckDB quick check (if present) =="
if [ -f "data/markets.duckdb" ]; then
  python - <<'PY'
import duckdb, os
con = duckdb.connect("data/markets.duckdb")
def safe(sql):
    try:
        return con.sql(sql).fetchdf()
    except Exception as e:
        print("DuckDB error:", e); raise
print("Tables:")
print(con.sql("select table_name from information_schema.tables where table_schema='main'").fetchdf())
for t in ("markets","indicators","scores"):
    try:
        n = con.sql(f"select count(*) as n from {t}").fetchdf().iloc[0,0]
        print(f"{t}: {n} rows")
    except Exception as e:
        print(f"{t}: not found ({e})")
con.close()
PY
else
  echo "⚠ data/markets.duckdb not in zip (ok if you exclude data/)"
fi

echo "== ✅ Backup test passed =="
echo "Extracted copy kept at: $TMPDIR"
chmod +x scripts/test_backup.sh
./scripts/backup.sh "Full backup — post-verify"
./scripts/test_backup.sh                   # tests newest zip
# or
./scripts/test_backup.sh backups/certus_p4.2_20251025-162510.zip
