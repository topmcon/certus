#!/usr/bin/env bash
set -Eeuo pipefail
trap 'echo "❌ FAILED at line $LINENO: $BASH_COMMAND"' ERR

echo "== Certus Verification Suite =="

# Respect pause flag in .env
if grep -q '^CERTUS_PAUSED=true' .env 2>/dev/null; then
  echo "[Certus] ⚠️ System paused — skipping verification per .env"
  exit 0
fi

echo "== Python & pip =="
python --version
pip list | head -n 25 || true

echo "== Syntax check (*.py) =="
py_files=$(git ls-files '*.py' || true)
if [ -n "${py_files:-}" ]; then
  python - <<'PY' <<< "$py_files"
import sys, py_compile
paths=sys.stdin.read().splitlines(); bad=[]
for p in paths:
    try: py_compile.compile(p, doraise=True)
    except Exception as e: bad.append((p,str(e)))
if bad:
    print("Syntax errors:"); [print(" -",p,"->",e) for p,e in bad]; sys.exit(1)
print("OK: no syntax errors")
PY
else
  echo "No Python files found."
fi

echo "== API & DB checks =="
python scripts/test_apis.py

echo "== Pipeline smoke =="
./scripts/smoke_check.sh

echo "== ✅ Verification complete =="
