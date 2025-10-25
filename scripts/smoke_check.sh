#!/usr/bin/env bash
set -Eeuo pipefail
trap 'echo "❌ FAILED at line $LINENO: $BASH_COMMAND"' ERR

echo "== Certus P4.2 Smoke Check =="

echo "== Python =="
python --version

echo "== Step 1: Fetch =="
python scripts/fetch_markets.py

echo "== Step 2: Indicators =="
python scripts/calc_indicators.py

echo "== Step 3: Scores =="
python scripts/calc_scores.py

echo "== Step 4: DB Health Check =="
python - <<'PY'
import duckdb
con = duckdb.connect("data/markets.duckdb")

def show(sql):
    df = con.sql(sql).fetchdf()
    print(df.to_string(index=False))

print("-- Row counts --")
for t in ("markets","indicators","scores"):
    show(f"SELECT '{t}' AS table, COUNT(*) AS rows FROM {t}")

print("\n-- Latest sample --")
show("""
WITH latest AS (
  SELECT *,
         row_number() OVER (
           PARTITION BY id
           ORDER BY COALESCE(last_updated, to_timestamp(ts/1000)) DESC
         ) rn
  FROM markets
)
SELECT id, symbol,
       ROUND(price,2)                       AS price,
       ROUND(price_change_percentage_24h,2) AS pct_24h,
       ROUND(high_24h,2)                    AS high_24h,
       ROUND(low_24h,2)                     AS low_24h
FROM latest
WHERE rn = 1
ORDER BY market_cap DESC NULLS LAST
LIMIT 10
""")
con.close()
PY

echo "== DONE ✅ Certus pipeline verified =="
