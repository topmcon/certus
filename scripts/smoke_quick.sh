#!/usr/bin/env bash
set -euo pipefail
python - <<'PY'
import duckdb
con=duckdb.connect("data/markets.duckdb")
for title, q in [
  ("INDICATORS", "SELECT symbol, ts, ROUND(price,4) price, ROUND(ema_9,4) ema9, ROUND(ema_20,4) ema20 FROM indicators ORDER BY ts DESC LIMIT 5"),
  ("SCORES",     "SELECT symbol, ts, ROUND(price,4) price, ROUND(trend_score,3) score FROM scores ORDER BY ts DESC LIMIT 5"),
  ("SIGNALS",    "SELECT symbol, ts, signal, ROUND(price,4) price FROM signals ORDER BY ts DESC LIMIT 5"),
]:
    print(f"\n== {title} ==")
    try:
        print(con.sql(q).fetchdf())
    except Exception as e:
        print("[error]", e)
con.close()
PY
