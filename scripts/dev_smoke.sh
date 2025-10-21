#!/usr/bin/env bash
set -euo pipefail

python scripts/calc_indicators.py
python scripts/calc_scores.py
python scripts/calc_signals.py

python - <<'PY'
import duckdb
con = duckdb.connect("data/markets.duckdb")

print("\n== INDICATORS (sample) ==")
print(con.sql("""
  SELECT symbol, ts, ROUND(price,4) price,
         ROUND(ema_9,4) ema9, ROUND(ema_20,4) ema20,
         ROUND(macd,6) macd, ROUND(macd_signal,6) macd_sig,
         ROUND(rsi_14,2) rsi
  FROM indicators
  ORDER BY ts DESC
  LIMIT 10
""").fetchdf())

print("\n== SCORES (top by trend_score) ==")
print(con.sql("""
  SELECT symbol, ts, ROUND(price,4) price, ROUND(trend_score,3) score
  FROM scores
  ORDER BY score DESC, ts DESC
  LIMIT 10
""").fetchdf())

print("\n== SIGNALS (latest 10) ==")
print(con.sql("""
  SELECT symbol, ts, signal, ROUND(price,4) price
  FROM signals
  ORDER BY ts DESC
  LIMIT 10
""").fetchdf())

con.close()
PY
