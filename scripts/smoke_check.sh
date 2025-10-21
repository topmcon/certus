#!/usr/bin/env bash
set -euo pipefail

echo "== Certus P3 Smoke Check =="

# Re-run batch quietly
python scripts/calc_signals.py >/dev/null

# Print summaries
python - <<'PY'
import duckdb
con = duckdb.connect("data/markets.duckdb")

print("\n-- Latest signals (top 10 by strength) --")
print(con.sql("""
  SELECT symbol, signal_type, ROUND(signal_strength,2) AS strength
  FROM signals
  WHERE ts = (SELECT MAX(ts) FROM signals)
  ORDER BY strength DESC
  LIMIT 10
""").fetchdf().to_string(index=False))

print("\n-- Latest scores (top 10 by score) --")
print(con.sql("""
  SELECT symbol, ROUND(price,2) AS price, ROUND(trend_score,2) AS trend_score, trend_tier
  FROM scores
  WHERE ts = (SELECT MAX(ts) FROM scores)
  ORDER BY trend_score DESC
  LIMIT 10
""").fetchdf().to_string(index=False))

print("\n-- Latest leaderboard (joined view) --")
print(con.sql("""
  SELECT symbol, ROUND(price,2) AS price, score, trend_tier, signal_type
  FROM latest_leaderboard
  LIMIT 10
""").fetchdf().to_string(index=False))

con.close()
PY
