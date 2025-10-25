#!/usr/bin/env python
import duckdb
con = duckdb.connect("data/markets.duckdb")
print(con.sql("""
  SELECT
    symbol,
    ROUND(price,2)       AS price,
    ROUND(trend_score,2) AS trend_score,
    score,               -- already rounded by the view
    trend_tier,
    signal_type
  FROM latest_leaderboard
  LIMIT 25
""").fetchdf().to_string(index=False))
con.close()
