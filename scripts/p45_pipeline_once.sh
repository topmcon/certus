#!/usr/bin/env bash
set -euo pipefail
echo "== P4.5 pipeline: ingest → views → snapshot =="

# 1) Ingest
python scripts/ingest_news.py
python scripts/ingest_events.py
python scripts/ingest_quotes.py

# 2) Ensure views/migrations applied
python scripts/run_migrations.py

# 3) Snapshot
python scripts/snapshot_trend.py

# 4) Quick sample
python - <<'PY'
import duckdb
con = duckdb.connect("data/markets.duckdb")
print(con.sql("select count(*) n from trend_feed_snap").fetchdf())
print(con.sql("""
  select kind, ts, coalesce(symbol_primary,'?') sym,
         left(coalesce(title,''),60) t
  from trend_feed_snap
  order by ts desc
  limit 5
""").fetchdf())
con.close()
PY
echo "== Done =="
