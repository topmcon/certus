from __future__ import annotations
import duckdb

con = duckdb.connect("data/markets.duckdb")

# 1) Create empty snapshot table (schema = trend_feed_enriched) if missing
con.execute("""
CREATE TABLE IF NOT EXISTS trend_feed_snap AS
SELECT * FROM trend_feed_enriched WHERE 1=0
""")

# 2) De-dupe source (latest per kind+source_id)
con.execute("""
WITH src AS (
  SELECT * FROM trend_feed_enriched
),
dedup AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY kind, source_id ORDER BY ts DESC) rn
  FROM src
)
DELETE FROM trend_feed_snap
USING (SELECT kind, source_id FROM dedup) s
WHERE trend_feed_snap.kind = s.kind AND trend_feed_snap.source_id = s.source_id;

INSERT INTO trend_feed_snap
SELECT * FROM (
  SELECT * FROM trend_feed_enriched
  QUALIFY ROW_NUMBER() OVER (PARTITION BY kind, source_id ORDER BY ts DESC) = 1
);
""")

# 3) Show a quick summary
print("trend_feed_snap rows =", con.sql("SELECT COUNT(*) FROM trend_feed_snap").fetchone()[0])
print(con.sql("""
  SELECT kind, ts, COALESCE(symbol_primary,'?') AS sym,
         LEFT(COALESCE(title,''), 60) AS title
  FROM trend_feed_snap
  ORDER BY ts DESC
  LIMIT 8
""").fetchdf())
con.close()
