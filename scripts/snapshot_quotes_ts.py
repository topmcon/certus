from __future__ import annotations
import duckdb, datetime as dt

con = duckdb.connect("data/markets.duckdb")
now = dt.datetime.now(dt.timezone.utc)

# Pull the freshest rows we already have (from both providers)
rows = con.execute("""
  WITH l AS (
    SELECT * FROM quote_latest
  )
  SELECT symbol, provider, ?::TIMESTAMP as ts_recorded,
         price, high, low, open, prev_close, NULL as volume
  FROM l
""", [now]).fetchall()

if rows:
    con.executemany("""
      INSERT OR IGNORE INTO quotes_ts
      (symbol, provider, ts_recorded, price, high, low, open, prev_close, volume)
      VALUES (?,?,?,?,?,?,?,?,?)
    """, rows)

print("appended points:", len(rows))
print(con.execute("SELECT COUNT(*) FROM quotes_ts").fetchone()[0], "total points")
con.close()
