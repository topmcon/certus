import duckdb, pandas as pd
from pathlib import Path
from certus.analytics.indicators import compute_indicators

DB_PATH = "data/markets.duckdb"
PARQUET_OUT = "data/indicators.parquet"

def main():
    con = duckdb.connect(DB_PATH)
    df = con.sql("""
      SELECT ts, id, UPPER(symbol) AS symbol, price
      FROM markets
      WHERE price IS NOT NULL
      ORDER BY ts
    """).fetchdf()
    if df.empty:
        raise SystemExit("No market rows found. Run scripts/fetch_markets.py first.")

    ind = compute_indicators(df)

    Path(PARQUET_OUT).parent.mkdir(parents=True, exist_ok=True)
    ind.to_parquet(PARQUET_OUT, index=False)

    con.register("ind", ind)
    con.execute("DROP TABLE IF EXISTS indicators")
    con.execute("CREATE TABLE indicators AS SELECT * FROM ind")
    con.close()
    print(f"[✔] indicators → {PARQUET_OUT} and DuckDB table 'indicators'")

if __name__ == "__main__":
    main()
