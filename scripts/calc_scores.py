import duckdb, pandas as pd
import numpy as np

DB_PATH = "data/markets.duckdb"

def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    g = df.copy()
    g["ema_trend"]    = np.where(g["ema_9"] > g["ema_20"],  1.0, -1.0)
    g["macd_signal2"] = np.sign(g["macd"])
    r = g["rsi_14"].clip(0,100)
    g["rsi_score"] = ((r - 50.0) / 50.0).clip(-1, 1)
    g["trend_score"] = (0.40 * g["ema_trend"]) + (0.30 * g["macd_signal2"]) + (0.30 * g["rsi_score"])
    return g

def main():
    con = duckdb.connect(DB_PATH)
    ind = con.sql("""
      SELECT ts,id,symbol,price,ema_9,ema_20,macd,macd_signal,macd_hist,rsi_14
      FROM indicators
      ORDER BY symbol, ts
    """).fetchdf()
    if ind.empty:
        raise SystemExit("No indicators found. Run scripts/calc_indicators.py first.")

    scores = compute_scores(ind)
    con.register("scores_df", scores)
    con.execute("DROP TABLE IF EXISTS scores")
    con.execute("CREATE TABLE scores AS SELECT * FROM scores_df")
    con.close()
    print("[✔] scores → DuckDB table 'scores'")

if __name__ == "__main__":
    main()
