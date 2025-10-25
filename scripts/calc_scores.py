#!/usr/bin/env python
# scripts/calc_scores.py

from certus.utils.pause_guard import guard_pause
guard_pause()

import logging
import duckdb
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
DB_PATH = "data/markets.duckdb"

def compute_trend_score(df: pd.DataFrame) -> pd.DataFrame:
    g = df.copy()

    rsi = pd.to_numeric(g["rsi_14"], errors="coerce")
    rsi_component = ((rsi - 50.0) / 50.0).clip(-1, 1).fillna(0)

    ema_align  = (g["price"] > g["ema_9"]).astype(float) - (g["price"] <= g["ema_9"]).astype(float)
    ema_trend  = (g["ema_9"] > g["ema_20"]).astype(float) - (g["ema_9"] <= g["ema_20"]).astype(float)
    ema_component = 0.5 * ema_align + 0.5 * ema_trend

    macd_bias = np.sign(
        pd.to_numeric(g["macd"], errors="coerce") - pd.to_numeric(g["macd_signal"], errors="coerce")
    ).fillna(0)

    g["trend_score"] = (0.5 * rsi_component + 0.3 * ema_component + 0.2 * macd_bias).astype(float)

    return g[["id", "symbol", "ts", "price", "trend_score"]]

def main():
    logging.info("Computing trend scores from latest indicators…")
    con = duckdb.connect(DB_PATH)

    con.execute("""
    CREATE TABLE IF NOT EXISTS scores (
        id           VARCHAR,
        symbol       VARCHAR,
        ts           TIMESTAMP,
        price        DOUBLE,
        trend_score  DOUBLE
    );
    """)

    latest = con.sql("""
        WITH x AS (
            SELECT
                id, UPPER(symbol) AS symbol, ts, price,
                rsi_14, ema_9, ema_20, macd, macd_signal,
                row_number() OVER (PARTITION BY id ORDER BY ts DESC) rn
            FROM indicators
        )
        SELECT id, symbol, ts, price, rsi_14, ema_9, ema_20, macd, macd_signal
        FROM x WHERE rn = 1
    """).fetchdf()

    if latest.empty:
        logging.warning("No indicator rows found; scores not updated.")
        con.close()
        return

    # normalize ts to tz-naive TIMESTAMP
    if pd.api.types.is_integer_dtype(latest["ts"]) or pd.api.types.is_float_dtype(latest["ts"]):
        latest["ts"] = pd.to_datetime(latest["ts"], unit="ms", utc=True).dt.tz_localize(None)
    else:
        latest["ts"] = pd.to_datetime(latest["ts"], utc=True, errors="coerce").dt.tz_localize(None)

    out = compute_trend_score(latest)

    ids = tuple(sorted(set(out["id"].tolist())))
    con.execute("BEGIN")
    try:
        if ids:
            if len(ids) == 1:
                con.execute("DELETE FROM scores WHERE id = ?", [ids[0]])
            else:
                placeholders = ",".join(["?"] * len(ids))
                con.execute(f"DELETE FROM scores WHERE id IN ({placeholders})", list(ids))

        con.register("scores_tmp", out)
        con.execute("""
            INSERT INTO scores (id, symbol, ts, price, trend_score)
            SELECT id, symbol, ts, price, trend_score
            FROM scores_tmp
        """)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    logging.info("Scores saved: %d rows.", len(out))
    con.close()
    logging.info("Done.")

if __name__ == "__main__":
    main()
