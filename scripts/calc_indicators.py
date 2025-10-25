#!/usr/bin/env python
# scripts/calc_indicators.py

from certus.utils.pause_guard import guard_pause
guard_pause()

import logging
import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

DB_PATH = "data/markets.duckdb"

# ---- pure pandas indicators ----
def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=span).mean()

def rsi_wilder(price: pd.Series, period: int = 14) -> pd.Series:
    delta = price.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # Expect columns: id, symbol, ts (datetime64[ns] naive), price (float)
    def _calc(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("ts").copy()
        g["ema_9"] = ema(g["price"], 9)
        g["ema_20"] = ema(g["price"], 20)
        macd_fast = ema(g["price"], 12)
        macd_slow = ema(g["price"], 26)
        g["macd"] = macd_fast - macd_slow
        g["macd_signal"] = g["macd"].ewm(span=9, adjust=False, min_periods=9).mean()
        g["macd_hist"] = g["macd"] - g["macd_signal"]
        g["rsi_14"] = rsi_wilder(g["price"], 14)
        return g

    # Avoid deprecation: group on a narrow frame (don’t include extra cols)
    base = df[["id","symbol","ts","price"]].copy()
    out = base.groupby("id", group_keys=False).apply(_calc)
    return out[["id","symbol","ts","price","rsi_14","ema_9","ema_20","macd","macd_signal","macd_hist"]]

def main():
    logging.info("Loading markets & computing indicators (pandas)…")
    con = duckdb.connect(DB_PATH)

    # Ensure fixed schema (10 columns)
    con.execute("""
    CREATE TABLE IF NOT EXISTS indicators (
        id           VARCHAR,
        symbol       VARCHAR,
        ts           TIMESTAMP,
        price        DOUBLE,
        rsi_14       DOUBLE,
        ema_9        DOUBLE,
        ema_20       DOUBLE,
        macd         DOUBLE,
        macd_signal  DOUBLE,
        macd_hist    DOUBLE
    );
    """)

    # Pull ordered price history; create a tz-naive UTC timestamp column
    df = con.sql("""
        SELECT
            id,
            UPPER(symbol) AS symbol,
            COALESCE(last_updated, TO_TIMESTAMP(ts/1000)) AS ts,
            price
        FROM markets
        WHERE price IS NOT NULL
        ORDER BY id, ts
    """).fetchdf()

    if df.empty:
        logging.warning("No market rows to compute.")
        con.close()
        return

    # Normalize ts → ensure tz-naive (UTC) to satisfy DuckDB TIMESTAMP (no TZ)
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("UTC").dt.tz_localize(None)

    ind = compute_indicators(df).dropna(subset=["price"])

    # Keep only the latest row per asset (snapshot table)
    ind_latest = (
        ind.sort_values(["id","ts"])
           .groupby("id", as_index=False)
           .tail(1)
           .reset_index(drop=True)
    )

    # Register and insert (explicit column list to match table)
    con.register("ind_latest", ind_latest)
    con.execute("""
        INSERT INTO indicators (id, symbol, ts, price, rsi_14, ema_9, ema_20, macd, macd_signal, macd_hist)
        SELECT id, symbol, ts, price, rsi_14, ema_9, ema_20, macd, macd_signal, macd_hist
        FROM ind_latest
    """)

    total = con.sql("SELECT COUNT(*) AS n FROM indicators").fetchdf().iloc[0,0]
    logging.info("Indicators saved. Table row count: %s", total)
    con.close()
    logging.info("Done.")

if __name__ == "__main__":
    main()
