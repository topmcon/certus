#!/usr/bin/env python
from __future__ import annotations
import time
import duckdb
import pandas as pd

from certus.analytics.signals import compute_signals
from certus.analytics.scores import compute_scores

DB_PATH = "data/markets.duckdb"

def main():
    con = duckdb.connect(DB_PATH)

    # 1) Load indicators
    indicators = con.sql("SELECT * FROM indicators").fetchdf()
    if indicators.empty:
        print("[calc_signals] No indicator rows found in 'indicators'.")
        con.close()
        return

    # 2) Compute signals
    sig_df = compute_signals(indicators)
    sig_df["ts"] = int(time.time() * 1000)

    # 3) Ensure signals table (id is VARCHAR — asset id string like 'bitcoin')
    con.sql("""
        CREATE TABLE IF NOT EXISTS signals (
            id VARCHAR,
            symbol VARCHAR,
            price DOUBLE,
            rsi_14 DOUBLE,
            ema_9 DOUBLE,
            ema_20 DOUBLE,
            macd DOUBLE,
            signal_type VARCHAR,
            signal_strength DOUBLE,
            ts BIGINT
        )
    """)
    con.register("sig_df", sig_df)
    con.sql("""
        INSERT INTO signals (id, symbol, price, rsi_14, ema_9, ema_20, macd, signal_type, signal_strength, ts)
        SELECT id, symbol, price, rsi_14, ema_9, ema_20, macd, signal_type, signal_strength, ts
        FROM sig_df
    """)

    # 4) Compute scores
    scores_in = sig_df[["symbol","signal_type","signal_strength"]].copy()
    sco_df = compute_scores(scores_in)
    # join price + same batch ts
    price_map = sig_df.set_index("symbol")["price"]
    sco_df["price"] = price_map.reindex(sco_df["symbol"]).values
    sco_df["ts"] = sig_df["ts"].iloc[0]

    # 5) Ensure scores table (includes trend_tier)
    con.sql("""
        CREATE TABLE IF NOT EXISTS scores (
            symbol VARCHAR,
            signal_type VARCHAR,
            signal_strength DOUBLE,
            trend_score DOUBLE,
            trend_tier VARCHAR,
            price DOUBLE,
            ts BIGINT
        )
    """)
    con.register("sco_df", sco_df)
    con.sql("""
        INSERT INTO scores (symbol, signal_type, signal_strength, trend_score, trend_tier, price, ts)
        SELECT symbol, signal_type, signal_strength, trend_score, trend_tier, price, ts
        FROM sco_df
    """)

    # 6) Print summary
    top = con.sql("""
        SELECT symbol, ROUND(price,2) AS price, ROUND(trend_score,2) AS score, trend_tier
        FROM scores
        WHERE ts = (SELECT MAX(ts) FROM scores)
        ORDER BY score DESC
        LIMIT 15
    """).fetchdf()
    print("\n== Top bullish (latest batch) ==")
    if isinstance(top, pd.DataFrame) and not top.empty:
        print(top.to_string(index=False))
    else:
        print("(no rows)")

    con.close()

if __name__ == "__main__":
    main()
