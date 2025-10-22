# certus/storage/schema.py
from __future__ import annotations
import duckdb
from pathlib import Path

DB_PATH = Path("data/markets.duckdb")

def ensure_db() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    # markets: latest snapshot rows from CoinGecko
    con.execute("""
    CREATE TABLE IF NOT EXISTS markets (
        id TEXT,
        symbol TEXT,
        name TEXT,
        price DOUBLE,
        pct_change_24h DOUBLE,
        market_cap DOUBLE,
        volume_24h DOUBLE,
        high_24h DOUBLE,
        low_24h DOUBLE,
        last_updated TIMESTAMP
    );
    """)
    # indicators computed per snapshot id+symbol
    con.execute("""
    CREATE TABLE IF NOT EXISTS indicators (
        id TEXT,
        symbol TEXT,
        price DOUBLE,
        rsi_14 DOUBLE,
        ema_9 DOUBLE,
        ema_20 DOUBLE,
        macd DOUBLE,
        macd_signal DOUBLE,
        macd_hist DOUBLE,
        trend TEXT,
        ts TIMESTAMP
    );
    """)
    # scores table derived from indicators
    con.execute("""
    CREATE TABLE IF NOT EXISTS scores (
        id TEXT,
        symbol TEXT,
        price DOUBLE,
        trend TEXT,
        trend_score DOUBLE,
        ts TIMESTAMP
    );
    """)
    # OHLCV cache (for charting), optional
    con.execute("""
    CREATE TABLE IF NOT EXISTS ohlcv (
        id TEXT,               -- coingecko id (e.g., 'bitcoin')
        symbol TEXT,           -- 'btc'
        ts TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE
    );
    """)
    return con
