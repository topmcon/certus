#!/usr/bin/env python3
"""
Reset and rebuild the DuckDB schema for Certus.
This ensures the 'markets' table has correct data types (VARCHAR for source).
"""

import duckdb
from pathlib import Path

DB_PATH = "data/markets.duckdb"

def main():
    Path("data").mkdir(exist_ok=True)
    con = duckdb.connect(DB_PATH)

    print(f"🧹 Connected to {DB_PATH}")

    # Drop existing tables if they exist
    con.execute("DROP TABLE IF EXISTS markets;")
    print("✅ Dropped old 'markets' table")

    # Recreate schema
    con.execute("""
        CREATE TABLE markets (
            ts BIGINT,
            id VARCHAR,
            symbol VARCHAR,
            vs_currency VARCHAR,
            price DOUBLE,
            market_cap DOUBLE,
            total_volume DOUBLE,
            pct_change_1h DOUBLE,
            pct_change_24h DOUBLE,
            pct_change_7d DOUBLE,
            source VARCHAR
        )
    """)
    print("✅ Created fresh 'markets' table with correct column types")

    con.close()
    print("🎯 Done! You can now re-run fetch_markets.py and backfill_history.py.")

if __name__ == "__main__":
    main()
