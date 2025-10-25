#!/usr/bin/env python3
"""
Full environment sanity + auto-fix script for Certus.
Ensures DuckDB tables, directories, and schemas are correct.
"""

import os, duckdb, pandas as pd, importlib

DATA_DIR = "data"
DB_PATH = f"{DATA_DIR}/markets.duckdb"

REQUIRED_COLUMNS = {
    "ts": "BIGINT",
    "id": "VARCHAR",
    "symbol": "VARCHAR",
    "vs_currency": "VARCHAR",
    "price": "DOUBLE",
    "market_cap": "DOUBLE",
    "total_volume": "DOUBLE",
    "pct_change_1h": "DOUBLE",
    "pct_change_24h": "DOUBLE",
    "pct_change_7d": "DOUBLE",
    "source": "VARCHAR",
}

def ensure_packages():
    print("== Checking packages ==")
    for pkg in ("pandas","duckdb","httpx"):
        try:
            importlib.import_module(pkg)
            print(f"[OK] {pkg}")
        except ImportError:
            print(f"[MISSING] {pkg} → installing...")
            os.system(f"pip install {pkg}")

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[OK] ensured {DATA_DIR}/ exists")

def ensure_schema():
    con = duckdb.connect(DB_PATH)
    tables = con.sql("SHOW TABLES").fetchdf()
    if "markets" not in tables["name"].values:
        print("markets table not found → creating new schema.")
        create_schema(con)
        con.close()
        return

    desc = con.sql("DESCRIBE markets").fetchdf()
    mismatch = False
    for col, dtype in REQUIRED_COLUMNS.items():
        if col not in desc["column_name"].values:
            print(f"❌ Missing column: {col}")
            mismatch = True
        else:
            existing = desc.loc[desc["column_name"]==col,"column_type"].values[0]
            if dtype.lower() not in existing.lower():
                print(f"❌ Type mismatch for {col}: {existing} → should be {dtype}")
                mismatch = True
    if mismatch:
        print("Recreating table with correct schema...")
        con.execute("DROP TABLE IF EXISTS markets;")
        create_schema(con)
    else:
        print("[OK] markets table schema valid")
    con.close()

def create_schema(con):
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
    print("✅ Created markets table with correct schema.")

def verify_insert():
    import subprocess
    print("== Running fetch_markets.py ==")
    subprocess.run(["python", "scripts/fetch_markets.py"], check=True)
    con = duckdb.connect(DB_PATH)
    rows = con.sql("SELECT COUNT(*) AS n FROM markets").fetchdf().iloc[0,0]
    print(f"✅ markets now contains {rows} rows.")
    con.close()

def main():
    print("🧠 Running Certus environment check & reset ...")
    ensure_packages()
    ensure_data_dir()
    ens
