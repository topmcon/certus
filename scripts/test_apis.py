#!/usr/bin/env python
import os, sys
from pathlib import Path

print("== API & DB checks ==")

# Load .env if present
env = Path(".env")
if env.exists():
    for line in env.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# Pause guard
if os.getenv("CERTUS_PAUSED", "").lower() in ("true", "1", "yes"):
    print("[Certus] ⚠️ System paused — skipping API/DB checks.")
    sys.exit(0)

# CoinGecko ping
try:
    from certus.data.coingecko_client import CoinGeckoClient
    import asyncio
    async def _ping():
        async with CoinGeckoClient(timeout=10.0) as cg:
            r = await cg.ping()
            print("CoinGecko ping:", r)
    asyncio.run(_ping())
    print("✔ CoinGecko reachable")
except Exception as e:
    print("✖ CoinGecko check failed:", e); sys.exit(1)

# DuckDB open + markets table
try:
    import duckdb
    con = duckdb.connect("data/markets.duckdb")
    tables = {t[0] for t in con.sql(
        "select table_name from information_schema.tables where table_schema='main'"
    ).fetchall()}
    print("DuckDB tables:", sorted(tables))
    if "markets" not in tables:
        raise SystemExit("✖ Missing required table: markets")
    # tiny sample
    print(con.sql("select id,symbol,price from markets limit 5").fetchdf().to_string(index=False))
    con.close()
    print("✔ DuckDB OK")
except Exception as e:
    print("✖ DuckDB check failed:", e); sys.exit(1)
