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

# CoinMarketCap ping
try:
    from certus.data.coinmarketcap_client import CoinMarketCapClient
    import asyncio as _asyncio
    async def _ping_cmc():
        cmc = CoinMarketCapClient(timeout=10.0)
        try:
            r = await cmc.ping()
            # print a tiny summary rather than the full payload
            if isinstance(r, dict) and 'status' in r:
                print('CoinMarketCap ping status:', r.get('status'))
            else:
                print('CoinMarketCap ping: <response present>')
        finally:
            try:
                await cmc.close()
            except Exception:
                pass

    _asyncio.run(_ping_cmc())
    print("✔ CoinMarketCap reachable")
except Exception as e:
    print("✖ CoinMarketCap check failed:", e); # don't exit here to allow DuckDB checks to run

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
