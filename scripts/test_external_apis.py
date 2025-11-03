#!/usr/bin/env python3
"""Lightweight connectivity checks for external APIs used by this repo.

This script is safe to run locally: it checks for required env vars and skips
tests when keys are missing. It performs minimal requests to verify
connectivity and credentials.
"""
from __future__ import annotations
import asyncio
import os
import sys

def ok(msg: str):
    print(f"[OK] {msg}")

def fail(msg: str):
    print(f"[FAIL] {msg}")

def skip(msg: str):
    print(f"[SKIP] {msg}")


async def check_coingecko():
    try:
        from certus.data.coingecko_client import CoinGeckoClient
    except Exception as e:
        fail(f"CoinGecko client import failed: {e}")
        return

    client = CoinGeckoClient()
    try:
        pong = await client.ping()
        ok(f"CoinGecko ping responded: type={type(pong).__name__}")
    except Exception as e:
        fail(f"CoinGecko ping failed: {e}")
    finally:
        await client.close()


def check_alphavantage():
    from certus.ingest import alphavantage_client
    key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not key:
        skip("AlphaVantage: ALPHAVANTAGE_API_KEY not set")
        return
    try:
        r = alphavantage_client.global_quote("IBM")
        ok(f"AlphaVantage global_quote returned type={type(r).__name__}")
    except Exception as e:
        fail(f"AlphaVantage request failed: {e}")


def check_finnhub():
    from certus.ingest import finnhub_client
    key = os.getenv("FINNHUB_API_KEY")
    if not key:
        skip("Finnhub: FINNHUB_API_KEY not set")
        return
    try:
        r = finnhub_client.quote("AAPL")
        ok(f"Finnhub quote returned type={type(r).__name__}")
    except Exception as e:
        fail(f"Finnhub request failed: {e}")


def check_cryptopanic():
    from certus.ingest import cryptopanic
    key = os.getenv("CRYPTOPANIC_API_KEY")
    if not key:
        skip("Cryptopanic: CRYPTOPANIC_API_KEY not set")
        return
    try:
        r = cryptopanic.latest_posts(public=True, page=1)
        ok(f"Cryptopanic latest_posts returned type={type(r).__name__}")
    except Exception as e:
        fail(f"Cryptopanic request failed: {e}")


def check_coinmarketcal():
    from certus.ingest import coinmarketcal
    key = os.getenv("COINMARKETCAL_API_KEY")
    if not key:
        skip("CoinMarketCal: COINMARKETCAL_API_KEY not set")
        return
    try:
        r = coinmarketcal.fetch_events(max_items=1, page=1)
        ok(f"CoinMarketCal fetch_events returned type={type(r).__name__}")
    except Exception as e:
        fail(f"CoinMarketCal request failed: {e}")


def main():
    print("Checking external API connectivity (skips if env var missing)")
    # CoinGecko (async)
    asyncio.run(check_coingecko())

    # Sync checks
    check_alphavantage()
    check_finnhub()
    check_cryptopanic()
    check_coinmarketcal()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(2)
