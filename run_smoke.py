#!/usr/bin/env python3
"""Minimal smoke runner to verify API clients (safe, lightweight).

This script performs a small, non-destructive check against CoinGecko using
the in-repo client. It does not require any API keys and uses the public
CoinGecko API when no key is present.

It writes a short JSON summary to `results/smoke.json`.
"""
from __future__ import annotations
import asyncio
import json
from pathlib import Path


RESULTS = Path("results")
RESULTS.mkdir(parents=True, exist_ok=True)
OUTFILE = RESULTS / "smoke.json"


async def run_coingecko_smoke() -> dict:
    try:
        from certus.data.coingecko_client import CoinGeckoClient
    except Exception as e:
        return {"coingecko": {"ok": False, "error": f"import failed: {e}"}}

    client = CoinGeckoClient()
    try:
        pong = await client.ping()
        # fetch a tiny sample of markets (per_page=2)
        sample = await client.coins_markets(per_page=2, page=1)
        await client.close()
        return {"coingecko": {"ok": True, "ping_type": type(pong).__name__, "sample_count": len(sample)}}
    except Exception as e:
        try:
            await client.close()
        except Exception:
            pass
        return {"coingecko": {"ok": False, "error": str(e)}}


async def main() -> int:
    out = {"summary": {}}
    out.update(await run_coingecko_smoke())

    with OUTFILE.open("w") as f:
        json.dump(out, f, indent=2)

    print("Wrote:", OUTFILE)
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    code = asyncio.run(main())
    raise SystemExit(code)
