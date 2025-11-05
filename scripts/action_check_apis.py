#!/usr/bin/env python3
"""Action-friendly API connectivity checker.

This script is safe to run in CI. It reads credentials from environment variables
and performs lightweight checks for each service. It writes a JSON summary to
`results/api_check.json` and exits with code 0 when all REQUIRED services
succeed, or 2 when any REQUIRED service fails.

Do NOT hard-code secrets into files. Provide them to the runner via secrets.
"""
from __future__ import annotations
import asyncio
import httpx
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

RESULTS_PATH = Path("results")
RESULTS_PATH.mkdir(parents=True, exist_ok=True)
OUTFILE = RESULTS_PATH / "api_check.json"

SERVICES = [
    {
        "name": "CoinGecko",
        "env": "COINGECKO_API_KEY",
        "required": False,
    },
    {
        "name": "CoinMarketCal",
        "env": "COINMARKETCAL_API_KEY",
        "required": True,
    },
]


async def check_coingecko(key: str | None) -> Dict[str, Any]:
    # Prefer using the in-repo client if available to keep behavior consistent.
    try:
        from certus.data.coingecko_client import CoinGeckoClient
    except Exception:
        # Fallback: simple ping using public/pro base
        base = os.getenv("CG_BASE_URL") or os.getenv("COINGECKO_BASE_URL") or (
            "https://pro-api.coingecko.com/api/v3" if key else "https://api.coingecko.com/api/v3"
        )
        url = base.rstrip("/") + "/ping"
        try:
            r = httpx.get(url, timeout=15.0)
            r.raise_for_status()
            return {"ok": True, "detail": "ping"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    client = CoinGeckoClient()
    try:
        pong = await client.ping()
        await client.close()
        return {"ok": True, "detail": type(pong).__name__}
    except Exception as e:
        try:
            await client.close()
        except Exception:
            pass
        return {"ok": False, "error": str(e)}


def check_coinmarketcal(key: str) -> Dict[str, Any]:
    base = "https://developers.coinmarketcal.com/v1/events"
    headers = {"x-api-key": key, "Accept": "application/json"}
    params = {"max": 1, "page": 1}
    try:
        r = httpx.get(base, headers=headers, params=params, timeout=20.0)
        r.raise_for_status()
        return {"ok": True, "status_code": r.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def main() -> int:
    results: Dict[str, Any] = {"services": {}, "summary": {"required_failed": 0}}
    for s in SERVICES:
        name = s["name"]
        env = s["env"]
        required = s["required"]
        key = os.getenv(env)
        if key is None or key.strip() == "":
            # If not present and not required, skip; if required, count as failure
            if required:
                results["services"][name] = {"ok": False, "skipped": False, "error": f"Missing env: {env}", "required": True}
                results["summary"]["required_failed"] += 1
            else:
                results["services"][name] = {"ok": True, "skipped": True, "required": False}
            continue

        # Run checks
        try:
            if name == "CoinGecko":
                r = await check_coingecko(key)
            elif name == "CoinMarketCal":
                r = check_coinmarketcal(key)
            else:
                r = {"ok": False, "error": "No check implemented"}
        except Exception as e:
            r = {"ok": False, "error": str(e)}

        r["required"] = required
        results["services"][name] = r
        if required and not r.get("ok"):
            results["summary"]["required_failed"] += 1

    # Write results
    with OUTFILE.open("w") as f:
        json.dump(results, f, indent=2)

    # Print concise summary
    print(json.dumps({"required_failed": results["summary"]["required_failed"]}, indent=2))
    if results["summary"]["required_failed"] > 0:
        print("One or more REQUIRED services failed. See results/api_check.json for details.")
        return 2
    print("All required services OK.")
    return 0


if __name__ == "__main__":
    code = asyncio.run(main())
    sys.exit(code)
