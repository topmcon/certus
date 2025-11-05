"""Simple async client for CoinMarketCap Professional API (pro-api.coinmarketcap.com).

Provides ping() and listings(start=1, limit=100) helpers using async httpx.
Reads `COINMARKETCAP_API_KEY` from environment; falls back to `COINMARKETCAL_API_KEY`
if present to make the transition smooth for existing .env files.
"""
from __future__ import annotations
from typing import Any, Dict, List
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv


# Load .env from repo root to make local dev predictable
_REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=_REPO_ROOT / ".env", override=False)


class CoinMarketCapClient:
    def __init__(self, timeout: float = 20.0):
        self.base_url = os.getenv("COINMARKETCAP_BASE_URL", "https://pro-api.coinmarketcap.com")
        # Prefer an explicit CoinMarketCap key name; fall back to the older/mistyped env if present
        api_key = os.getenv("COINMARKETCAP_API_KEY") or os.getenv("COINMARKETCAL_API_KEY")
        default_headers = {"Accept": "application/json"}
        if api_key:
            # CoinMarketCap expects the key in this header
            default_headers["X-CMC_PRO_API_KEY"] = api_key
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, headers=default_headers)

    async def close(self) -> None:
        await self._client.aclose()

    async def ping(self) -> Any:
        # Use a tiny listings request as a health check
        resp = await self._client.get("/v1/cryptocurrency/listings/latest", params={"start": 1, "limit": 1})
        resp.raise_for_status()
        return resp.json()

    async def listings(self, start: int = 1, limit: int = 100) -> List[Dict[str, Any]]:
        resp = await self._client.get("/v1/cryptocurrency/listings/latest", params={"start": start, "limit": limit, "convert": "USD"})
        resp.raise_for_status()
        data = resp.json()
        # API returns {"status":..., "data": [...]}
        if isinstance(data, dict) and "data" in data:
            return data.get("data") or []
        if isinstance(data, list):
            return data
        return []


__all__ = ["CoinMarketCapClient"]
