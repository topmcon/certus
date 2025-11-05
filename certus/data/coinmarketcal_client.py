"""Simple async client for CoinMarketCal (developers API v1).

Provides a small, testable interface: ping() and events(max=1).
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv


# Load .env from repo root to make local dev predictable
_REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=_REPO_ROOT / ".env", override=False)


class CoinMarketCalClient:
    def __init__(self, timeout: float = 20.0):
        self.base_url = os.getenv("COINMARKETCAL_BASE_URL", "https://developers.coinmarketcal.com/api")
        # The public developer path used in older code is /v1/events
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def close(self) -> None:
        await self._client.aclose()

    async def ping(self) -> Any:
        # Use the events endpoint as a lightweight existence check
        resp = await self._client.get("/v1/events", params={"max": 1, "page": 1}, headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json()

    async def events(self, max: int = 10, page: int = 1) -> List[Dict[str, Any]]:
        resp = await self._client.get("/v1/events", params={"max": max, "page": page}, headers={"Accept": "application/json"})
        resp.raise_for_status()
        data = resp.json()
        # API returns {"body": [...] } in our fixtures
        if isinstance(data, dict) and "body" in data:
            return data.get("body") or []
        # otherwise try to coerce
        if isinstance(data, list):
            return data
        return []


__all__ = ["CoinMarketCalClient"]
