# certus/data/coingecko_client.py
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv


class CoinGeckoHTTPError(Exception):
    """Raised when CoinGecko returns a non-2xx response with useful context."""


def _mask(s: str, head: int = 6) -> str:
    if not s:
        return ""
    return s[:head] + "…" + f"({len(s)} chars)"


# Load .env from repo root, and allow it to override the process env for certainty
_REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=_REPO_ROOT / ".env", override=True)


class CoinGeckoClient:
    """
    Async CoinGecko client.

    Base URL priority:
      1) CG_BASE_URL (if set)
      2) Pro URL if COINGECKO_API_KEY present
      3) Public URL fallback

    Notes:
      - coins/markets requires 'vs_currency'
      - To receive percent change columns, you MUST pass 'price_change_percentage'
        (we default to "1h,24h,7d")
    """

    def __init__(self, timeout: float = 60.0):
        api_key = (os.getenv("COINGECKO_API_KEY") or "").strip()
        env_base = (os.getenv("CG_BASE_URL") or "").strip()

        if env_base:
            base_url = env_base.rstrip("/")
            is_pro = base_url.startswith("https://pro-api.coingecko.com")
        else:
            is_pro = bool(api_key)
            base_url = (
                "https://pro-api.coingecko.com/api/v3" if is_pro else "https://api.coingecko.com/api/v3"
            )

        headers = {}
        if api_key:
            headers["x-cg-pro-api-key"] = api_key

        # A couple of polite headers (not required but nice to have)
        headers["Accept"] = "application/json"
        headers["User-Agent"] = "Certus/1.0 (+https://github.com/topmcon/certus)"

        # Diagnostics (safe/masked)
        print(f"[CG] Base URL: {base_url}")
        print(f"[CG] Using Pro: {is_pro} | Key present: {bool(api_key)} ({_mask(api_key) if api_key else ''})")

        self.base_url = base_url
        self.is_pro = is_pro
        self._client = httpx.AsyncClient(base_url=self.base_url, headers=headers, timeout=timeout)

        # Default params we always apply to coins/markets
        self._markets_params_base: Dict[str, Any] = {
            "order": "market_cap_desc",
            "sparkline": "false",
            # REQUIRED to receive *_percentage_* columns like price_change_percentage_24h
            "price_change_percentage": "1h,24h,7d",
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self._client.aclose()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1.2))
    async def _get(self, endpoint: str, params: Dict[str, Any]) -> Any:
        try:
            r = await self._client.get(endpoint, params=params)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            body = ""
            try:
                body = r.text[:500]  # type: ignore[name-defined]
            except Exception:
                pass
            raise CoinGeckoHTTPError(
                f"HTTP {getattr(e.response, 'status_code', '???')} on {self.base_url}{endpoint} "
                f"params={params} body={body}"
            ) from e
        except httpx.RequestError as e:
            raise CoinGeckoHTTPError(f"Request error calling {self.base_url}{endpoint}: {e}") from e

    async def ping(self) -> Any:
        return await self._get("/ping", {})

    async def coins_markets(
        self,
        vs_currency: str = "usd",
        per_page: int = 250,
        page: int = 1,
        order: Optional[str] = None,
        price_change_percentage: Optional[str] = None,
        sparkline: Optional[bool] = None,
        **extra: Any,
    ) -> List[Dict[str, Any]]:
        """
        GET /coins/markets

        Returns a list of market snapshots. Ensures 'price_change_percentage=1h,24h,7d'
        unless explicitly overridden. This is what populates:
          - price_change_percentage_1h
          - price_change_percentage_24h
          - price_change_percentage_7d
        """
        # Normalize inputs
        vs_currency = (vs_currency or "usd").lower()

        params: Dict[str, Any] = {
            "vs_currency": vs_currency,
            "per_page": per_page,
            "page": page,
        }

        # Merge defaults with user overrides
        merged = dict(self._markets_params_base)
        if order is not None:
            merged["order"] = order
        if price_change_percentage is not None:
            merged["price_change_percentage"] = price_change_percentage
        if sparkline is not None:
            merged["sparkline"] = "true" if sparkline else "false"

        # Pass through any extra params if ever needed (e.g., category filters)
        merged.update(extra)
        params.update(merged)

        return await self._get("/coins/markets", params)
