import os
import httpx
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv

class CoinGeckoHTTPError(Exception):
    pass

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
    Priority for base URL:
      1) CG_BASE_URL (if set)
      2) Pro URL if COINGECKO_API_KEY present
      3) Public URL fallback
    """
    def __init__(self, timeout: float = 60.0):
        api_key = (os.getenv("COINGECKO_API_KEY") or "").strip()
        env_base = (os.getenv("CG_BASE_URL") or "").strip()

        if env_base:
            base_url = env_base
            is_pro = base_url.startswith("https://pro-api.coingecko.com")
        else:
            is_pro = bool(api_key)
            base_url = "https://pro-api.coingecko.com/api/v3" if is_pro else "https://api.coingecko.com/api/v3"

        headers = {"x-cg-pro-api-key": api_key} if api_key else {}

        # Diagnostics (safe/masked)
        print(f"[CG] Base URL: {base_url}")
        print(f"[CG] Using Pro: {is_pro} | Key present: {bool(api_key)} ({_mask(api_key) if api_key else ''})")

        self.base_url = base_url
        self.is_pro = is_pro
        self._client = httpx.AsyncClient(base_url=self.base_url, headers=headers, timeout=timeout)

    async def close(self):
        await self._client.aclose()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1.2))
    async def _get(self, endpoint: str, params: dict):
        r = await self._client.get(endpoint, params=params)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            body = r.text[:500]
            raise CoinGeckoHTTPError(
                f"HTTP {r.status_code} on {self.base_url}{endpoint} params={params} body={body}"
            ) from e
        return r.json()

    async def ping(self):
        return await self._get("/ping", {})

    async def coins_markets(
        self,
        vs_currency: str = "usd",
        per_page: int = 250,
        page: int = 1,
        order: str = "market_cap_desc",
        price_change_percentage: str = "1h,24h,7d",
        sparkline: bool = False,
    ):
        params = {
            "vs_currency": vs_currency,
            "order": order,
            "per_page": per_page,
            "page": page,
            "sparkline": "true" if sparkline else "false",
            "price_change_percentage": price_change_percentage,
        }
        return await self._get("/coins/markets", params)
