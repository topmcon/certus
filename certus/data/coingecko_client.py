from __future__ import annotations
import httpx
from typing import Any, Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from httpx import HTTPError, TimeoutException
import os

PUBLIC_BASE = "https://api.coingecko.com/api/v3"

def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutException, HTTPError)):
        resp = getattr(exc, "response", None)
        if resp is None:
            return True
        return 500 <= resp.status_code < 600
    return False

class CoinGeckoClient:
    def __init__(self, base_url: str | None = None, api_key: str | None = None, timeout: float = 30.0):
        self.api_key = api_key or os.getenv("CG_API_KEY")
        self.base_url = (base_url or os.getenv("CG_BASE_URL") or PUBLIC_BASE) if self.api_key else PUBLIC_BASE
        headers = {"accept": "application/json"}
        if self.api_key:
            headers["x-cg-pro-api-key"] = self.api_key
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, headers=headers)

    async def close(self):
        await self._client.aclose()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(0.5, max=6),
           retry=retry_if_exception(_should_retry), reraise=True)
    async def _get(self, path: str, params: Dict[str, Any]):
        r = await self._client.get(path, params=params)
        if r.status_code >= 400:
            # show helpful debug
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise httpx.HTTPStatusError(
                f"{r.status_code} {self.base_url}{path} params={params} body={detail}",
                request=r.request, response=r
            )
        return r.json()

    async def coins_markets(
        self,
        vs_currency: str = "usd",
        order: str = "market_cap_desc",
        per_page: int = 25,
        page: int = 1,
        price_change_percentage: str = "1h,24h,7d",
        sparkline: bool = False
    ) -> List[Dict[str, Any]]:
        params = {
            "vs_currency": vs_currency,
            "order": order,
            "per_page": per_page,
            "page": page,
            "sparkline": str(sparkline).lower(),
            "price_change_percentage": price_change_percentage,  # critical: commas, not %2C
        }
        return await self._get("/coins/markets", params)
