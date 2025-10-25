from __future__ import annotations
from typing import Any, Dict
from certus.utils.env import get_env
from certus.utils.http import get_json

BASE = "https://developers.coinmarketcal.com/v1"

def fetch_events(max_items: int = 20, page: int = 1, days_ahead: int = 0) -> Dict[str, Any]:
    api_key = get_env("COINMARKETCAL_API_KEY")
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    params = {"max": max(1, min(max_items, 50)), "page": max(1, page)}
    j = get_json(f"{BASE}/events", headers=headers, params=params)
    body = j.get("body")
    if isinstance(body, list):
        return {"events": body, "_metadata": j.get("_metadata")}
    if isinstance(body, dict) and isinstance(body.get("events"), list):
        return {"events": body["events"], "_metadata": j.get("_metadata")}
    return j
