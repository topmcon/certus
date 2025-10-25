from __future__ import annotations
import httpx
from .env import HTTP_TIMEOUT

def get_json(url: str, headers: dict | None = None, params: dict | None = None) -> dict:
    with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True) as c:
        r = c.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()
