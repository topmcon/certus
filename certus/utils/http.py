from __future__ import annotations
import time, random
import httpx
from .env import HTTP_TIMEOUT
from .logging import logger

RETRY_STATUS = {429, 500, 502, 503, 504}

def _sleep(backoff: float, attempt: int):
    # expo backoff with jitter (cap at ~20s)
    delay = min(backoff * (2 ** (attempt - 1)), 20.0) * (0.7 + 0.6 * random.random())
    time.sleep(delay)

def get_json(url: str, headers: dict | None = None, params: dict | None = None) -> dict:
    # one-shot (kept for compatibility); now uses retry client under the hood
    return get_json_retry(url, headers=headers, params=params)

def get_json_retry(url: str, headers: dict | None = None, params: dict | None = None,
                   retries: int = 4, backoff: float = 0.8) -> dict:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True) as c:
                r = c.get(url, headers=headers, params=params)
                if r.status_code in RETRY_STATUS:
                    raise httpx.HTTPStatusError(f"retryable {r.status_code}", request=r.request, response=r)
                r.raise_for_status()
                return r.json()
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            last_err = e
            logger.warning(f"[HTTP retry {attempt}/{retries}] {url} — {e}")
            if attempt >= retries: break
            _sleep(backoff, attempt)
    assert last_err is not None
    logger.error(f"[HTTP failed] {url} — {last_err}")
    raise last_err
