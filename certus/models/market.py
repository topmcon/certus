from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional

class MarketQuote(BaseModel):
    ts: int = Field(..., description="unix ms")
    id: str
    symbol: str
    name: str
    vs_currency: str
    price: float
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    pct_change_24h: Optional[float] = None
