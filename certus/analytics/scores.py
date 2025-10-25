# certus/analytics/scores.py
from __future__ import annotations
import pandas as pd

def score_row(trend: str, rsi: float, macd_hist: float) -> float:
    base = {"Bullish": 70, "Neutral": 50, "Bearish": 30}.get(trend, 50)
    rsi_adj = ((rsi or 50) - 50) * 0.5
    macd_adj = (macd_hist or 0) * 10.0
    return float(round(base + rsi_adj + macd_adj, 1))

def build_scores(ind_df: pd.DataFrame) -> pd.DataFrame:
    # Keep last row per symbol
    last = ind_df.sort_values("ts").groupby("symbol").tail(1).copy()
    last["trend_score"] = [
        score_row(t, r, h) for t, r, h in zip(last["trend"], last["rsi_14"], last["macd_hist"])
    ]
    return last[["id", "symbol", "price", "trend", "trend_score", "ts"]]
