# certus/analytics/indicators.py
from __future__ import annotations
import pandas as pd
import numpy as np

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0.0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def macd_line(series: pd.Series, fast: int = 12, slow: int = 26) -> pd.Series:
    return ema(series, fast) - ema(series, slow)

def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    m = macd_line(series, fast, slow)
    sig = ema(m, signal)
    hist = m - sig
    return m, sig, hist

def classify_trend(rsi_val, ema9, ema20, macd_val):
    if pd.notna(rsi_val) and pd.notna(ema9) and pd.notna(ema20) and pd.notna(macd_val):
        if rsi_val > 60 and ema9 > ema20 and macd_val > 0:
            return "Bullish"
        if rsi_val < 40 and ema9 < ema20 and macd_val < 0:
            return "Bearish"
    return "Neutral"

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expects columns: id, symbol, price, last_updated (timestamp).
    Computes rolling indicators per (id, symbol) ordered by time.
    """
    if "last_updated" not in df.columns:
        raise ValueError("compute_indicators requires 'last_updated' column")
    df = df.sort_values(["symbol", "last_updated"]).copy()

    def _apply(group: pd.DataFrame):
        px = group["price"]
        out = group[["id", "symbol", "price", "last_updated"]].copy()
        out["rsi_14"] = rsi(px, 14)
        out["ema_9"] = ema(px, 9)
        out["ema_20"] = ema(px, 20)
        m, s, h = macd(px)
        out["macd"] = m
        out["macd_signal"] = s
        out["macd_hist"] = h
        out["trend"] = [
            classify_trend(r, e9, e20, mm)
            for r, e9, e20, mm in zip(out["rsi_14"], out["ema_9"], out["ema_20"], out["macd"])
        ]
        out.rename(columns={"last_updated": "ts"}, inplace=True)
        return out

    res = df.groupby(["symbol"], group_keys=False).apply(_apply)
    return res
