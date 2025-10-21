import pandas as pd
import numpy as np

def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    need = {'ts','id','symbol','price'}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"compute_indicators missing columns: {missing}")

    df = df.copy().sort_values(["symbol","ts"])
    parts = []
    for sym, g in df.groupby("symbol", sort=False):
        g = g.copy()
        close = g["price"].astype(float)

        g["ema_9"]  = _ema(close, 9)
        g["ema_20"] = _ema(close, 20)

        ema12 = _ema(close, 12)
        ema26 = _ema(close, 26)
        macd = ema12 - ema26
        g["macd"] = macd
        g["macd_signal"] = _ema(macd, 9)
        g["macd_hist"] = g["macd"] - g["macd_signal"]

        g["rsi_14"] = _rsi(close, 14)
        parts.append(g)

    return pd.concat(parts, ignore_index=True)
