# certus/analytics/scoring.py
import pandas as pd
import numpy as np

def trend_score(row: pd.Series) -> float:
    score = 0.0
    # RSI contribution (40–60 neutral band)
    rsi = row.get("rsi_14")
    if pd.notna(rsi):
        score += np.clip((rsi - 50) * 1.5, -30, 30)  # ±30 max from RSI

    # EMA alignment
    ema9, ema20, price = row.get("ema_9"), row.get("ema_20"), row.get("price")
    if pd.notna(ema9) and pd.notna(ema20) and pd.notna(price):
        if ema9 > ema20:
            score += 20
        else:
            score -= 10
        # distance of price from ema20 (momentum push)
        dist = (price - ema20) / ema20 if ema20 else 0
        score += np.clip(dist * 100, -15, 15)

    # MACD momentum (hist sign)
    hist = row.get("macd_hist")
    if pd.notna(hist):
        score += np.clip(hist * 10, -15, 15)

    return float(np.clip(score + 50, 0, 100))  # baseline 50 → clamp 0..100
