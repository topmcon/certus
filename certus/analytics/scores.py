from __future__ import annotations
import pandas as pd

"""
Combine signals into a normalized trend score in [-1, 1] and a tier.
Now includes weights for state tags: ema_trend_bull/bear, macd_pos/neg.
"""

def compute_scores(signals: pd.DataFrame) -> pd.DataFrame:
    df = signals.copy()

    score = df["signal_strength"].astype(float)

    has = lambda tag: df["signal_type"].str.contains(tag, na=False)

    # event boosts/penalties
    score += has("ema_bull_cross").astype(float) * 0.25
    score -= has("ema_bear_cross").astype(float) * 0.25
    score += has("macd_above_zero").astype(float) * 0.10
    score -= has("macd_below_zero").astype(float) * 0.10

    # state influences
    score += has("ema_trend_bull").astype(float) * 0.15
    score -= has("ema_trend_bear").astype(float) * 0.15
    score += has("macd_pos").astype(float) * 0.10
    score -= has("macd_neg").astype(float) * 0.10

    # RSI extremes
    score -= has("rsi_overbought").astype(float) * 0.05
    score += has("rsi_oversold").astype(float) * 0.05

    df["trend_score"] = score.clip(-1, 1)

    # tiers
    df["trend_tier"] = "neutral"
    df.loc[df["trend_score"] >= 0.50, "trend_tier"] = "bullish"
    df.loc[df["trend_score"] <= -0.25, "trend_tier"] = "bearish"

    return df[["symbol","signal_type","signal_strength","trend_score","trend_tier"]].reset_index(drop=True)
