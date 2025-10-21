from __future__ import annotations
import pandas as pd

"""
Derive event-like signals + always-on state tags from the indicators layer.

Expected input columns (any extras are ignored):
  ['id','symbol','price','rsi_14','ema_9','ema_20','macd','ts'?]

Notes
- 'id' in your dataset is an asset id string (e.g., 'bitcoin'), not a row counter.
- For chronological logic (prev_*), we prefer 'ts' if present. Otherwise we
  fall back to a stable synthetic order.
"""

# ---------- helpers ----------

def _pick_order_column(df: pd.DataFrame) -> str:
    if "ts" in df.columns:
        return "ts"
    if "id" in df.columns and pd.api.types.is_integer_dtype(df["id"]):
        return "id"
    df["_rowpos"] = range(len(df))
    return "_rowpos"

def add_prev_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    order_col = _pick_order_column(df)
    df = df.sort_values(["symbol", order_col], ascending=[True, True]).copy()
    for c in cols:
        if c in df.columns:
            df[f"prev_{c}"] = df.groupby("symbol")[c].shift(1)
    if "_rowpos" in df.columns:
        df = df.drop(columns=["_rowpos"])
    return df

# ---------- event rules (fire on transitions) ----------

def ema_cross_signals(df: pd.DataFrame) -> pd.Series:
    up = (df["ema_9"] > df["ema_20"]) & (df["prev_ema_9"] <= df["prev_ema_20"])
    dn = (df["ema_9"] < df["ema_20"]) & (df["prev_ema_9"] >= df["prev_ema_20"])
    s = pd.Series(pd.NA, index=df.index, dtype="object")
    s = s.mask(up, "ema_bull_cross").mask(dn, "ema_bear_cross")
    return s

def macd_zero_cross(df: pd.DataFrame) -> pd.Series:
    up = (df["macd"] > 0) & (df["prev_macd"] <= 0)
    dn = (df["macd"] < 0) & (df["prev_macd"] >= 0)
    s = pd.Series(pd.NA, index=df.index, dtype="object")
    s = s.mask(up, "macd_above_zero").mask(dn, "macd_below_zero")
    return s

# ---------- state tags (always reflect current condition) ----------

def ema_trend_state(df: pd.DataFrame) -> pd.Series:
    s = pd.Series(pd.NA, index=df.index, dtype="object")
    s = s.mask(df["ema_9"] > df["ema_20"], "ema_trend_bull")
    s = s.mask(df["ema_9"] < df["ema_20"], "ema_trend_bear")
    return s

def macd_state(df: pd.DataFrame) -> pd.Series:
    s = pd.Series(pd.NA, index=df.index, dtype="object")
    s = s.mask(df["macd"] > 0, "macd_pos")
    s = s.mask(df["macd"] < 0, "macd_neg")
    return s

def rsi_bucket(df: pd.DataFrame) -> pd.Series:
    s = pd.Series("rsi_neutral", index=df.index, dtype="object")
    s = s.mask(df["rsi_14"] < 30, "rsi_oversold")
    s = s.mask(df["rsi_14"] > 70, "rsi_overbought")
    return s

# ---------- public API ----------

def compute_signals(indicators: pd.DataFrame) -> pd.DataFrame:
    need = ["id","symbol","price","rsi_14","ema_9","ema_20","macd"]
    df = indicators.copy()
    for c in need:
        if c not in df.columns:
            df[c] = pd.NA

    df = add_prev_cols(df, ["ema_9","ema_20","macd"])

    # events
    df["sig_ema_cross"]  = ema_cross_signals(df)
    df["sig_macd_zero"]  = macd_zero_cross(df)
    # states
    df["sig_ema_state"]  = ema_trend_state(df)
    df["sig_macd_state"] = macd_state(df)
    df["sig_rsi"]        = rsi_bucket(df)

    # pick latest row per symbol (prefer ts desc)
    order_col = "ts" if "ts" in df.columns else None
    if order_col is None:
        df["_rowpos"] = range(len(df))
        order_col = "_rowpos"
    latest_idx = df.groupby("symbol")[order_col].idxmax()
    out = df.loc[latest_idx].sort_values("symbol").copy()
    if "_rowpos" in out.columns:
        out.drop(columns=["_rowpos"], inplace=True)

    # compact aggregated tag string
    def join_active(row) -> str:
        tags = []
        for c in ["sig_ema_cross","sig_macd_zero","sig_ema_state","sig_macd_state","sig_rsi"]:
            v = row.get(c)
            if pd.notna(v):
                tags.append(str(v))
        return ",".join(tags) if tags else "neutral"

    out["signal_type"] = out.apply(join_active, axis=1)

    # heuristic strength in [-1, 1] — keep prior weighting, state implied via comparisons
    strength = 0.0
    strength += (out["ema_9"] > out["ema_20"]).astype(float) * 0.35
    strength -= (out["ema_9"] < out["ema_20"]).astype(float) * 0.35
    strength += ((out["rsi_14"] >= 50) & (out["rsi_14"] <= 60)).astype(float) * 0.10
    strength += (out["rsi_14"] < 30).astype(float) * 0.05     # bounce potential
    strength -= (out["rsi_14"] > 70).astype(float) * 0.10     # overbought risk
    strength += (out["macd"] > 0).astype(float) * 0.20
    strength -= (out["macd"] < 0).astype(float) * 0.20
    out["signal_strength"] = strength.clip(-1, 1)

    return out[["id","symbol","price","rsi_14","ema_9","ema_20","macd","signal_type","signal_strength"]].reset_index(drop=True)
