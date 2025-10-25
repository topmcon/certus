# certus/utils/layout.py
import streamlit as st
import pandas as pd

DEFAULT_COLUMNS = ["market", "name", "price", "24h_chg_%", "trend", "24h_vol", "mcap", "high_24h", "low_24h"]

def column_picker(df: pd.DataFrame, key: str = "colpick"):
    st.subheader("Columns")
    # Build friendly label map & available cols superset
    label_map = {
        "market": "Market (Symbol)",
        "name": "Name",
        "price": "Price",
        "24h_chg_%": "24h Change %",
        "trend": "Trend",
        "24h_vol": "24h Volume",
        "mcap": "Market Cap",
        "high_24h": "24h High",
        "low_24h": "24h Low",
        "rsi_14": "RSI(14)",
        "ema_9": "EMA(9)",
        "ema_20": "EMA(20)",
        "macd": "MACD",
        "trend_score": "Trend Score",
    }
    candidates = [c for c in label_map.keys() if c in df.columns]
    default = [c for c in DEFAULT_COLUMNS if c in df.columns]
    chosen = st.multiselect("Select columns", options=candidates, default=default, key=key, format_func=lambda c: label_map.get(c, c))
    if not chosen:
        st.info("No columns selected — showing all.")
        return df
    return df[chosen]
