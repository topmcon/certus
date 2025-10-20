import streamlit as st
from certus.utils.layout import render_header, render_kpis, section

st.set_page_config(page_title="Stocks • Certus", page_icon="📊", layout="wide")

render_header("📊 Stocks Dashboard", "Equities overview, watchlists, and signals (P2+).")

render_kpis(
    {
        "Market Breadth": "—",
        "Most Active": "—",
        "New Highs/Lows": "—",
    }
)

section("Watchlists", "Curate tickers to track intraday and swing setups.")
st.placeholder()  # reserved: table/list

section("Signals", "Rule-based entries/exits populate here in P3.")
st.info("Signals will activate once Trend Score and rules are configured.")

section("Notes")
st.text_area("Scratchpad", placeholder="e.g., AAPL above VWAP; watch for EMA9/20 pullback + bounce.")
