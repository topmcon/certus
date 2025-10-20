import streamlit as st
from certus.utils.layout import render_header, render_kpis, section

st.set_page_config(page_title="Crypto • Certus", page_icon="🪙", layout="wide")

render_header("🪙 Crypto Dashboard", "Market overview, watchlists, and signals (P2 will add live data).")

render_kpis(
    {
        "Top Gainers": "—",
        "Top Losers": "—",
        "Volatility (24h)": "—",
    }
)

section("Watchlists", "Pin assets to monitor in real time (coming in P2).")
st.placeholder()  # reserved: table/list

section("Signals", "Momentum / trend signals appear here once indicators are wired in P3.")
st.info("No signals yet — add indicators in P3 (EMA, RSI, MACD, ATR).")

section("Notes")
st.text_area("Scratchpad", placeholder="e.g., BTC holding EMA20; wait for pullback to 9 & bounce...")
