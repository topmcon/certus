from app_pages import crypto_quotes
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import streamlit as st
from certus.utils.layout import render_header, render_kpis, section, footer

st.set_page_config(
    page_title="Certus",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

with st.sidebar:
    st.title("Certus")
    st.caption("TrendLab Intelligence Engine")
    st.markdown("---")
    st.success("P0 ✅  •  P1 (Shell) in progress")
    st.markdown("**Pages** live under the left “Pages” icon (📄).")

render_header(
    "Certus — TrendLab Intelligence Engine",
    "Unified analytics across crypto & equities. P1 focuses on structure; P2 adds data.",
)

render_kpis(
    {
        "Tracked Assets": "—",
        "Signals (24h)": "—",
        "Uptime": "Online",
    }
)

section("Quick Notes")
st.info("Use the **Pages** panel (left) to open **Crypto** and **Stocks**. "
        "These are structured placeholders ready for data in P2.")

st.markdown("---")
st.markdown("**Next:** Add CoinGecko (P2) and indicators/Trend Score (P3).")

footer()
