import streamlit as st

st.set_page_config(page_title="Certus", page_icon="📈", layout="wide")

# Sidebar
st.sidebar.title("Certus")
st.sidebar.caption("TrendLab Intelligence Engine")
st.sidebar.markdown("---")
st.sidebar.success("P0 → P1 Shell ready")

# Header
st.title("📈 Certus — TrendLab Intelligence Engine")
st.write("This is the dashboard shell. Data connectors will arrive in P2.")

# Layout example
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Tracked Assets", "—")
with col2:
    st.metric("Signals (24h)", "—")
with col3:
    st.metric("Uptime", "Online")

st.markdown("### Quick Notes")
st.info("Use the sidebar to navigate pages. Crypto/Stocks pages are placeholders.")
st.markdown("---")
st.markdown("Next steps: add CoinGecko (P2), indicators & Trend Score (P3).")