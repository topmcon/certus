import subprocess
import pandas as pd
import duckdb
import streamlit as st

st.set_page_config(page_title="Certus — Trends", layout="wide")
st.title("📈 Certus — Trend Feed")

DB = "data/markets.duckdb"

@st.cache_data(ttl=60)
def load_data(limit:int=100, symbol:str|None=None):
    con = duckdb.connect(DB)
    con.execute("SET TimeZone='UTC';")
    q = """
      SELECT kind, ts, symbol_clean AS symbol,
             title, trend_score, last_price, quote_provider
      FROM trend_feed_exploded
      WHERE symbol_clean IS NOT NULL
      {sym_clause}
      ORDER BY trend_score DESC, ts DESC
      LIMIT {lim}
    """.format(
        sym_clause = "AND upper(symbol_clean)=upper(?)" if symbol else "",
        lim = max(1, min(limit, 1000)),
    )
    df = con.execute(q, [symbol] if symbol else []).fetchdf()
    con.close()
    return df

# Sidebar controls
with st.sidebar:
    st.header("Filters")
    sym = st.text_input("Symbol filter (e.g., BTC, GNO, AAPL)", "")
    limit = st.slider("Rows", 10, 300, 100, 10)
    st.divider()
    if st.button("�� Refresh pipeline now"):
        with st.status("Refreshing…", expanded=False) as s:
            res = subprocess.run(
                ["python", "scripts/refresh_trends.py"],
                capture_output=True, text=True
            )
            if res.returncode == 0:
                s.update(label="✅ Refresh done", state="complete")
                st.toast("Refresh complete", icon="✅")
                load_data.clear()
            else:
                s.update(label="⚠️ Refresh had errors", state="error")
                st.code(res.stderr or res.stdout)

# Main table + KPIs
df = load_data(limit=limit, symbol=(sym.strip().upper() or None))
c1, c2, c3 = st.columns(3)
c1.metric("Rows", len(df))
c2.metric("Kinds", df["kind"].nunique() if not df.empty else 0)
c3.metric("Symbols", df["symbol"].nunique() if not df.empty else 0)

st.dataframe(df, width="stretch", height=520)

# Simple symbol popularity chart
if not df.empty:
    counts = df["symbol"].value_counts().head(15).sort_values(ascending=False)
    st.bar_chart(counts)
