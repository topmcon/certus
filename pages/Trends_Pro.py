import duckdb, streamlit as st

st.set_page_config(page_title="Certus — Trends Pro", layout="wide")
st.title("⚡️ Certus — Trends (Pro)")

DB = "data/markets.duckdb"

def q(sql, *params):
    con = duckdb.connect(DB)
    con.execute("SET TimeZone='UTC';")
    df = con.execute(sql, params).fetchdf()
    con.close()
    return df

# Available categories from watchlist-scoped categorized view
cats = q("select distinct category from trend_feed_categorized order by 1")["category"].tolist()

with st.sidebar:
    st.subheader("Filters")
    sel_cats = st.multiselect("Categories", options=cats, default=cats)
    limit = st.slider("Rows per section", 5, 100, 20, 5)
    st.caption("Universe: Watchlist only")

def where_clause():
    if sel_cats:
        return "WHERE category IN (" + ",".join(["?"]*len(sel_cats)) + ")"
    return ""

def fetch_section(order_sql: str):
    base = f"""
      SELECT ts, symbol, category,
             LEFT(title, 120) AS title,
             trend_score, priority_score, last_price, quote_provider, url, source
      FROM trend_feed_categorized
      {where_clause()}
      {order_sql}
      LIMIT ?
    """
    params = (*sel_cats, limit) if sel_cats else (limit,)
    return q(base, *params)

tab_top, tab_trend, tab_recent = st.tabs(["🏆 Top", "📈 Trending", "🕒 Recent"])

with tab_top:
    df = fetch_section("ORDER BY priority_score DESC, ts DESC")
    st.metric("Rows", len(df))
    st.dataframe(df, width="stretch", height=420)

with tab_trend:
    df = fetch_section("ORDER BY trend_score DESC, ts DESC")
    st.metric("Rows", len(df))
    st.dataframe(df, width="stretch", height=420)

with tab_recent:
    df = fetch_section("ORDER BY ts DESC")
    st.metric("Rows", len(df))
    st.dataframe(df, width="stretch", height=420)
