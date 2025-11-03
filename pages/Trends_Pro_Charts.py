import duckdb, streamlit as st
import plotly.express as px

st.set_page_config(page_title="Certus — Charts", layout="wide")
st.title("📊 Certus — Price & Changes")

DB = "data/markets.duckdb"

def q(sql, *params):
    con = duckdb.connect(DB)
    con.execute("SET TimeZone='UTC';")
    df = con.execute(sql, params).fetchdf()
    con.close()
    return df

symbols = q("select distinct symbol from quotes_ts order by 1")["symbol"].tolist()
symbol = st.selectbox("Symbol", symbols)

colA, colB, colC, colD, colE, colF = st.columns(6)
pw = q("select * from price_windows where symbol = ?", symbol)
if not pw.empty:
    row = pw.iloc[0]
    colA.metric("Price", f"{row['price_now']:.4f}")
    for col, key, lbl in [(colB,"pct_1h","1h"), (colC,"pct_24h","24h"),
                          (colD,"pct_48h","48h"), (colE,"pct_72h","72h"), (colF,"pct_7d","7d")]:
        val = row.get(key)
        col.metric(lbl, ("{:+.2f}%".format(val) if val==val else "—"))

ts = q("""
  select ts_recorded as ts, price
  from quotes_ts
  where symbol = ?
  order by ts
""", symbol)

fig = px.line(ts, x="ts", y="price", title=f"{symbol} — Price over Time")
st.plotly_chart(fig, use_container_width=True)
