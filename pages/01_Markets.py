# pages/01_Markets.py
import duckdb
import pandas as pd
import streamlit as st

DB_PATH = "data/markets.duckdb"

# ---------- Layout tuning ----------
st.set_page_config(page_title="Markets", layout="wide", initial_sidebar_state="collapsed")

# Global CSS: shrink paddings, widen container, compact table option
COMPACT_CSS = """
<style>
/* Wider content area, smaller outer padding */
.main .block-container {padding-top: 0.6rem; padding-bottom: 0.6rem; max-width: 96vw;}
/* Tighter H1 */
h1 {margin: 0.3rem 0 0.6rem 0;}
/* Base table typography */
[data-testid="stDataFrame"] table, [data-testid="stDataEditor"] table {
  font-size: 0.92rem;
}
/* Compact mode overrides (activated below) */
.compact [data-testid="stDataFrame"] table, .compact [data-testid="stDataEditor"] table {
  font-size: 0.86rem;
}
.compact [data-testid="stDataFrame"] tbody tr td, .compact [data-testid="stDataFrame"] thead th {
  padding-top: 4px !important; padding-bottom: 4px !important;
}
</style>
"""
st.markdown(COMPACT_CSS, unsafe_allow_html=True)

# ---------- DB helpers ----------
@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

# Money / price / units formatters
def money_short(x):
    if x is None or pd.isna(x): return ""
    try: x = float(x)
    except: return ""
    if x >= 1e9:  return f"${x/1e9:.2f}B"
    if x >= 1e6:  return f"${x/1e6:.2f}M"
    if x >= 1e3:  return f"${x/1e3:.2f}K"
    return f"${x:,.2f}"

def price_fmt(x):
    if x is None or pd.isna(x): return ""
    try: x = float(x)
    except: return ""
    if x >= 1000: return f"${x:,.2f}"
    if x >= 1:    return f"${x:,.2f}"
    if x >= 0.1:  return f"${x:,.4f}"
    if x >= 0.01: return f"${x:,.5f}"
    if x >= 0.001:return f"${x:,.6f}"
    return f"${x:,.8f}"

def units_short(x):
    if x is None or pd.isna(x): return ""
    try: x = float(x)
    except: return ""
    if x >= 1e9:  return f"{x/1e9:.2f}B"
    if x >= 1e6:  return f"{x/1e6:.2f}M"
    if x >= 1e3:  return f"{x/1e3:.2f}K"
    return f"{x:,.0f}"

@st.cache_data(ttl=30)
def load_top_markets_with_extras(quote: str | None = None):
    con = get_con()

    tm = con.sql("SELECT * FROM top_markets").fetchdf()
    if tm.empty:
        return pd.DataFrame(columns=["market","name","price","pct_change_24h","total_volume","market_cap","symbol","trend"])

    # Base display columns
    vs = tm.get("vs_currency", pd.Series(["USD"]*len(tm))).astype(str)
    tm["market"] = tm["symbol"].str.upper() + "/" + vs.str.upper()
    tm["24h_chg_%"] = tm.get("pct_change_24h", pd.Series([None]*len(tm)))
    tm["24h_vol"]   = tm.get("total_volume", pd.Series([None]*len(tm)))
    tm["mcap"]      = tm.get("market_cap", pd.Series([None]*len(tm)))

    if quote:
        tm = tm[tm["market"].str.endswith("/" + quote.upper())]

    # Optional extras from markets (latest row per symbol)
    cols = con.sql("PRAGMA table_info('markets')").fetchdf()["name"].tolist()
    extras = [c for c in ["high_24h","low_24h","circulating_supply","total_supply","max_supply"] if c in cols]
    if extras:
        sql_cols = ", ".join(["m.symbol"] + [f"m.{c}" for c in extras])
        extra_df = con.sql(f"""
            WITH last_ts AS (SELECT symbol, MAX(ts) ts FROM markets GROUP BY 1)
            SELECT {sql_cols}
            FROM markets m
            JOIN last_ts t ON m.symbol=t.symbol AND m.ts=t.ts
        """).fetchdf()
        tm = tm.merge(extra_df, on="symbol", how="left")

    # Sparklines (last 50)
    hist = con.sql("""
        SELECT symbol, ts, price,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts DESC) rn
        FROM markets
    """).fetchdf()
    hist = hist[hist["rn"] <= 50].sort_values(["symbol","ts"])
    spark = hist.groupby("symbol")["price"].apply(list).rename("trend")
    tm = tm.merge(spark, on="symbol", how="left")

    # Guarantee valid trend lists (avoid chart errors)
    tm["trend"] = tm.apply(lambda r: r["trend"] if isinstance(r["trend"], list) and len(r["trend"]) >= 2
                           else ([r["price"]]*10 if pd.notna(r.get("price")) else [0,0]), axis=1)

    # Presentation fields
    tm["PRICE"]  = tm.get("price", pd.Series([None]*len(tm))).map(price_fmt)
    tm["24H VOL"] = tm["24h_vol"].map(money_short)
    tm["MKT CAP"] = tm["mcap"].map(money_short)

    if "high_24h" in tm.columns: tm["HIGH 24H"] = tm["high_24h"].map(price_fmt)
    if "low_24h"  in tm.columns: tm["LOW 24H"]  = tm["low_24h"].map(price_fmt)
    if "circulating_supply" in tm.columns: tm["CIRC SUPPLY"] = tm["circulating_supply"].map(units_short)
    if "total_supply" in tm.columns:       tm["TOTAL SUPPLY"]= tm["total_supply"].map(units_short)
    if "max_supply" in tm.columns:         tm["MAX SUPPLY"]  = tm["max_supply"].map(units_short)

    if "market_cap" in tm.columns:
        tm = tm.sort_values("market_cap", ascending=False)
    return tm

# ---------- UI ----------
st.title("Markets")

# Controls row (labels collapsed to save vertical space)
c1, c2, c3, c4, c5 = st.columns([3,1,1,0.9,0.8])
with c1:
    q = st.text_input("Search", placeholder="e.g. BTC, ETH, SOL…", label_visibility="collapsed")
with c2:
    quote = st.selectbox("Quote", ["All","USD","USDT","USDC","EUR"], index=0)
with c3:
    min_vol = st.number_input("Min 24h Vol (USD)", value=0.0, step=1000.0, min_value=0.0, label_visibility="collapsed")
with c4:
    compact = st.toggle("Compact", value=True)
with c5:
    st.button("Refresh", on_click=lambda: st.cache_data.clear())

# Apply compact class to body
if compact:
    st.markdown('<div class="compact"></div>', unsafe_allow_html=True)

df = load_top_markets_with_extras(None if quote == "All" else quote)

# Search + volume filters
if not df.empty and q:
    s = q.lower()
    mask = pd.Series(False, index=df.index)
    for col in ["symbol","name","id"]:
        if col in df.columns:
            mask |= df[col].astype(str).str.lower().str.contains(s)
    df = df[mask]
if not df.empty and min_vol > 0 and "24h_vol" in df.columns:
    df = df[df["24h_vol"].fillna(0) >= min_vol]

# Columns and dynamic height
cols = ["market","name","PRICE","24h_chg_%","trend","24H VOL","MKT CAP"]
for opt in ["HIGH 24H","LOW 24H","CIRC SUPPLY","TOTAL SUPPLY","MAX SUPPLY"]:
    if opt in df.columns: cols.append(opt)

view = pd.DataFrame() if df.empty else df[cols].rename(columns={
    "market":"MARKET","name":"NAME / ID","24h_chg_%":"24H CHG","trend":"24H TREND"
})

# Compute a sensible table height based on rows shown (compact vs normal)
rows = len(view)
row_px = 28 if compact else 34
header_px = 52
pad_px = 120
table_height = min(max(12, rows), 30) * row_px + header_px + pad_px

# Column widths: give sparkline more space
cfg = {
    "MARKET": st.column_config.TextColumn("MARKET", width="small"),
    "NAME / ID": st.column_config.TextColumn("NAME / ID", width=220),
    "PRICE": st.column_config.TextColumn("PRICE", width=110),
    "24H CHG": st.column_config.NumberColumn("24H CHG", format="%.2f%%", width=100),
    "24H TREND": st.column_config.LineChartColumn("24H TREND", width=520 if compact else 560),
    "24H VOL": st.column_config.TextColumn("24H VOL", width=120),
    "MKT CAP": st.column_config.TextColumn("MKT CAP", width=120),
}
for txt in ["HIGH 24H","LOW 24H","CIRC SUPPLY","TOTAL SUPPLY","MAX SUPPLY"]:
    if txt in view.columns:
        cfg[txt] = st.column_config.TextColumn(txt, width=120)

st.dataframe(
    view,
    width="stretch",
    height=table_height,
    hide_index=True,
    column_config=cfg
)
