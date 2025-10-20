import os, duckdb, pandas as pd, streamlit as st

DB_PATH = os.getenv("DUCKDB_PATH", "./data/certus.duckdb")

@st.cache_data(ttl=60)
def load_quotes():
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("select * from v_top_mcap").fetchdf()
    con.close()
    return df

def render():
    st.title("Crypto Quotes (CoinGecko)")
    df = load_quotes()
    symbols = st.multiselect("Filter symbols", sorted(df["symbol"].unique()))
    if symbols:
        df = df[df["symbol"].isin(symbols)]
    st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    render()
