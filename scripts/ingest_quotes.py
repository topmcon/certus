from __future__ import annotations
import duckdb, datetime as dt
from certus.ingest.finnhub_client import quote as fh_quote
from certus.ingest.alphavantage_client import global_quote as av_quote

FH_SYMBOLS = ["AAPL","MSFT","TSLA"]
AV_SYMBOLS = ["IBM","AAPL"]

def upsert_fh(con, sym):
    j = fh_quote(sym)
    t = (sym, float(j.get("c") or 0), float(j.get("h") or 0), float(j.get("l") or 0),
         float(j.get("o") or 0), float(j.get("pc") or 0), int(j.get("t") or 0), j)
    con.execute("DELETE FROM quotes_finnhub WHERE symbol = ?", [sym])
    con.execute("""
      INSERT INTO quotes_finnhub (symbol,price,high,low,open,prev_close,t_unix_ms,raw)
      VALUES (?,?,?,?,?,?,?,to_json(?))
    """, t)

def upsert_av(con, sym):
    j = av_quote(sym)
    g = j.get("Global Quote", {})
    t = (sym,
         float(g.get("05. price") or 0),
         float(g.get("03. high") or 0),
         float(g.get("04. low") or 0),
         float(g.get("02. open") or 0),
         float(g.get("08. previous close") or 0),
         float(g.get("06. volume") or 0),
         None,
         j)
    con.execute("DELETE FROM quotes_av WHERE symbol = ?", [sym])
    con.execute("""
      INSERT INTO quotes_av (symbol,price,high,low,open,prev_close,volume,ts,raw)
      VALUES (?,?,?,?,?,?,?,?,to_json(?))
    """, t)

con = duckdb.connect("data/markets.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS quotes_finnhub (symbol VARCHAR, price DOUBLE, high DOUBLE, low DOUBLE, open DOUBLE, prev_close DOUBLE, t_unix_ms BIGINT, raw JSON, ingested_at TIMESTAMP DEFAULT now())")
con.execute("CREATE TABLE IF NOT EXISTS quotes_av (symbol VARCHAR, price DOUBLE, high DOUBLE, low DOUBLE, open DOUBLE, prev_close DOUBLE, volume DOUBLE, ts TIMESTAMP, raw JSON, ingested_at TIMESTAMP DEFAULT now())")

for s in FH_SYMBOLS: upsert_fh(con, s)
for s in AV_SYMBOLS: upsert_av(con, s)
print(f"quotes_finnhub upserted: {len(FH_SYMBOLS)}; quotes_av upserted: {len(AV_SYMBOLS)}")
con.close()
