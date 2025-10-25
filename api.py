from __future__ import annotations
import duckdb
from fastapi import FastAPI, Query
# NOTE: no need for JSONResponse; FastAPI will encode datetimes automatically

app = FastAPI(title="Certus Trend API", version="1.0")
DB_PATH = "data/markets.duckdb"

def fetch_trends(symbol: str | None = None, limit: int = 20):
    con = duckdb.connect(DB_PATH)
    sql = """
      SELECT kind,
             ts,                                -- timestamp; FastAPI encodes to ISO8601
             symbol_clean AS symbol,
             title,
             trend_score,
             last_price,
             quote_provider
      FROM trend_feed_exploded
      WHERE symbol_clean IS NOT NULL
      {sym_clause}
      ORDER BY trend_score DESC, ts DESC
      LIMIT {lim}
    """.format(
        sym_clause="AND upper(symbol_clean)=upper(?)" if symbol else "",
        lim=max(1, min(limit, 200))
    )
    rows = con.execute(sql, [symbol] if symbol else []).fetchall()
    cols = [d[0] for d in con.description]
    con.close()
    return [dict(zip(cols, r)) for r in rows]

@app.get("/")
def root():
    return {
        "status": "ok",
        "endpoints": ["/health", "/trends?limit=5", "/trends?symbol=GNO&limit=5", "/docs"]
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/trends")
def get_trends(symbol: str | None = Query(None), limit: int = Query(20)):
    data = fetch_trends(symbol, limit)
    return {"count": len(data), "results": data}