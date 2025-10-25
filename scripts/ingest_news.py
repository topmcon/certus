from __future__ import annotations
import duckdb, datetime as dt
from typing import Any
from certus.ingest.cryptopanic import latest_posts

def norm(item: dict[str, Any]) -> tuple:
    id_ = str(item.get("id"))
    pub = item.get("published_at")
    published_at = None
    if pub:
        try: published_at = dt.datetime.fromisoformat(pub.replace("Z","+00:00"))
        except: published_at = None
    title = item.get("title")
    url = item.get("url")
    domain = item.get("domain")
    source = item.get("source") or (item.get("source_domain") or "")
    kind = item.get("kind")
    votes = 0
    v = item.get("votes") or {}
    if isinstance(v, dict): votes = int(v.get("total") or 0)
    # currencies could be list of dicts or strings
    cur = item.get("currencies")
    if isinstance(cur, list):
        def pick(x): 
            if isinstance(x, dict): return x.get("code") or x.get("symbol") or x.get("currency")
            return str(x)
        currencies = ",".join([str(pick(x)).upper() for x in cur if x])
    else:
        currencies = str(cur or "")
    raw = item
    return (id_, published_at, title, url, domain, source, currencies, kind, votes, raw)

rows = latest_posts(public=True, page=1).get("results", [])[:25]
tuples = [norm(x) for x in rows if x.get("id")]

con = duckdb.connect("data/markets.duckdb")
con.execute("""
  CREATE TABLE IF NOT EXISTS news_cryptopanic (
    id            VARCHAR PRIMARY KEY,
    published_at  TIMESTAMP,
    title         VARCHAR,
    url           VARCHAR,
    domain        VARCHAR,
    source        VARCHAR,
    currencies    VARCHAR,
    kind          VARCHAR,
    votes         BIGINT,
    raw           JSON,
    ingested_at   TIMESTAMP DEFAULT now()
  )
""")
for t in tuples:
    con.execute("DELETE FROM news_cryptopanic WHERE id = ?", [t[0]])
    con.execute("""
      INSERT INTO news_cryptopanic (id,published_at,title,url,domain,source,currencies,kind,votes,raw)
      VALUES (?,?,?,?,?,?,?,?,?,to_json(?))
    """, t)
print(f"news_cryptopanic inserted: {len(tuples)}")
con.close()
