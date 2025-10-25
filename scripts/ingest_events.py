from __future__ import annotations
import duckdb, datetime as dt
from certus.ingest.coinmarketcal import fetch_events

def page(p:int): 
    j = fetch_events(max_items=20, page=p, days_ahead=45)
    return j.get("events", j if isinstance(j, list) else [])

def pick_coin(ev): 
    coins = ev.get("coins") or ev.get("coinsList") or []
    if coins: 
        c = coins[0]; return (c.get("symbol") or c.get("code") or c.get("ticker")), c.get("name")
    return None, None

def norm(ev):
    sym,name = pick_coin(ev)
    iso = (ev.get("date_event") or ev.get("event_date") or "") or None
    ts = None
    if iso:
        try: ts = dt.datetime.fromisoformat(str(iso).replace("Z","+00:00"))
        except: ts = None
    return (
        str(ev.get("id")),
        ev.get("title"),
        ev.get("description") or ev.get("source_description"),
        sym, name, ts,
        bool(ev.get("is_hot") or ev.get("hot")),
        ev.get("source") or "", ev.get("proof") or "",
        ev.get("url") or ev.get("link") or "",
        ev
    )

evs = page(1) + page(2)
rows = [norm(e) for e in evs if e.get("id")][:40]

con = duckdb.connect("data/markets.duckdb")
con.execute("""CREATE TABLE IF NOT EXISTS events_coinmarketcal (
  id VARCHAR PRIMARY KEY, title VARCHAR, description VARCHAR,
  coin_symbol VARCHAR, coin_name VARCHAR, date_event TIMESTAMP,
  is_hot BOOLEAN, source VARCHAR, proof VARCHAR, url VARCHAR,
  raw JSON, ingested_at TIMESTAMP DEFAULT now()
)""")
for r in rows:
    con.execute("DELETE FROM events_coinmarketcal WHERE id = ?", [r[0]])
    con.execute("""INSERT INTO events_coinmarketcal
      (id,title,description,coin_symbol,coin_name,date_event,is_hot,source,proof,url,raw)
      VALUES (?,?,?,?,?,?,?,?,?,?,to_json(?))""", r)
print("events_coinmarketcal inserted:", len(rows))
con.close()
