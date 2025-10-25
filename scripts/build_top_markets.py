#!/usr/bin/env python3
import duckdb, time

DB_PATH = "data/markets.duckdb"

def build_top_markets():
    con = duckdb.connect(DB_PATH)
    print("[Certus] Building Top 500 Trending…")

    cols = con.sql("PRAGMA table_info('markets')").fetchdf()["name"].tolist()
    vol_col = "total_volume" if "total_volume" in cols else "volume_24h" if "volume_24h" in cols else None
    cap_col = "market_cap" if "market_cap" in cols else None
    if not vol_col or not cap_col:
        raise RuntimeError("markets table missing volume or market_cap")

    # prefer API % if present (in_currency), fall back to generic, else compute
    pct_api  = "price_change_percentage_24h"
    has_pct  = pct_api in cols

    con.sql("DROP TABLE IF EXISTS top_markets;")

    now_ms = int(time.time() * 1000)
    one_day_ms = 24 * 60 * 60 * 1000

    con.sql(f"""
        CREATE TABLE top_markets AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts DESC) rn,
                   LAG(price) OVER (PARTITION BY symbol ORDER BY ts) AS prev_price
            FROM markets
        ),
        latest AS (
            SELECT *
            FROM ranked
            WHERE rn = 1
              AND ts > {now_ms - one_day_ms}
              AND {cap_col} > 10_000_000
              AND {vol_col} > 100_000
        )
        SELECT
            ts,
            id,
            UPPER(symbol) AS symbol,
            COALESCE(name, id, symbol) AS name,
            vs_currency,
            price,
            {cap_col} AS market_cap,
            {vol_col} AS total_volume,
            /* prefer API 24h %, else compute */
            COALESCE({pct_api if has_pct else 'NULL'},
                     ((price - prev_price) / NULLIF(prev_price,0)) * 100) AS pct_change_24h
        FROM latest
        ORDER BY market_cap DESC
        LIMIT 500;
    """)

    n = con.sql("SELECT COUNT(*) AS n FROM top_markets").fetchone()[0]
    con.close()
    print(f"[✔] Top 500 Trending updated with {n} rows.")

if __name__ == "__main__":
    build_top_markets()
