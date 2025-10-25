from __future__ import annotations
import sys, duckdb, argparse, pathlib, datetime as dt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--symbol", type=str, default="", help="Filter by symbol (e.g., BTC, SOL, AAPL)")
    ap.add_argument("--csv", type=str, default="", help="Optional: path to export CSV")
    args = ap.parse_args()

    con = duckdb.connect("data/markets.duckdb")
    con.execute("SET TimeZone='UTC';")

    sql = """
      SELECT kind, ts, symbol_clean AS symbol, 
             left(coalesce(title,''), 90) AS title,
             trend_score, last_price, quote_provider
      FROM trend_feed_exploded
      WHERE symbol_clean IS NOT NULL
      {sym_clause}
      ORDER BY trend_score DESC, ts DESC
      LIMIT {lim}
    """.format(
        sym_clause = "AND upper(symbol_clean)=upper(?)" if args.symbol else "",
        lim = max(1, min(args.limit, 200))
    )

    df = con.execute(sql, [args.symbol] if args.symbol else []).fetchdf()
    con.close()

    if args.csv:
        out = pathlib.Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        print(f"Exported {len(df)} rows → {out}")
    else:
        # pretty print
        if df.empty:
            print("No rows.")
            return
        print(df.to_string(index=False))

if __name__ == "__main__":
    main()
