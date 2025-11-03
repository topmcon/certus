from __future__ import annotations
import subprocess, datetime, duckdb, sys

def run(cmd: str):
    print(f"\n[run] {cmd}")
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Command failed: {cmd}\n{res.stderr}")
    else:
        print(res.stdout.strip())

def main():
    t0 = datetime.datetime.utcnow()
    print("== Certus Trend Refresh started", t0.isoformat(), "==")

    run("python scripts/ingest_news.py")
    run("python scripts/ingest_events.py")
    run("python scripts/ingest_quotes.py")
    run("python scripts/run_migrations.py")

    # NEW: append a time-series snapshot point
    run("python scripts/snapshot_quotes_ts.py")

    con = duckdb.connect("data/markets.duckdb")
    con.execute("DROP TABLE IF EXISTS trend_feed_snap;")
    con.execute("CREATE TABLE trend_feed_snap AS SELECT * FROM trend_feed_enriched;")
    rows = con.sql("SELECT COUNT(*) FROM trend_feed_snap").fetchone()[0]
    con.close()

    t1 = datetime.datetime.utcnow()
    print(f"== Refresh complete: {rows} rows in trend_feed_snap "
          f"({(t1 - t0).total_seconds():.1f}s) ==")

if __name__ == "__main__":
    main()