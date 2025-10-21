#!/usr/bin/env python
import glob, sys, duckdb, os

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/markets.duckdb"
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

con = duckdb.connect(DB_PATH)
for path in sorted(glob.glob("db/migrations/*.sql")):
    print(f"Running {path}")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    con.execute(sql)
con.close()
print("✅ All migrations applied.")
