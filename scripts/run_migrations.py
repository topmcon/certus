import duckdb, glob, os
os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/markets.duckdb")
for path in sorted(glob.glob("sql/*.sql")):
    print("== Applying:", path)
    with open(path, "r") as f:
        con.execute(f.read())
print("== Migrations complete.")
con.close()
