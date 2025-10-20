from __future__ import annotations
import os, time
import duckdb, pandas as pd
from typing import Iterable

def ensure_dirs(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def to_parquet(df: pd.DataFrame, data_dir: str, name: str) -> str:
    ensure_dirs(data_dir)
    ts = int(time.time())
    pth = os.path.join(data_dir, f"{name}_{ts}.parquet")
    df.to_parquet(pth, index=False)
    return pth

def to_duckdb(df: pd.DataFrame, db_path: str, table: str) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    con = duckdb.connect(db_path)
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
        con.register("df", df)
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
    finally:
        con.close()
