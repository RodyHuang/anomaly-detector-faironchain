import os
import duckdb
from flask import Flask, request, jsonify

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def wei_to_eth(wei):
    return f"{int(wei) / 1e18:.6f}"

def build_month_parquet_path(chain: str, year: int, month: int) -> str:
    data_root = os.path.join(
        BASE_DIR, "data", "output", "graph", chain, f"{year:04d}", f"{month:02d}"
    )
    return os.path.join(data_root, f"{chain}__analysis_result__{year}_{month:02d}.parquet")

def query_duckdb(sql: str, params: list):
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(sql, params).df()
    finally:
        con.close()

def pack_rules(row: dict, ids=range(1, 7)):
    items = []
    for i in ids:
        flag = row.get(f"H{i}_flag", 0)
        if flag == 1:
            desc = row.get(f"H{i}_description")
            items.append({
                "rule": f"H{i}",
                "description": str(desc)
                })
    return items