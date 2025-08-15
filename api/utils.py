import os
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def wei_to_eth(wei):
    """
    Convert a value from Wei to ETH.

    Parameters:
        wei (int or str): Amount in Wei.
    Returns:
        str: Formatted ETH string with 6 decimal places.
    """
    return f"{int(wei) / 1e18:.6f}"

def build_month_parquet_path(chain: str, year: int, month: int) -> str:
    """
    Build the file path for a given chain/year/month parquet file
    containing analysis results.

    Parameters:
        chain (str): Blockchain name (e.g., "ethereum").
        year (int): Year of the data.
        month (int): Month of the data (1–12).
    Returns:
        str: Full path to the parquet file.
    """
    data_root = os.path.join(
        BASE_DIR, "data", "output", "graph", chain, f"{year:04d}", f"{month:02d}"
    )
    return os.path.join(data_root, f"{chain}__analysis_result__{year}_{month:02d}.parquet")

def query_duckdb(sql: str, params: list):
    """
    Execute a parameterized SQL query on DuckDB (in-memory)
    and return the results as a pandas DataFrame.

    Parameters:
        sql (str): SQL query string with placeholders (?).
        params (list): Parameters to bind to the query.
    Returns:
        pandas.DataFrame: Query results.
    """
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(sql, params).df()
    finally:
        con.close()

def pack_rules(row: dict, ids=range(1, 7)):
    """
    Extract triggered anomaly rules from a row.

    Parameters:
        row (dict): Dictionary representing one address/row of data.
        ids (iterable): Iterable of rule IDs to check (default H1–H6).
    Returns:
        list[dict]: List of triggered rules with IDs and descriptions.
                    Each dict has keys: "rule", "description".
    """
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