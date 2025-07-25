import pandas as pd
import numpy as np
from igraph import Graph
from graph_utils import ensure_label_column


def extract_node_features(g: Graph, whitelist_path: str) -> pd.DataFrame:
    """
    Extract node-level features from an igraph token transfer graph,
    with exclusion of known infrastructure addresses.

    Parameters:
        g (igraph.Graph): Directed igraph object
        whitelist_path (str): Path to CSV file with whitelist address

    Returns:
        pd.DataFrame: Node-level feature DataFrame indexed by node ID
    """
    # === Load whitelist
    whitelist_set = set()
    if whitelist_path:
        df_whitelist = pd.read_csv(whitelist_path)
        whitelist_set = set(df_whitelist["address"].str.strip().str.lower())

    # === Ensure 'label' exists
    ensure_label_column(g)

    rows = []

    for v in g.vs:
        node_id = v.index
        address = v["label"].lower()

        if address in whitelist_set:
            # print(f"⚪ Skipping whitelisted node {node_id}: {address}")
            rows.append({
                "node": node_id,
                "in_degree": np.nan,
                "out_degree": np.nan,
                "in_out_degree_ratio": np.nan,
                "unique_in_degree": np.nan,
                "unique_out_degree": np.nan,
                "total_input_amount": np.nan,
                "total_output_amount": np.nan,
                "input_output_amount_ratio": np.nan,
                "balance_proxy": np.nan
            })
            continue

        in_eids = g.incident(node_id, mode="IN")
        out_eids = g.incident(node_id, mode="OUT")
        in_edges = g.es.select(in_eids)
        out_edges = g.es.select(out_eids)

        total_in = sum(e["amount"] for e in in_edges)
        total_out = sum(e["amount"] for e in out_edges)
        in_count = sum(e["count"] for e in in_edges)
        out_count = sum(e["count"] for e in out_edges)

        row = {
            "node": node_id,
            "in_degree": in_count,
            "out_degree": out_count,
            "in_out_degree_ratio": in_count / (out_count + 1),
            "unique_in_degree": len(in_edges),
            "unique_out_degree": len(out_edges),
            "total_input_amount": total_in,
            "total_output_amount": total_out,
            "input_output_amount_ratio": total_in / (total_out + 1),
            "balance_proxy": total_in - total_out
        }

        rows.append(row)

    return pd.DataFrame(rows).set_index("node")
