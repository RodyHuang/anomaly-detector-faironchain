import pandas as pd
import numpy as np
from igraph import Graph
from graph.feature.graph_utils import load_whitelist_addresses


def extract_node_features(g: Graph, whitelist_path: str = None) -> pd.DataFrame:
    """
    Extract node-level features from an igraph token transfer graph,
    with exclusion of known infrastructure addresses.

    Parameters:
        g (igraph.Graph): Directed igraph object
        whitelist_path (str): Path to CSV file with whitelist address

    Returns:
        pd.DataFrame: Node-level feature DataFrame indexed by node ID
    
    Definitions:
      - in_degree/out_degree: number of distinct inbound/outbound neighbors
        (since edges are aggregated, this equals the count of incident edges).
      - in_transfer_count/out_transfer_count: total number of transfers, i.e., sum of edge 'count'.
      - total_input_amount/total_output_amount: sum of edge 'amount'.
      - balance_proxy: total_input_amount - total_output_amount.
    """
    # === Load whitelist
    whitelist_set = load_whitelist_addresses(whitelist_path) if whitelist_path else set()

    rows = []

    # Iterate vertices and aggregate features.
    for v in g.vs:
        node_id = v.index
        address = v["label"].lower()

        if address in whitelist_set:
            rows.append({
                "node": node_id,
                "in_degree": np.nan,
                "out_degree": np.nan,
                "in_transfer_count": np.nan,
                "out_transfer_count": np.nan,
                "total_input_amount": np.nan,
                "total_output_amount": np.nan,
                "balance_proxy": np.nan
            })
            continue
        
         # Incident edges (already aggregated: one edge per (u->v))
        in_eids = g.incident(node_id, mode="IN") # IDs of incoming edges
        out_eids = g.incident(node_id, mode="OUT")
        in_edges = g.es.select(in_eids) # incoming edge objects
        out_edges = g.es.select(out_eids)

        # Sums (assume each edge has 'amount' and 'count' attributes)
        total_in = sum(e["amount"] for e in in_edges)
        total_out = sum(e["amount"] for e in out_edges)
        in_count = sum(e["count"] for e in in_edges)
        out_count = sum(e["count"] for e in out_edges)

        row = {
            "node": node_id,
            "in_degree": len(in_edges), 
            "out_degree": len(out_edges),
            "in_transfer_count": in_count, 
            "out_transfer_count": out_count,
            "total_input_amount": total_in,
            "total_output_amount": total_out,
            "balance_proxy": total_in - total_out
        }

        rows.append(row)

    return pd.DataFrame(rows).set_index("node")
