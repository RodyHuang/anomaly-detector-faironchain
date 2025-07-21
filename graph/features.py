import pandas as pd
from igraph import Graph

def extract_node_level_features_igraph(g: Graph) -> pd.DataFrame:
    """
    Extract node-level features from an igraph token transfer graph.

    Parameters:
        g (igraph.Graph): Directed igraph object with edge attributes:
            - weight
            - count
            - timestamp
            - token_sid

    Returns:
        pd.DataFrame: Node-level feature DataFrame indexed by node ID.
    """
    rows = []

    for v in g.vs:
        node_id = v.index

        # Incoming and outgoing edge indices
        in_eids = g.incident(node_id, mode="IN") # Get edge IDs where node_id is the target
        out_eids = g.incident(node_id, mode="OUT") # Get edge IDs where node_id is the source.

        # Corresponding edge objects
        in_edges = [g.es[e] for e in in_eids]
        out_edges = [g.es[e] for e in out_eids]

        # Aggregate stats
        total_in = sum(e["weight"] for e in in_edges)
        total_out = sum(e["weight"] for e in out_edges)
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
