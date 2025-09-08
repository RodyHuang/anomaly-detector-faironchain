import pandas as pd
from igraph import Graph
from tqdm import tqdm
from graph.feature.graph_utils import load_whitelist_addresses

def extract_egonet_features(g: Graph, whitelist_path: str = None) -> pd.DataFrame:
    """
    Extract egonet-based features from the graph (node count, edge count, density),
    skipping nodes in the whitelist.

    Parameters:
        g (igraph.Graph): Directed igraph object
        whitelist_path (str): Path to CSV file with whitelist addresses

    Returns:
        pd.DataFrame: Egonet features indexed by node ID
    
    Definitions:
        - egonet_node_count (n): |ego(v)|
        - egonet_edge_count (m): number of directed edges whose both endpoints lie in ego(v),
                                 excluding self-loops
        - egonet_density: m / [n * (n - 1)]  (density of a simple directed graph without self-loops)

    """
    # === Load whitelist
    whitelist_set = load_whitelist_addresses(whitelist_path) if whitelist_path else set()

    # === Build a mapping from address label to vertex ID
    address_to_vid = {v["label"]: v.index for v in g.vs}

    # === Map whitelisted addresses to vertex IDs
    #     These infra nodes will be skipped in egonet feature calculation
    skip_vids = set(address_to_vid[a] for a in whitelist_set if a in address_to_vid)

    N = g.vcount()

    # Precompute adjacency sets for all nodes:
    #   neighbors_all[v] = all neighbors of v (in + out)
    #   neighbors_out[v] = outgoing neighbors of v
    # Doing this once avoids repeated g.neighbors() calls inside the loop
    neighbors_all = [set(g.neighbors(v, mode="ALL")) for v in range(N)]
    neighbors_out = [set(g.neighbors(v, mode="OUT")) for v in range(N)]

    rows = []
    for vid in tqdm(range(N), desc="🧠 Extracting Egonet Features (fast)"):
        if vid in skip_vids: # Skip whitelist center nodes
            rows.append({
                "node": vid,
                "egonet_node_count": None,
                "egonet_edge_count": None,
                "egonet_density": None
            })
            continue  

        # 1-hop ego nodes (all directions), exclude whitelist, and include the node itself
        ego_nodes = {n for n in neighbors_all[vid] if n not in skip_vids}
        ego_nodes.add(vid)

        n = len(ego_nodes)
    
        # Count the number of unique directed edges inside the egonet:
        # For each node u in the egonet:
        #   1. Look at all its outgoing neighbors (out_u).
        #   2. Keep only those neighbors that are also inside the egonet (excluding u itself).
        #   3. Count them (this is the number of directed edges from u to other ego nodes).
        # Summing over all u gives the total number of directed edges m inside the egonet.
        m = 0
        for u in ego_nodes:
            out_u = neighbors_out[u]
            # Restrict neighbors to nodes inside egonet
            m_u = len(out_u & ego_nodes)
            # Remove self-loop if present
            if u in out_u:
                m_u -= 1
            m += m_u

        max_edges = n * (n - 1)  # directed simple graph
        density = m / max_edges if max_edges > 0 else 0.0

        rows.append({
            "node": vid,
            "egonet_node_count": n,
            "egonet_edge_count": m,
            "egonet_density": density,
        })

    return pd.DataFrame(rows).set_index("node")
