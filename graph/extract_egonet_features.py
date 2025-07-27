import pandas as pd
from igraph import Graph
from tqdm import tqdm
from graph_utils import ensure_label_column, load_whitelist_addresses

def extract_egonet_features(g: Graph, whitelist_path: str = None) -> pd.DataFrame:
    """
    Extract egonet-based features from the graph (node count, edge count, density),
    skipping nodes in the whitelist.

    Parameters:
        g (igraph.Graph): Directed igraph object
        whitelist_path (str): Path to CSV file with whitelist addresses

    Returns:
        pd.DataFrame: Egonet features indexed by node ID
    """
    # === Load whitelist
    whitelist_set = load_whitelist_addresses(whitelist_path) if whitelist_path else set()
   
    # === Ensure the 'label' column exists so it can be matched with the whitelist
    ensure_label_column(g)

    # === Build a mapping from address label (string) to vertex ID
    address_to_vid = {v["label"]: v.index for v in g.vs}

    # === Convert whitelisted addresses to vertex IDs set, only if they exist in the graph
    skip_vids = set(address_to_vid[a] for a in whitelist_set if a in address_to_vid)

    rows = []

    for v in tqdm(g.vs, desc="🧠 Extracting Egonet Features (whitelist-aware)"):
        vid = v.index
        if vid in skip_vids:
            rows.append({
                "node": vid,
                "egonet_node_count": None,
                "egonet_edge_count": None,
                "egonet_density": None
            })
            continue  # Skip whitelist center nodes

        # === 1-hop neighbors (ALL directions), excluding whitelisted
        ego_nodes = {
            n for n in g.neighbors(vid, mode="ALL")
            if n not in skip_vids
        }
        ego_nodes.add(vid)  # Add self

        # === Induced subgraph
        ego_subgraph = g.subgraph(ego_nodes)
        n = ego_subgraph.vcount()
        m = ego_subgraph.ecount()
        max_edges = n * (n - 1)  # directed
        density = m / max_edges if max_edges > 0 else 0

        rows.append({
            "node": vid,
            "egonet_node_count": n,
            "egonet_edge_count": m,
            "egonet_density": density,
        })

    return pd.DataFrame(rows).set_index("node")
