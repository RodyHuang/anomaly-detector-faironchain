import pandas as pd
from igraph import Graph
from collections import defaultdict

try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda x, **kwargs: x

def extract_egonet_features(g: Graph, max_egonet_size: int = 5000) -> pd.DataFrame:
    """
    Fast egonet feature extractor (no clustering coefficient).
    Skips egonets with more than `max_egonet_size` nodes.
    """
    rows = []

    for v in tqdm(g.vs, desc="🧠 Extracting Egonet Features (fast)"):
        vid = v.index

        # 1-hop neighbors + self
        ego_nodes = set(g.neighbors(vid, mode="ALL"))
        ego_nodes.add(vid)

        if len(ego_nodes) > max_egonet_size:
            tqdm.write(f"🚫 Skipping large egonet for node {vid} (size={len(ego_nodes)})")
            continue

        # Induced subgraph
        ego_subgraph = g.subgraph(ego_nodes)
        n = ego_subgraph.vcount()
        m = ego_subgraph.ecount()
        max_edges = n * (n - 1)
        density = m / max_edges if max_edges > 0 else 0

        rows.append({
            "node": vid,
            "egonet_node_count": n,
            "egonet_edge_count": m,
            "egonet_density": density,
            # Skipped clustering
        })

    return pd.DataFrame(rows).set_index("node")
