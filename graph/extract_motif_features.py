import pandas as pd
from igraph import Graph
from collections import defaultdict
from tqdm import tqdm
from graph_utils import ensure_label_column


def extract_motif_features(g: Graph, whitelist_path: str = None, MAX_OUT_DEGREE: int = 100_000) -> pd.DataFrame:
    """
    Extract motif-based features from the graph (self-loops, 2-node cycles, 3-node triangle loops),
    skipping nodes in the whitelist or exceeding MAX_OUT_DEGREE.

    Parameters:
        g (igraph.Graph): Directed igraph object
        whitelist_path (str): Path to CSV file with whitelist addresses
        MAX_OUT_DEGREE (int): Max out-degree allowed for triangle loop computation

    Returns:
        pd.DataFrame: Motif features indexed by node ID
    """
    # === Load whitelist
    whitelist_set = set()
    if whitelist_path:
        df_whitelist = pd.read_csv(whitelist_path)
        df_whitelist["address"] = df_whitelist["address"].str.strip().str.lower()  # ✅ 標準化地址
        whitelist_set = set(df_whitelist["address"])

    # === Ensure 'label' column exists
    ensure_label_column(g)

    # === Build label → vertex ID map
    address_to_vid = {v["label"]: v.index for v in g.vs if "label" in v.attributes()}
    skip_vids = set(address_to_vid[a] for a in whitelist_set if a in address_to_vid)
    print(f"✅ Whitelist match: {len(skip_vids)} / {len(whitelist_set)} addresses found in graph")

    # === Build original adjacency list and edge set
    out_neighbors = defaultdict(set)
    for e in g.es:
        src, tgt = e.source, e.target
        out_neighbors[src].add(tgt)

    has_edge = set((src, tgt) for src, tgts in out_neighbors.items() for tgt in tgts)

    # === Build filtered adjacency list & record skipped nodes
    filtered_out_neighbors = {}
    skipped_nodes = []

    for u in list(out_neighbors.keys()):
        neighbors = out_neighbors[u]
        if u in skip_vids or len(neighbors) > MAX_OUT_DEGREE:
            if u not in skip_vids and len(neighbors) > MAX_OUT_DEGREE:
                skipped_nodes.append((u, g.vs[u]["label"], len(neighbors)))
            continue
        filtered_out_neighbors[u] = {
            v for v in neighbors
            if v not in skip_vids and v in out_neighbors and len(out_neighbors[v]) <= MAX_OUT_DEGREE
        }

    # === Filtered edge set
    filtered_has_edge = {
        (u, v) for u, v in has_edge
        if u in filtered_out_neighbors and v in filtered_out_neighbors
    }

    # === Count triangle loops (A→B→C→A)
    triangle_loop_counts = defaultdict(int)
    node_list = list(filtered_out_neighbors.keys())

    for u in tqdm(node_list, desc="🔁 Counting directed triangle loops (filtered + degree limit)"):
        for w in filtered_out_neighbors.get(u, set()):
            for v in filtered_out_neighbors.get(w, set()):
                if (v, u) in filtered_has_edge and u < w < v:
                    triangle_loop_counts[u] += 1
                    triangle_loop_counts[w] += 1
                    triangle_loop_counts[v] += 1

    # === Aggregate node-level motif features
    rows = []
    for v in tqdm(g.vs, desc="🧱 Finalizing Motif Features"):
        vid = v.index
        if vid in skip_vids or len(out_neighbors[vid]) > MAX_OUT_DEGREE:
            rows.append({
                "node": vid,
                "self_loop_count": None,
                "two_node_cycle_count": None,
                "triangle_loop_count": None
            })
            continue

        self_loop_count = int(vid in out_neighbors[vid])
        two_node_cycle_count = sum(
            1 for u in out_neighbors[vid]
            if vid in out_neighbors.get(u, set())
        )
        triangle_loop_count = triangle_loop_counts[vid]

        rows.append({
            "node": vid,
            "self_loop_count": self_loop_count,
            "two_node_cycle_count": two_node_cycle_count,
            "triangle_loop_count": triangle_loop_count
        })

    # === Show skipped nodes (not in whitelist but over MAX)
    if skipped_nodes:
        df_skipped = pd.DataFrame(skipped_nodes, columns=["node", "label", "out_degree"])
        df_skipped = df_skipped.sort_values("out_degree", ascending=False)
        print("\n🚫 Skipped high-out-degree nodes (not whitelisted):")
        print(df_skipped.to_string(index=False))

    return pd.DataFrame(rows).set_index("node")
