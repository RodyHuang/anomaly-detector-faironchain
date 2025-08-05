import pandas as pd
from igraph import Graph
from collections import defaultdict
from tqdm import tqdm
from graph_utils import ensure_label_column, load_whitelist_addresses, build_filtered_adjacent_list_and_edges


def extract_motif_features(g: Graph, whitelist_path: str = None) -> pd.DataFrame:
    """
    Extract motif-based features from the graph (self-loops, 2-node loops, 3-node triangle loops),
    skipping nodes in the whitelist.

    Parameters:
        g (igraph.Graph): Directed igraph object
        whitelist_path (str): Path to CSV file with whitelist addresses

    Returns:
        pd.DataFrame: Motif features indexed by node ID
    """
    # === Load whitelist
    whitelist_set = load_whitelist_addresses(whitelist_path) if whitelist_path else set()
   
    # === Ensure the 'label' column exists so it can be matched with the whitelist
    ensure_label_column(g)

    # === Build a mapping from address label (string) to vertex ID
    address_to_vid = {v["label"]: v.index for v in g.vs}

    # === Convert whitelisted addresses to vertex IDs set, only if they exist in the graph
    skip_vids = set(address_to_vid[a] for a in whitelist_set if a in address_to_vid)
    print(f"✅ Whitelist match: {len(skip_vids)} / {len(whitelist_set)} addresses found in graph")

    # === Build adjacency list and edge set
    out_neighbors = {v.index: set(g.successors(v.index)) for v in g.vs}
    
    # === Build whitelist-filtered adjacency list and edge set for triangle loop (A→B→C→A) counting
    filtered_out_neighbors, filtered_edge = build_filtered_adjacent_list_and_edges(out_neighbors, skip_vids)

    # === Count triangle loops (A→B→C→A)
    triangle_loop_counts = defaultdict(int)
    triangle_loop_amounts = defaultdict(float)
    triangle_loop_tx_counts = defaultdict(int)

    for u in tqdm(filtered_out_neighbors, desc="🔁 Counting directed triangle loops (filtered)"):
        for w in filtered_out_neighbors[u]:
            for v in filtered_out_neighbors[w]:
                if (v, u) in filtered_edge and u < w < v:
                    # Triangle edges: u→w, w→v, v→u
                    edges = [(u, w), (w, v), (v, u)]
                    participants = {u, w, v}

                    total_amount = 0.0
                    total_count = 0

                    for src, tgt in edges:
                        eid = g.get_eid(src, tgt, directed=True, error=False)
                        total_amount += g.es[eid]["amount"]
                        total_count += g.es[eid]["count"]

                    # Assign total triangle metrics to all 3 participants
                    for node in participants:
                        triangle_loop_counts[node] += 1
                        triangle_loop_amounts[node] += total_amount
                        triangle_loop_tx_counts[node] += total_count

    # === Aggregate node-level motif features
    rows = []
    for v in tqdm(g.vs, desc="🧱 Finalizing Motif Features"):
        vid = v.index
        if vid in skip_vids:
            rows.append({
                "node": vid,
                "self_loop_count": None,
                "two_node_loop_count": None,
                "two_node_loop_amount": None,
                "two_node_loop_tx_count": None,
                "triangle_loop_count": None,
                "triangle_loop_amount": None,
                "triangle_loop_tx_count": None
            })
            continue
        
        # === Calculate self-loop count
        self_loop_count = int(vid in filtered_out_neighbors[vid])

        # === Calculate two-node-loop count
        two_node_loop_count = 0
        two_node_loop_amount = 0.0
        two_node_loop_tx_count = 0

        for u in filtered_out_neighbors[vid]:
            if vid in filtered_out_neighbors[u]:
                two_node_loop_count += 1
                
                eid1 = g.get_eid(vid, u, directed=True)
                eid2 = g.get_eid(u, vid, directed=True)
                
                two_node_loop_amount += g.es[eid1]["amount"] + g.es[eid2]["amount"]
                two_node_loop_tx_count += g.es[eid1]["count"] + g.es[eid2]["count"]
        
        # === Asign three-node-loop count
        triangle_loop_count = triangle_loop_counts[vid]

        rows.append({
            "node": vid,
            "self_loop_count": self_loop_count,
            "two_node_loop_count": two_node_loop_count,
            "two_node_loop_amount": two_node_loop_amount,
            "two_node_loop_tx_count": two_node_loop_tx_count,
            "triangle_loop_count": triangle_loop_count,
            "triangle_loop_amount": triangle_loop_amounts[vid],
            "triangle_loop_tx_count": triangle_loop_tx_counts[vid]
        })

    return pd.DataFrame(rows).set_index("node")
