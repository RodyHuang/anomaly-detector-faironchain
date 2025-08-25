import pandas as pd
from igraph import Graph
from collections import defaultdict
from tqdm import tqdm
from feature.graph_utils import load_whitelist_addresses, build_filtered_adjacent_list_and_edges


def extract_motif_features(g: Graph, whitelist_path: str = None) -> pd.DataFrame:
    """
    Extract motif-based features from the graph, skipping nodes in the whitelist.

    Parameters:
        g (igraph.Graph): Directed igraph object
        whitelist_path (str): Path to CSV file with whitelist addresses

    Returns:
        pd.DataFrame: Motif features indexed by node ID
    
    Definitions:
      - self_loop_count: presence of a self-loop (u -> u) as 0/1
      - two_node_loop_count: number of distinct mutual pairs (u <-> v) that the node participates in
      - two_node_loop_amount / _tx_count: sum of amounts / transfer counts over both directions of each mutual pair
      - triangle_loop_count: number of distinct directed 3-cycles (u -> w -> v -> u) the node participates in
      - triangle_loop_amount / _tx_count: sum of amounts / transfer counts over the 3 edges,
        accumulated to each participant in that triangle

    """
    # --- Load whitelist
    whitelist_set = load_whitelist_addresses(whitelist_path) if whitelist_path else set()

    # --- Build a mapping from address label (string) to vertex ID
    address_to_vid = {v["label"]: v.index for v in g.vs}

    # --- Convert whitelisted addresses to vertex IDs set, only if they exist in the graph
    skip_vids = set(address_to_vid[a] for a in whitelist_set if a in address_to_vid)
    print(f"✅ Whitelist match: {len(skip_vids)} / {len(whitelist_set)} addresses found in graph")

    # ---  Build full out-neighbor sets for every vertex: out_neighbors[u] = {v | (u -> v) exists}
    out_neighbors = {v.index: set(g.successors(v.index)) for v in g.vs}
    
    # --- Build whitelist-filtered adjacency + edge set for motif counting (exclude any node in skip_vids)
    # filtered_out_neighbors[u] = {v | (u -> v) exists and u,v not in skip_vids}
    # filtered_edge = set of (u, v) pairs after whitelist filtering
    filtered_out_neighbors, filtered_edge = build_filtered_adjacent_list_and_edges(out_neighbors, skip_vids)

    # === Triangle (directed 3-cycle) counting ===
    # Count each triangle (u -> w -> v -> u) exactly once using the order constraint u < w < v,
    # Then accumulate its total amount and transfer count to all three participating nodes.
    triangle_loop_counts = defaultdict(int)
    triangle_loop_amounts = defaultdict(float)
    triangle_loop_tx_counts = defaultdict(int)

    for u in tqdm(filtered_out_neighbors, desc="🔁 Counting directed triangle loops (filtered)"):
        for w in filtered_out_neighbors[u]:
            # second hop: w -> v
            for v in filtered_out_neighbors[w]:
                # close the cycle: v -> u
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

    # === Aggregate motif features
    rows = []
    for v in tqdm(g.vs, desc="🧱 Finalizing Motif Features"):
        vid = v.index

        # Whitelisted nodes: emit NA-like values (None) to be easily filtered downstream
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
        
        # Self-loop: check if vid is among its own out-neighbors (after whitelist filtering)
        self_loop_count = int(vid in filtered_out_neighbors[vid])

        # Two-node loops (mutual pairs): count each neighbor u such that (vid -> u) and (u -> vid).
        two_node_loop_count = 0
        two_node_loop_amount = 0.0
        two_node_loop_tx_count = 0

        for u in filtered_out_neighbors[vid]:
            if vid in filtered_out_neighbors[u]:
                two_node_loop_count += 1
                
                eid1 = g.get_eid(vid, u, directed=True) # vid -> u
                eid2 = g.get_eid(u, vid, directed=True) # u -> vid
                
                two_node_loop_amount += g.es[eid1]["amount"] + g.es[eid2]["amount"]
                two_node_loop_tx_count += g.es[eid1]["count"] + g.es[eid2]["count"]
             
        # Triangle loops already accumulated in the dictionaries above
        rows.append({
            "node": vid,
            "self_loop_count": self_loop_count,
            "two_node_loop_count": two_node_loop_count,
            "two_node_loop_amount": two_node_loop_amount,
            "two_node_loop_tx_count": two_node_loop_tx_count,
            "triangle_loop_count": triangle_loop_counts[vid],
            "triangle_loop_amount": triangle_loop_amounts[vid],
            "triangle_loop_tx_count": triangle_loop_tx_counts[vid]
        })

    return pd.DataFrame(rows).set_index("node")
