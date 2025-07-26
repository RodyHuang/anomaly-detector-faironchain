from igraph import Graph
import pandas as pd
from collections import defaultdict


def load_whitelist_addresses(path: str) -> set[str]:
    """
    Load a CSV of Ethereum addresses and return a cleaned set of lowercase addresses.
    
    Parameters:
        path (str): Path to the CSV file with an 'address' column.
    
    Returns:
        Set[str]: Cleaned, lowercased addresses.
    """
    df = pd.read_csv(path)
    return set(df["address"].str.strip().str.lower())

def ensure_label_column(g: Graph) -> None:
    """
    Ensure that each node in the graph has a 'label' attribute
    (the pure address in lowercase), derived from 'name'.

    Modifies g.vs in-place.
    """
    if "label" not in g.vs.attributes():
        g.vs["label"] = [name.split("_", 1)[1].lower() for name in g.vs["name"]]

def build_filtered_adjacent_list_and_edges(out_neighbors: dict[int, set[int]], skip_vids: set[int]) -> tuple[dict[int, set[int]], set[tuple[int, int]]]:
    """
    From the full out-neighbors and whitelist (skip_vids), build:
        - a filtered adjacency list (excluding any node in skip_vids)
        - a filtered edge set (u, v) where both u and v are not in skip_vids

    Parameters:
        out_neighbors (dict): Mapping of node ID to out-neighbor set
        skip_vids (set): Set of node IDs to skip (e.g., whitelisted addresses)

    Returns:
        Tuple:
            - filtered_out_neighbors: dict[int, set[int]]
            - filtered_edge: set[tuple[int, int]]
    """
    filtered_out_neighbors = defaultdict(set)

    for u, neighbors in out_neighbors.items():
        if u in skip_vids:
            continue
        filtered_out_neighbors[u] = {
            v for v in neighbors if v not in skip_vids
        }

    filtered_edge = {
        (u, v) for u, neighbors in filtered_out_neighbors.items()
        for v in neighbors
    }

    return filtered_out_neighbors, filtered_edge