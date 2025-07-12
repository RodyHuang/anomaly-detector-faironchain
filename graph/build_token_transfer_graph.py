from igraph import Graph
import pandas as pd

def build_igraph_from_edgelist(edgelist_df):
    """
    Construct igraph object from token transfer edgelist.

    Parameters:
        edgelist_df (pd.DataFrame): Must contain from_address_sid, to_address_sid, amount

    Returns:
        g (igraph.Graph): Directed graph
        account_to_idx (dict): Mapping from address_sid to vertex ID
    """
    # 1. Collect unique account IDs
    unique_accounts = pd.unique(edgelist_df[["from_address_sid", "to_address_sid"]].values.ravel())
    account_to_idx = {addr: i for i, addr in enumerate(unique_accounts)}

    # 2. Encode edges as integers
    edgelist_df["from_idx"] = edgelist_df["from_address_sid"].map(account_to_idx)
    edgelist_df["to_idx"] = edgelist_df["to_address_sid"].map(account_to_idx)
    edges = list(zip(edgelist_df["from_idx"], edgelist_df["to_idx"]))

    # 3. Create igraph object
    g = Graph(directed=True)
    g.add_vertices(len(unique_accounts))
    g.add_edges(edges)
    
    assert g.ecount() == len(edgelist_df), "Edge count mismatch!"

    # 4. Set attributes
    g.vs["name"] = unique_accounts
    g.es["weight"] = edgelist_df["amount"].tolist()
    g.es["token_sid"] = edgelist_df["token_sid"].tolist()
    g.es["tx_sid"] = edgelist_df["tx_sid"].tolist()
    g.es["timestamp"] = edgelist_df["timestamp"].tolist()

    return g, account_to_idx
