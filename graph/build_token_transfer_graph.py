from igraph import Graph
import pandas as pd

def build_igraph_from_edgelist(edgelist_df):
    """
    Construct igraph object from token transfer edgelist.

    Aggregates multiple transfers between the same sender and receiver 
    into a single edge, storing:
        - weight: total amount transferred
        - count: number of transfers
        - first_timestamp: earliest observed timestamp

    Parameters:
        edgelist_df (pd.DataFrame): Must contain from_address_sid, to_address_sid, amount

    Returns:
        g (igraph.Graph): Directed graph
        account_to_idx (dict): Mapping from address_sid to vertex ID
    """

     # 1. Aggregate by sender → receiver
    before_agg = len(edgelist_df) 
    agg_df = edgelist_df.groupby(["from_address_sid", "to_address_sid"]).agg(
        weight=("amount", "sum"),
        count=("transfer_sid", "count"),
        first_timestamp=("timestamp", "min"),
        token_sid=("token_sid", "first")
    ).reset_index()
    after_agg = len(agg_df)
    print(f"🔗 Aggregated transfers: {before_agg:,} → {after_agg:,} unique edges")
    
    # 2. Build address index mapping
    unique_accounts = pd.unique(agg_df[["from_address_sid", "to_address_sid"]].values.ravel())
    account_to_idx = {addr: i for i, addr in enumerate(unique_accounts)}

    # 3. Map to index-based edges
    agg_df["from_idx"] = agg_df["from_address_sid"].map(account_to_idx)
    agg_df["to_idx"] = agg_df["to_address_sid"].map(account_to_idx)
    edges = list(zip(agg_df["from_idx"], agg_df["to_idx"]))

    # 4. Create graph
    g = Graph(directed=True)
    g.add_vertices(len(unique_accounts))
    g.add_edges(edges)
    
    # 5. Attach attributes
    g.vs["name"] = unique_accounts
    g.es["weight"] = agg_df["weight"].tolist()
    g.es["count"] = agg_df["count"].tolist()
    g.es["timestamp"] = agg_df["first_timestamp"].tolist()
    g.es["token_sid"] = agg_df["token_sid"].tolist()

    return g, account_to_idx
