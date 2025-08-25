from igraph import Graph
import pandas as pd

def build_igraph_from_edgelist(edgelist_df):
    """
    Construct a directed igraph from a token transfer edgelist.

    Aggregates multiple transfers between the same sender and receiver
    into a single edge, storing:
        - amount: total amount transferred (sum)
        - count: number of transfers
        - first_timestamp: earliest observed timestamp
        - token_sid: first observed token_sid for that (u,v) pair  (*see note below)

    Parameters:
        edgelist_df (pd.DataFrame): must contain columns:
            ['from_address_sid', 'to_address_sid', 'amount', 'transfer_sid', 'timestamp', 'token_sid']

    Returns:
        g (igraph.Graph): directed graph
        account_to_idx (dict): mapping from address_sid -> vertex index
    """

     # Aggregate by sender → receiver
    before_agg = len(edgelist_df) 
    agg_df = edgelist_df.groupby(["from_address_sid", "to_address_sid"]).agg(
        amount=("amount", "sum"),
        count=("transfer_sid", "count"),
        first_timestamp=("timestamp", "min"),
        token_sid=("token_sid", "first")
    ).reset_index()
    after_agg = len(agg_df)
    print(f"🔗 Aggregated transfers: {before_agg:,} → {after_agg:,} unique edges")
    
    # Build address index mapping
    unique_accounts = pd.unique(agg_df[["from_address_sid", "to_address_sid"]].values.ravel())
    account_to_idx = {addr: i for i, addr in enumerate(unique_accounts)}

    # Map to index-based edges
    agg_df["from_idx"] = agg_df["from_address_sid"].map(account_to_idx)
    agg_df["to_idx"] = agg_df["to_address_sid"].map(account_to_idx)
    edges = list(zip(agg_df["from_idx"], agg_df["to_idx"]))

    # Create graph
    g = Graph(directed=True)
    g.add_vertices(len(unique_accounts))
    g.add_edges(edges)
    
    # Attach attributes
    #   Vertex attributes
    g.vs["name"] = unique_accounts  # name  = address_sid (as provided)
    g.vs["label"] = [name.split("_", 1)[1].lower() for name in g.vs["name"]] # label = pure address (lowercase)

    #   Edge attributes
    g.es["amount"] = agg_df["amount"].tolist()
    g.es["count"] = agg_df["count"].tolist()
    g.es["first_timestamp"] = agg_df["first_timestamp"].tolist()
    g.es["token_sid"] = agg_df["token_sid"].tolist()

    return g, account_to_idx
