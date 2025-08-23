import os
import pandas as pd

def load_clean_edgelist(year, month, chain_name="ethereum"):
    """
    Load and merge abstract token transfer data with timestamp to build a base edgelist.
    
    Returns:
        DataFrame with columns:
            from_address_sid, to_address_sid, amount, token_sid, tx_sid, timestamp
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    abstract_dir = os.path.join(base_dir, "data", "intermediate", "abstract", chain_name, f"{year:04d}", f"{month:02d}")

    # Load tables
    token_transfer_fp = os.path.join(abstract_dir, f"{chain_name}__abstract_token_transfer__{year}_{month:02d}.parquet")
    transaction_fp = os.path.join(abstract_dir, f"{chain_name}__abstract_transaction__{year}_{month:02d}.parquet")
    block_fp = os.path.join(abstract_dir, f"{chain_name}__abstract_block__{year}_{month:02d}.parquet")

    df_transfer = pd.read_parquet(token_transfer_fp)
    df_tx = pd.read_parquet(transaction_fp)
    df_block = pd.read_parquet(block_fp)

    print("📥 Loaded transfer:", df_transfer.shape)
    print("📥 Loaded transaction:", df_tx.shape)
    print("📥 Loaded block:", df_block.shape)

    # Merge transfer → tx to get block_sid
    merged = df_transfer.merge(df_tx[["tx_sid", "block_sid"]], on="tx_sid", how="left")

    # Merge to get timestamp
    merged = merged.merge(df_block[["block_sid", "timestamp"]], on="block_sid", how="left")

    # Select and reorder needed columns
    edgelist_df = merged[[
        "transfer_sid",
        "spender_address_sid",     # from
        "receiver_address_sid",    # to
        "amount",
        "token_sid",
        "tx_sid",
        "timestamp"
    ]].rename(columns={
        "spender_address_sid": "from_address_sid",
        "receiver_address_sid": "to_address_sid"
    })

    print("✅ Edgelist constructed:", edgelist_df.shape)
    return edgelist_df
