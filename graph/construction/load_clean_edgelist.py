import os
import pandas as pd

def load_clean_edgelist(year, month, chain_name="ethereum"):
    """
    Load transfer / transaction / block tables for a given (year, month),
    and join them to produce a clean edgelist for graph construction.

    Parameters:
        year (e.g., 2023)
        month (1–12)
        chain_name (str, default="ethereum")

    Returns:
        pd.DataFrame with columns:
            - transfer_sid
            - from_address_sid
            - to_address_sid
            - amount
            - token_sid
            - tx_sid
            - timestamp
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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

    # Safety check: count transfers missing a matching tx (should be 0)
    _missing_tx = merged["block_sid"].isna().sum()
    if _missing_tx:
        print(f"⚠️ { _missing_tx:, } transfers have no matching tx_sid (block_sid is NaN).")

    # Merge to get timestamp
    merged = merged.merge(df_block[["block_sid", "timestamp"]], on="block_sid", how="left")

    # Safety check: count missing timestamps (should be 0)
    _missing_ts = merged["timestamp"].isna().sum()
    if _missing_ts:
        print(f"⚠️ { _missing_ts:, } transfers have no block timestamp (timestamp is NaN).")

    # Select and rename to the final edgelist schema
    edgelist_df = merged[[
        "transfer_sid",
        "spender_address_sid",     
        "receiver_address_sid",    
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
