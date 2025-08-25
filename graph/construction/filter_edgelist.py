import pandas as pd

# Blacklist sid
ADDRESS_BLACKLIST = {
    "1_0x0000000000000000000000000000000000000000",
    "1_0x000000000000000000000000000000000000dead",
    "1_0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
}

def filter_edgelist(df, min_amount_wei=1_000_000_000_000):
    """
    Filter token transfer edgelist by:
    - Removing micro transfers
    - Removing blacklist accounts
    
    Parameters:
        df (pd.DataFrame): Raw edgelist DataFrame
        min_amount_wei (int): Minimum transfer amount (in wei)
    
    Returns:
        pd.DataFrame: Filtered edgelist
    """
    before = len(df)

    # Normalize amount to integer
    df["amount"] = df["amount"].apply(int)

    # Minimum amount filter ( amount < 1e-6 ETH)
    df = df[df["amount"] >= min_amount_wei]

    # Blacklist filter
    df = df[
        (~df["from_address_sid"].isin(ADDRESS_BLACKLIST)) &
        (~df["to_address_sid"].isin(ADDRESS_BLACKLIST))
    ]
    
    print(f"🧹 Filtered edgelist: {before} → {len(df)} rows retained")
    return df
