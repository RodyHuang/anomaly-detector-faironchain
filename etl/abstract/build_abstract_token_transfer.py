import os
import pandas as pd

def build_abstract_token_transfer(input_dir, output_path):
    """
    Build AbstractTokenTransfer table by merging all cleaned native transfer files in a directory.

    Inputs (per cleaned transfer CSV):
        Required columns:
            - chain_id
            - transaction_hash
            - transfer_index
            - from_address
            - to_address
            - value_binary  (hex string, e.g., "0x...", representing Wei)
    Output CSV schema:
        - transfer_sid: f"{chain_id}_{tx_hash}_{transfer_index}"
        - transfer_index: int
        - amount: int (Wei, non-negative)
        - category: "transfer"
        - tx_sid: f"{chain_id}_{tx_hash}"
        - spender_address_sid: f"{chain_id}_{from_address_lower}"
        - receiver_address_sid: f"{chain_id}_{to_address_lower}"
        - token_sid: f"{chain_id}_native"
    """

    all_transfers = []

    # Step 1: Iterate over all transfer files
    for fname in sorted(os.listdir(input_dir)):
        if not fname.endswith(".csv"):
            continue
        print(f"📄 Processing file: {fname}")
        file_path = os.path.join(input_dir, fname)
        df = pd.read_csv(file_path, usecols=["chain_id", "transaction_hash", "transfer_index","from_address", "to_address", "value_binary"],)

        # --- Normalization before building SIDs ---
        df["transaction_hash"] = df["transaction_hash"].astype(str).str.strip().str.lower()
        df["from_address"] = df["from_address"].astype(str).str.strip().str.lower()
        df["to_address"] = df["to_address"].astype(str).str.strip().str.lower()

        # Step 2: Build fields
        df["transfer_index"] = df["transfer_index"].astype(int)
        df["amount"] = df["value_binary"].apply(lambda x: int(x, 16))
        df["transfer_sid"] = df["chain_id"].astype(str) + "_" + df["transaction_hash"] + "_" + df["transfer_index"].astype(str)
        df["tx_sid"] = df["chain_id"].astype(str) + "_" + df["transaction_hash"]
        df["spender_address_sid"] = df["chain_id"].astype(str) + "_" + df["from_address"]
        df["receiver_address_sid"] = df["chain_id"].astype(str) + "_" + df["to_address"]
        df["token_sid"] = df["chain_id"].astype(str) + "_native"
        df["category"] = "transfer"

        # Filter: positive amount only
        df = df[df["amount"] > 0]

        df = df[[
            "transfer_sid",
            "transfer_index",
            "amount",
            "category",
            "tx_sid",
            "spender_address_sid",
            "receiver_address_sid",
            "token_sid"
        ]]

        all_transfers.append(df)

    # Step 3: Merge all
    print("🔄 Concatenating all dataframes...")
    df_all = pd.concat(all_transfers, ignore_index=True)
    print(f"   ✅ Concatenated: {len(df_all):,} rows")

    print("🔄 Dropping duplicates...")
    before = len(df_all)
    df_all = df_all.drop_duplicates(subset=["transfer_sid"])
    print(f"   ✅ Dropped duplicates: {before:,} -> {len(df_all):,}")

    print("🔄 Dropping NA rows...")
    before = len(df_all)
    df_all = df_all.dropna(subset=[
        "transfer_sid", "transfer_index", "amount",
        "tx_sid", "spender_address_sid", "receiver_address_sid", "token_sid"
    ])
    print(f"   ✅ Dropped NAs: {before:,} -> {len(df_all):,}")

    # Step 4: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_all.to_csv(output_path, index=False)
    print(f"✅ AbstractTokenTransfer saved to {output_path}")
