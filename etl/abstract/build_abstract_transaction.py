import os
import pandas as pd

def build_abstract_transaction(input_dir, output_path):
    """
    Build AbstractTransaction table by merging all cleaned native transfer files in a directory.

    Inputs (per cleaned transfer CSV):
        - chain_id
        - transaction_hash
        - block_number

    Output CSV schema:
        - tx_sid: f"{chain_id}_{tx_hash_lower}"
        - tx_hash: normalized (lowercased, stripped)
        - block_sid: f"{chain_id}_{block_number}"
    """
    all_tx = []

    # Step 1: Load all transfer files
    for fname in sorted(os.listdir(input_dir)):
        if not fname.endswith(".csv"):
            continue

        print(f"📄 Processing file: {fname}")
        file_path = os.path.join(input_dir, fname)
        df = pd.read_csv(file_path, usecols=["chain_id", "transaction_hash", "block_number"])
        all_tx.append(df)

    # Step 2: Combine and clean
    print("🔄 Concatenating all dataframes...")
    df_all = pd.concat(all_tx, ignore_index=True, copy=False)

    print("🔄 Dropping NA rows...")
    df_all = df_all.dropna(subset=["chain_id", "transaction_hash", "block_number"])

    # Step 3: Build tx_sid and block_sid
    df_all["transaction_hash"] = df_all["transaction_hash"].astype(str).str.strip().str.lower()
    df_all["tx_sid"] = df_all["chain_id"].astype(str) + "_" + df_all["transaction_hash"]
    df_all["block_sid"] = df_all["chain_id"].astype(str) + "_" + df_all["block_number"].astype(str)

    print("🔄 Dropping duplicates...")
    df_all = df_all.drop_duplicates(subset=["tx_sid"])

    # Step 4: Select and rename columns
    abstract_transaction = df_all[[
        "tx_sid",
        "transaction_hash",
        "block_sid"
    ]].rename(columns={
        "transaction_hash": "tx_hash"
    })

    # Step 5: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    abstract_transaction.to_csv(output_path, index=False)
    print(f"✅ AbstractTransaction saved to {output_path}")
