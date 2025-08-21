import os
import pandas as pd

def build_abstract_transaction(input_dir, output_path):
    """
    Build AbstractTransaction table by merging all cleaned native transfer files in a directory.

    Parameters:
        input_dir (str): Path to folder containing cleaned transfer CSVs
        output_path (str): Output path for abstract_transaction CSV
    """

    all_tx = []

    # Step 1: Load all transfer files
    for fname in sorted(os.listdir(input_dir)):
        if not fname.endswith(".csv"):
            continue
        file_path = os.path.join(input_dir, fname)
        df = pd.read_csv(file_path, usecols=["chain_id", "transaction_hash", "block_number"])
        all_tx.append(df)

    if not all_tx:
        print("⚠️ No input transaction data found.")
        return

    # Step 2: Combine and clean
    df_all = pd.concat(all_tx).dropna().drop_duplicates()

    # Step 3: Build tx_sid and block_sid
    df_all["transaction_hash"] = df_all["transaction_hash"].str.strip().str.lower()
    df_all["tx_sid"] = df_all["chain_id"].astype(str) + "_" + df_all["transaction_hash"]
    df_all["block_sid"] = df_all["chain_id"].astype(str) + "_" + df_all["block_number"].astype(str)

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
