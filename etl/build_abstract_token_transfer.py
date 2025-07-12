import os
import pandas as pd

def build_abstract_token_transfer(input_dir, output_path):
    """
    Build AbstractTokenTransfer table by merging all cleaned native transfer files in a directory.

    Parameters:
        input_dir (str): Path to the folder containing cleaned native transfers
        output_path (str): Output path for the abstract_token_transfer CSV
    """

    all_transfers = []

    # Step 1: Iterate over all transfer files
    for fname in sorted(os.listdir(input_dir)):
        if not fname.endswith(".csv"):
            continue
        print(f"📄 Processing file: {fname}")
        file_path = os.path.join(input_dir, fname)
        df = pd.read_csv(file_path)

        # Step 2: Build fields
        df["transfer_index"] = df["transfer_index"].astype(int)
        df["transfer_sid"] = df["chain_id"].astype(str) + "_" + df["transaction_hash"] + "_" + df["transfer_index"].astype(str)
        df["tx_sid"] = df["chain_id"].astype(str) + "_" + df["transaction_hash"]
        df["spender_address_sid"] = df["chain_id"].astype(str) + "_" + df["from_address"].str.strip().str.lower()
        df["receiver_address_sid"] = df["chain_id"].astype(str) + "_" + df["to_address"].str.strip().str.lower()
        df["amount"] = df["value_binary"].apply(lambda x: int(x, 16))
        df["amount"] = df["amount"].astype("float64")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0).astype(float)
        df["token_sid"] = df["chain_id"].astype(str) + "_native"
        df["category"] = "transfer"

        # Filter: positive amount only
        df = df[df["amount"] > 0]

        all_transfers.append(df)

    if not all_transfers:
        print("⚠️ No valid transfer data found.")
        return

    # Step 3: Combine and format output
    df_all = pd.concat(all_transfers).dropna().drop_duplicates()

    abstract_transfer = df_all[[
        "transfer_sid",
        "transfer_index",
        "amount",
        "category",
        "tx_sid",
        "spender_address_sid",
        "receiver_address_sid",
        "token_sid"
    ]]

    # Step 4: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    abstract_transfer.to_csv(output_path, index=False)
    print(f"✅ AbstractTokenTransfer saved to {output_path}")
