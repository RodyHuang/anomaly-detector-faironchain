import os
import pandas as pd

def build_abstract_block(input_dir, output_path):
    """
    Build AbstractBlock table by merging all cleaned block files.

    Inputs (per cleaned block CSV):
        - chain_id
        - block_number
        - timestamp  (EXPECTED UNIT: epoch seconds, integer)

    Output CSV schema:
        - block_sid: f"{chain_id}_{block_number}"
        - block_number
        - timestamp  (epoch seconds, int)
    """
    all_blocks = []

    # Step 1: Load all block files
    for fname in sorted(os.listdir(input_dir)):
        if not fname.endswith(".csv"):
            continue
        file_path = os.path.join(input_dir, fname)
        df = pd.read_csv(file_path, usecols=["chain_id", "block_number", "timestamp"])
        all_blocks.append(df)

    # Step 2: Combine and clean
    df = pd.concat(all_blocks, ignore_index=True)
    df = df.dropna(subset=["chain_id", "block_number", "timestamp"]).drop_duplicates()

    # Step 3: Data Normalization
    df["block_number"] = pd.to_numeric(df["block_number"], errors="coerce")
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["block_number", "timestamp"])

    df["block_number"] = df["block_number"].astype("int64")
    df["timestamp"] = df["timestamp"].astype("int64")

    df["block_sid"] = df["chain_id"].astype(str) + "_" + df["block_number"].astype(str)

    # Step 4: Select columns
    abstract_block = df[["block_sid", "block_number", "timestamp"]]

    # Step 5: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    abstract_block.to_csv(output_path, index=False)
    print(f"✅ AbstractBlock written to {output_path}")
