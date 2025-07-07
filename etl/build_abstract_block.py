import os
import pandas as pd

def build_abstract_block(input_dir, output_path):
    """
    Build AbstractBlock table by merging all cleaned block files.
    
    Parameters:
        input_dir (str): Path to folder containing cleaned block CSVs
        output_path (str): Output path for the abstract_block CSV
    """

    all_blocks = []

    # Step 1: Load all block files
    for fname in sorted(os.listdir(input_dir)):
        if not fname.endswith(".csv"):
            continue
        file_path = os.path.join(input_dir, fname)
        df = pd.read_csv(file_path, usecols=["chain_id", "block_number", "timestamp"])
        all_blocks.append(df)

    if not all_blocks:
        print("⚠️ No input block data found for AbstractBlock.")
        return

    # Step 2: Combine and clean
    df = pd.concat(all_blocks).dropna().drop_duplicates()

    # Step 3: Generate block_sid
    df["block_sid"] = df["chain_id"].astype(str) + "_" + df["block_number"].astype(str)

    # Step 4: Select columns
    abstract_block = df[["block_sid", "block_number", "timestamp"]]

    # Step 5: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    abstract_block.to_csv(output_path, index=False)
    print(f"✅ AbstractBlock written to {output_path}")
