import os
import argparse
from etl.preprocess.preprocess_native_transfer import preprocess_transactions
from etl.preprocess.preprocess_blocks import preprocess_blocks

def run_preprocessing(year, month, chain_name="ethereum"):
    """
    Preprocess one month's raw CSVs (blocks & transfers) into cleaned CSVs.
    Input:  data/raw/{chain}/{dataset}/YYYY/MM/*.csv
    Output: data/intermediate/cleaned/{chain}/{dataset}/YYYY/MM/*__cleaned.csv
    """
    # project root = parent of this file's directory
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, "data", "raw", chain_name)
    cleaned_dir = os.path.join(base_dir, "data", "intermediate", "cleaned", chain_name)

    # monthly input/output dirs
    tx_input_dir = os.path.join(raw_dir, "transfers", f"{year:04d}", f"{month:02d}")
    block_input_dir = os.path.join(raw_dir, "blocks", f"{year:04d}", f"{month:02d}")
    tx_output_dir = os.path.join(cleaned_dir, "transfers", f"{year:04d}", f"{month:02d}")
    block_output_dir = os.path.join(cleaned_dir, "blocks", f"{year:04d}", f"{month:02d}")

    os.makedirs(tx_output_dir, exist_ok=True)
    os.makedirs(block_output_dir, exist_ok=True)

    # transfers
    for filename in sorted(os.listdir(tx_input_dir)):
        if not filename.endswith(".csv"):
            continue
        input_path = os.path.join(tx_input_dir, filename)
        output_filename = filename.replace(".csv", "__cleaned.csv")
        output_path = os.path.join(tx_output_dir, output_filename)
        print(f"Preprocessing transfer file: {filename}")
        preprocess_transactions(input_path, output_path)

    # blocks
    for filename in sorted(os.listdir(block_input_dir)):
        if not filename.endswith(".csv"):
            continue
        input_path = os.path.join(block_input_dir, filename)
        output_filename = filename.replace(".csv", "__cleaned.csv")
        output_path = os.path.join(block_output_dir, output_filename)
        print(f"Preprocessing block file: {filename}")
        preprocess_blocks(input_path, output_path)

    print("✅ Finished preprocessing all raw files for the month.")

# ===== CLI entry =====
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True, help="Year of the data (e.g., 2025)")
    parser.add_argument("--month", type=int, required=True, help="Month of the data (e.g., 1 for January)")
    args = parser.parse_args()

    run_preprocessing(args.year, args.month)