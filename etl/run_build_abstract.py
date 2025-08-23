import os
import argparse
from etl.abstract.build_abstract_block import build_abstract_block
from etl.abstract.build_abstract_transaction import build_abstract_transaction
from etl.abstract.build_abstract_token_transfer import build_abstract_token_transfer
from etl.abstract.build_abstract_token import build_abstract_token
from etl.abstract.build_abstract_account import build_abstract_account
from etl.abstract.convert_abstract_csv_to_parquet import convert_csv_to_parquet

def run_build_abstract(year, month, chain_name="ethereum"):
    """
    Run all abstract builders for a given year/month.

    Assumes this file is located at: PROJECT_ROOT/etl
    Data directories are under:      PROJECT_ROOT/data/...
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Define paths
    cleaned_dir = os.path.join(base_dir, "data", "intermediate", "cleaned", chain_name)
    abstract_dir = os.path.join(base_dir, "data", "intermediate", "abstract", chain_name, f"{year:04d}", f"{month:02d}")
    os.makedirs(abstract_dir, exist_ok=True)

    # Input subfolders
    tx_input_dir = os.path.join(cleaned_dir, "transfers", f"{year:04d}", f"{month:02d}")
    block_input_dir = os.path.join(cleaned_dir, "blocks", f"{year:04d}", f"{month:02d}")

    # Output paths
    token_transfer_output = os.path.join(abstract_dir, f"{chain_name}__abstract_token_transfer__{year}_{month:02d}.csv")
    block_output = os.path.join(abstract_dir, f"{chain_name}__abstract_block__{year}_{month:02d}.csv")
    transaction_output = os.path.join(abstract_dir, f"{chain_name}__abstract_transaction__{year}_{month:02d}.csv")
    token_output = os.path.join(abstract_dir, f"{chain_name}__abstract_token__{year}_{month:02d}.csv")
    account_output = os.path.join(abstract_dir, f"{chain_name}__abstract_account__{year}_{month:02d}.csv")

    # Run each abstract builder
    print("🚧 Building AbstractTokenTransfer...")
    build_abstract_token_transfer(tx_input_dir, token_transfer_output)

    print("🚧 Building AbstractBlock...")
    build_abstract_block(block_input_dir, block_output)

    print("🚧 Building AbstractTransaction...")
    build_abstract_transaction(tx_input_dir, transaction_output)

    print("🚧 Building AbstractToken...")
    build_abstract_token(tx_input_dir, token_output)

    print("🚧 Building AbstractAccount...")
    build_abstract_account(tx_input_dir, account_output)

    print("✅ Finished building all abstract tables.")

# ===== CLI entry =====
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--chain_name", type=str, default="ethereum")
    args = parser.parse_args()

    run_build_abstract(args.year, args.month, args.chain_name)
    convert_csv_to_parquet(args.year, args.month, args.chain_name)
