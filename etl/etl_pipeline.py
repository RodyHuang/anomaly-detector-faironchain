import os
import argparse
from etl.build_abstract_block import build_abstract_block
from etl.build_abstract_transaction import build_abstract_transaction
from etl.build_abstract_token_transfer import build_abstract_token_transfer
from etl.build_abstract_account import build_abstract_account
from etl.build_abstract_token import build_abstract_token
from etl.preprocess_blocks import preprocess_blocks
from etl.preprocess_native_transfer import preprocess_transactions

# ===== Configuration =====
CHAIN_NAME = "ethereum"
CHAIN_ID = 1
LEDGER_SID = "eth-main"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", CHAIN_NAME)
CLEANED_DIR = os.path.join(BASE_DIR, "data", "intermediate", "cleaned", CHAIN_NAME)
ABSTRACT_DIR = os.path.join(BASE_DIR, "data", "intermediate", "abstract", CHAIN_NAME)

# ===== Pipeline Entrypoint =====
def run_pipeline(start_block, end_block):
    block_range = f"{start_block}_to_{end_block}"

    # 1. Paths
    raw_tx_path = os.path.join(RAW_DIR, "transactions", f"{CHAIN_NAME}__transactions__{block_range}.csv")
    raw_block_path = os.path.join(RAW_DIR, "blocks", f"{CHAIN_NAME}__blocks__{block_range}.csv")
    raw_contract_path = os.path.join(RAW_DIR, "contracts", f"{CHAIN_NAME}__contracts__{block_range}.csv")

    cleaned_tx_path = os.path.join(CLEANED_DIR, "transactions", f"{CHAIN_NAME}__transactions__cleaned__{block_range}.csv")
    cleaned_block_path = os.path.join(CLEANED_DIR, "blocks", f"{CHAIN_NAME}__blocks__cleaned__{block_range}.csv")

    abstract_prefix = f"{CHAIN_NAME}__"
    abstract_suffix = f"__{block_range}.csv"

    output_block = os.path.join(ABSTRACT_DIR, "abstract_block", abstract_prefix + "abstract_block" + abstract_suffix)
    output_tx = os.path.join(ABSTRACT_DIR, "abstract_transaction", abstract_prefix + "abstract_transaction" + abstract_suffix)
    output_transfer = os.path.join(ABSTRACT_DIR, "abstract_token_transfer", abstract_prefix + "abstract_token_transfer" + abstract_suffix)
    output_account = os.path.join(ABSTRACT_DIR, "abstract_account", abstract_prefix + "abstract_account" + abstract_suffix)
    output_token = os.path.join(ABSTRACT_DIR, "abstract_token", abstract_prefix + "abstract_token" + abstract_suffix)

    # 2. Run each builder
    build_abstract_block(cleaned_block_path, output_block)
    build_abstract_transaction(cleaned_tx_path, output_tx)
    build_abstract_token_transfer(cleaned_tx_path, output_transfer)
    build_abstract_account(cleaned_tx_path, raw_contract_path, output_account)
    build_abstract_token(output_token)

    print("✅ All abstract tables generated.")

# ===== CLI Entry =====
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_block", type=int, required=True)
    parser.add_argument("--end_block", type=int, required=True)
    args = parser.parse_args()

    run_pipeline(args.start_block, args.end_block)
