import pandas as pd
import re

def preprocess_transactions(input_path, output_path):
    """
    Clean raw native transfer CSV for one day:
    - select stable columns
    - validate/normalize block_number, transaction_hash, addresses, value_binary, chain_id
    - drop rows with missing/invalid critical fields
    """
    usecols = [
        "block_number",
        "transfer_index",
        "transaction_hash",
        "from_address",
        "to_address",
        "value_binary",
        "chain_id"
    ]

    df = pd.read_csv(input_path, usecols=usecols)
    print(f"Loaded {len(df)} rows from: {input_path}")

    # default chain_id if missing
    df["chain_id"] = df["chain_id"].fillna(1)

    # drop rows with any missing values in critical columns
    before_dropna = len(df)
    df = df.dropna(subset=usecols)
    after_dropna = len(df)
    print(f"Missing values removed: {before_dropna - after_dropna} rows")

    # block number validation
    def is_valid_block_number(val, max_block=999_999_999):
        try:
            val_str = str(val).strip()
            if val_str.lower().startswith("0x") or len(val_str) > 20:
                return False
            num = int(float(val_str))
            return 10_000 <= num <= max_block
        except:
            return False

    before_bn = len(df)
    df = df[df["block_number"].apply(is_valid_block_number)]
    df["block_number"] = df["block_number"].astype(int)
    print(f"Block number cleaned: {before_bn - len(df)} rows removed")

    # transaction hash (0x + 64 hex)
    def is_valid_transaction_hash(val):
        try:
            val_str = str(val).strip().lower()
            return val_str.startswith("0x") and len(val_str) == 66
        except:
            return False

    before_tx = len(df)
    df = df[df["transaction_hash"].apply(is_valid_transaction_hash)]
    df["transaction_hash"] = df["transaction_hash"].str.strip().str.lower()
    print(f"Transaction hash cleaned: {before_tx - len(df)} rows removed")

    # address format (0x + 40 hex)
    def is_valid_eth_address(val):
        try:
            val_str = str(val).strip().lower()
            return bool(re.fullmatch(r"0x[0-9a-f]{40}", val_str))
        except:
            return False

    before_from = len(df)
    df = df[df["from_address"].apply(is_valid_eth_address)]
    df["from_address"] = df["from_address"].str.strip().str.lower()
    print(f"From address cleaned: {before_from - len(df)} rows removed")

    before_to = len(df)
    df = df[df["to_address"].apply(is_valid_eth_address)]
    df["to_address"] = df["to_address"].str.strip().str.lower()
    print(f"To address cleaned: {before_to - len(df)} rows removed")

    # value_binary (0x + 64 hex)
    def is_valid_value_binary(val):
        try:
            val_str = str(val).strip().lower()
            return bool(re.fullmatch(r"0x[0-9a-f]{64}", val_str))
        except:
            return False

    before_val = len(df)
    df = df[df["value_binary"].apply(is_valid_value_binary)]
    df["value_binary"] = df["value_binary"].str.strip().str.lower()
    print(f"value_binary cleaned: {before_val - len(df)} rows removed")

    # chain_id to int
    df["chain_id"] = df["chain_id"].astype(int)
    print("Unique chain_id values:", df["chain_id"].unique())

    # summary
    original_len = before_dropna
    final_len = len(df)
    print(f"Summary: {original_len} → {final_len} rows kept ({original_len - final_len} removed)")

    # save
    df.to_csv(output_path, index=False)
    print(f"Cleaned native transfer data saved to: {output_path}")
