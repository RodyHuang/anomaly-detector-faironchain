import os
import re
import pandas as pd

# ========== Define file paths ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_filename = "ethereum__native_transfers__16308189_to_16315360.csv"
output_filename = "ethereum__native_transfer__cleaned__16308189_to_16315360.csv"

input_path = os.path.join(BASE_DIR, "data", "raw", "ethereum", "transfers", input_filename)
output_path = os.path.join(BASE_DIR, "data", "intermediate", "cleaned", "ethereum", "transfers", output_filename)

# ========== Step 1: Load data ==========
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
print(f"✅ Loaded {len(df)} rows from native transfer file.")

# ========== Step 2: Fill missing chain_id with Ethereum mainnet ID = 1 ==========
df["chain_id"] = df["chain_id"].fillna(1)

# ========== Step 3.1: Drop rows with critical missing values ==========
before_dropna = len(df)
df = df.dropna(subset=usecols)
after_dropna = len(df)
print(f"Missing value rows dropped: {before_dropna - after_dropna} rows removed, {after_dropna} rows remaining.")

# ========== Step 3.2: Block Number Validation ==========
def is_valid_block_number(val, max_block=999_999_999):
    try:
        val_str = str(val).strip()
        if val_str.lower().startswith("0x") or not val_str.isdigit() or len(val_str) > 20:
            return False
        num = int(val_str)
        return 10_000 <= num <= max_block
    except:
        return False

original_len = len(df)
df = df[df["block_number"].apply(is_valid_block_number)]
df["block_number"] = df["block_number"].astype(int)
print(f"Block number cleaned: {original_len - len(df)} rows removed, {len(df)} rows remaining.")

# ========== Step 3.3: Transaction Hash Validation ==========
def is_valid_transaction_hash(val):
    try:
        val_str = str(val).strip().lower()
        return val_str.startswith("0x") and len(val_str) == 66
    except:
        return False

original_len = len(df)
df = df[df["transaction_hash"].apply(is_valid_transaction_hash)]
df["transaction_hash"] = df["transaction_hash"].str.strip().str.lower()
print(f"Transaction hash cleaned: {original_len - len(df)} rows removed, {len(df)} rows remaining.")

# ========== Step 3.4: From/To Address Validation ==========
def is_valid_eth_address(val):
    try:
        val_str = str(val).strip().lower()
        return bool(re.fullmatch(r"0x[0-9a-f]{40}", val_str))
    except:
        return False

# Clean from_address
before_from = len(df)
df = df[df["from_address"].apply(is_valid_eth_address)]
df["from_address"] = df["from_address"].str.strip().str.lower()
after_from = len(df)
print(f"From address cleaned: {before_from - after_from} rows removed, {after_from} rows remaining.")

# Clean to_address
before_to = len(df)
df = df[df["to_address"].apply(is_valid_eth_address)]
df["to_address"] = df["to_address"].str.strip().str.lower()
after_to = len(df)
print(f"To address cleaned: {before_to - after_to} rows removed, {after_to} rows remaining.")

# ========== Step 3.5: value_binary Validation ==========
def is_valid_value_binary(val):
    try:
        val_str = str(val).strip().lower()
        return bool(re.fullmatch(r"0x[0-9a-f]{64}", val_str))
    except:
        return False

before_val = len(df)
df = df[df["value_binary"].apply(is_valid_value_binary)]
df["value_binary"] = df["value_binary"].str.strip().str.lower()
after_val = len(df)
print(f"value_binary cleaned: {before_val - after_val} rows removed, {after_val} rows remaining.")

# ========== Step 3.6: chain_id Sanity Check ==========
unique_chain_ids = df["chain_id"].unique()
df["chain_id"] = df["chain_id"].astype(int)
print("Unique chain_id values:", unique_chain_ids)

# ========== Step 4: Export cleaned result ==========
df.to_csv(output_path, index=False)
print(f"✅ Cleaned native transfer data saved to: {output_path}")