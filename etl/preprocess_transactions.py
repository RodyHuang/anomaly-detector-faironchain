import os
import re
import pandas as pd
import numpy as np

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
filepath = os.path.join(BASE_DIR, "data", "raw", "ethereum__transactions__21525890_to_21533057.csv")
output_path = os.path.join(BASE_DIR, "data", "intermediate", "ethereum__transactions___cleaned__21525890_to_21533057.csv")

# Step 1: Select only necessary columns
usecols = [
    "block_number",
    "transaction_hash",
    "from_address",
    "to_address",
    "value_binary",
    "chain_id" 
]

df = pd.read_csv(filepath, usecols=usecols)

# Step 2: Fill missing chain_id with Ethereum mainnet ID = 1
df["chain_id"] = df["chain_id"].fillna(1)

# Step 3: Data cleaning
# Step 3.1: Drop rows with critical missing values
before_dropna = len(df)
df = df.dropna(subset = usecols)
after_dropna = len(df)
dropped_na = before_dropna - after_dropna
print(f"Original number of rows: {before_dropna}")
print(f"Missing value rows dropped: {dropped_na} rows removed, {after_dropna} rows remaining.")

# Step 3.2: Block Number Validation
# - Excludes values that are hex, overly long strings, or out of expected numeric range
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
final_len = len(df)
removed = original_len - final_len
print(f"Block number cleaned: {removed} rows removed, {final_len} rows remaining.")

# Step 3.3: Transaction Hash Validation
# - Valid Ethereum tx hash: 66 chars, starts with 0x
def is_valid_transaction_hash(val):
    try:
        val_str = str(val).strip().lower()
        if not val_str.startswith("0x"):
            return False
        if len(val_str) != 66:
            return False
        return True
    except:
        return False

original_len = len(df)
df = df[df["transaction_hash"].apply(is_valid_transaction_hash)]
df["transaction_hash"] = df["transaction_hash"].str.strip().str.lower()
final_len = len(df)
removed = original_len - final_len
print(f"Transaction hash cleaned: {removed} rows removed, {final_len} rows remaining.")

# Step 3.4: From/To Address Validation
# - Validate Ethereum address format: 42 chars, starts with 0x
def is_valid_eth_address(val):
    try:
        val_str = str(val).strip().lower()
        return bool(re.fullmatch(r"0x[0-9a-f]{40}", val_str))
    except:
        return False

# - Clean from_address
before_from = len(df)
df = df[df["from_address"].apply(is_valid_eth_address)]
df["from_address"] = df["from_address"].str.strip().str.lower()
after_from = len(df)
print(f"From address cleaned: {before_from - after_from} rows removed, {after_from} rows remaining.")

# - Clean to_address
before_to = len(df)
df = df[df["to_address"].apply(is_valid_eth_address)]
df["to_address"] = df["to_address"].str.strip().str.lower()
after_to = len(df)
print(f"To address cleaned: {before_to - after_to} rows removed, {after_to} rows remaining.")

# Step 3.5: value_binary Validation
# - Validate Ethereum transaction value format: Encoded as 0x + 64 hex characters
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
removed_val = before_val - after_val
print(f"value_binary cleaned: {removed_val} rows removed, {after_val} rows remaining.")

# Step 3.6: chain_id Sanity Check
# - Since missing values were filled with 1 (Ethereum Mainnet), we just verify all values are 1
unique_chain_ids = df["chain_id"].unique()
df["chain_id"] = df["chain_id"].astype(int)
print("Unique chain_id values:", unique_chain_ids)

# Step 4: Save to intermediate (cleaned, but not yet transformed)
df.to_csv(output_path, index=False)
print(f"✅ Cleaned transaction data saved to: {output_path}")