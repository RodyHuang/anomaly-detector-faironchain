import os
import pandas as pd
import numpy as np

# Define file paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(BASE_DIR, "data", "raw", "ethereum__blocks__21525890_to_21533057.csv")
output_path = os.path.join(BASE_DIR, "data", "intermediate", "cleaned", "ethereum__blocks__cleaned__21525890_to_21533057.csv")


# Step 1: Select only necessary columns
usecols = ["block_number", "chain_id", "block_hash", "timestamp", "parent_hash"]
df = pd.read_csv(input_path, usecols=usecols)

# Step 2: Fill missing chain_id with Ethereum mainnet ID = 1
df["chain_id"] = df["chain_id"].fillna(1)

# Step 3: Data cleaning
# Step 3.1: Drop rows with missing essential values
before_dropna = len(df)
df = df.dropna(subset = usecols)
df = df.dropna(subset=["block_number", "block_hash", "timestamp"])

# Step 3.2: Block Number Validation
# - Must be numeric, non-hex, and within reasonable range
def is_valid_block_number(val, max_block=999_999_999):
    try:
        val_str = str(val).strip()
        if val_str.lower().startswith("0x") or not val_str.isdigit() or len(val_str) > 20:
            return False
        num = int(val_str)
        return 10_000 <= num <= max_block
    except:
        return False

before_bn = len(df)
df = df[df["block_number"].apply(is_valid_block_number)]
df["block_number"] = df["block_number"].astype("Int64")
after_bn = len(df)
print(f"block_number cleaned: {before_bn - after_bn} rows removed, {after_bn} rows remaining.")

# Step 3.3: Block Hash Validation
# - Must be 66 characters, start with '0x'
def is_valid_block_hash(val):
    try:
        val_str = str(val).strip().lower()
        return val_str.startswith("0x") and len(val_str) == 66
    except:
        return False

before_bh = len(df)
df = df[df["block_hash"].apply(is_valid_block_hash)]
df["block_hash"] = df["block_hash"].str.strip().str.lower()
after_bh = len(df)
print(f"block_hash cleaned: {before_bh - after_bh} rows removed, {after_bh} rows remaining.")

# Step 3.4: Timestamp Validation
before_ts = len(df)
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
df = df.dropna(subset=["timestamp"])
after_ts = len(df)
print(f"timestamp cleaned: {before_ts - after_ts} rows removed, {after_ts} rows remaining.")

# Step 3.5: chain_id Sanity Check
# - Since missing values were filled with 1 (Ethereum Mainnet), we just verify all values are 1
unique_chain_ids = df["chain_id"].unique()
df["chain_id"] = df["chain_id"].astype(int)
print("Unique chain_id values:", unique_chain_ids)

# Step 4: Save to intermediate (cleaned, but not yet transformed)
df.to_csv(output_path, index=False)
print(f"✅ Cleaned transaction data saved to: {output_path}")