import os
import pandas as pd

# Step 0: Define file paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(BASE_DIR, "data", "intermediate", "cleaned", "ethereum", "transfers", "ethereum__native_transfer__cleaned__16308189_to_16315360.csv")
output_path = os.path.join(BASE_DIR, "data", "intermediate", "abstract",  "ethereum", "abstract_account", "ethereum__abstract_account__16308189_to_16315360.csv")

# Step 1: Load cleaned transaction data
df = pd.read_csv(input_path)

# Step 2: Extract unique addresses from 'from' and 'to'
from_addrs = df[["chain_id", "from_address"]].rename(columns={"from_address": "address"})
to_addrs = df[["chain_id", "to_address"]].rename(columns={"to_address": "address"})
all_addrs = pd.concat([from_addrs, to_addrs]).dropna().drop_duplicates()

# Step 3: Generate account_sid and normalize address
all_addrs["address"] = all_addrs["address"].str.lower()
all_addrs["account_sid"] = all_addrs["chain_id"].astype(str) + "_" + all_addrs["address"]

# Step 4: Temporarily set type as unknown (SC/EOA detection to be added later)
all_addrs["type"] = "unknown"

# Step 5: Rearrange columns
abstract_account = all_addrs[["account_sid", "address", "type"]]

# Step 6: Save
abstract_account.to_csv(output_path, index=False)
print(f"✅ AbstractAccount written to {output_path}")
