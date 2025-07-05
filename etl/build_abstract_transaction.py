import os
import pandas as pd

# Step 0: Define file paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(BASE_DIR, "data", "intermediate", "cleaned", "ethereum__transactions___cleaned__21525890_to_21533057.csv")
output_path = os.path.join(BASE_DIR, "data", "intermediate", "abstract", "ethereum__abstract_transaction__21525890_to_21533057.csv")

# Step 1: Load cleaned transaction data
df = pd.read_csv(input_path)

# Step 2: Build 'tx_sid' and 'block_sid'
df["tx_sid"] = df["chain_id"].astype(str) + "_" + df["transaction_hash"]
df["block_sid"] = df["chain_id"].astype(str) + "_" + df["block_number"].astype(str)

# Step 3: Select and reorder columns
abstract_transaction = df[[
    "tx_sid",
    "transaction_hash",
    "block_sid"
]].rename(columns={
    "transaction_hash": "tx_hash"
})

# Step 4: Save to intermediate/abstract
abstract_transaction.to_csv(output_path, index=False)
print(f"✅ AbstractTransaction saved to {output_path}")
