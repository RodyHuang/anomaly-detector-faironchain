import os
import pandas as pd

# ========== Step 0: Define file paths ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(BASE_DIR, "data", "intermediate", "cleaned", "ethereum__transactions___cleaned__21525890_to_21533057.csv")
output_path = os.path.join(BASE_DIR, "data", "intermediate", "abstract", "ethereum__abstract_token_transfer__21525890_to_21533057.csv")

# ========== Step 1: Load cleaned transaction data ==========
df = pd.read_csv(input_path)

# ========== Step 2: Build required fields ==========
# Transfer index is always 0 for ETH native
df["transfer_index"] = 0

# Token SID is fixed to ETH native
df["token_sid"] = "1_native"

# Transfer category is always 'transfer'
df["category"] = "transfer"

# Compose tx_sid
df["tx_sid"] = df["chain_id"].astype(str) + "_" + df["transaction_hash"]
2
# Compose transfer_sid = chain_id + "_" + tx_hash + "_0"
df["transfer_sid"] = df["chain_id"].astype(str) + "_" + df["transaction_hash"] + "_0"

# Compose spender and receiver address SIDs
df["spender_address_sid"] = df["chain_id"].astype(str) + "_" + df["from_address"]
df["receiver_address_sid"] = df["chain_id"].astype(str) + "_" + df["to_address"]

# Convert amount (value_binary) from hex to decimal
df["amount"] = df["value_binary"].apply(lambda x: int(x, 16))
df = df[df["amount"] > 0] 

# ========== Step 3: Select and reorder columns ==========
abstract_transfer = df[[
    "transfer_sid",
    "transfer_index",
    "amount",
    "category",
    "tx_sid",
    "spender_address_sid",
    "receiver_address_sid",
    "token_sid"
]]

# ========== Step 4: Save to CSV ==========
abstract_transfer.to_csv(output_path, index=False)
print(f"✅ AbstractTokenTransfer saved to {output_path}")

