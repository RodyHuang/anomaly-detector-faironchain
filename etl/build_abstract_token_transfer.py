import os
import pandas as pd

# ========== Step 0: Define file paths ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(BASE_DIR, "data", "intermediate", "cleaned", "ethereum", "transfers", "ethereum__native_transfer__cleaned__16308189_to_16315360.csv")
output_path = os.path.join(BASE_DIR, "data", "intermediate", "abstract", "ethereum", "abstract_token_transfer", "ethereum__abstract_token_transfer__16308189_to_16315360.csv")
# ========== Step 1: Load cleaned transaction data ==========
df = pd.read_csv(input_path)

# ========== Step 2: Build required fields ==========

# Compose transfer_sid = chain_id + "_" + tx_hash + "_" + transfer_index
df["transfer_sid"] = df.apply(lambda row: f"{row['chain_id']}_{row['transaction_hash']}_{row['transfer_index']}", axis=1)

# Compose tx_sid = chain_id + "_" + tx_hash
df["tx_sid"] = df["chain_id"].astype(str) + "_" + df["transaction_hash"]

# Compose spender/receiver address SID
df["spender_address_sid"] = df["chain_id"].astype(str) + "_" + df["from_address"].str.strip().str.lower()
df["receiver_address_sid"] = df["chain_id"].astype(str) + "_" + df["to_address"].str.strip().str.lower()

# Convert value_binary to amount in wei
df["amount"] = df["value_binary"].apply(lambda x: int(x, 16))
df = df[df["amount"] > 0]  # Filter out trivial transfers (1 wei)

# Fixed fields
df["transfer_index"] = df["transfer_index"].astype(int)
df["token_sid"] = df["chain_id"].astype(str) + "_native"
df["category"] = "transfer" 

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

