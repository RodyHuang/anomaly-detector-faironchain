import os
import pandas as pd

# Define root path and data file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
filepath = os.path.join(BASE_DIR, "data", "raw", "ethereum__transactions__21525890_to_21533057.csv")

# Step 1: Select only necessary columns
usecols = [
    "transaction_hash",
    "block_number",
    "from_address",
    "to_address",
    "value_binary",
    "chain_id" 
]

df = pd.read_csv(filepath, usecols=usecols, nrows=100000)

# Step 2: Fill missing chain_id with Ethereum mainnet ID = 1
df["chain_id"] = df["chain_id"].fillna(1)

# Step 3: Drop rows with critical missing values
df = df.dropna(subset=[
    "transaction_hash",
    "block_number", 
    "from_address", 
    "to_address", 
    "value_binary"
])

# Step 4: Print summary
print("Unique chain_id values:", df["chain_id"].unique())
print("Column names:", df.columns.tolist())
print("Missing value counts per column:\n", df.isnull().sum())
print("Number of rows after cleaning:", len(df))