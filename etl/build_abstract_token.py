import os
import pandas as pd

# Step 0: Define output path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_path = os.path.join(BASE_DIR, "data", "intermediate", "abstract", "ethereum", "abstract_token", "ethereum__abstract_token__16308189_to_16315360.csv")

# Step 1: Construct a single-row dataframe for ETH native token
abstract_token = pd.DataFrame([{
    "token_sid": "1_native",
    "token_address": None,
    "token_symbol": "ETH",
    "token_standard": "native",
    "token_decimals": 18
}])

# Step 2: Save
abstract_token.to_csv(output_path, index=False)
print(f"✅ AbstractToken written to {output_path}")
