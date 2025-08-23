import os
import pandas as pd

def build_abstract_token(input_dir, output_path):
    """
    Build AbstractToken table for Ethereum.

    Assumptions:
        - Pipeline is Ethereum-only (chain_id == 1).
        - token_sid is hard-coded to "1_native" to match Ethereum's native asset.
        - token_symbol/decimals are fixed for ETH.

    Output CSV schema:
        - token_sid: "1_native"
        - token_address: "" (empty for native asset)
        - token_symbol: "ETH"
        - token_standard: "native"
        - token_decimals: 18
    """
    # Step 1: Define the native token (ETH)
    abstract_token = pd.DataFrame([{
        "token_sid": "1_native",
        "token_address": "",
        "token_symbol": "ETH",
        "token_standard": "native",
        "token_decimals": 18
    }])

    abstract_token["token_address"] = abstract_token["token_address"].astype("string")

    # Step 2: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    abstract_token.to_csv(output_path, index=False)
    print(f"✅ AbstractToken written to {output_path}")
