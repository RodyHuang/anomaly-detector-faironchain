import os
import pandas as pd

def build_abstract_token(input_dir, output_path):
    """
    Build AbstractToken table. Since native tokens are static, no need to read input_dir.
    
    Parameters:
        input_dir (str): Unused, included for interface compatibility
        output_path (str): Output path for abstract_token CSV
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
