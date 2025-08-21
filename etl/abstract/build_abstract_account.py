import os
import pandas as pd

def build_abstract_account(input_dir, output_path):
    """
    Build AbstractAccount table from cleaned native transfers data
    
    Parameters:
        input_dir (str): Path to the folder containing cleaned transfer files
        output_path (str): Final output path for the abstract_account CSV
    """
    all_addrs = []

    # Step 1: Iterate through all cleaned transfer files
    for fname in sorted(os.listdir(input_dir)):
        if not fname.endswith(".csv"):
            continue
        file_path = os.path.join(input_dir, fname)
        df = pd.read_csv(file_path, usecols=["chain_id", "from_address", "to_address"])
        
        from_addrs = df[["chain_id", "from_address"]].rename(columns={"from_address": "address"})
        to_addrs = df[["chain_id", "to_address"]].rename(columns={"to_address": "address"})
        all_addrs.append(from_addrs)
        all_addrs.append(to_addrs)

    if not all_addrs:
        print("⚠️ No input transfer data found for AbstractAccount.")
        return

    # Step 2: Combine all addresses, normalize, deduplicate
    addr_df = pd.concat(all_addrs).dropna().drop_duplicates()
    addr_df["address"] = addr_df["address"].str.lower()
    addr_df["account_sid"] = addr_df["chain_id"].astype(str) + "_" + addr_df["address"]
    addr_df["type"] = "unknown"  # To be enhanced with SC/EOA detection

    abstract_account = addr_df[["account_sid", "address", "type"]].drop_duplicates()

    # Step 3: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    abstract_account.to_csv(output_path, index=False)
    print(f"✅ AbstractAccount written to {output_path}")