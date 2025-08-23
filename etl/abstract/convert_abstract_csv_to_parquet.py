import os
import pandas as pd

def convert_csv_to_parquet(year, month, chain_name="ethereum"):
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    abstract_dir = os.path.join(
        base_dir, "data", "intermediate", "abstract", chain_name, f"{year:04d}", f"{month:02d}"
    )

    filenames = [
        f"{chain_name}__abstract_token_transfer__{year}_{month:02d}",
        f"{chain_name}__abstract_block__{year}_{month:02d}",
        f"{chain_name}__abstract_transaction__{year}_{month:02d}",
        f"{chain_name}__abstract_token__{year}_{month:02d}",
        f"{chain_name}__abstract_account__{year}_{month:02d}",
    ]

    for fname in filenames:
        csv_path = os.path.join(abstract_dir, fname + ".csv")
        parquet_path = os.path.join(abstract_dir, fname + ".parquet")

        if not os.path.exists(csv_path):
            print(f"❌ Skipped (file not found): {csv_path}")
            continue

        try:
            df = pd.read_csv(csv_path)
            # Wei may exceed int64; store as string to avoid overflow/precision loss
            if "amount" in df.columns:
                df["amount"] = df["amount"].astype("string")
            df.to_parquet(parquet_path, index=False)
            print(f"✅ Converted: {csv_path} → {parquet_path}")
        except Exception as e:
            print(f"⚠️ Error converting {csv_path}: {e}")