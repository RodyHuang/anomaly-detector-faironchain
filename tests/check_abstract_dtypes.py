import os
import pandas as pd

def check_dtypes(file_path):
    print(f"\n📄 Checking: {os.path.basename(file_path)}")
    try:
        df = pd.read_csv(file_path)
        print("🧪 Data Types:")
        print(df.dtypes)
        
        # Additional checks
        if 'amount' in df.columns:
            amount_dtype = df['amount'].dtype
            print(f"🔍 amount dtype: {amount_dtype}")
            if not pd.api.types.is_numeric_dtype(df['amount']):
                print("⚠️ WARNING: 'amount' is not numeric!")

        if 'timestamp' in df.columns:
            ts_dtype = df['timestamp'].dtype
            print(f"🔍 timestamp dtype: {ts_dtype}")
            if 'datetime64' in str(ts_dtype):
                if df['timestamp'].dt.tz is not None:
                    print("⚠️ WARNING: 'timestamp' has timezone info!")
            elif not pd.api.types.is_integer_dtype(df['timestamp']):
                print("⚠️ WARNING: 'timestamp' is not integer or datetime!")

        if df.dtypes.isin(['object']).any():
            print("⚠️ WARNING: Some columns are still object type!")

    except Exception as e:
        print(f"❌ Failed to load {file_path}: {e}")


def run_dtype_checks(base_dir, year, month, chain_name="ethereum"):
    base_path = os.path.join(base_dir, "data", "intermediate", "abstract", chain_name, f"{year:04d}", f"{month:02d}")
    
    filenames = [
        f"{chain_name}__abstract_token_transfer__{year}_{month:02d}.csv",
        f"{chain_name}__abstract_block__{year}_{month:02d}.csv",
        f"{chain_name}__abstract_transaction__{year}_{month:02d}.csv",
        f"{chain_name}__abstract_token__{year}_{month:02d}.csv",
        f"{chain_name}__abstract_account__{year}_{month:02d}.csv",
    ]

    for filename in filenames:
        file_path = os.path.join(base_path, filename)
        check_dtypes(file_path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--chain_name", type=str, default="ethereum")
    args = parser.parse_args()

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    run_dtype_checks(BASE_DIR, args.year, args.month, args.chain_name)
