import pandas as pd

def preprocess_blocks(input_path, output_path):
    usecols = ["block_number", "chain_id", "block_hash", "timestamp", "parent_hash"]
    df = pd.read_csv(input_path, usecols=usecols)
    print(f"Loaded {len(df)} rows from: {input_path}")

    df["chain_id"] = df["chain_id"].fillna(1)

    # Drop missing values
    before_dropna = len(df)
    df = df.dropna(subset=["block_number", "block_hash", "timestamp"])
    print(f"Missing value rows dropped: {before_dropna - len(df)}")

    # Block number validation
    def is_valid_block_number(val, max_block=999_999_999):
        try:
            val_str = str(val).strip()
            if val_str.lower().startswith("0x") or not val_str.isdigit() or len(val_str) > 20:
                return False
            num = int(val_str)
            return 10_000 <= num <= max_block
        except:
            return False

    before_bn = len(df)
    df = df[df["block_number"].apply(is_valid_block_number)]
    df["block_number"] = df["block_number"].astype("Int64")
    print(f"Block number cleaned: {before_bn - len(df)} rows removed")

    # Block hash validation
    def is_valid_block_hash(val):
        try:
            val_str = str(val).strip().lower()
            return val_str.startswith("0x") and len(val_str) == 66
        except:
            return False

    before_bh = len(df)
    df = df[df["block_hash"].apply(is_valid_block_hash)]
    df["block_hash"] = df["block_hash"].str.strip().str.lower()
    print(f"Block hash cleaned: {before_bh - len(df)} rows removed")

    # Timestamp validation
    before_ts = len(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
    df = df.dropna(subset=["timestamp"])
    print(f"Timestamp cleaned: {before_ts - len(df)} rows removed")

    # Chain ID check
    df["chain_id"] = df["chain_id"].astype(int)
    print("Unique chain_id values:", df["chain_id"].unique())

    # Summary
    original_len = before_dropna
    final_len = len(df)
    print(f"📊 Summary: {original_len} → {final_len} rows kept ({original_len - final_len} removed)")

    # Save
    df.to_csv(output_path, index=False)
    print(f"Cleaned block data saved to: {output_path}")
