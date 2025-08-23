import pandas as pd

def preprocess_blocks(input_path, output_path):
    """
    Clean raw block CSV for one day:
    - select stable columns
    - validate/normalize block_number, block_hash, timestamp, chain_id
    - drop rows with missing/invalid critical fields
    """
    usecols = ["block_number", "chain_id", "block_hash", "timestamp", "parent_hash"]
    df = pd.read_csv(input_path, usecols=usecols)
    print(f"Loaded {len(df)} rows from: {input_path}")

    # default chain_id if missing
    df["chain_id"] = df["chain_id"].fillna(1)

     # drop rows with critical nulls
    before_dropna = len(df)
    df = df.dropna(subset=["block_number", "block_hash", "timestamp"])
    print(f"Missing value rows dropped: {before_dropna - len(df)}")

    # block number validation
    def is_valid_block_number(val, max_block=999_999_999):
        try:
            val_str = str(val).strip()
            if val_str.lower().startswith("0x") or len(val_str) > 20:
                return False
            
            num = int(float(val_str))
            return 10_000 <= num <= max_block
        except:
            return False

    before_bn = len(df)
    df = df[df["block_number"].apply(is_valid_block_number)]
    df["block_number"] = df["block_number"].astype("Int64")
    print(f"Block number cleaned: {before_bn - len(df)} rows removed")

    # block hash validation (0x + 64 hex)
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

    # timetamp: assume epoch second
    before_ts = len(df)

    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    removed = before_ts - df["timestamp"].notna().sum()
    df = df.dropna(subset=["timestamp"])
    print(f"Timestamp cleaned: {removed} rows removed")
    
    df["timestamp"] = df["timestamp"].astype("int64")

    # chain_id to int
    df["chain_id"] = df["chain_id"].astype(int)
    print("Unique chain_id values:", df["chain_id"].unique())

    # summary
    original_len = before_dropna
    final_len = len(df)
    print(f"📊 Summary: {original_len} → {final_len} rows kept ({original_len - final_len} removed)")

    # save
    df.to_csv(output_path, index=False)
    print(f"Cleaned block data saved to: {output_path}")
