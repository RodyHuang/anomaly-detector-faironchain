# simple_validate_abstract_month.py
import pandas as pd
from pathlib import Path
from pandas.api.types import is_integer_dtype, is_string_dtype

base = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\intermediate\abstract\ethereum\2023\01"

def load_parquet(name):
    path = Path(base) / name
    return pd.read_parquet(path)

def check(msg, cond):
    if cond:
        print(f"✅ {msg}")
    else:
        print(f"❌ {msg}")

# Load files
tt  = load_parquet("ethereum__abstract_token_transfer__2023_01.parquet")
tx  = load_parquet("ethereum__abstract_transaction__2023_01.parquet")
blk = load_parquet("ethereum__abstract_block__2023_01.parquet")
acc = load_parquet("ethereum__abstract_account__2023_01.parquet")
tok = load_parquet("ethereum__abstract_token__2023_01.parquet")

print("\n=== Basic Info ===")
print("rows:", len(tt), len(tx), len(blk), len(acc), len(tok))

print("\n=== Primary Keys ===")
check("tt.transfer_sid unique & not null", tt["transfer_sid"].notna().all() and tt["transfer_sid"].is_unique)
check("tx.tx_sid unique & not null", tx["tx_sid"].notna().all() and tx["tx_sid"].is_unique)
check("blk.block_sid unique & not null", blk["block_sid"].notna().all() and blk["block_sid"].is_unique)
check("acc.account_sid unique & not null", acc["account_sid"].notna().all() and acc["account_sid"].is_unique)

print("\n=== Required Fields Non-Null ===")
check("tt key fields non-null", tt[["tx_sid","spender_address_sid","receiver_address_sid","token_sid","amount"]].notna().all().all())
check("tx key fields non-null", tx[["tx_sid","tx_hash","block_sid"]].notna().all().all())
check("blk key fields non-null", blk[["block_sid","block_number","timestamp"]].notna().all().all())
check("acc key fields non-null", acc[["account_sid","address","type"]].notna().all().all())

print("\n=== Dtype Rules ===")
check("blk.block_number int", is_integer_dtype(blk["block_number"]))
check("blk.timestamp int", is_integer_dtype(blk["timestamp"]))
check("tt.transfer_index int", is_integer_dtype(tt["transfer_index"]))
check("tt.amount string-like", is_string_dtype(tt["amount"]) or tt["amount"].dtype == "object")

print("\n=== Referential Integrity ===")
acc_sid_set = set(acc["account_sid"])
check("tt.spender exists in acc", (~tt["spender_address_sid"].isin(acc_sid_set)).sum() == 0)
check("tt.receiver exists in acc", (~tt["receiver_address_sid"].isin(acc_sid_set)).sum() == 0)
check("tt.tx_sid subset of tx", set(tt["tx_sid"]).issubset(set(tx["tx_sid"])))
check("tx.block_sid subset of blk", set(tx["block_sid"]).issubset(set(blk["block_sid"])))

print("\n=== Timestamp ===")
check("blk.timestamp plausible [2014..2050]", blk["timestamp"].between(1_400_000_000, 2_500_000_000).all())

print("\n=== Token Rules ===")
check("tt.token_sid only {1_native}", set(tt["token_sid"].unique()) == {"1_native"})

print("\n=== Amount Rules ===")
check("amount non-null", tt["amount"].notna().all())
check("amount digits only", tt["amount"].astype(str).str.fullmatch(r"\d+").all())
check("amount > 0", (tt["amount"] != "0").all())
check("amount length <= 78", tt["amount"].astype(str).str.len().max() <= 78)

print("\n=== Done ===")
