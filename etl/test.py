# validate_abstract_month.py
import os
from pathlib import Path
import re
import pandas as pd
from pandas.api.types import is_integer_dtype, is_string_dtype

# ========= 配置 =========
base = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\intermediate\abstract\ethereum\2023\01"

# ========= 輔助函式 =========
def show_dtypes(df: pd.DataFrame, name: str):
    print(f"\n=== {name} | rows={len(df):,}, cols={df.shape[1]} ===")
    dtypes_str = df.dtypes.sort_index().astype(str)
    print(dtypes_str.to_string())
    print("\n[dtype counts]")
    print(dtypes_str.value_counts().to_string())

def show_na(df: pd.DataFrame, name: str):
    cols = df.columns[df.isna().any()]
    if len(cols):
        print(f"\n{name} NA counts:")
        print(df[cols].isna().sum().sort_values(ascending=False).to_string())
    else:
        print(f"\n{name}: no NA")

def assert_dtype(df: pd.DataFrame, col: str, kind: str):
    s = df[col]
    if kind == "int":
        assert is_integer_dtype(s), f"{col} expected integer dtype, got {s.dtype}"
    elif kind == "string":
        # 允許 pandas StringDtype 或 object（字串）
        assert is_string_dtype(s) or s.dtype == "object", f"{col} expected string-like dtype, got {s.dtype}"

def load_parquet(path: str) -> pd.DataFrame:
    p = Path(path)
    assert p.exists(), f"File not found: {p}"
    return pd.read_parquet(p)

# ========= 讀檔 =========
tt  = load_parquet(fr"{base}\ethereum__abstract_token_transfer__2023_01.parquet")
tx  = load_parquet(fr"{base}\ethereum__abstract_transaction__2023_01.parquet")
blk = load_parquet(fr"{base}\ethereum__abstract_block__2023_01.parquet")
acc = load_parquet(fr"{base}\ethereum__abstract_account__2023_01.parquet")
tok = load_parquet(fr"{base}\ethereum__abstract_token__2023_01.parquet")

# ========= 顯示 dtype / NA =========
show_dtypes(tt,  "AbstractTokenTransfer (tt)")
show_dtypes(tx,  "AbstractTransaction (tx)")
show_dtypes(blk, "AbstractBlock (blk)")
show_dtypes(acc, "AbstractAccount (acc)")
show_dtypes(tok, "AbstractToken (tok)")

show_na(tt,  "tt")
show_na(tx,  "tx")
show_na(blk, "blk")
show_na(acc, "acc")
show_na(tok, "tok")

# ========= 基本筆數 =========
print("\nrow counts:", len(tt), len(tx), len(blk), len(acc), len(tok))

# ========= 主鍵唯一 + 非空 =========
assert tt["transfer_sid"].notna().all() and tt["transfer_sid"].is_unique, "tt.transfer_sid 唯一性或 NA 失敗"
assert tx["tx_sid"].notna().all()       and tx["tx_sid"].is_unique,       "tx.tx_sid 唯一性或 NA 失敗"
assert blk["block_sid"].notna().all()   and blk["block_sid"].is_unique,   "blk.block_sid 唯一性或 NA 失敗"
assert acc["account_sid"].notna().all() and acc["account_sid"].is_unique, "acc.account_sid 唯一性或 NA 失敗"

# ========= 重要欄位非空 =========
assert tt[["tx_sid","spender_address_sid","receiver_address_sid","token_sid","amount"]].notna().all().all(), "tt 關鍵欄位有 NA"
assert tx[["tx_sid","tx_hash","block_sid"]].notna().all().all(),                                               "tx 關鍵欄位有 NA"
assert blk[["block_sid","block_number","timestamp"]].notna().all().all(),                                      "blk 關鍵欄位有 NA"
assert acc[["account_sid","address","type"]].notna().all().all(),                                              "acc 關鍵欄位有 NA"
# token 表：僅檢查常用欄位（address 可能為空，若是原生幣）
tok_need = [c for c in ["token_sid","token_standard","token_symbol"] if c in tok.columns]
if tok_need:
    assert tok[tok_need].notna().all().all(), f"tok 欄位 {tok_need} 有 NA"

# ========= dtype 規則（輕量） =========
assert_dtype(blk, "block_number", "int")
assert_dtype(blk, "timestamp", "int")
assert_dtype(tt,  "transfer_index", "int")
assert_dtype(tt,  "amount", "string")

# ========= 關聯完整性 =========
# transfer → account（spender/receiver 都存在）
acc_sid_set = set(acc["account_sid"])
missing_spender  = (~tt["spender_address_sid"].isin(acc_sid_set)).sum()
missing_receiver = (~tt["receiver_address_sid"].isin(acc_sid_set)).sum()
print("missing_spender:", missing_spender, "missing_receiver:", missing_receiver)
assert missing_spender == 0 and missing_receiver == 0, "tt.spender/receiver 有不存在的 account_sid"

# transfer → transaction
assert set(tt["tx_sid"]).issubset(set(tx["tx_sid"])), "some tt.tx_sid not in tx"

# transaction → block
assert set(tx["block_sid"]).issubset(set(blk["block_sid"])), "some tx.block_sid not in blk"

# ========= 時間檢查 =========
assert blk["timestamp"].dtype.kind in ("i","u"), "blk.timestamp 不是整數"
assert blk["timestamp"].between(1_400_000_000, 2_500_000_000).all(), "blk.timestamp 超出秒級合理範圍(2014~2050)"

# ========= token_sid（以太坊原生幣） =========
assert set(tt["token_sid"].unique()) == {"1_native"}, "tt.token_sid 非預期（應為 1_native）"

# ========= amount 檢查 =========
print("\namount dtype:", tt["amount"].dtype)
assert tt["amount"].notna().all(), "amount 出現 NA"

only_digits = tt["amount"].astype(str).str.fullmatch(r"\d+")
bad_rows = (~only_digits).sum()
print("non-digit amount rows:", bad_rows)
assert bad_rows == 0, "amount 含非數字字元"

nonpos = (tt["amount"] == "0").sum()
print("zero amount rows:", nonpos)
assert nonpos == 0, "有 0 金額的轉帳"

max_len = tt["amount"].str.len().max()
print("max digit length:", max_len)
assert max_len <= 78, "金額超過 uint256 上限？"

# （可選）分位數概覽（ETH）
# amt_eth = tt["amount"].map(int) / 10**18
# print("\namount quantiles (ETH):")
# print(amt_eth.quantile([0.5, 0.9, 0.99, 0.999]))

print("\n✅ ALL CHECKS PASSED")
