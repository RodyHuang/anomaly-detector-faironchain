import pandas as pd
import numpy as np
import os
from typing import List

base_dir = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\output\graph\ethereum\2023\02"
file1 = os.path.join(base_dir, "ethereum__features__2023_02_0.csv")
file2 = os.path.join(base_dir, "ethereum__features__2023_02.csv")

TARGET_COLS: List[str] = ["egonet_node_count", "egonet_edge_count", "egonet_density"]
CANDIDATE_KEYS: List[str] = [
    "account_sid","address_sid","account_id","vertex_id","node_id",
    "address","label","account","id"
]

def pick_key(path1, path2, candidates):
    cols1 = set(pd.read_csv(path1, nrows=0).columns)
    cols2 = set(pd.read_csv(path2, nrows=0).columns)
    for k in candidates:
        if k in cols1 and k in cols2:
            return k
    raise RuntimeError("找不到可共同對齊的鍵欄位，請確認兩檔共有的唯一ID（如 account_sid / address / vertex_id 等）。")

key = pick_key(file1, file2, CANDIDATE_KEYS)
print(f"√ 對齊鍵：{key}")

usecols = [key] + TARGET_COLS

# 只載需要的欄位並確保型別
df1 = pd.read_csv(file1, usecols=lambda c: c in usecols)
df2 = pd.read_csv(file2, usecols=lambda c: c in usecols)

# 去重（保留第一筆）
df1 = df1.drop_duplicates(subset=[key])
df2 = df2.drop_duplicates(subset=[key])

# 轉成數值（非數值變 NaN），避免字串/型別不一致
for c in TARGET_COLS:
    if c in df1: df1[c] = pd.to_numeric(df1[c], errors="coerce")
    if c in df2: df2[c] = pd.to_numeric(df2[c], errors="coerce")

# 基本統計
only1 = set(df1[key]) - set(df2[key])
only2 = set(df2[key]) - set(df1[key])
both = set(df1[key]) & set(df2[key])

print(f"檔案1筆數: {len(df1):,} | 檔案2筆數: {len(df2):,}")
print(f"共同鍵數: {len(both):,}")
print(f"只在檔案1: {len(only1):,} | 只在檔案2: {len(only2):,}")

# 只比對共同鍵，避免無意義的 NaN
df1c = df1[df1[key].isin(both)].set_index(key).sort_index()
df2c = df2[df2[key].isin(both)].set_index(key).sort_index()

merged = df1c.join(df2c, how="inner", lsuffix="_1", rsuffix="_2")

# 比對：整數欄位用相等，浮點欄位用 isclose
def compare_series(s1, s2, float_tolerance=True):
    if float_tolerance:
        return np.isclose(s1.to_numpy(dtype="float64"),
                          s2.to_numpy(dtype="float64"),
                          rtol=1e-9, atol=1e-12, equal_nan=True)
    else:
        return (s1.values == s2.values) | (pd.isna(s1.values) & pd.isna(s2.values))

results = {}
for col in TARGET_COLS:
    col1, col2 = f"{col}_1", f"{col}_2"
    if col not in ["egonet_density"]:
        eq_mask = compare_series(merged[col1], merged[col2], float_tolerance=False)
    else:
        eq_mask = compare_series(merged[col1], merged[col2], float_tolerance=True)
    results[col] = eq_mask

all_equal_mask = np.logical_and.reduce(list(results.values()))
equal_count = int(all_equal_mask.sum())
diff_count = int((~all_equal_mask).sum())

print("\n=== 比對結果（只看共同鍵） ===")
print(f"完全相同的列數：{equal_count:,}")
print(f"有差異的列數：{diff_count:,}")

# 各欄位的不一致數
for col, mask in results.items():
    ne = int((~mask).sum())
    print(f"- {col} 不一致：{ne:,}")

if diff_count > 0:
    print("\n前 10 筆差異（含鍵與兩邊數值）:")
    diff_rows = merged.loc[~all_equal_mask, [c for col in TARGET_COLS for c in (f"{col}_1", f"{col}_2")]]
    # 把 index（鍵）帶回來看
    preview = diff_rows.reset_index().head(10)
    print(preview.to_string(index=False))
