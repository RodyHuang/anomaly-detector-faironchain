import pandas as pd
import numpy as np
import os
from typing import List

base_dir = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\output\graph\ethereum\2023\02"
file1 = os.path.join(base_dir, "ethereum__features__2023_02_0.csv")  # 舊命名版
file2 = os.path.join(base_dir, "ethereum__features__2023_02.csv")    # 新命名版

# 自動找對齊鍵
CANDIDATE_KEYS: List[str] = ["node","node_id","account_sid","address","vertex_id","id"]
def pick_key(p1, p2, candidates):
    c1 = set(pd.read_csv(p1, nrows=0).columns)
    c2 = set(pd.read_csv(p2, nrows=0).columns)
    for k in candidates:
        if k in c1 and k in c2:
            return k
    raise RuntimeError("找不到共同鍵欄位，請確認兩檔共有的唯一ID（如 node）。")

key = pick_key(file1, file2, CANDIDATE_KEYS)
print(f"√ 對齊鍵：{key}")

# 讀完整欄位
df_old = pd.read_csv(file1)
df_new = pd.read_csv(file2)

# 先去重
df_old = df_old.drop_duplicates(subset=[key])
df_new = df_new.drop_duplicates(subset=[key])

# 針對舊檔 → 新檔的欄位對齊（你說的對應關係）
rename_map = {
    "in_degree": "in_transfer_count",
    "out_degree": "out_transfer_count",
    "unique_in_degree": "in_degree",
    "unique_out_degree": "out_degree",
}
# 只改舊檔欄位名
df_old = df_old.rename(columns={k:v for k,v in rename_map.items() if k in df_old.columns})

# 只比共同鍵的交集列，避免 NaN 干擾
both = set(df_old[key]) & set(df_new[key])
df_old = df_old[df_old[key].isin(both)].set_index(key).sort_index()
df_new = df_new[df_new[key].isin(both)].set_index(key).sort_index()

# 欄位集合對齊（缺誰就補誰為 NaN），確保能逐格比較
all_cols = sorted(set(df_old.columns) | set(df_new.columns))
for c in all_cols:
    if c not in df_old.columns:
        df_old[c] = np.nan
    if c not in df_new.columns:
        df_new[c] = np.nan

df_old = df_old[all_cols]
df_new = df_new[all_cols]

# 型別寬鬆統一（把能轉數值的都轉數值，其他保持字串），避免 '1' vs 1 的假差異
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        # 嘗試轉數值；全 NaN 就維持原狀
        s = pd.to_numeric(out[c], errors="coerce")
        if s.notna().sum() > 0 or out[c].isna().all():
            out[c] = s
        else:
            out[c] = out[c].astype(str)
    return out

df_old_n = normalize_df(df_old)
df_new_n = normalize_df(df_new)

# 先用 equals 快速檢查
if df_old_n.equals(df_new_n):
    print("🎉 兩檔內容在對齊命名後完全一致（含 NaN 位置與所有欄位）")
else:
    print("⚠️ 發現差異，統計如下：")
    diff_mask = (df_old_n != df_new_n) & ~(df_old_n.isna() & df_new_n.isna())
    total_diff = int(diff_mask.to_numpy().sum())
    row_diff = int(diff_mask.any(axis=1).sum())
    col_diff = int(diff_mask.any(axis=0).sum())
    print(f"- 不同的儲存格數：{total_diff:,}")
    print(f"- 涉及的列數量：{row_diff:,}")
    print(f"- 涉及的欄位數量：{col_diff:,}")

    # 各欄位不一致數
    print("\n各欄位不一致數：")
    per_col = diff_mask.sum(axis=0).sort_values(ascending=False)
    print(per_col[per_col > 0].to_string())

    # 列出前 10 列差異（附兩邊值）
    preview = pd.concat(
        [df_old_n[diff_mask.any(axis=1)].add_suffix("_old"),
         df_new_n[diff_mask.any(axis=1)].add_suffix("_new")],
        axis=1
    ).reset_index().head(10)
    print("\n前 10 筆差異：")
    print(preview.to_string(index=False))

    # 可選：輸出完整差異預覽
    preview.to_csv(os.path.join(base_dir, "diff_preview_after_rename.csv"), index=False)
