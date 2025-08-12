# analysis/check_csv_dtypes.py
import pandas as pd
import argparse
import os

def check_csv_dtypes(path: str):
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        return

    print(f"📂 Loading CSV: {path}")
    df = pd.read_csv(path, low_memory=False)

    print(f"\n📊 Shape: {df.shape[0]} rows × {df.shape[1]} cols")
    print("\n📄 Column Data Types:")
    print(df.dtypes)

    # 檢查疑似型態錯誤欄位
    print("\n🔍 Checking for potential dtype issues...")
    suspect_cols = []
    for col in df.columns:
        if df[col].dtype == "object":
            # 嘗試轉成數字
            try:
                pd.to_numeric(df[col], errors="raise")
                suspect_cols.append(col)
            except Exception:
                pass

    if suspect_cols:
        print("⚠️ Potential numeric columns stored as object (string):")
        for col in suspect_cols:
            print(f"  - {col}")
        print("\n💡 Suggested fix when reading CSV:")
        dtype_dict = {col: "float64" for col in suspect_cols}
        print(f"dtype = {dtype_dict}")
        print('df = pd.read_csv(path, dtype=dtype)')
    else:
        print("✅ No obvious numeric-object dtype issues found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check CSV column data types")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    args = parser.parse_args()

    check_csv_dtypes(args.csv)
