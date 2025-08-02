import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# === 路徑與檔案名稱
path = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\output\graph\ethereum\2023\01\ethereum__features__2023_01.csv"

# === 載入資料
df = pd.read_csv(path)

# === 檢查欄位
if "out_degree" not in df.columns:
    raise ValueError("❌ 欄位 'out_degree' 不存在，請確認特徵檔內容。")

# === 基本統計資訊
print("📊 out-degree 統計摘要：")
print(df["out_degree"].describe(percentiles=[0.9, 0.95, 0.99, 0.999]))

# === 建議門檻：Top 10%, 5%, 1%
q90 = df["out_degree"].quantile(0.90)
q95 = df["out_degree"].quantile(0.95)
q99 = df["out_degree"].quantile(0.99)
max_val = df["out_degree"].max()

print("\n📌 建議門檻選項：")
print(f"Top 10% (90% quantile): out_degree ≥ {q90:.2f}")
print(f"Top 5%  (95% quantile): out_degree ≥ {q95:.2f}")
print(f"Top 1%  (99% quantile): out_degree ≥ {q99:.2f}")
print(f"最大值: {max_val}")

# === 畫 log-scale 分布圖
plt.figure(figsize=(8, 5))
plt.hist(np.log1p(df["out_degree"]), bins=100, color='darkorange')
plt.xlabel("log(1 + out-degree)")
plt.ylabel("Address count")
plt.title("Out-degree distribution (log scale)")
plt.grid(True)
plt.tight_layout()
plt.show()
