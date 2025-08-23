import pickle
import pandas as pd
import os

# === 路徑設定 ===
graph_path = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\output\graph\ethereum\2023\01\ethereum__token_transfer_graph__2023_01.pkl"
whitelist_path = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\graph\infra_whitelist.csv"
out_path = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\graph\infra_candidates_2023_01.csv"

# === 1. 載入 graph ===
with open(graph_path, "rb") as f:
    g, account_to_idx = pickle.load(f)

# === 2. 載入 whitelist ===
wl = pd.read_csv(whitelist_path)

# 標準化：去掉空白 + 小寫
wl_addrs = wl["address"].astype(str).str.strip().str.lower()
whitelist = set(wl_addrs)

# 同時建一份帶 chain_id 前綴的（例如 "1_0x..."）
chain_id = "1"  # Ethereum mainnet
whitelist_with_prefix = set(chain_id + "_" + addr for addr in whitelist)

print(f"📋 Whitelist loaded: {len(whitelist)} addresses")

# === 3. 計算度數 ===
in_deg = g.indegree()
out_deg = g.outdegree()
total_deg = [i + o for i, o in zip(in_deg, out_deg)]

# === 4. 建立 DataFrame ===
vid_to_addr = {v: a.lower() for a, v in account_to_idx.items()}

df = pd.DataFrame({
    "address": [vid_to_addr[v] for v in range(g.vcount())],
    "in_degree": in_deg,
    "out_degree": out_deg,
    "total_degree": total_deg
})

# === 5. 過濾 whitelist ===
before = len(df)
df = df[~df["address"].isin(whitelist)]
df = df[~df["address"].isin(whitelist_with_prefix)]
after = len(df)

print(f"🧹 Removed {before - after} whitelisted addresses")

# === 6. 取前 100 大 ===
candidates = df.sort_values("total_degree", ascending=False).head(100)

# === 7. 輸出 CSV ===
os.makedirs(os.path.dirname(out_path), exist_ok=True)
candidates.to_csv(out_path, index=False)
print(f"✅ Saved {len(candidates)} candidates to {out_path}")
