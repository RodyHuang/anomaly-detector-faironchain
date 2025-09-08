import pandas as pd
import numpy as np
from itertools import combinations
from scipy.stats import spearmanr

try:
    from sklearn.metrics import cohen_kappa_score
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False

# File path
path = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\output\graph\ethereum\2023\01\ethereum__analysis_result__2023_01.csv"

# Load file
df = pd.read_csv(path)

# Column mapping (higher values = more anomalous, already on 0–100 scale)
cols = {
    "Rule": "rule_score_100",
    "Mahalanobis": "mahalanobis_distance_stats_score_100",
    "IForest": "iforest_stats_score_100"
}

# Clean up (keep only required columns and address; drop rows with NaN in any score column)
need_cols = ["address"] + list(cols.values())
df = df[need_cols].dropna(subset=list(cols.values())).reset_index(drop=True)

n = len(df)
print(f"Total samples: {n}")

# ==== Utility functions ====
def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if (a | b) else 0.0

def overlap_at_k(a: set, b: set, k: int) -> float:
    return (len(a & b) / k) if k > 0 else 0.0

def topk_set(series: pd.Series, k: int) -> set:
    if k <= 0:
        return set()
    # nlargest breaks ties arbitrarily, which is fine; 
    # for stable results, add a secondary sort (e.g., by address)
    return set(series.nlargest(k).index)

def topq_binary(series: pd.Series, q: float) -> pd.Series:
    """Mark the top q% as 1, the rest as 0."""
    thr = series.quantile(1 - q)
    return (series >= thr).astype(int)

# === (1) Top-q Overlap: q ∈ {0.1%, 0.5%, 1%} ===
q_list = [0.001, 0.005, 0.01]  # 0.1%, 0.5%, 1%
overlap_rows = []
triple_rows = []

# Prepare each detector’s score series (aligned by index)
scoreS = {name: df[col] for name, col in cols.items()}

for q in q_list:
    k = max(1, int(round(n * q)))
    # Get top-k indices for each detector (use index as ID, then map back to address)
    top_idx = {name: topk_set(scoreS[name], k) for name in scoreS}
    top_addr = {name: set(df.loc[list(top_idx[name]), "address"]) for name in scoreS}

    # Pairwise overlap metrics
    for a, b in combinations(scoreS.keys(), 2):
        A, B = top_addr[a], top_addr[b]
        inter = len(A & B)
        union = len(A | B)
        overlap_rows.append({
            "q_percent": q * 100,
            "k": k,
            "Pair": f"{a} vs {b}",
            "Intersection": inter,
            "Union": union,
            "Jaccard": jaccard(A, B),
            "Overlap": overlap_at_k(A, B, k)
        })

    # Triple intersection size
    A, B, C = top_addr["Rule"], top_addr["Mahalanobis"], top_addr["IForest"]
    triple_rows.append({
        "q_percent": q * 100,
        "k": k,
        "Triple_Intersection_Size": len(A & B & C)
    })

overlap_df = pd.DataFrame(overlap_rows).sort_values(["q_percent", "Pair"]).reset_index(drop=True)
triple_df = pd.DataFrame(triple_rows).sort_values("q_percent").reset_index(drop=True)

print("\n=== Top-q Overlap (pairwise) ===")
print(overlap_df)

print("\n=== Triple Intersection Size ===")
print(triple_df)

# === (2) Global ranking consistency: Spearman ρ (ranking correlation across all samples) ===
spearman_rows = []
for a, b in combinations(scoreS.keys(), 2):
    rho, p = spearmanr(scoreS[a], scoreS[b])
    spearman_rows.append({"Pair": f"{a} vs {b}", "Spearman_rho": rho, "p_value": p})
spearman_df = pd.DataFrame(spearman_rows).sort_values("Pair").reset_index(drop=True)

print("\n=== Spearman Rank Correlation (global) ===")
print(spearman_df)

# === (3) Binary agreement: Cohen’s κ (binarized at top 1%) ===
if SKLEARN_OK:
    q_bin = 0.01
    bin_labels = {name: topq_binary(scoreS[name], q_bin) for name in scoreS}
    kappa_rows = []
    for a, b in combinations(bin_labels.keys(), 2):
        kappa = cohen_kappa_score(bin_labels[a], bin_labels[b])
        kappa_rows.append({"Pair": f"{a} vs {b}", "q_percent": q_bin * 100, "Cohen_kappa": kappa})
    kappa_df = pd.DataFrame(kappa_rows).sort_values("Pair").reset_index(drop=True)
    print("\n=== Cohen’s κ (binary agreement at top 1%) ===")
    print(kappa_df)
else:
    print("\n[Warning] scikit-learn not installed, skipping Cohen’s κ calculation. Please install: pip install scikit-learn")
