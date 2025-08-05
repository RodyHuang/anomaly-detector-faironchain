import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

def plot_distribution(df, col_name, output_dir):
    # Prepare values
    raw = df[col_name].fillna(0)
    logged = np.log1p(raw)

    # Plot
    fig, axs = plt.subplots(1, 2, figsize=(12, 4))
    
    axs[0].hist(raw, bins=50, color='gray')
    axs[0].set_title(f"{col_name} - Raw")
    
    axs[1].hist(logged, bins=50)
    axs[1].set_title(f"{col_name} - log(x+1)")
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{col_name}_distribution.png"))
    plt.close()


def analyze_feature(df, col_name):
    x = df[col_name].fillna(0)
    x_nonzero = x[x > 0]

    stats = {
        "count": len(x),
        "non_zero_count": (x > 0).sum(),
        "zero_ratio": (x == 0).mean(),
        "mean": x.mean(),
        "median": x.median(),
        "std": x.std(),
        "max": x.max(),
        "99% quantile": x.quantile(0.99),
        "log_max": np.log1p(x.max())
    }

    print(f"\n📊 Feature: {col_name}")
    for k, v in stats.items():
        print(f"{k:>18}: {v:,.2f}")
    return stats


if __name__ == "__main__":
    input_csv = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\output\graph\ethereum\2023\01\ethereum__features__2023_01.csv"
   # 🔁 你可以改成變數或 argparse
    output_dir = "feature_analysis_output"
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(input_csv)

    for feature in ["two_node_loop_count", "triangle_loop_count"]:
        analyze_feature(df, feature)
        plot_distribution(df, feature, output_dir)
