import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import skew, kurtosis


def evaluate_distribution(series: pd.Series, name: str = ""):
    """
    Print basic distribution stats: skewness, kurtosis, % of zero values.
    """
    series = series.dropna()
    sk = skew(series)
    kt = kurtosis(series, fisher=False)
    pct_zero = (series == 0).mean()
    print(f"📊 Feature: {name}")
    print(f"  Skewness: {sk:.2f}")
    print(f"  Kurtosis: {kt:.2f}")
    print(f"  % Zeros:  {pct_zero:.2%}")
    print("-" * 40)


def plot_distributions(series: pd.Series, title: str = ""):
    """
    Plot raw, log1p, and double-log1p histograms side-by-side.
    """
    series = series.dropna()

    transformed = {
        "Raw": series,
        "Log1p": np.log1p(series),
        "Double Log": np.log1p(np.log1p(series))
    }

    plt.figure(figsize=(15, 4))
    for i, (label, s) in enumerate(transformed.items(), start=1):
        plt.subplot(1, 3, i)
        plt.hist(s, bins=100, color="steelblue")
        plt.title(f"{title} - {label}")
        plt.grid(True)

    plt.tight_layout()
    plt.show()


def suggest_transformation(series: pd.Series) -> str:
    """
    Suggest whether to apply double log transformation based on Z-score spread.
    """
    series = series.dropna()
    if (series <= 0).all():
        return "⚠️ All zeros or negative — skip"

    # Compute z-scores for both log and double log
    log1p = np.log1p(series)
    dlog = np.log1p(log1p)

    log_z = (log1p - log1p.mean()) / log1p.std()
    dlog_z = (dlog - dlog.mean()) / dlog.std()

    log_z_max = np.abs(log_z).max()
    dlog_z_max = np.abs(dlog_z).max()

    if dlog_z_max < 6 and log_z_max > 10:
        return "✅ Suggest: Double Log"
    elif log_z_max > 8:
        return "⚠️ Consider: Log1p"
    else:
        return "👍 Log1p sufficient"


def diagnose_feature(df: pd.DataFrame, feature_name: str):
    """
    Diagnose a single feature: print stats, show plots, and suggest transformation.
    """
    print(f"\n🔍 Diagnosing feature: {feature_name}")
    series = df[feature_name]

    evaluate_distribution(series, "Raw")
    evaluate_distribution(np.log1p(series), "Log1p")
    evaluate_distribution(np.log1p(np.log1p(series)), "Double Log")

    suggestion = suggest_transformation(series)
    print(f"📌 Transformation Suggestion: {suggestion}")
    plot_distributions(series, title=feature_name)


def batch_diagnose(df: pd.DataFrame, features: list[str]):
    """
    Run diagnosis for a list of features.
    """
    for feature in features:
        diagnose_feature(df, feature)
        print("\n" + "=" * 60 + "\n")

features = [
    "unique_in_degree", "unique_out_degree",
    "total_input_amount", "total_output_amount",
    "two_node_loop_count", "triangle_loop_count",
    "egonet_node_count", "egonet_density"
]

csv_path = r"C:\Users\rodyh\Desktop\FairOnChain\Code\whale-anomaly-detector-faironchain\data\output\graph\ethereum\2023\01\ethereum__features__2023_01.csv"
df = pd.read_csv(csv_path)

batch_diagnose(df, features)