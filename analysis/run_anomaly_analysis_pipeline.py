import os
import argparse
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
from rule_based_anomaly_detection import compute_thresholds, apply_all_rules
from statistical_anomaly_detection import preprocess_features, compute_mahalanobis_distance
from unsupervised_learning_anomaly_detection import fit_iforest_and_score  
from scoring import score_rule_based, score_statistical_percentile,score_iforest_percentile, combine_scores

def get_input_path(base_dir, chain, year, month):
    return os.path.join(
        base_dir, "data", "output", "graph", chain, f"{year:04d}", f"{month:02d}",
        f"{chain}__features__{year}_{month:02d}.csv"
    )

def get_output_path(base_dir, chain, year, month):
    return os.path.join(
        base_dir, "data", "output", "graph", chain, f"{year:04d}", f"{month:02d}",
        f"{chain}__analysis_result__{year}_{month:02d}.csv"
    )

def run_anomaly_analysis_pipeline(chain: str, year: int, month: int):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_path = get_input_path(base_dir, chain, year, month)
    output_path = get_output_path(base_dir, chain, year, month)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Feature file not found: {input_path}")

    # === Load data and preserve original index ===
    df = pd.read_csv(input_path)
    df["original_index"] = df.index

    # === Split infra and non-infra ===
    df_infra = df[df["is_infra"] == 1].copy()
    df_non_infra = df[df["is_infra"] == 0].copy()

    num_cols = [
    "total_input_amount", "total_output_amount",
    "unique_in_degree", "unique_out_degree",
    "two_node_loop_amount", "two_node_loop_tx_count",
    "triangle_loop_amount", "triangle_loop_tx_count"
    ]
    present = [c for c in num_cols if c in df_non_infra.columns]
    df_non_infra[present] = df_non_infra[present].apply(pd.to_numeric, errors="coerce").fillna(0)
    
    # === 1: Rule-based anomaly detection ===
    thresholds = compute_thresholds(df_non_infra, [
        "unique_in_degree", "unique_out_degree",
        "two_node_loop_amount", "two_node_loop_tx_count",
        "triangle_loop_amount", "triangle_loop_tx_count"
    ], ignore_zeros_columns=[
    "two_node_loop_amount", "two_node_loop_tx_count",
    "triangle_loop_amount", "triangle_loop_tx_count"
    ])

    df_non_infra = apply_all_rules(df_non_infra, thresholds)

    # === 2: Statistical anomaly detection ===
    df_non_infra = preprocess_features(df_non_infra)

    statistical_features = [
        "unique_in_degree_log_z", "unique_out_degree_log_z",
        "total_input_amount_log_z", "total_output_amount_log_z",
        "two_node_loop_count_log_z", "triangle_loop_count_log_z",
        "log_degree_ratio_z", "log_amount_ratio_z",
        "egonet_density_z"
    ]

    df_non_infra = compute_mahalanobis_distance(df_non_infra, statistical_features)

    # === 3: Isolation Forest ===
    df_non_infra = fit_iforest_and_score(
        df_non_infra,
        features=statistical_features,
        max_samples=100_000,
        n_estimators=300
    )

    # === 4: Scoring (0–100) ===
    df_non_infra = score_rule_based(df_non_infra)
    df_non_infra = score_statistical_percentile(df_non_infra)   # uses 'mahalanobis_distance'
    df_non_infra = score_iforest_percentile(df_non_infra)       # uses 'iforest_score'
    df_non_infra = combine_scores(df_non_infra)                  # makes 'final_score_0_100'

    # === 5: Merge and restore original order ===
    df_combined = pd.concat([df_non_infra, df_infra], axis=0)
    df_combined = df_combined.sort_values("original_index").drop(columns=["original_index"])
    # === Drop *_log, *_z, and *_ratio columns unless needed ===
    drop_cols = [
        col for col in df_combined.columns
        if col.endswith("_log") or col.endswith("_z") or col.endswith("_ratio")
    ]
    df_combined = df_combined.drop(columns=drop_cols)

    # === Save to CSV & Parquet ===
    df_combined.to_csv(output_path, index=False)
    output_parquet_path = output_path.replace(".csv", ".parquet")
    df_combined.to_parquet(output_parquet_path, index=False)

    print(f"✅ Saved CSV to: {output_path}")
    print(f"✅ Saved Parquet to: {output_parquet_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain", type=str, default="ethereum", help="Target blockchain (default: ethereum)")
    parser.add_argument("--year", type=int, required=True, help="Target year (e.g., 2024)")
    parser.add_argument("--month", type=int, required=True, help="Target month (1–12)")

    args = parser.parse_args()
    run_anomaly_analysis_pipeline(args.chain, args.year, args.month)
