import pandas as pd
import numpy as np
import os
import argparse


def get_input_base_dir() -> str:
    """
    Return the absolute path to the data/output/graph directory.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))         # .../analysis
    project_root = os.path.dirname(current_dir)                      # .../whale-anomaly-detector-faironchain
    return os.path.join(project_root, "data", "output", "graph")     # .../data/output/graph


def compute_thresholds(df: pd.DataFrame, columns: list[str], quantile: float = 0.99) -> dict:
    """
    Compute the top quantile thresholds for a given list of columns.
    """
    return {col: df[col].quantile(quantile) for col in columns}


def apply_H1_rule(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Apply H1 anomaly rule: 'High-inflow aggregation account with limited outflow'

    Criteria:
        - unique_in_degree ≥ top 1%
        - unique_out_degree ≤ 3
        - |total_input_amount - total_output_amount| / total_input_amount ≤ 0.05

    Returns:
        DataFrame with new cols of H1_flag and H1_description.
    """
    unique_in_deg_threshold = thresholds["unique_in_degree"]

    # Safe computation of relative difference
    safe_ratio = np.where(
        df["total_input_amount"] > 0,
        np.abs(df["total_input_amount"] - df["total_output_amount"]) / df["total_input_amount"],
        np.nan
    )

    # Apply rule logic
    df["H1_flag"] = (
        (df["unique_in_degree"] >= unique_in_deg_threshold) &
        (df["unique_out_degree"] <= 3) &
        (safe_ratio <= 0.05)
    ).astype(int)

    # Attach explanation only if flagged
    df["H1_description"] = df["H1_flag"].apply(
        lambda x: (
            "H1: Aggregates from many sources and forwards almost unchanged to few addresses. May indicate ransomware or scam fund routing."
        ) if x == 1 else ""
    )

    return df
