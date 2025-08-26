import numpy as np
import pandas as pd

def hazen_percentile_0_100(s: pd.Series) -> pd.Series:
    """
    Convert a numeric Series to Hazen percentile scores in the range 0–100.

    Formula:
        percentile = (rank - 0.5) / n * 100
        - rank: 1..n, average rank for ties. Smaller values get rank 1.
        - n: number of elements

    Parameters:
        s (pd.Series): Input numeric series

    Returns:
        pd.Series: Percentile scores (0–100)
    """
    ranks = s.rank(method="average", ascending=True)
    n = float(len(s))
    return ((ranks - 0.5) / n * 100.0).astype(np.float32)


def score_rule_based(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rule-based anomaly score.

     Scoring logic:
        - Main types (H1–H4) are mutually exclusive: 1 point if any is triggered.
        - Cycle types (H5, H6) each add 1 point.
        - Total score range: 0–3.
        - Scaled score: raw_score * (100 / 3) → 0, 33.33, 66.67, 100.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing integer flags for each rule.

    Returns:
        pd.DataFrame: Copy of input with added raw and scaled rule-based scores.
    """
    df = df.copy()
    hcols_main = ["H1_flag", "H2_flag", "H3_flag", "H4_flag"]
    hcols_cycle = ["H5_flag", "H6_flag"]

    main_any = df[hcols_main].any(axis=1).astype(int)
    cycle_sum = df[hcols_cycle].sum(axis=1).astype(int)

    df["rule_score_raw"] = (main_any + cycle_sum).astype(np.float32)
    df["rule_score_100"] = (df["rule_score_raw"] * (100.0 / 3.0)).round(2).astype(np.float32)
    return df


def score_statistical_percentile(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Mahalanobis distance to Hazen percentile (0–100). Larger = more anomalous.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing Mahalanobis distance.

    Returns:
        pd.DataFrame: Copy of input with added Hazen percentile for Mahalanobis_distance.
    """
    df = df.copy()
    df["mahalanobis_distance_stats_score_100"] = hazen_percentile_0_100(df["mahalanobis_distance"])
    return df



def score_iforest_percentile(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Isolation Forest score to Hazen percentile (0–100).

    Notes:
      - If you stored '-decision_function(X)', then higher_is_more_anomalous=True (default).
      - If you stored the raw scikit-learn decision_function (higher=more normal),
        set higher_is_more_anomalous=False to flip the direction.
    """
    df = df.copy()
    df["iforest_stats_score_100"] = hazen_percentile_0_100(df["iforest_score"])
    return df


def combine_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the final anomaly score as the simple average of:
      - rule_score_100
      - mahalanobis_distance_stats_score_100
      - iforest_stats_score_100

    Parameters:
        df (pd.DataFrame): Input DataFrame with component scores.

    Returns:
        pd.DataFrame: Copy of input with an additional column:
            - final_score_0_100: Row-wise mean of the three component scores.
    """
    df = df.copy()
    cols = ["rule_score_100", "mahalanobis_distance_stats_score_100", "iforest_stats_score_100"]
    df["final_score_0_100"] = df[cols].mean(axis=1)

    # === Calculate Top % ranking ===
    n = len(df)
    ranks_desc = df["final_score_0_100"].rank(method="average", ascending=False)
    df["final_score_top_percent"] = (ranks_desc / n * 100).round(2).astype(np.float32)
    df["final_score_top_percent_display"] = df["final_score_top_percent"].map(lambda x: f"Top {x:.2f}%")

    return df