import pandas as pd
import numpy as np
from scipy.spatial.distance import mahalanobis
from scipy.linalg import inv

def robust_zscore(series: pd.Series) -> pd.Series:
    """
    Compute the robust z-score of a pandas Series.

    Formula:
        robust_z = (x - median) / (MAD * 1.4826)

    Parameters:
        Input pd.Series

    Returns:
        Transformed pd.Series
    """
    median = series.median()
    mad = np.median(np.abs(series - median))
    return (series - median) / (mad * 1.4826)

def standard_zscore(series: pd.Series) -> pd.Series:
    """
    Compute the standard z-score of a pandas Series.

    Formula:
        z = (x - mean) / std

    Parameters:
        Input pd.Series

    Returns:
        Transformed pd.Series
    """
    mean = series.mean()
    std = series.std()
    return (series - mean) / std

def preprocess_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess features:
    - Apply log(x+1) + robust z-score to unique_in/out_degree, total_input/output_amount, two-node/triangle_loop_count
    - Create log ratios for degree and amount, and apply robust z-score
    - Apply standard z-score to egonet_density

    Returns:
        DataFrame with added processed feature columns.
    """
    # === Log(x+1) + Robust Z-score features
    log_robust_features = [
        'unique_in_degree', 'unique_out_degree',
        'total_input_amount', 'total_output_amount',
        'two_node_loop_count', 'triangle_loop_count'
    ]
    for col in log_robust_features:
        log_col = f"{col}_log"
        z_col = f"{col}_z"
        df[log_col] = np.log1p(df[col])
        df[z_col] = standard_zscore(df[log_col])

    # === Ratio-based features: degree and amount
    df['log_degree_ratio'] = np.log((df['unique_in_degree'] + 1) / (df['unique_out_degree'] + 1))
    df['log_degree_ratio_z'] = standard_zscore(df['log_degree_ratio'])

    df['log_amount_ratio'] = np.log((df['total_input_amount'] + 1) / (df['total_output_amount'] + 1))
    df['log_amount_ratio_z'] = standard_zscore(df['log_amount_ratio'])

    # === Standard Z-score for normally distributed feature
    df['egonet_density_z'] = standard_zscore(df['egonet_density'])

    # === 🔍 Debug: Output summary statistics for Z-score features
    zscore_cols = [f"{col}_z" for col in log_robust_features] + [
        "log_degree_ratio_z", "log_amount_ratio_z", "egonet_density_z"
    ]
    
    print("\n🔎 Z-score Summary Statistics:")
    for col in zscore_cols:
        print(f"\n📊 {col}")
        print(df[col].describe())
        print(f"  NaN count: {df[col].isna().sum()}, Inf count: {(~np.isfinite(df[col])).sum()}")

    return df

def compute_mahalanobis_distance(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """
    Compute Mahalanobis distance for each row based on selected feature columns.

    Parameters:
        df (pd.DataFrame): Input DataFrame with standardized features.
        feature_cols (list[str]): List of column names to be used in Mahalanobis computation.

    Returns:
        pd.DataFrame: The original DataFrame with an added 'mahalanobis_distance' column.
    """
    data = df[feature_cols].values
    mean_vec = np.mean(data, axis=0)
    cov_matrix = np.cov(data, rowvar=False)
    inv_cov = inv(cov_matrix)

    dists = [mahalanobis(row, mean_vec, inv_cov) for row in data]
    df = df.copy()
    df["mahalanobis_distance"] = dists

    # === Debug: Output summary statistics for Mahalanobis distance
    print("\n📐 Mahalanobis Distance Summary:")
    print(df["mahalanobis_distance"].describe())
    print("\n🚨 Top 10 accounts by Mahalanobis distance:")
    print(df[["mahalanobis_distance"]].sort_values(by="mahalanobis_distance", ascending=False).head(10))

    return df