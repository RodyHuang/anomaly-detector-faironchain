import pandas as pd
import numpy as np
from scipy.spatial.distance import mahalanobis
from scipy.linalg import inv

def zscore(series: pd.Series) -> pd.Series:
    """
    Compute the standard z-score of a pandas Series.

    Formula:
        z = (x - mean) / std
    """
    mean = series.mean()
    std = series.std()
    return (series - mean) / std


def preprocess_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess features:
    - Apply log1p transform to skewed count/amount columns
    - Create log-ratio features
    - Apply z-score to all transformed columns

    Expected input columns (renamed):
        - in_degree, out_degree            
        - total_input_amount, total_output_amount
        - two_node_loop_count, triangle_loop_count
        - egonet_density          

    Returns:
        DataFrame with added processed feature columns.
    """
    
    # === Step 1: Log(x+1) transformed features
    base_log_features = [
        'in_degree', 'out_degree',
        'total_input_amount', 'total_output_amount',
        'two_node_loop_count', 'triangle_loop_count'
    ]
    for col in base_log_features:
        df[f'{col}_log'] = np.log1p(df[col])
    
    # === Step 2: Log-ratio features
    df['log_degree_ratio'] = np.log((df['in_degree'] + 1) / (df['out_degree'] + 1))
    df['log_amount_ratio'] = np.log((df['total_input_amount'] + 1) / (df['total_output_amount'] + 1))

    # === Step 3: Features to apply z-score
    transformed_features = [f'{col}_log' for col in base_log_features] + [
        'log_degree_ratio',
        'log_amount_ratio',
        'egonet_density'
    ]
    for col in transformed_features:
        df[f'{col}_z'] = zscore(df[col])

    return df


def compute_mahalanobis_distance(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """
    Compute Mahalanobis distance for each row based on selected feature columns.
    Skip zero-variance (all-zero or constant) features.

    Parameters:
        df (pd.DataFrame): Input DataFrame with standardized features.
        feature_cols (list[str]): List of column names to be used in Mahalanobis computation.

    Returns:
        pd.DataFrame: The original DataFrame with an added 'mahalanobis_distance' column.
    """
    X = df[feature_cols].copy()

    # Identify and drop zero-variance (all-zero or constant) features
    zero_var_cols = [c for c in X.columns if X[c].nunique(dropna=True) <= 1]
    if zero_var_cols:
        print(f"[Info] Dropping zero-variance features: {zero_var_cols}")
        X = X.drop(columns=zero_var_cols)
    
    if X.shape[1] == 0:
        # If all features have zero variance, nothing can be computed
        df["mahalanobis_distance"] = 0.0
        return df

    # Extract data matrix
    data = X.values

    # Mean and covariance of the selected features.
    mean_vec = np.mean(data, axis=0)
    cov_matrix = np.cov(data, rowvar=False)
    inv_cov = inv(cov_matrix)

    # Compute Mahalanobis distance for each row relative to the mean.
    dists = [mahalanobis(row, mean_vec, inv_cov) for row in data]
    df["mahalanobis_distance"] = dists

    # === Summary of Mahalanobis
    print("\n📐 Mahalanobis Distance Summary:")
    print(df["mahalanobis_distance"].describe())

    print("\n🚨 Top 10 accounts by Mahalanobis distance:")
    print(df[["mahalanobis_distance"]].sort_values(by="mahalanobis_distance", ascending=False).head(10))

    return df