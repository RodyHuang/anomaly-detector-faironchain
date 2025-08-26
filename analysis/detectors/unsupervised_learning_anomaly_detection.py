import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

def fit_iforest_and_score(
    df: pd.DataFrame,
    features: list[str],
    max_samples: int | float = 100_000,   # Number of subsamples for training
    n_estimators: int = 300,
    random_state: int = 42,
    n_jobs: int = -1
) -> pd.DataFrame:
    """
    Apply Isolation Forest to compute anomaly scores.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        features (list[str]): List of feature column names to use for training.
        max_samples (int | float): Subsample size for training.
        n_estimators (int): Number of trees in the forest.
        random_state (int): Random seed for reproducibility.
        n_jobs (int): Number of parallel jobs (-1 to use all cores).

    Returns:
        pd.DataFrame: A copy of the input DataFrame with an additional column:
            - iforest_score: Continuous anomaly score (higher -> more anomalous).
    """
    work = df.copy()
    X = work[features].to_numpy() 

    # --- Fit model ---
    print("🚀 Training Isolation Forest...")
    iforest = IsolationForest(
        n_estimators=n_estimators,
        max_samples=max_samples,
        contamination="auto",   # Let the model infer the threshold
        n_jobs=n_jobs,
        random_state=random_state
    )
    iforest.fit(X)
    print("✅ Training done. Scoring...")

    # sklearn: decision_function higher = more normal
    work["iforest_score"] = -iforest.decision_function(X)
    print("✅ Scoring done.")
    
    return work