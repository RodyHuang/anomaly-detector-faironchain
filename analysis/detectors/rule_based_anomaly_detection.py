import pandas as pd
import numpy as np

def compute_thresholds(df: pd.DataFrame, columns: list[str], ignore_zeros_columns: list[str] = None, quantile: float = 0.99) -> dict:
    """
    Compute the quantile-based threshold for anomaly rule comparisons.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing feature columns.
        columns (list[str]): List of column names to compute quantile thresholds for.
        ignore_zeros_columns (list[str]): Columns for which zero values should be excluded from the quantile computation.
        quantile (float): The quantile to compute (Default: 0.99).

    Returns:
        dict: A dictionary mapping each column name to its computed threshold value.
    """
    thresholds = {}
    ignore_zeros_columns = ignore_zeros_columns or []

    for col in columns:
        if col in ignore_zeros_columns:
            series = df[col][df[col] > 0]
        else:
            series = df[col]
        thresholds[col] = series.quantile(quantile)

    return thresholds


def apply_H1_rule(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Apply H1 anomaly rule: 'Multi-source inflow aggregation account with limited outflow'

    Criteria:
        - in_degree ≥ top 1%          
        - out_degree ≤ 3              
        - |total_input_amount - total_output_amount| / total_input_amount ≤ 0.05

    Returns:
        DataFrame with new cols of H1_flag and H1_description.
    """
    # Uses threshold on unique inbound degree
    in_deg_threshold = thresholds["in_degree"]

    # Safe computation of relative difference; NaN when total_input_amount == 0 yields no flag.
    safe_ratio = np.where(
        df["total_input_amount"] > 0,
        np.abs(df["total_input_amount"] - df["total_output_amount"]) / df["total_input_amount"],
        np.nan
    )

    # Apply rule logic
    df["H1_flag"] = (
        (df["in_degree"] >= in_deg_threshold) &
        (df["out_degree"] <= 3) &
        (safe_ratio <= 0.05)
    ).astype(int)

    # Attach explanation only if flagged
    df["H1_description"] = df["H1_flag"].apply(
        lambda x: (
            "H1: Aggregates from many sources and forwards almost unchanged to few addresses. May indicate ransomware or scam fund routing."
        ) if x == 1 else ""
    )

    return df


def apply_H2_rule(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Apply H2 anomaly rule: 'Multi-source inflow aggregation account with zero outflow'

    Criteria:
        - in_degree ≥ top 1%
        - out_degree == 0

    Returns:
        DataFrame with H2_flag and H2_description added.
    """
    in_deg_threshold = thresholds["in_degree"]

    df["H2_flag"] = (
        (df["in_degree"] >= in_deg_threshold) &
        (df["out_degree"] == 0)
    ).astype(int)

    df["H2_description"] = df["H2_flag"].apply(
        lambda x: (
            "H2: Aggregates from many sources but shows no outgoing transfers. May indicate scam fund storage or ransomware holding address."
        ) if x == 1 else ""
    )

    return df


def apply_H3_rule(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Apply H3 anomaly rule: 'Single-inflow account with high-diversity outflow'

    Criteria:
        - in_degree == 1
        - out_degree ≥ top 1%

    Returns:
        DataFrame with H3_flag and H3_description added.
    """
    out_deg_threshold = thresholds["out_degree"]

    df["H3_flag"] = (
        (df["in_degree"] == 1) &
        (df["out_degree"] >= out_deg_threshold)
    ).astype(int)

    df["H3_description"] = df["H3_flag"].apply(
        lambda x: (
            "H3: Receives funds from a single source and distributes to many addresses. May indicate laundering or scam profit distribution."
        ) if x == 1 else ""
    )

    return df


def apply_H4_rule(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Apply H4 anomaly rule: 'High-diversity inflow and outflow with minimal retention'

    Criteria:
        - in_degree ≥ top 1%
        - out_degree ≥ top 1%
        - |total_input_amount - total_output_amount| / total_input_amount ≤ 0.05

    Returns:
        DataFrame with H4_flag and H4_description added.
    """
    in_deg_threshold = thresholds["in_degree"]
    out_deg_threshold = thresholds["out_degree"]

    # Safe computation of relative difference
    safe_ratio = np.where(
        df["total_input_amount"] > 0,
        np.abs(df["total_input_amount"] - df["total_output_amount"]) / df["total_input_amount"],
        np.nan
    )

    # Apply rule logic
    df["H4_flag"] = (
        (df["in_degree"] >= in_deg_threshold) &
        (df["out_degree"] >= out_deg_threshold) &
        (safe_ratio <= 0.05)
    ).astype(int)

    # Attach explanation only if flagged
    df["H4_description"] = df["H4_flag"].apply(
        lambda x: (
            "H4: Receives from many sources and distributes to many others with minimal balance retained. Possible mixer or laundering relay."
        ) if x == 1 else ""
    )

    return df


def apply_H5_rule(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Apply H5 anomaly rule: 'Two-node cyclic accounts with abnormal transfer volume or frequency'
    Criteria:
        - two_node_loop_count ≥ 1
        - AND (
            two_node_loop_amount ≥ top 1%
            OR
            two_node_loop_tx_count ≥ top 1%
        )

    Returns:
        DataFrame with H5_flag and H5_description added.
    """
    amount_threshold = thresholds["two_node_loop_amount"]
    tx_count_threshold = thresholds["two_node_loop_tx_count"]

    df["H5_flag"] = (
        (df["two_node_loop_count"] >= 1) &
        (
            (df["two_node_loop_amount"] >= amount_threshold) |
            (df["two_node_loop_tx_count"] >= tx_count_threshold)
        )
    ).astype(int)

    df["H5_description"] = df["H5_flag"].apply(
        lambda x: (
            "H5: Participates in closed two-node loops with high value or frequent transfers. May indicate wash trading or self-laundering."
        ) if x == 1 else ""
    )

    return df


def apply_H6_rule(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Apply H6 anomaly rule: 'Triangle cyclic transfer accounts with abnormal transfer volume or frequency'

    Criteria:
        - triangle_loop_count ≥ 1
        - AND (
            triangle_loop_amount ≥ top 1%
            OR
            triangle_loop_tx_count ≥ top 1%
        )

    Returns:
        DataFrame with H6_flag and H6_description added.
    """
    amount_threshold = thresholds["triangle_loop_amount"]
    tx_count_threshold = thresholds["triangle_loop_tx_count"]

    df["H6_flag"] = (
        (df["triangle_loop_count"] >= 1) &
        (
            (df["triangle_loop_amount"] >= amount_threshold) |
            (df["triangle_loop_tx_count"] >= tx_count_threshold)
        )
    ).astype(int)

    df["H6_description"] = df["H6_flag"].apply(
        lambda x: (
            "H6: Participates in closed triangle-shaped loops with high value or frequent transfers. May indicate self-laundering or obfuscation."
        ) if x == 1 else ""
    )

    return df

def apply_all_rules(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    total_nodes = len(df)
    print(f"📊 Total nodes: {total_nodes}\n")

    print("🧠 Applying H1 rule (Single-point aggregation)...")
    df = apply_H1_rule(df, thresholds)
    print(f"➡️  H1 flagged accounts: {df['H1_flag'].sum()}")

    print("🧠 Applying H2 rule (Aggregation with zero outflow)...")
    df = apply_H2_rule(df, thresholds)
    print(f"➡️  H2 flagged accounts: {df['H2_flag'].sum()}")

    print("🧠 Applying H3 rule (Single-inflow with high outflow diversity)...")
    df = apply_H3_rule(df, thresholds)
    print(f"➡️  H3 flagged accounts: {df['H3_flag'].sum()}")

    print("🧠 Applying H4 rule (High inflow/outflow diversity with minimal retention)...")
    df = apply_H4_rule(df, thresholds)
    print(f"➡️  H4 flagged accounts: {df['H4_flag'].sum()}")

    print("🧠 Applying H5 rule (Two-node cyclic transfer accounts)...")
    df = apply_H5_rule(df, thresholds)
    print(f"➡️  H5 flagged accounts: {df['H5_flag'].sum()}")

    print("🧠 Applying H6 rule (Triangle cyclic transfer accounts)...")
    df = apply_H6_rule(df, thresholds)
    print(f"➡️  H6 flagged accounts: {df['H6_flag'].sum()}")

    print("✅ All heuristic rules applied.\n")
    return df

