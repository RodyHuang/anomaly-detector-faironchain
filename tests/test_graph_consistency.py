import argparse
import pickle
import pandas as pd
from pathlib import Path
import numpy as np

def test_graph_matches_edgelist(year: int, month: int, chain: str = "ethereum"):
    # Path
    base_dir = Path(__file__).resolve().parents[1]
    pkl_path = base_dir / "data" / "output" / "graph" / chain / f"{year:04d}" / f"{month:02d}" / f"{chain}__token_transfer_graph__{year}_{month:02d}.pkl"
    edgelist_path = base_dir / "data" / "intermediate" / "abstract" / chain / f"{year:04d}" / f"{month:02d}" / f"{chain}__token_transfer_edgelist__{year}_{month:02d}.parquet"

    # Load data
    print(f"📥 Loading graph from {pkl_path}")
    with open(pkl_path, "rb") as f:
        g, account_to_idx = pickle.load(f)

    print(f"📥 Loading edgelist from {edgelist_path}")
    df = pd.read_parquet(edgelist_path)

    # Check nodes
    unique_accounts = pd.unique(df[["from_address_sid", "to_address_sid"]].values.ravel())
    print(f"🔍 Checking node count...")
    assert g.vcount() == len(unique_accounts), f"❌ Node count mismatch: graph={g.vcount()} vs unique={len(unique_accounts)}"

    # Check edges
    print(f"🔍 Checking edge count...")
    assert g.ecount() == len(df), f"❌ Edge count mismatch: graph={g.ecount()} vs edgelist={len(df)}"

    # Check edges attributes
    attribute_mapping = {
        "weight": "amount",
        "token_sid": "token_sid",
        "timestamp": "timestamp",
        "tx_sid": "tx_sid",
    }

    for graph_attr, df_col in attribute_mapping.items():
        print(f"🔍 Checking edge attribute: {graph_attr} ↔ {df_col}")
        g_attr = np.array(g.es[graph_attr])
        df_attr = df[df_col].values
        assert np.array_equal(g_attr, df_attr), f"❌ Attribute '{graph_attr}' mismatch!"

    print("✅ Graph consistency test passed — all checks successful.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test graph consistency against edgelist.")
    parser.add_argument("--year", type=int, required=True, help="Year of the graph (e.g., 2023)")
    parser.add_argument("--month", type=int, required=True, help="Month of the graph (e.g., 1 for January)")
    parser.add_argument("--chain", type=str, default="ethereum", help="Chain name (default: ethereum)")
    args = parser.parse_args()

    test_graph_matches_edgelist(args.year, args.month, args.chain)
