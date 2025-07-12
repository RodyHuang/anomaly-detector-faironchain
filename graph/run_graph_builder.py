import argparse
import pickle
import os
from pathlib import Path

from load_clean_edgelist import load_clean_edgelist
from filter_edgelist import filter_edgelist
from build_token_transfer_graph import build_igraph_from_edgelist

def run_graph_builder(year: int, month: int):
    # === 1. Load raw edgelist ===
    df = load_clean_edgelist(year, month)
    print(f"📥 Loaded raw edgelist: {len(df):,} rows")

    # === 2. Filter ===
    df_filtered = filter_edgelist(df, min_amount_wei=1_000_000_000_000)
    print(f"🧹 Filtered edgelist: {len(df_filtered):,} rows")

    # === 3. Build graph ===
    g, account_to_idx = build_igraph_from_edgelist(df_filtered)
    print(f"✅ Graph: {g.vcount()} nodes, {g.ecount()} edges")

    # === 4. Save ===
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(base_dir, "data", "output", "graph", "ethereum", f"{year:04d}", f"{month:02d}")
    os.makedirs(output_dir, exist_ok=True)

    filename = f"ethereum__token_transfer_graph__{year}_{month:02d}.pkl"
    output_path = os.path.join(output_dir, filename)

    with open(output_path, "wb") as f:
        pickle.dump((g, account_to_idx), f)
    print(f"💾 Saved to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    run_graph_builder(args.year, args.month)
