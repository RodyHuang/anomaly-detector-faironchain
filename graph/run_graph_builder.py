import argparse
import pickle
import os

from graph.construction.load_clean_edgelist import load_clean_edgelist
from graph.construction.filter_edgelist import filter_edgelist
from graph.construction.build_token_transfer_graph import build_igraph_from_edgelist

def run_graph_builder(year: int, month: int):
    """
    Orchestrate the graph construction pipeline:
      1) Load raw edgelist from abstraction layer
      2) Filter (min amount, blacklist)
      3) Persist filtered edgelist for traceability
      4) Build igraph
      5) Persist graph artifact (pickle)
    """
    # === 1) Load raw edgelist ===
    df = load_clean_edgelist(year, month)
    print(f"📥 Loaded raw edgelist: {len(df):,} rows")

    # === 2) Filter ===
    df_filtered = filter_edgelist(df, min_amount_wei=1_000_000_000_000)
    print(f"🧹 Filtered edgelist: {len(df_filtered):,} rows")

    # === 3) Save filtered edgelist for traceability ===
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(base_dir, "data", "output", "graph", "ethereum", f"{year:04d}", f"{month:02d}")
    os.makedirs(output_dir, exist_ok=True)

    edgelist_filename = f"ethereum__token_transfer_edgelist__{year}_{month:02d}.parquet"
    edgelist_path = os.path.join(output_dir, edgelist_filename)

    # Store 'amount' as string to avoid any parquet/engine int overflow or precision issues.
    df_filtered.assign(amount=df_filtered["amount"].astype("string")).to_parquet(edgelist_path, index=False)
    print(f"📄 Saved filtered edgelist to {edgelist_path}")

    # === 4) Build graph ===
    g, account_to_idx = build_igraph_from_edgelist(df_filtered)
    print(f"✅ Graph: {g.vcount()} nodes, {g.ecount()} edges")

    # === 5) Save graph artifact (pickle) ===
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
    
