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

    # # === 2.5 (Optional) Show top interacting addresses for whitelist generation ===
    # print("\n⚪ Top sender addresses:")
    # from_counts = df_filtered["from_address_sid"].value_counts()
    # print(from_counts.head(10))

    # print("\n⚪ Top receiver addresses:")
    # to_counts = df_filtered["to_address_sid"].value_counts()
    # print(to_counts.head(10))

    # # === Combine send and receive counts into total degree ===
    # total_degree = (from_counts + to_counts).sort_values(ascending=False)

    # # === Extract top 50 high-degree addresses ===
    # top_50_addrs = total_degree.head(50)
    # print("\n🔗 Top 50 high-degree addresses (pure address only):")
    # for sid in top_50_addrs.index:
    #     try:
    #         addr = sid.split("_", 1)[1]  # 去掉 chain_id 前綴
    #         print(addr)
    #     except IndexError:
    #         print(f"[!] Unexpected SID format: {sid}")

    # === 3 Save filtered edgelist for traceability ===
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    edgelist_dir = os.path.join(base_dir, "data", "intermediate", "abstract", "ethereum", f"{year:04d}", f"{month:02d}")
    edgelist_filename = f"ethereum__token_transfer_edgelist__{year}_{month:02d}.parquet"
    edgelist_path = os.path.join(edgelist_dir, edgelist_filename)

    df_filtered.to_parquet(edgelist_path, index=False)
    print(f"📄 Saved filtered edgelist to {edgelist_path}")

    # === 4. Build graph ===
    g, account_to_idx = build_igraph_from_edgelist(df_filtered)
    print(f"✅ Graph: {g.vcount()} nodes, {g.ecount()} edges")

    # === 5. Save ===
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
