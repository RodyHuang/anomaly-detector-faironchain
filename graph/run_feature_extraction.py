import os
import argparse
import pickle

from extract_node_features import extract_node_features
from extract_motif_features import extract_motif_features
from extract_egonet_features import extract_egonet_features

def get_graph_path(base_dir, chain, year, month):
    return os.path.join(
        base_dir, "data", "output", "graph", chain, f"{year:04d}", f"{month:02d}",
        f"{chain}__token_transfer_graph__{year}_{month:02d}.pkl"
    )

def run_feature_extraction(graph_path: str):
    print(f"📥 Loading graph from {graph_path} ...")
    with open(graph_path, "rb") as f:
        g, account_to_idx = pickle.load(f)

    # === Build output path ===
    folder = os.path.dirname(graph_path)
    filename = os.path.basename(graph_path).replace("token_transfer_graph", "features").replace(".pkl", ".csv")
    output_csv_path = os.path.join(folder, filename)

    # === Whitelist path ===
    whitelist_path = os.path.join(os.path.dirname(__file__), "infra_whitelist.csv")

    # === Feature extraction ===
    print("📊 Extracting node-level features...")
    df_node = extract_node_features(g, whitelist_path=whitelist_path)

    print("🔺 Extracting motif-level features...")
    df_motif = extract_motif_features(g, whitelist_path=whitelist_path)

    print("🕸️ Extracting egonet-level features...")
    df_egonet = extract_egonet_features(g, whitelist_path=whitelist_path)

    # === Merge all ===
    print("🔗 Merging all features...")
    assert (
    df_node.index.equals(df_motif.index) and df_node.index.equals(df_egonet.index)
), "❌ Index mismatch: One of the feature sets is missing nodes."
    
    df_features = df_node.join(df_motif, how="left").join(df_egonet, how="left")

    print(f"💾 Saving to {output_csv_path}")
    df_features.to_csv(output_csv_path)
    print("✅ Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain", type=str, default="ethereum")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    graph_path = get_graph_path(base_dir, args.chain, args.year, args.month)

    if not os.path.exists(graph_path):
        raise FileNotFoundError(f"Graph file not found: {graph_path}")

    run_feature_extraction(graph_path)
