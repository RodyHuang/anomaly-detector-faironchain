import os
from flask import Flask, request, jsonify
from utils import wei_to_eth, build_month_parquet_path, query_duckdb, pack_rules

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
app.json.sort_keys = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ===== Endpoints: /v1/top and /v1/address =====
@app.route("/v1/top", methods=["GET"])
def top():
    """
    Example:
      /v1/top?chain=ethereum&year=2023&month=1&n=100
    """
    # Data file path
    try:
        chain = request.args.get("chain", "ethereum")
        year = int(request.args.get("year"))
        month = int(request.args.get("month"))
    except Exception:
        return jsonify({"error": "missing or invalid chain/year/month"}), 400

    path = build_month_parquet_path(chain, year, month)
    if not os.path.exists(path):
        return jsonify({"error": f"parquet not found for {chain} {year}-{month:02d}", "path": path}), 404

    # Top-N
    try:
        n = int(request.args.get("n", 100))
    except Exception:
        return jsonify({"error": "invalid n"}), 400

    # SQL query
    sql = """
        SELECT address, final_score_0_100
        FROM read_parquet(?)
        ORDER BY final_score_0_100 DESC
        LIMIT ?
    """

    df = query_duckdb(sql, [path, n])
    df.insert(0, "ranking", range(1, len(df) + 1))
    df["final_score_0_100"] = df["final_score_0_100"].astype("float64").round(1)

    return df.to_json(orient="records", double_precision=2), 200, {"Content-Type": "application/json"}


@app.route("/v1/address", methods=["GET"])
def get_address():
    """
    Example:：
      /v1/address?chain=ethereum&year=2023&month=1&addr=0xabc...
    """
    
    # Data file path
    try:
        chain = request.args.get("chain", "ethereum")
        year = int(request.args.get("year"))
        month = int(request.args.get("month"))
        addr = request.args.get("addr", "").strip().lower()
    except Exception:
        return jsonify({"error": "missing or invalid chain/year/month/addr"}), 400

    if not addr:
        return jsonify({"error": "missing addr"}), 400

    path = build_month_parquet_path(chain, year, month)
    if not os.path.exists(path):
        return jsonify({"error": f"parquet not found for {chain} {year}-{month:02d}", "path": path}), 404

    # Cols list
    BASE_COLS = [
        "address",
        "is_infra",
        "in_degree", "out_degree",
        "unique_in_degree", "unique_out_degree",
        "total_input_amount", "total_output_amount",
        "self_loop_count", "two_node_loop_count", "triangle_loop_count",
        "egonet_density",
        "rule_score_100", "mahalanobis_distance_stats_score_100",
        "iforest_stats_score_100", "final_score_0_100",
    ]
    RULE_FLAG_COLS = [f"H{i}_flag" for i in range(1, 7)]
    RULE_DESC_COLS = [f"H{i}_description" for i in range(1, 7)]
    COLS = BASE_COLS + RULE_FLAG_COLS + RULE_DESC_COLS
    cols_sql = ", ".join(COLS)


    # SQL query
    sql = f"""
        SELECT {cols_sql}
        FROM read_parquet(?)
        WHERE lower(address) = ?
    """
    df = query_duckdb(sql, [path, addr])

    if df.empty:
        return jsonify([]), 200
    
    r = df.iloc[0].to_dict()

    if bool(r["is_infra"]):
        resp = {
        "meta": {
            "chain": chain,
            "period": f"{year:04d}-{month:02d}",
            "address": r["address"],
            "units": {
                "amounts": "ether (ETH)",
                "egonet_density": "0–1",
                "degree": "count",
                "scores": "0–100"
            }
        },
        "features": {
            "is_infra": True,
            "degree": None,
            "amounts": None,
            "motifs": None,
            "egonet": None
        },
        "scores": None,
        "explanations": None
    }
        return jsonify(resp), 200, {"Content-Type": "application/json"}

    resp = {
        "meta": {
            "chain": chain,
            "period": f"{year:04d}-{month:02d}",
            "address": r["address"],
            "units": {
                "amounts": "ether (ETH)",
                "egonet_density": "0–1",
                "degree": "count",
                "scores": "0–100"
    },
        },
        "features": {
            "is_infra": bool(r["is_infra"]),
            "degree": {
                "in_degree": int(r["in_degree"]),
                "out_degree": int(r["out_degree"]),
                "unique_in_degree": int(r["unique_in_degree"]),
                "unique_out_degree": int(r["unique_out_degree"]),
            },
            "amounts": {
                "total_input_amount_eth": wei_to_eth(r["total_input_amount"]),
                "total_output_amount_eth": wei_to_eth(r["total_output_amount"]),
            },
            "motifs": {
                "self_loop_count": int(r["self_loop_count"]),
                "two_node_loop_count": int(r["two_node_loop_count"]),
                "triangle_loop_count": int(r["triangle_loop_count"]),
            },
            "egonet": {
                "egonet_density": float(r["egonet_density"])
            }
        },
        "scores": {
            "rule_score_100": round(float(r["rule_score_100"]), 1),
            "mahalanobis_stats_100": round(float(r["mahalanobis_distance_stats_score_100"]), 1),
            "iforest_stats_100": round(float(r["iforest_stats_score_100"]), 1),
            "final_score_0_100": round(float(r["final_score_0_100"]), 1)
        },
        "explanations": {
            "rule_ids": [item["rule"] for item in pack_rules(r)],
            "rules": pack_rules(r)
        }
    }

    return jsonify(resp), 200, {"Content-Type": "application/json"}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)