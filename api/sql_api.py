import re, duckdb
from flask import request, jsonify, Response
from utils import build_month_parquet_path

def register_sql_endpoint(app):
    """
    Register a POST /v1/sql endpoint to run safe SELECT queries.

    Security:
        - Only allows SELECT queries
        - Blocks potentially dangerous keywords (DDL/DML)
        - Reads parquet into a DuckDB in-memory view
    """

    # Regex to ensure query starts with "SELECT"
    SQL_SELECT = re.compile(r"^\s*select\b", re.I)
    # Regex to forbid keywords that could modify the DB or system
    SQL_FORBIDDEN = re.compile(r"\b(attach|install|load|pragma|copy|insert|update|delete|create|drop|alter|grant|revoke|call|execute)\b", re.I)

    @app.route("/v1/sql", methods=["POST"])
    def sql_query():
        """
        Execute a user-supplied SQL SELECT query on a monthly parquet dataset.

        Request JSON body(exanmple):
            {
                "chain": "ethereum",
                "year": 2023,
                "month": 1,
                "sql": "SELECT address, final_score_0_100 FROM t ORDER BY final_score_0_100 DESC"
            }

        Optional query parameter:
            format=csv (default is JSON output)
        """

        # --- Parse request payload ---
        data = request.get_json(silent=True) or {}
        required = ("chain", "year", "month", "sql")
        missing = [k for k in required if data.get(k) in (None, "")]
        if missing:
            return jsonify({"error": f"missing required fields: {', '.join(missing)}"}), 400
        
        chain = (data.get("chain")).lower().strip()
        year = data.get("year")
        month = data.get("month")
        user_sql = (data.get("sql")).strip()
        fmt = (request.args.get("format") or "json").lower()

        # Resolve parquet file path
        path = build_month_parquet_path(chain, year, month)

         # --- Validate SQL ---
        if not SQL_SELECT.match(user_sql):
            return jsonify({"error": "only SELECT queries are allowed"}), 400
        if SQL_FORBIDDEN.search(user_sql):
            return jsonify({"error": "forbidden keyword detected"}), 400
        
        # --- Open in-memory DuckDB connection ---
        con = duckdb.connect(database=":memory:")

        try:
            # Create a view 't' pointing to the parquet file
            con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{path}')")
            # Execute the user's query
            df = con.execute(user_sql).df()
        except Exception as e:
            return jsonify({"error": str(e)}), 400
        finally:
            con.close()
        
        # --- Return output in desired format ---
        if fmt == "csv":
            return Response(df.to_csv(index=False), mimetype="text/csv")
        else:
            return df.to_json(orient="records", double_precision=6), 200, {"Content-Type": "application/json"}
