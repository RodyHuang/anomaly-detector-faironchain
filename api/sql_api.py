import os, re, duckdb
from flask import Flask, request, jsonify, Response
from utils import build_month_parquet_path

def register_sql_endpoint(app):

    SQL_SELECT = re.compile(r"^\s*select\b", re.I)
    SQL_FORBIDDEN = re.compile(r"\b(attach|install|load|pragma|copy|insert|update|delete|create|drop|alter|grant|revoke|call|execute)\b", re.I)

    @app.route("/v1/sql", methods=["POST"])
    def sql_query():
        data = request.get_json()
        chain = (data.get("chain")).lower().strip()
        year = data.get("year")
        month = data.get("month")
        user_sql = (data.get("sql")).strip()
        fmt = (request.args.get("format") or "json").lower()
        path = build_month_parquet_path(chain, year, month)

        if not SQL_SELECT.match(user_sql):
            return jsonify({"error": "only SELECT queries are allowed"}), 400
        if SQL_FORBIDDEN.search(user_sql):
            return jsonify({"error": "forbidden keyword detected"}), 400
        
        con = duckdb.connect(database=":memory:")

        wrapped_sql = user_sql
        try:
            con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{path}')")
            df = con.execute(wrapped_sql).df()
        except Exception as e:
            return jsonify({"error": str(e)}), 400
        finally:
            con.close()
        
        if fmt == "csv":
            return Response(df.to_csv(index=False), mimetype="text/csv")
        else:
            return df.to_json(orient="records", double_precision=6), 200, {"Content-Type": "application/json"}
