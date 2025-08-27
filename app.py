from __future__ import annotations
import os, io, csv, time
from typing import Any
from flask import Flask, request, jsonify, send_file, render_template
from werkzeug.exceptions import BadRequest

from aws_db import aws_db_connection  # your existing module

app = Flask(__name__, template_folder="templates", static_folder="static")

# Single shared connection (simple demo)
db = aws_db_connection()
_current_db = os.getenv("DB_NAME") or "postgres"  # for display only


# ----------------------------- Helpers -----------------------------
def _exec(query: str):
    """Execute query via raw cursor to always return (columns, rows, rowcount, duration_ms)."""
    if not query or not query.strip():
        raise BadRequest("No query provided.")
    started = time.perf_counter()
    try:
        with db.conn.cursor() as cur:
            cur.execute(query)
            cols = [c[0] for c in (cur.description or [])]
            rows = cur.fetchall() if cur.description else []
            rowcount = cur.rowcount
    except Exception as e:
        raise BadRequest(str(e))
    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    return cols or [], rows or [], rowcount, duration_ms


# ----------------------------- Pages ------------------------------
@app.get("/")
def index():
    return render_template("index.html", current_db=_current_db)


# ----------------------------- DB mgmt APIs ------------------------
@app.get("/api/databases")
def api_list_databases():
    try:
        names = db.list_databases()
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"databases": names, "current": _current_db})


@app.post("/api/databases")
def api_create_database():
    payload = request.get_json(silent=True) or {}
    dbname: str = (payload.get("name") or "").strip()
    if not dbname:
        raise BadRequest("Database name is required.")
    try:
        db.create_database(dbname)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True, "created": dbname})


@app.post("/api/connect")
def api_connect_to():
    global _current_db
    payload = request.get_json(silent=True) or {}
    dbname: str = (payload.get("name") or "").strip()
    if not dbname:
        raise BadRequest("Database name is required.")
    try:
        db.connect_to(dbname)
        _current_db = dbname
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True, "current": _current_db})


# ----------------------------- Tables & Schemas --------------------
@app.get("/api/table-schemas")
def api_table_schemas():
    """
    Returns all base tables in the given schema and their CREATE TABLE DDL,
    plus index and partition metadata.
    """
    schema = request.args.get("schema", "public")
    try:
        # Existing: use your helper to get CREATE TABLE DDL
        mapping = db.list_table_schemas(schema=schema)  # {table_name: CREATE TABLE ...}
        ordered_items = sorted(mapping.items(), key=lambda kv: kv[0])
        table_names = [name for name, _ in ordered_items]

        indexes: dict[str, list[dict]] = {t: [] for t in table_names}
        partitions: dict[str, dict] = {t: {} for t in table_names}

        with db.conn.cursor() as cur:
            # ---- Indexes for all tables in schema
            cur.execute(
                """
                SELECT tablename, indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = %s
                ORDER BY tablename, indexname;
                """,
                (schema,),
            )
            for tablename, indexname, indexdef in cur.fetchall():
                if tablename in indexes:
                    indexes[tablename].append({"name": indexname, "def": indexdef})

            # ---- Partitioned parents: strategy + key columns
            cur.execute(
                """
                WITH part AS (
                  SELECT
                    c.relname AS tablename,
                    CASE pt.partstrat
                      WHEN 'l' THEN 'LIST'
                      WHEN 'r' THEN 'RANGE'
                      WHEN 'h' THEN 'HASH'
                    END AS strategy,
                    pt.partattrs
                  FROM pg_partitioned_table pt
                  JOIN pg_class c ON c.oid = pt.partrelid
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname = %s
                )
                SELECT
                  p.tablename,
                  p.strategy,
                  array_agg(a.attname ORDER BY a.attnum) AS key_columns
                FROM part p
                JOIN pg_class c ON c.relname = p.tablename
                JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = %s
                JOIN unnest(p.partattrs) WITH ORDINALITY AS pa(attnum, ord) ON true
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = pa.attnum
                GROUP BY p.tablename, p.strategy
                ORDER BY p.tablename;
                """,
                (schema, schema),
            )
            for tablename, strategy, key_columns in cur.fetchall():
                if tablename in partitions:
                    partitions[tablename].update({
                        "is_partitioned": True,
                        "strategy": strategy,
                        "key_columns": key_columns or [],
                        "children": [],
                    })

            # ---- Children (the actual partitions) with bounds; also record parent links
            cur.execute(
                """
                SELECT
                  parent.relname AS parent,
                  child.relname  AS partition_name,
                  pg_get_expr(child.relpartbound, child.oid, true) AS bounds
                FROM pg_inherits i
                JOIN pg_class child  ON child.oid = i.inhrelid
                JOIN pg_class parent ON parent.oid = i.inhparent
                JOIN pg_namespace np ON np.oid = parent.relnamespace
                JOIN pg_namespace nc ON nc.oid = child.relnamespace
                WHERE np.nspname = %s AND nc.nspname = %s
                ORDER BY parent.relname, child.relname;
                """,
                (schema, schema),
            )
            rows = cur.fetchall()
            # Add child listing to parents, and mark children as partitions
            for parent, child, bounds in rows:
                if parent in partitions:
                    if not partitions[parent].get("is_partitioned"):
                        partitions[parent].update({
                            "is_partitioned": True,
                            "strategy": None,
                            "key_columns": [],
                            "children": [],
                        })
                    partitions[parent]["children"].append({
                        "name": child, "bounds": bounds or ""
                    })
                if child in partitions:
                    partitions[child].update({
                        "is_partition": True,
                        "parent": parent,
                        "bounds": bounds or "",
                    })

        return jsonify({
            "schema": schema,
            "tables": table_names,
            "ddl": {name: ddl for name, ddl in ordered_items},
            "indexes": indexes,
            "partitions": partitions,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400



# ----------------------------- SQL & CSV ---------------------------
@app.post("/api/sql")
def api_sql():
    payload = request.get_json(silent=True) or {}
    query: str = (payload.get("query") or "").strip()
    cols, rows, rowcount, duration_ms = _exec(query)

    def clip(v: Any) -> Any:
        s = "" if v is None else str(v)
        return s if len(s) <= 2000 else s[:2000] + "…"

    return jsonify({
        "columns": cols,
        "rows": [[clip(c) for c in r] for r in rows],
        "rowcount": rowcount,
        "duration_ms": duration_ms,
    })


@app.post("/api/sql/csv")
def api_sql_csv():
    payload = request.get_json(silent=True) or {}
    query: str = (payload.get("query") or "").strip()
    cols, rows, _, _ = _exec(query)

    import csv, io
    sio = io.StringIO()
    w = csv.writer(sio)
    if cols:
        w.writerow(cols)
    w.writerows(rows)
    data = io.BytesIO(sio.getvalue().encode("utf-8"))
    return send_file(
        data,
        mimetype="text/csv",
        as_attachment=True,
        download_name="query_results.csv",
    )


@app.get("/healthz")
def healthz():
    try:
        db.execute("SELECT 1")
        return {"ok": True, "db": _current_db}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
