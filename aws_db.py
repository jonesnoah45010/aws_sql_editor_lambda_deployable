import os
from typing import Any, Iterable, Optional, Sequence, Tuple, Union, List, Literal, Dict

import psycopg2
from psycopg2 import sql


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT") or 5432
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME") or "postgres"

FetchMode = Literal["none", "one", "all"]

class aws_db_connection:
    def __init__(
        self,
        host: str = DB_HOST,
        port: Union[str, int] = DB_PORT,
        user: str = DB_USER,
        password: str = DB_PASS,
        dbname: str = DB_NAME,
        admin_dbname: str = "postgres",
        autocommit: bool = True,
    ) -> None:
        self.host = host
        self.port = int(port) if port else 5432
        self.user = user
        self.password = password
        self.admin_dbname = admin_dbname
        self.autocommit = autocommit
        self.conn = self._get_or_create_connection(dbname)

    # ---------- core connect/create ----------
    def _connect(self, dbname: str):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=dbname,
            user=self.user,
            password=self.password,
        )
        conn.autocommit = self.autocommit
        return conn

    def _get_or_create_connection(self, dbname: str):
        try:
            return self._connect(dbname)
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                self._create_database_internal(dbname)
                return self._connect(dbname)
            raise

    def _create_database_internal(self, dbname: str) -> None:
        admin = self._connect(self.admin_dbname)
        try:
            with admin.cursor() as cur:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        finally:
            admin.close()

    # ---------- public API ----------
    def execute(
        self,
        query: Union[str, sql.SQL],
        params: Optional[Union[Sequence[Any], Iterable[Any], dict]] = None,
        fetch: FetchMode = "none",
    ) -> Optional[Union[Tuple, List[Tuple]]]:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
            return None

    def create_database(self, dbname: str) -> None:
        if dbname in self.list_databases():
            return
        self._create_database_internal(dbname)

    def connect_to(self, dbname: str) -> None:
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = self._get_or_create_connection(dbname)

    def list_databases(self) -> List[str]:
        admin = self._connect(self.admin_dbname)
        try:
            with admin.cursor() as cur:
                cur.execute(
                    """
                    SELECT datname
                    FROM pg_database
                    WHERE datistemplate = false
                    ORDER BY datname;
                    """
                )
                return [r[0] for r in cur.fetchall()]
        finally:
            admin.close()

    def list_tables(self, schema: str = "public") -> List[str]:
        rows = self.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type='BASE TABLE'
            ORDER BY table_name;
            """,
            (schema,),
            fetch="all",
        )
        return [r[0] for r in rows] if rows else []

    # ---------- NEW: schema extraction ----------
    def _redshift_pg_get_tabledef_available(self) -> bool:
        """
        Detect if Redshift-style pg_get_tabledef(text) exists.
        """
        row = self.execute(
            """
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'pg_get_tabledef';
            """,
            fetch="one",
        )
        return bool(row and row[0] > 0)

    def table_schema(self, table_name: str, schema: str = "public") -> str:
        """
        Return a CREATE TABLE statement for one table.
        Works on Postgres (reconstructs) and Redshift (native function if present).
        """
        # Redshift shortcut if available
        if self._redshift_pg_get_tabledef_available():
            row = self.execute("SELECT pg_get_tabledef(%s);", (f'{schema}.{table_name}',), fetch="one")
            if row and row[0]:
                return row[0].rstrip(";") + ";"

        # Validate table exists
        exists = self.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s AND table_type='BASE TABLE';
            """,
            (schema, table_name),
            fetch="one",
        )
        if not exists:
            raise ValueError(f"Table {schema}.{table_name} does not exist.")

        # Columns (order preserved), types, nullability, defaults, identity
        cols = self.execute(
            """
            SELECT
              a.attnum,
              a.attname,
              pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
              a.attnotnull AS not_null,
              pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
              a.attidentity AS identity_kind  -- '' | 'a' (ALWAYS) | 'd' (BY DEFAULT)
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE c.relkind IN ('r','p')         -- table or partitioned table
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname = %s
              AND c.relname = %s
            ORDER BY a.attnum;
            """,
            (schema, table_name),
            fetch="all",
        )

        col_lines = []
        for _attnum, name, dtype, not_null, default_expr, identity_kind in cols:
            parts = [sql.Identifier(name).as_string(self.conn), dtype]
            if identity_kind in ("a", "d"):
                parts.append(
                    "GENERATED ALWAYS AS IDENTITY" if identity_kind == "a" else "GENERATED BY DEFAULT AS IDENTITY"
                )
                # If identity, ignore default_expr (Postgres handles it via identity)
            elif default_expr:
                parts.append(f"DEFAULT {default_expr}")
            if not_null:
                parts.append("NOT NULL")
            col_lines.append(" ".join(parts))

        # Constraints (PK/UNIQUE/CHECK/FK) inside CREATE TABLE
        constraints = self.execute(
            """
            SELECT
              conname,
              contype,
              pg_get_constraintdef(oid, true) AS condef
            FROM pg_constraint
            WHERE conrelid = (
              SELECT c.oid
              FROM pg_class c
              JOIN pg_namespace n ON n.oid=c.relnamespace
              WHERE n.nspname=%s AND c.relname=%s
            )
            ORDER BY contype DESC, conname;
            """,
            (schema, table_name),
            fetch="all",
        )

        for conname, contype, condef in constraints:
            # contype: p=primary, u=unique, f=foreign, c=check, x=exclusion
            # We’ll name constraints to be explicit
            ident = sql.Identifier(conname).as_string(self.conn)
            col_lines.append(f"CONSTRAINT {ident} {condef}")

        create_stmt = (
            f"CREATE TABLE {sql.Identifier(schema).as_string(self.conn)}."
            f"{sql.Identifier(table_name).as_string(self.conn)} (\n  "
            + ",\n  ".join(col_lines)
            + "\n);"
        )
        return create_stmt

    def list_table_schemas(self, schema: str = "public") -> Dict[str, str]:
        """
        Return a mapping of table_name -> CREATE TABLE statement for all tables in `schema`.
        """
        tables = self.list_tables(schema=schema)
        return {t: self.table_schema(t, schema=schema) for t in tables}

    def close(self) -> None:
        if getattr(self, "conn", None):
            try:
                self.conn.close()
            except Exception:
                pass
            finally:
                self.conn = None


# ---------- Example usage ----------
if __name__ == "__main__":
    db = aws_db_connection()

    # Ensure a DB exists and connect
    db.create_database("my_app_db")
    db.connect_to("my_app_db")

    # Demo table
    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INT,
            email TEXT UNIQUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT age_check CHECK (age IS NULL OR age >= 0)
        );
    """)

    # Single table CREATE statement
    print(db.table_schema("users"))

    # All tables in schema
    all_creates = db.list_table_schemas()
    for tbl, ddl in all_creates.items():
        print(f"\n-- {tbl}\n{ddl}")

    db.close()
