"""Where clean rows land. Every sink upserts on the pipeline's key and ignores keys it
already has, so replays and upstream resends never double-load.

    sink = make_sink(spec, data_dir)       # from healer.yaml's `sink:`
    inserted, ignored = sink.write(df)     # df has exactly the contract's columns

Backends:
  duckdb    a local file; the default, and what the demo uses
  postgres  any Postgres (RDS, Cloud SQL, Neon, Supabase, your own); needs `pip install "psycopg[binary]"`

The table is created on first use from the contract's columns, plus `loaded_at`. An
existing table is used as it is, so long as it has those columns and a unique
constraint (or primary key) on the key column.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pandas as pd

from healer.spec import PipelineSpec

_DUCK_TYPES = {"string": "VARCHAR", "float": "DOUBLE", "int": "BIGINT", "bool": "BOOLEAN",
               "datetime": "TIMESTAMP"}
_PG_TYPES = {"string": "TEXT", "float": "DOUBLE PRECISION", "int": "BIGINT", "bool": "BOOLEAN",
             "datetime": "TIMESTAMPTZ"}


class SinkUnavailable(RuntimeError):
    """The warehouse can't be reached right now. The processor waits and retries; it
    never commits the stream offset past rows it couldn't write."""


class Sink:
    name = "sink"

    def __init__(self, table: str, columns: list[str], types: dict[str, str], key: str):
        if key not in columns:
            raise ValueError(f"key {key!r} is not one of the contract's columns {columns}")
        self.table, self.columns, self.types, self.key = table, list(columns), types, key
        self._lock = threading.Lock()

    def _prepare(self, df: pd.DataFrame | None) -> pd.DataFrame | None:
        if df is None or df.empty:
            return None
        return df[self.columns].drop_duplicates(subset=[self.key])

    def write(self, df: pd.DataFrame | None) -> tuple[int, int]:
        """Returns (inserted, ignored as already present)."""
        raise NotImplementedError

    def count(self) -> int:
        raise NotImplementedError

    def recent(self, n: int = 20) -> list[dict]:
        raise NotImplementedError

    def describe(self) -> str:
        return f"{self.name}:{self.table}"

    def close(self) -> None:
        pass


# ---- DuckDB -------------------------------------------------------------------------------

def _duck_ident(name: str) -> str:
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


class DuckDBSink(Sink):
    name = "duckdb"

    def __init__(self, path: Path, table: str, columns: list[str], types: dict[str, str], key: str):
        super().__init__(table, columns, types, key)
        import duckdb

        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._con = duckdb.connect(str(path))
        cols = ", ".join(f"{_duck_ident(c)} {_DUCK_TYPES.get(types.get(c, 'string'), 'VARCHAR')}"
                         + (" PRIMARY KEY" if c == key else "") for c in self.columns)
        if "." in table:
            self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {_duck_ident(table.rsplit('.', 1)[0])}")
        self._con.execute(f"CREATE TABLE IF NOT EXISTS {_duck_ident(table)} ({cols}, loaded_at TIMESTAMP)")
        self._cols = ", ".join(_duck_ident(c) for c in self.columns)

    def write(self, df):
        df = self._prepare(df)
        if df is None:
            return 0, 0
        with self._lock:
            self._con.register("incoming", df)
            try:
                n = self._con.execute(
                    f"INSERT INTO {_duck_ident(self.table)} ({self._cols}, loaded_at) "
                    f"SELECT {self._cols}, now() FROM incoming ON CONFLICT DO NOTHING").fetchone()[0]
            finally:
                self._con.unregister("incoming")
        return int(n), len(df) - int(n)

    def count(self):
        with self._lock:
            return int(self._con.execute(f"SELECT count(*) FROM {_duck_ident(self.table)}").fetchone()[0])

    def recent(self, n=20):
        with self._lock:
            df = self._con.execute(f"SELECT {self._cols} FROM {_duck_ident(self.table)} "
                                   "ORDER BY loaded_at DESC LIMIT ?", [n]).df()
        return df.to_dict(orient="records")

    def describe(self):
        return f"duckdb:{self.path.name}:{self.table}"

    def close(self):
        with self._lock:
            self._con.close()


# ---- Postgres -----------------------------------------------------------------------------

class PostgresSink(Sink):
    """Writes each micro-batch with COPY into a temp table, then one
    INSERT ... ON CONFLICT (key) DO NOTHING, so a batch is one round trip and atomic."""

    name = "postgres"

    def __init__(self, dsn: str, table: str, columns: list[str], types: dict[str, str], key: str,
                 connect_timeout: int = 10):
        super().__init__(table, columns, types, key)
        try:
            import psycopg
            from psycopg import sql
        except ImportError as e:     # optional dependency
            raise RuntimeError('the postgres sink needs psycopg: pip install "psycopg[binary]"') from e
        self._psycopg, self._sql = psycopg, sql
        self._dsn, self._timeout = dsn, connect_timeout
        parts = table.split(".")
        if len(parts) > 2 or not all(parts):
            raise ValueError(f"table must be 'name' or 'schema.name', got {table!r}")
        self._schema = parts[0] if len(parts) == 2 else None
        self._table_id = sql.Identifier(*parts)
        self._con = None
        with self._lock:
            self._connect()
            self._ensure_table()

    # -- connection --
    def _connect(self):
        try:
            if self._con is not None:
                self._con.close()
        except Exception:
            pass
        try:
            self._con = self._psycopg.connect(self._dsn, connect_timeout=self._timeout, autocommit=False)
        except self._psycopg.OperationalError as e:
            self._con = None
            raise SinkUnavailable(f"postgres unreachable: {_redact(e)}") from None

    def _run(self, fn):
        """Run fn(cursor) in a transaction; reconnect once if the connection dropped."""
        for attempt in (1, 2):
            if self._con is None or self._con.closed:
                self._connect()
            try:
                with self._con.transaction(), self._con.cursor() as cur:
                    return fn(cur)
            except (self._psycopg.OperationalError, self._psycopg.InterfaceError) as e:
                self._con = None
                if attempt == 2:
                    raise SinkUnavailable(f"postgres write failed: {_redact(e)}") from None
                time.sleep(0.2)

    def _ensure_table(self):
        sql = self._sql
        cols = [sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(_PG_TYPES.get(self.types.get(c, "string"), "TEXT")))
                for c in self.columns]
        stmts = []
        if self._schema:
            stmts.append(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self._schema)))
        stmts.append(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({}, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                             "PRIMARY KEY ({}))").format(self._table_id, sql.SQL(", ").join(cols),
                                                        sql.Identifier(self.key)))

        def go(cur):
            for s in stmts:
                cur.execute(s)
        self._run(go)

    def write(self, df):
        df = self._prepare(df)
        if df is None:
            return 0, 0
        sql = self._sql
        rows = [tuple(None if (isinstance(v, float) and v != v) else
                      (v.item() if hasattr(v, "item") else v) for v in r)
                for r in df.itertuples(index=False, name=None)]
        col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in self.columns)

        def go(cur):
            cur.execute(sql.SQL("CREATE TEMP TABLE IF NOT EXISTS shp_incoming "
                                "(LIKE {} INCLUDING DEFAULTS) ON COMMIT DELETE ROWS").format(self._table_id))
            with cur.copy(sql.SQL("COPY shp_incoming ({}) FROM STDIN").format(col_ids)) as cp:
                for r in rows:
                    cp.write_row(r)
            cur.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM shp_incoming "
                                "ON CONFLICT ({}) DO NOTHING").format(
                self._table_id, col_ids, col_ids, sql.Identifier(self.key)))
            return cur.rowcount

        with self._lock:
            n = int(self._run(go))
        return n, len(rows) - n

    def count(self):
        with self._lock:
            return int(self._run(lambda cur: cur.execute(
                self._sql.SQL("SELECT count(*) FROM {}").format(self._table_id)).fetchone()[0]))

    def recent(self, n=20):
        sql = self._sql
        col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in self.columns)

        def go(cur):
            cur.execute(sql.SQL("SELECT {} FROM {} ORDER BY loaded_at DESC LIMIT %s").format(
                col_ids, self._table_id), [n])
            return [dict(zip(self.columns, r)) for r in cur.fetchall()]
        with self._lock:
            return self._run(go)

    def describe(self):
        return f"postgres:{self.table}"

    def close(self):
        with self._lock:
            try:
                self._con and self._con.close()
            except Exception:
                pass


def _redact(e: BaseException) -> str:
    """Error text without credentials: libpq messages can echo the connection string."""
    import re
    msg = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
    return re.sub(r"(postgres(?:ql)?://[^:/@\s]+:)[^@\s]+@", r"\1***@", msg)[:300]


# ---- factory -------------------------------------------------------------------------------

def make_sink(spec: PipelineSpec, data_dir: Path) -> Sink:
    cfg = spec.sink
    kind = cfg.get("type", "duckdb")
    table = cfg.get("table") or f"{spec.name}_clean"
    columns, types = spec.columns(), spec.column_types()
    if kind == "postgres":
        return PostgresSink(cfg["dsn"], table, columns, types, spec.key)
    path = Path(cfg.get("path") or f"{spec.name}_warehouse.duckdb")
    if not path.is_absolute():
        path = Path(data_dir) / path
    return DuckDBSink(path, table, columns, types, spec.key)
