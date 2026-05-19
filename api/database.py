import os
import sqlite3
from contextlib import contextmanager


def database_backend() -> str:
    return os.getenv("DATABASE_BACKEND", "postgres").lower()


def postgres_dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'ecommerce_ml')} "
        f"user={os.getenv('POSTGRES_USER', 'ml_user')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'ml_password')}"
    )


@contextmanager
def get_connection():
    if database_backend() == "sqlite":
        db_path = os.getenv("SQLITE_DB_PATH", "data/processed/ecommerce_ml.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
        return

    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(postgres_dsn(), row_factory=dict_row) as conn:
        yield conn


def normalize_sql(sql: str) -> str:
    if database_backend() == "sqlite":
        return sql.replace("%s", "?")
    return sql


def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def fetch_one(sql: str, params: tuple = ()):
    with get_connection() as conn:
        row = conn.execute(normalize_sql(sql), params).fetchone()
    return row_to_dict(row)


def fetch_all(sql: str, params: tuple = ()) -> list[dict[str, object]]:
    with get_connection() as conn:
        rows = conn.execute(normalize_sql(sql), params).fetchall()
    return [dict(row) for row in rows]
