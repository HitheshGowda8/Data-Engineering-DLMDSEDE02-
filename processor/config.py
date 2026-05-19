import os


POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce_ml")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ml_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ml_password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
RAW_LAKE_DIR = os.getenv("RAW_LAKE_DIR", "data/raw/kafka_ingested")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "data/processed")


def postgres_dsn() -> str:
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )

