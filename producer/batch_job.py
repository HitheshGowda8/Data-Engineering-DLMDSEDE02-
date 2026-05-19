import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd
import psycopg

from processor.config import PROCESSED_DIR, RAW_LAKE_DIR, postgres_dsn
from processor.transformations import (
    clean_transactions,
    country_sales,
    customer_rfm_features,
    product_sales,
    sales_summary,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def load_raw_transactions(raw_lake_dir: str) -> pd.DataFrame:
    paths = sorted(Path(raw_lake_dir).glob("**/*.jsonl"))
    if not paths:
        raise FileNotFoundError(f"No JSONL files found under {raw_lake_dir}. Run producer and lake-writer first.")

    records: list[dict[str, object]] = []
    for path in paths:
        with path.open("r", encoding="utf-8") as raw_file:
            for line in raw_file:
                if line.strip():
                    records.append(json.loads(line))

    if not records:
        raise ValueError(f"Raw files under {raw_lake_dir} did not contain any records")
    return pd.DataFrame.from_records(records)


def execute_many(cursor, sql: str, rows: list[dict[str, object]]) -> None:
    for row in rows:
        cursor.execute(sql, row)


def write_outputs(
    summary: dict[str, object],
    countries: list[dict[str, object]],
    products: list[dict[str, object]],
    rfm: list[dict[str, object]],
) -> None:
    processed_dir = Path(PROCESSED_DIR)
    processed_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([summary]).to_csv(processed_dir / "sales_summary.csv", index=False)
    pd.DataFrame(countries).to_csv(processed_dir / "country_sales.csv", index=False)
    pd.DataFrame(products).to_csv(processed_dir / "product_sales.csv", index=False)
    pd.DataFrame(rfm).to_csv(processed_dir / "customer_rfm_features.csv", index=False)


def persist_to_postgres(
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    source_records: int,
    processed_records: int,
    summary: dict[str, object],
    countries: list[dict[str, object]],
    products: list[dict[str, object]],
    rfm: list[dict[str, object]],
) -> None:
    with psycopg.connect(postgres_dsn()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO sales_summary (
                    id, total_orders, total_customers, total_quantity, total_revenue,
                    average_order_value, updated_at
                )
                VALUES (
                    1, %(total_orders)s, %(total_customers)s, %(total_quantity)s,
                    %(total_revenue)s, %(average_order_value)s, %(updated_at)s
                )
                ON CONFLICT (id) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_customers = EXCLUDED.total_customers,
                    total_quantity = EXCLUDED.total_quantity,
                    total_revenue = EXCLUDED.total_revenue,
                    average_order_value = EXCLUDED.average_order_value,
                    updated_at = EXCLUDED.updated_at
                """,
                summary,
            )
            execute_many(
                cursor,
                """
                INSERT INTO country_sales (country, orders, customers, quantity, revenue, updated_at)
                VALUES (%(country)s, %(orders)s, %(customers)s, %(quantity)s, %(revenue)s, %(updated_at)s)
                ON CONFLICT (country) DO UPDATE SET
                    orders = EXCLUDED.orders,
                    customers = EXCLUDED.customers,
                    quantity = EXCLUDED.quantity,
                    revenue = EXCLUDED.revenue,
                    updated_at = EXCLUDED.updated_at
                """,
                countries,
            )
            execute_many(
                cursor,
                """
                INSERT INTO product_sales (stock_code, description, quantity, revenue, order_count, updated_at)
                VALUES (%(stock_code)s, %(description)s, %(quantity)s, %(revenue)s, %(order_count)s, %(updated_at)s)
                ON CONFLICT (stock_code) DO UPDATE SET
                    description = EXCLUDED.description,
                    quantity = EXCLUDED.quantity,
                    revenue = EXCLUDED.revenue,
                    order_count = EXCLUDED.order_count,
                    updated_at = EXCLUDED.updated_at
                """,
                products,
            )
            execute_many(
                cursor,
                """
                INSERT INTO customer_rfm_features (customer_id, recency_days, frequency, monetary_value, updated_at)
                VALUES (%(customer_id)s, %(recency_days)s, %(frequency)s, %(monetary_value)s, %(updated_at)s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    recency_days = EXCLUDED.recency_days,
                    frequency = EXCLUDED.frequency,
                    monetary_value = EXCLUDED.monetary_value,
                    updated_at = EXCLUDED.updated_at
                """,
                rfm,
            )
            cursor.execute(
                """
                INSERT INTO batch_runs (
                    run_id, started_at, finished_at, source_records,
                    processed_records, status
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (run_id, started_at, finished_at, source_records, processed_records, "SUCCEEDED"),
            )


def main() -> None:
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    raw = load_raw_transactions(RAW_LAKE_DIR)
    cleaned = clean_transactions(raw)
    updated_at = datetime.now(timezone.utc)

    summary = sales_summary(cleaned, updated_at)
    countries = country_sales(cleaned, updated_at)
    products = product_sales(cleaned, updated_at)
    rfm = customer_rfm_features(cleaned, updated_at)

    write_outputs(summary, countries, products, rfm)
    finished_at = datetime.now(timezone.utc)
    persist_to_postgres(
        run_id,
        started_at,
        finished_at,
        len(raw),
        len(cleaned),
        summary,
        countries,
        products,
        rfm,
    )
    LOGGER.info("Batch run %s completed: %s raw records, %s processed records", run_id, len(raw), len(cleaned))


if __name__ == "__main__":
    main()

