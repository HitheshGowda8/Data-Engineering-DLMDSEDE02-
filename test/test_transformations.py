from datetime import datetime, timezone

import pandas as pd

from processor.transformations import clean_transactions, sales_summary


def test_clean_transactions_filters_invalid_rows():
    frame = pd.DataFrame(
        [
            {
                "invoice_no": "INV-1",
                "stock_code": "SKU-1",
                "description": "Item",
                "quantity": "2",
                "invoice_date": "2024-01-01T00:00:00",
                "unit_price": "10.5",
                "customer_id": "CUST-1",
                "country": "United States",
            },
            {
                "invoice_no": "INV-2",
                "stock_code": "SKU-2",
                "description": "Bad Item",
                "quantity": "-1",
                "invoice_date": "2024-01-01T00:00:00",
                "unit_price": "5",
                "customer_id": "CUST-2",
                "country": "United States",
            },
        ]
    )

    cleaned = clean_transactions(frame)

    assert len(cleaned) == 1
    assert cleaned.iloc[0]["revenue"] == 21.0


def test_sales_summary_calculates_totals():
    frame = pd.DataFrame(
        [
            {
                "invoice_no": "INV-1",
                "stock_code": "SKU-1",
                "description": "Item",
                "quantity": 2,
                "invoice_date": pd.Timestamp("2024-01-01T00:00:00Z"),
                "unit_price": 10.0,
                "customer_id": "CUST-1",
                "country": "United States",
                "revenue": 20.0,
            },
            {
                "invoice_no": "INV-1",
                "stock_code": "SKU-2",
                "description": "Item 2",
                "quantity": 1,
                "invoice_date": pd.Timestamp("2024-01-01T00:00:00Z"),
                "unit_price": 5.0,
                "customer_id": "CUST-1",
                "country": "United States",
                "revenue": 5.0,
            },
        ]
    )

    summary = sales_summary(frame, datetime.now(timezone.utc))

    assert summary["total_orders"] == 1
    assert summary["total_customers"] == 1
    assert summary["total_revenue"] == 25.0
    assert summary["average_order_value"] == 25.0

