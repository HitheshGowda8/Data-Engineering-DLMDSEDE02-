from datetime import timezone

import pandas as pd

REQUIRED_COLUMNS = {
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
}


def validate_schema(frame: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")


def clean_transactions(frame: pd.DataFrame) -> pd.DataFrame:
    validate_schema(frame)
    cleaned = frame.copy()
    cleaned["quantity"] = pd.to_numeric(cleaned["quantity"], errors="coerce")
    cleaned["unit_price"] = pd.to_numeric(cleaned["unit_price"], errors="coerce")
    cleaned["invoice_date"] = pd.to_datetime(cleaned["invoice_date"], errors="coerce", utc=True)
    cleaned["customer_id"] = cleaned["customer_id"].fillna("UNKNOWN").astype(str)
    cleaned["country"] = cleaned["country"].fillna("Unknown").astype(str)
    cleaned["description"] = cleaned["description"].fillna("Unknown").astype(str)
    cleaned = cleaned.dropna(subset=["quantity", "unit_price", "invoice_date"])
    cleaned = cleaned[(cleaned["quantity"] > 0) & (cleaned["unit_price"] >= 0)]
    cleaned["revenue"] = cleaned["quantity"] * cleaned["unit_price"]
    return cleaned


def sales_summary(frame: pd.DataFrame, updated_at) -> dict[str, object]:
    order_revenue = frame.groupby("invoice_no", as_index=False)["revenue"].sum()
    total_revenue = float(frame["revenue"].sum())
    total_orders = int(frame["invoice_no"].nunique())
    return {
        "total_orders": total_orders,
        "total_customers": int(frame["customer_id"].nunique()),
        "total_quantity": float(frame["quantity"].sum()),
        "total_revenue": total_revenue,
        "average_order_value": float(order_revenue["revenue"].mean()) if total_orders else 0.0,
        "updated_at": updated_at,
    }


def country_sales(frame: pd.DataFrame, updated_at) -> list[dict[str, object]]:
    grouped = (
        frame.groupby("country")
        .agg(
            orders=("invoice_no", "nunique"),
            customers=("customer_id", "nunique"),
            quantity=("quantity", "sum"),
            revenue=("revenue", "sum"),
        )
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    grouped["updated_at"] = updated_at
    return grouped.to_dict("records")


def product_sales(frame: pd.DataFrame, updated_at) -> list[dict[str, object]]:
    grouped = (
        frame.groupby(["stock_code", "description"])
        .agg(
            quantity=("quantity", "sum"),
            revenue=("revenue", "sum"),
            order_count=("invoice_no", "nunique"),
        )
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    grouped["updated_at"] = updated_at
    return grouped.to_dict("records")


def customer_rfm_features(frame: pd.DataFrame, updated_at) -> list[dict[str, object]]:
    reference_date = frame["invoice_date"].max().to_pydatetime().astimezone(timezone.utc)
    grouped = (
        frame.groupby("customer_id")
        .agg(
            last_purchase=("invoice_date", "max"),
            frequency=("invoice_no", "nunique"),
            monetary_value=("revenue", "sum"),
        )
        .reset_index()
    )
    grouped["recency_days"] = grouped["last_purchase"].apply(
        lambda value: int((reference_date - value.to_pydatetime().astimezone(timezone.utc)).days)
    )
    grouped = grouped.drop(columns=["last_purchase"])
    grouped["updated_at"] = updated_at
    return grouped.to_dict("records")

