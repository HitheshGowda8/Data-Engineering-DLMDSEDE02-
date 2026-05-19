CREATE TABLE IF NOT EXISTS batch_runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    source_records BIGINT NOT NULL,
    processed_records BIGINT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales_summary (
    id INTEGER PRIMARY KEY DEFAULT 1,
    total_orders BIGINT NOT NULL,
    total_customers BIGINT NOT NULL,
    total_quantity NUMERIC(18, 2) NOT NULL,
    total_revenue NUMERIC(18, 2) NOT NULL,
    average_order_value NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT one_summary_row CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS country_sales (
    country TEXT PRIMARY KEY,
    orders BIGINT NOT NULL,
    customers BIGINT NOT NULL,
    quantity NUMERIC(18, 2) NOT NULL,
    revenue NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS product_sales (
    stock_code TEXT PRIMARY KEY,
    description TEXT,
    quantity NUMERIC(18, 2) NOT NULL,
    revenue NUMERIC(18, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_rfm_features (
    customer_id TEXT PRIMARY KEY,
    recency_days INTEGER NOT NULL,
    frequency BIGINT NOT NULL,
    monetary_value NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

