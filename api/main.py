from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from api.database import fetch_all, fetch_one, get_connection

app = FastAPI(
    title="E-commerce Batch ML Data API",
    version="1.0.0",
    description="Serving API for aggregated e-commerce batch features and metrics.",
)


@app.get("/health")
def health() -> dict[str, str]:
    with get_connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    summary = fetch_one("SELECT * FROM sales_summary WHERE id = 1") or {}
    countries = fetch_all(
        """
        SELECT country, orders, customers, revenue
        FROM country_sales
        ORDER BY revenue DESC
        LIMIT 5
        """
    )
    products = fetch_all(
        """
        SELECT stock_code, description, quantity, revenue
        FROM product_sales
        ORDER BY revenue DESC
        LIMIT 5
        """
    )

    def money(value) -> str:
        return f"${float(value or 0):,.2f}"

    country_rows = "".join(
        f"<tr><td>{row['country']}</td><td>{row['orders']}</td><td>{money(row['revenue'])}</td></tr>"
        for row in countries
    )
    product_rows = "".join(
        f"<tr><td>{row['stock_code']}</td><td>{row['description']}</td><td>{money(row['revenue'])}</td></tr>"
        for row in products
    )
    return f"""
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>E-commerce Batch ML Data Platform</title>
      <style>
        body {{ margin: 0; font-family: Arial, sans-serif; color: #172026; background: #f5f7fa; }}
        header {{ background: #102a43; color: white; padding: 28px 40px; }}
        h1 {{ margin: 0 0 8px; font-size: 30px; }}
        main {{ padding: 28px 40px; }}
        .grid {{ display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 16px; }}
        .card {{ background: white; border: 1px solid #d9e2ec; border-radius: 8px; padding: 18px; }}
        .label {{ color: #627d98; font-size: 13px; text-transform: uppercase; }}
        .value {{ font-size: 26px; font-weight: 700; margin-top: 8px; }}
        .columns {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 22px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
        th, td {{ text-align: left; padding: 10px; border-bottom: 1px solid #e6edf3; }}
        th {{ color: #486581; font-size: 13px; }}
        .flow {{ margin-top: 22px; display: flex; gap: 10px; flex-wrap: wrap; }}
        .step {{ background: #e0f2fe; color: #0c4a6e; padding: 10px 12px; border-radius: 6px; font-weight: 600; }}
      </style>
    </head>
    <body>
      <header>
        <h1>E-commerce Batch ML Data Platform</h1>
        <div>Kafka ingestion, raw data lake, batch aggregation, feature serving, and REST API</div>
      </header>
      <main>
        <section class="grid">
          <div class="card"><div class="label">Orders</div><div class="value">{summary.get('total_orders', 0)}</div></div>
          <div class="card"><div class="label">Customers</div><div class="value">{summary.get('total_customers', 0)}</div></div>
          <div class="card"><div class="label">Quantity</div><div class="value">{int(float(summary.get('total_quantity', 0) or 0))}</div></div>
          <div class="card"><div class="label">Revenue</div><div class="value">{money(summary.get('total_revenue'))}</div></div>
        </section>
        <section class="flow">
          <div class="step">CSV Dataset</div><div class="step">Kafka Producer</div><div class="step">Raw Data Lake</div>
          <div class="step">Batch Processor</div><div class="step">SQLite/PostgreSQL</div><div class="step">REST API</div>
        </section>
        <section class="columns">
          <div class="card">
            <h2>Top Countries</h2>
            <table><thead><tr><th>Country</th><th>Orders</th><th>Revenue</th></tr></thead><tbody>{country_rows}</tbody></table>
          </div>
          <div class="card">
            <h2>Top Products</h2>
            <table><thead><tr><th>SKU</th><th>Description</th><th>Revenue</th></tr></thead><tbody>{product_rows}</tbody></table>
          </div>
        </section>
      </main>
    </body>
    </html>
    """


@app.get("/metrics/summary")
def get_sales_summary() -> dict[str, object]:
    return fetch_one("SELECT * FROM sales_summary WHERE id = 1") or {}


@app.get("/metrics/countries")
def get_country_sales(limit: int = Query(default=25, ge=1, le=250)) -> list[dict[str, object]]:
    return fetch_all(
        """
        SELECT country, orders, customers, quantity, revenue, updated_at
        FROM country_sales
        ORDER BY revenue DESC
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/metrics/products/top")
def get_top_products(limit: int = Query(default=10, ge=1, le=250)) -> list[dict[str, object]]:
    return fetch_all(
        """
        SELECT stock_code, description, quantity, revenue, order_count, updated_at
        FROM product_sales
        ORDER BY revenue DESC
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/features/customer-rfm")
def get_customer_rfm(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, object]]:
    return fetch_all(
        """
        SELECT customer_id, recency_days, frequency, monetary_value, updated_at
        FROM customer_rfm_features
        ORDER BY monetary_value DESC
        LIMIT %s
        """,
        (limit,),
    )
