from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="Monthly ingestion and quarterly aggregation for e-commerce ML data.",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["batch", "ml", "ecommerce"],
) as dag:
    generate_sample_data = BashOperator(
        task_id="generate_sample_data",
        bash_command="python /opt/airflow/scripts/generate_sample_data.py --rows 10000 --output /opt/airflow/data/raw/ecommerce_sample.csv",
    )

    produce_to_kafka = BashOperator(
        task_id="produce_to_kafka",
        bash_command="echo 'Run producer container in Docker Compose for local architecture demo'",
    )

    quarterly_aggregation = BashOperator(
        task_id="quarterly_aggregation",
        bash_command="echo 'Run processor container at quarter end or trigger processor.batch_job from Spark submit in production'",
    )

    generate_sample_data >> produce_to_kafka >> quarterly_aggregation
