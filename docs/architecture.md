# Architecture Notes

## Conception Phase Scope

The project implements the major architectural components described in the conception document:

- Producer microservice reads an e-commerce CSV dataset and publishes normalized records to Kafka.
- Kafka decouples ingestion from downstream storage.
- Lake writer consumes Kafka records and persists raw immutable JSONL files into the data lake.
- Hadoop HDFS services are included for distributed storage demonstration; local shared storage is used as the default reliable laptop path.
- Batch processor performs schema validation, cleaning, aggregation, and ML feature preparation.
- PostgreSQL stores structured serving tables.
- FastAPI exposes metrics and customer RFM features to downstream ML services.
- Airflow DAG models monthly ingestion and quarterly aggregation scheduling.

## Batch Cycles

- Monthly ingestion: run `producer` followed by `lake-writer`.
- Quarterly aggregation: run `processor` to refresh PostgreSQL serving tables and processed CSV outputs.

## Governance Controls

- Schema validation in `processor/transformations.py`.
- Immutable raw JSONL files partitioned by ingestion year and month.
- PostgreSQL serving schema initialized from `database/init.sql`.
- Docker Compose defines repeatable infrastructure.
- Batch run metadata is recorded in `batch_runs`.

