import csv
import json
import logging
import time
from pathlib import Path

from confluent_kafka import Producer

from producer.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SOURCE_CSV

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def normalize_row(row: dict[str, str]) -> dict[str, str | int | float]:
    return {
        "invoice_no": row["invoice_no"],
        "stock_code": row["stock_code"],
        "description": row.get("description") or "Unknown",
        "quantity": int(float(row["quantity"])),
        "invoice_date": row["invoice_date"],
        "unit_price": float(row["unit_price"]),
        "customer_id": row.get("customer_id") or "UNKNOWN",
        "country": row.get("country") or "Unknown",
    }


def delivery_report(error, message) -> None:
    if error is not None:
        LOGGER.error("Kafka delivery failed: %s", error)
    else:
        LOGGER.debug("Delivered record to %s [%s]", message.topic(), message.partition())


def wait_for_kafka(producer: Producer, attempts: int = 30) -> None:
    for attempt in range(1, attempts + 1):
        metadata = producer.list_topics(timeout=5)
        if metadata.brokers:
            return
        LOGGER.info("Waiting for Kafka broker, attempt %s/%s", attempt, attempts)
        time.sleep(2)
    raise RuntimeError("Kafka broker was not reachable")


def main() -> None:
    source_path = Path(SOURCE_CSV)
    if not source_path.exists():
        raise FileNotFoundError(
            f"CSV file not found at {source_path}. Run scripts/generate_sample_data.py first."
        )

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    wait_for_kafka(producer)

    sent = 0
    with source_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            record = normalize_row(row)
            producer.produce(
                KAFKA_TOPIC,
                key=record["invoice_no"],
                value=json.dumps(record, separators=(",", ":")).encode("utf-8"),
                callback=delivery_report,
            )
            sent += 1
            if sent % 1000 == 0:
                producer.poll(0)
                LOGGER.info("Produced %s records", sent)

    producer.flush()
    LOGGER.info("Finished producing %s records to topic %s", sent, KAFKA_TOPIC)


if __name__ == "__main__":
    main()

