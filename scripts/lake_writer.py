import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Consumer, KafkaException

from producer.config import (
    HDFS_NAMENODE_WEBHDFS_URL,
    HDFS_RAW_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    RAW_LAKE_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def build_output_path() -> Path:
    now = datetime.now(timezone.utc)
    return Path(RAW_LAKE_DIR) / f"year={now.year}" / f"month={now.month:02d}" / "transactions.jsonl"


def upload_to_webhdfs(local_path: Path) -> None:
    if not HDFS_NAMENODE_WEBHDFS_URL:
        return

    hdfs_path = f"{HDFS_RAW_DIR}/{local_path.parent.name}/{local_path.name}"
    create_url = f"{HDFS_NAMENODE_WEBHDFS_URL}/webhdfs/v1{hdfs_path}"
    params = {"op": "CREATE", "overwrite": "true"}
    response = requests.put(create_url, params=params, allow_redirects=False, timeout=10)
    if response.status_code not in {307, 201}:
        LOGGER.warning("WebHDFS create request failed: %s %s", response.status_code, response.text[:300])
        return

    upload_url = response.headers.get("Location", create_url)
    with local_path.open("rb") as payload:
        upload_response = requests.put(upload_url, data=payload, timeout=30)
    if upload_response.status_code not in {200, 201}:
        LOGGER.warning("WebHDFS upload failed: %s %s", upload_response.status_code, upload_response.text[:300])
    else:
        LOGGER.info("Uploaded %s to WebHDFS path %s", local_path, hdfs_path)


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "raw-lake-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    output_path = build_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    consumed = 0
    idle_rounds = 0
    LOGGER.info("Consuming topic %s into %s", KAFKA_TOPIC, output_path)
    with output_path.open("a", encoding="utf-8") as raw_file:
        while idle_rounds < 10:
            message = consumer.poll(timeout=2.0)
            if message is None:
                idle_rounds += 1
                continue
            if message.error():
                raise KafkaException(message.error())

            value = message.value().decode("utf-8")
            json.loads(value)
            raw_file.write(value + "\n")
            consumed += 1
            idle_rounds = 0
            if consumed % 1000 == 0:
                raw_file.flush()
                LOGGER.info("Persisted %s records", consumed)

    consumer.close()
    time.sleep(1)
    LOGGER.info("Finished writing %s records to raw lake", consumed)
    if consumed:
        upload_to_webhdfs(output_path)


if __name__ == "__main__":
    main()

