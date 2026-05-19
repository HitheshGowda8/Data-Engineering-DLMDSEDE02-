import os


def getenv(name: str, default: str) -> str:
    return os.getenv(name, default)


KAFKA_BOOTSTRAP_SERVERS = getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = getenv("KAFKA_TOPIC", "ecommerce-transactions")
SOURCE_CSV = getenv("SOURCE_CSV", "data/raw/ecommerce_sample.csv")
RAW_LAKE_DIR = getenv("RAW_LAKE_DIR", "data/raw/kafka_ingested")
HDFS_NAMENODE_WEBHDFS_URL = os.getenv("HDFS_NAMENODE_WEBHDFS_URL")
HDFS_RAW_DIR = getenv("HDFS_RAW_DIR", "/data/ecommerce/raw")

