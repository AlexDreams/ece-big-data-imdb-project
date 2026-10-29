"""Shared SparkSession factory and project paths."""
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
OUTPUT_DIR = PROJECT_DIR / "output"

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def get_spark(app_name, kafka=False, driver_memory="5g"):
    """Return a local SparkSession sized for the course container."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        # 8 concurrent tasks and small shuffle partitions: title.principals
        # (100M rows) must fit in the memory of a single local driver
        .master("local[8]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
