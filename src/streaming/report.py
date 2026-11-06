"""Summarise a streaming run (batch read of the streaming sinks).

Usage: python -m src.streaming.report
"""
import json

from pyspark.sql import functions as F

from src.common.io import write_result
from src.common.spark import OUTPUT_DIR, get_spark
from src.streaming.imdb_wiki_stream import STREAM_DIR

SECTION = "streaming"


def main():
    spark = get_spark("imdb_stream_report")
    matches = spark.read.parquet(str(STREAM_DIR / "matches"))
    windows = spark.read.parquet(str(STREAM_DIR / "window_counts"))

    write_result(
        matches.agg(
            F.count("*").alias("matched_events"),
            F.countDistinct("imdb_id").alias("distinct_imdb_entities"),
            F.min("event_time").alias("first_event"),
            F.max("event_time").alias("last_event"),
            F.round(F.avg((F.unix_timestamp("kafka_time")
                           - F.unix_timestamp("event_time"))), 1)
            .alias("avg_seconds_event_to_kafka"),
        ),
        SECTION, "summary",
    )
    write_result(matches.groupBy("entity_type", "bot").count().orderBy("entity_type", "bot"),
                 SECTION, "matches_by_type")
    write_result(
        matches.groupBy("entity_type", "imdb_id", "imdb_name").agg(
            F.count("*").alias("n_events"), F.sum("size_change").alias("net_size_change"),
            F.max("n_candidates").alias("homonyms"),
        ).orderBy(F.desc("n_events")).limit(25),
        SECTION, "top_entities",
    )
    write_result(
        matches.select("event_time", "title", "entity_type", "imdb_name",
                       "user", "size_change").orderBy(F.desc("event_time")).limit(20),
        SECTION, "latest_matches", show=10,
    )
    write_result(windows.orderBy("window_start", "entity_type"), SECTION, "window_counts")
    sliding_dir = STREAM_DIR / "sliding_counts"
    if sliding_dir.exists():
        write_result(
            spark.read.parquet(str(sliding_dir)).orderBy("window_start", "entity_type"),
            SECTION, "sliding_counts",
        )

    progress_file = OUTPUT_DIR / SECTION / "progress.json"
    if progress_file.exists():
        progress = json.loads(progress_file.read_text())
        dropped = sum(
            (p.get("stateOperators") or [{}])[0].get("numRowsDroppedByWatermark", 0)
            for p in progress if p["name"] == "window_counts"
        )
        rows = sum(p["numInputRows"] for p in progress if p["name"] == "matches")
        print(f"\nInput rows read by the matches query: {rows:,}")
        print(f"Rows dropped by the watermark (late data): {dropped:,}")
    spark.stop()


if __name__ == "__main__":
    main()
