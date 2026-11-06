"""Structured Streaming: Wikipedia changes from Kafka matched against IMDb.

Matching strategy: a Wikipedia page matches when its title, without the
trailing qualifier in parentheses ("Heat (1995 film)" -> "heat"), equals the
normalised title of a popular IMDb movie/series or the name of a person
credited on one. When the page has a qualifier, it must be film related
("film", "TV series", "actor", "película", "Schauspieler", ...), otherwise
songs, footballers or disambiguation pages with the same name would match.
The reference set is a static DataFrame joined to the stream.

Late data: event time = Wikimedia `timestamp`; a watermark of
WATERMARK_DELAY drops events arriving later than that. Window counts are
emitted in append mode once their window is closed by the watermark.

Sinks (Parquet, with checkpoints so a restart resumes exactly where it stopped):
- data/streaming/matches          every matched event
- data/streaming/window_counts    5 min tumbling windows per entity type
- data/streaming/sliding_counts   10 min windows sliding every 5 min
Progress metrics are saved to output/streaming/progress.json.
Usage: python -m src.streaming.imdb_wiki_stream --minutes 20
"""
import argparse
import json
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, StringType, StructField, StructType,
)

from src.common.io import read_silver
from src.common.spark import DATA_DIR, OUTPUT_DIR, get_spark

KAFKA_SERVERS = "kafka:19092"
TOPIC = "wikistreams"
STREAM_DIR = DATA_DIR / "streaming"
MIN_VOTES = 1000
WATERMARK_DELAY = "10 minutes"
TUMBLING = "5 minutes"
SLIDING = ("10 minutes", "5 minutes")
TRIGGER = "30 seconds"

# Accepted qualifiers (lower case, en/fr/de/es/it wikis)
TITLE_QUALIFIER = (
    r"(film|movie|pel[ií]cula|t[ée]l[ée]film|fernsehfilm|tv series|series|serie|"
    r"s[ée]rie t[ée]l[ée]vis[ée]e|fernsehserie|serie televisiva|serie de televisi[oó]n|"
    r"miniseries|mini-s[ée]rie|anime|animated series|web series|sitcom)"
)
PERSON_QUALIFIER = (
    r"(actor|actress|acteur|actrice|schauspieler(in)?|attore|attrice|actriz|"
    r"director|filmmaker|r[ée]alisateur|r[ée]alisatrice|regisseur(in)?|regista|"
    r"screenwriter|sc[ée]nariste|drehbuchautor(in)?|sceneggiatore|guionista|"
    r"comedian|humoriste|komiker(in)?|comico|c[oó]mico|producer|producteur)"
)

EVENT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("title_url", StringType()),
    StructField("comment", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("wiki", StringType()),
    StructField("length", StructType([
        StructField("old", IntegerType()), StructField("new", IntegerType()),
    ])),
    StructField("revision", StructType([
        StructField("old", LongType()), StructField("new", LongType()),
    ])),
])


def match_key(col):
    """Lower-case title without trailing '(...)' and extra spaces."""
    stripped = F.regexp_replace(col, r"\s*\([^)]*\)\s*$", "")
    return F.lower(F.trim(F.regexp_replace(stripped, r"\s+", " ")))


def qualifier(col):
    """Lower-case text of the trailing '(...)', null when there is none."""
    q = F.lower(F.regexp_extract(col, r"\(([^)]*)\)\s*$", 1))
    return F.when(q != "", q)


def reference_set(spark):
    titles = read_silver(spark, "title_basics").filter(
        F.col("titleType").isin("movie", "tvSeries", "tvMiniSeries")
    )
    popular = titles.join(
        read_silver(spark, "title_ratings").filter(F.col("numVotes") >= MIN_VOTES),
        "tconst",
    )
    title_ref = popular.select(
        match_key(F.col("primaryTitle")).alias("match_key"),
        F.lit("title").alias("entity_type"),
        F.col("tconst").alias("imdb_id"),
        F.col("primaryTitle").alias("imdb_name"),
        "numVotes",
    )
    people_ids = (
        read_silver(spark, "title_principals")
        .filter(F.col("category").isin("actor", "actress", "director"))
        .join(popular.select("tconst", "numVotes"), "tconst")
        .groupBy("nconst").agg(F.sum("numVotes").alias("numVotes"))
    )
    person_ref = (
        read_silver(spark, "name_basics").join(people_ids, "nconst")
        .select(
            match_key(F.col("primaryName")).alias("match_key"),
            F.lit("person").alias("entity_type"),
            F.col("nconst").alias("imdb_id"),
            F.col("primaryName").alias("imdb_name"),
            "numVotes",
        )
    )
    # Homonyms: keep the most voted entity and remember how many candidates
    ref = title_ref.unionByName(person_ref).filter(F.length("match_key") >= 3)
    return ref.groupBy("match_key").agg(
        F.max(F.struct("numVotes", "entity_type", "imdb_id", "imdb_name")).alias("best"),
        F.count("*").alias("n_candidates"),
    ).select(
        "match_key", "best.entity_type", "best.imdb_id", "best.imdb_name",
        F.col("best.numVotes").alias("imdb_votes"), "n_candidates",
    )


def parsed_stream(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            F.col("timestamp").alias("kafka_time"),
        )
        .select("e.*", "kafka_time")
        .withColumn("event_time", F.timestamp_seconds("timestamp"))
        .withColumn("size_change", F.col("length.new") - F.coalesce(F.col("length.old"), F.lit(0)))
        .withColumn("match_key", match_key(F.col("title")))
        .withColumn("qualifier", qualifier(F.col("title")))
        .drop("length")
    )


def window_query(matched, window_col, name):
    return (
        matched.groupBy(window_col, "entity_type")
        .agg(
            F.count("*").alias("n_events"),
            F.approx_count_distinct("imdb_id").alias("n_entities"),
            F.sum(F.col("bot").cast("int")).alias("n_bot_events"),
            F.sum("size_change").alias("net_size_change"),
        )
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"), "entity_type",
                "n_events", "n_entities", "n_bot_events", "net_size_change")
        .writeStream.format("parquet")
        .option("path", str(STREAM_DIR / name))
        .option("checkpointLocation", str(STREAM_DIR / "checkpoints" / name))
        .outputMode("append").trigger(processingTime=TRIGGER)
        .queryName(name).start()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=20)
    args = parser.parse_args()

    spark = get_spark("imdb_wiki_stream", kafka=True)
    ref = reference_set(spark).cache()
    print(f"reference set: {ref.count():,} keys", flush=True)

    events = parsed_stream(spark).withWatermark("event_time", WATERMARK_DELAY)
    matched = (
        events.join(F.broadcast(ref), "match_key")
        .filter(
            F.col("qualifier").isNull()
            | ((F.col("entity_type") == "title") & F.col("qualifier").rlike(TITLE_QUALIFIER))
            | ((F.col("entity_type") == "person") & F.col("qualifier").rlike(PERSON_QUALIFIER))
        )
    )

    queries = [
        matched.select(
            "event_time", "kafka_time", "wiki", "type", "title", "title_url",
            "user", "bot", "minor", "size_change", "comment",
            F.col("revision.new").alias("revision_id"),
            "entity_type", "imdb_id", "imdb_name", "imdb_votes", "n_candidates",
        )
        .writeStream.format("parquet")
        .option("path", str(STREAM_DIR / "matches"))
        .option("checkpointLocation", str(STREAM_DIR / "checkpoints" / "matches"))
        .outputMode("append").trigger(processingTime=TRIGGER)
        .queryName("matches").start(),
        window_query(matched, F.window("event_time", TUMBLING), "window_counts"),
        window_query(matched, F.window("event_time", *SLIDING), "sliding_counts"),
    ]

    progress, seen = [], set()
    deadline = time.time() + args.minutes * 60
    while time.time() < deadline:
        time.sleep(30)
        for q in queries:
            p = q.lastProgress
            if not p or (p["name"], p["batchId"]) in seen:
                continue
            seen.add((p["name"], p["batchId"]))
            progress.append(p)
            state = p.get("stateOperators") or [{}]
            print(f"[{p['name']}] batch {p['batchId']} rows={p['numInputRows']} "
                  f"in/s={p.get('inputRowsPerSecond', 0):.1f} "
                  f"wm={p.get('eventTime', {}).get('watermark')} "
                  f"dropped_late={state[0].get('numRowsDroppedByWatermark', 0)}",
                  flush=True)
    for q in queries:
        q.stop()
    out = OUTPUT_DIR / "streaming"
    out.mkdir(parents=True, exist_ok=True)
    # progress contains UUIDs and timestamps: default=str serialises them
    (out / "progress.json").write_text(json.dumps(progress, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
