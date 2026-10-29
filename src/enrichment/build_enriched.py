"""Clean the gathered Wikipedia/Wikidata data and join it to IMDb (Spark).

Explicit schemas are enforced when reading the raw JSONL, texts are cleaned,
and every enriched row keeps its lineage (Wikipedia URL, revision permalink,
Wikidata URL, license, retrieval time). Missing information is explained in
a `note` column instead of being silently dropped.
Output: data/gold/enriched_films, data/gold/enriched_principals,
        output/enrichment/*.csv
Usage: python -m src.enrichment.build_enriched
"""
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, LongType, StringType, StructField, StructType,
)

from src.common.io import read_silver, write_result
from src.common.spark import DATA_DIR, GOLD_DIR, get_spark

RAW_DIR = DATA_DIR / "enrichment" / "raw"
TARGET_DIR = DATA_DIR / "enrichment" / "targets"
MIN_TEXT_LENGTH = 50
S = StringType()

WIKIDATA_SCHEMA = StructType([
    StructField("imdb_id", S, False), StructField("wikidata_url", S),
    StructField("wikipedia_url", S), StructField("birth_date", S),
    StructField("death_date", S), StructField("birth_place", S),
    StructField("citizenship", ArrayType(S)), StructField("note", S),
    StructField("retrieved_at", S),
])
WIKIPEDIA_SCHEMA = StructType([
    StructField("imdb_id", S, False), StructField("wikidata_url", S),
    StructField("source_url", S), StructField("source_revision_id", LongType()),
    StructField("source_license", S), StructField("retrieved_at", S),
    StructField("synopsis", S), StructField("synopsis_section", S),
    StructField("biography", S), StructField("note", S),
])


def read_jsonl(spark, name, schema):
    return (
        spark.read.schema(schema).option("mode", "FAILFAST")
        .json(str(RAW_DIR / f"{name}.jsonl"))
        .dropDuplicates(["imdb_id"])
    )


def clean_text(col):
    """Drop sub-headings left by the plain-text export and normalise spaces."""
    c = F.regexp_replace(col, r"(?m)^=+[^=\n]+=+\s*$", "")
    # pronunciations stripped by the plain-text export leave "()" or "( ; ...)"
    c = F.regexp_replace(c, r"\s*\(\s*[;,]?\s*\)", "")
    c = F.regexp_replace(c, r"\(\s*[;,]\s*", "(")
    c = F.regexp_replace(c, r"[ \t]+", " ")
    c = F.regexp_replace(c, r"\n{2,}", "\n\n")
    c = F.trim(c)
    return F.when(F.length(c) >= MIN_TEXT_LENGTH, c)


def wikidata_date(name):
    """ISO date, or null for Wikidata 'unknown value' (a genid URL) and BC dates."""
    col = F.col(name)
    return F.when(col.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(F.substring(col, 1, 10)))


def permalink(rev_col):
    return F.when(
        rev_col.isNotNull(),
        F.concat(F.lit("https://en.wikipedia.org/w/index.php?oldid="), rev_col.cast("string")),
    )


def lineage(wp="wp", wd="wd"):
    """Lineage columns from the aliased Wikipedia (wp) and Wikidata (wd) inputs."""
    def c(alias, name):
        return F.col(f"{alias}.{name}")
    return [
        F.coalesce(c(wp, "source_url"), c(wd, "wikipedia_url")).alias("source_url"),
        permalink(c(wp, "source_revision_id")).alias("source_permalink"),
        c(wd, "wikidata_url").alias("wikidata_url"),
        F.when(c(wp, "source_url").isNotNull(), c(wp, "source_license")).alias("source_license"),
        F.to_timestamp(F.coalesce(c(wp, "retrieved_at"), c(wd, "retrieved_at"))).alias("retrieved_at"),
    ]


def build_films(spark):
    targets = spark.read.option("header", True).csv(str(TARGET_DIR / "films.csv"))
    wd = read_jsonl(spark, "wikidata_films", WIKIDATA_SCHEMA).alias("wd")
    wp = read_jsonl(spark, "wikipedia_films", WIKIPEDIA_SCHEMA).alias("wp")
    synopsis = clean_text(F.col("wp.synopsis"))
    return (
        targets.alias("t")
        .join(wd, F.col("t.tconst") == F.col("wd.imdb_id"), "left")
        .join(wp, F.col("t.tconst") == F.col("wp.imdb_id"), "left")
        .select(
            F.col("t.tconst"), F.col("t.primaryTitle"),
            F.col("t.startYear").cast("int").alias("startYear"),
            F.split("t.selected_genres", r"\|").alias("selected_genres"),
            synopsis.alias("synopsis"),
            *lineage(),
            F.coalesce(
                F.col("wp.note"), F.col("wd.note"),
                F.when(F.col("wd.imdb_id").isNull(), F.lit("not resolved yet")),
                F.when(synopsis.isNull(), F.lit("synopsis too short or empty")),
            ).alias("note"),
        )
    )


def build_principals(spark):
    credits = spark.read.option("header", True).csv(str(TARGET_DIR / "credits.csv"))
    people = read_silver(spark, "name_basics").select(
        "nconst", "primaryName", "primaryProfession"
    )
    wd = read_jsonl(spark, "wikidata_people", WIKIDATA_SCHEMA).alias("wd")
    wp = read_jsonl(spark, "wikipedia_people", WIKIPEDIA_SCHEMA).alias("wp")
    bio = clean_text(F.col("wp.biography"))
    return (
        credits.alias("c")
        .join(people.alias("p"), "nconst", "left")
        .join(wd, F.col("c.nconst") == F.col("wd.imdb_id"), "left")
        .join(wp, F.col("c.nconst") == F.col("wp.imdb_id"), "left")
        .select(
            F.col("c.tconst"), F.col("nconst"),
            F.col("c.ordering").cast("int").alias("ordering"), F.col("c.category"),
            F.col("p.primaryName"), F.col("p.primaryProfession"),
            wikidata_date("wd.birth_date").alias("birth_date"),
            wikidata_date("wd.death_date").alias("death_date"),
            F.col("wd.birth_place"), F.col("wd.citizenship"),
            bio.alias("biography"),
            *lineage(),
            F.coalesce(
                F.col("wp.note"), F.col("wd.note"),
                F.when(F.col("wd.imdb_id").isNull(), F.lit("not resolved yet")),
                F.when(bio.isNull(), F.lit("biography too short or empty")),
            ).alias("note"),
        )
    )


def coverage(df, text_col, label):
    return df.agg(
        F.lit(label).alias("dataset"),
        F.count("*").alias("rows"),
        F.sum(F.col("wikidata_url").isNotNull().cast("int")).alias("with_wikidata"),
        F.sum(F.col("source_url").isNotNull().cast("int")).alias("with_wikipedia"),
        F.sum(F.col(text_col).isNotNull().cast("int")).alias("with_text"),
        F.round(100 * F.avg(F.col(text_col).isNotNull().cast("int")), 1).alias("coverage_pct"),
    )


def main():
    spark = get_spark("imdb_build_enriched")
    films = build_films(spark)
    principals = build_principals(spark)
    films.write.mode("overwrite").parquet(str(GOLD_DIR / "enriched_films"))
    principals.write.mode("overwrite").parquet(str(GOLD_DIR / "enriched_principals"))
    films = spark.read.parquet(str(GOLD_DIR / "enriched_films"))
    principals = spark.read.parquet(str(GOLD_DIR / "enriched_principals"))

    write_result(
        coverage(films, "synopsis", "films")
        .unionByName(coverage(principals, "biography", "principals")),
        "enrichment", "coverage",
    )
    write_result(films.groupBy("note").count().orderBy(F.desc("count")),
                 "enrichment", "films_notes")
    write_result(principals.groupBy("note").count().orderBy(F.desc("count")),
                 "enrichment", "principals_notes")
    write_result(
        films.filter(F.col("synopsis").isNotNull())
        .select("tconst", "primaryTitle", "startYear",
                F.substring("synopsis", 1, 200).alias("synopsis_start"),
                "source_permalink")
        .orderBy(F.desc("startYear")).limit(20),
        "enrichment", "films_sample", show=5,
    )
    write_result(
        principals.filter(F.col("biography").isNotNull())
        .select("tconst", "primaryName", "category", "birth_date", "birth_place",
                F.substring("biography", 1, 200).alias("biography_start"),
                "source_permalink")
        .limit(20),
        "enrichment", "principals_sample", show=5,
    )
    spark.stop()


if __name__ == "__main__":
    main()
