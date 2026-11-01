"""Bronze -> silver: typed, de-duplicated, referentially consistent tables.

Handling rules (one per issue found by quality_checks):
- invalid casts and blank strings -> null
- inconsistent values are nulled, never guessed, and the row keeps a
  `dq_flags` array explaining what was changed
- rows whose foreign key has no parent are dropped from fact tables
- comma separated lists become arrays; crew is normalised into bridge tables
Results: data/silver/<table>/ and output/preparation/cleaning_summary.csv
Usage: python -m src.preparation.clean_to_silver
"""
from datetime import date

from pyspark.sql import functions as F

from src.common.io import write_result
from src.common.schemas import table_dir_name
from src.common.spark import SILVER_DIR, get_spark
from src.preparation.typed import split_list, typed

CURRENT_YEAR = date.today().year
MAX_RUNTIME = 1440


def flag(condition, label):
    return F.when(condition, F.lit(label))


def with_flags(df, *flags):
    """Collect the non-null flag labels into a `dq_flags` array column."""
    return df.withColumn("dq_flags", F.filter(F.array(*flags), lambda x: x.isNotNull()))


def clean_names(spark):
    df = typed(spark, "name.basics").dropDuplicates(["nconst"])
    future_birth = F.col("birthYear") > CURRENT_YEAR
    future_death = F.col("deathYear") > CURRENT_YEAR
    death_first = F.col("deathYear") < F.col("birthYear")
    df = with_flags(
        df,
        flag(future_birth, "birth_year_in_future"),
        flag(future_death, "death_year_in_future"),
        flag(death_first, "death_before_birth"),
    )
    return (
        df
        .withColumn("birthYear", F.when(future_birth | death_first, None)
                    .otherwise(F.col("birthYear")))
        .withColumn("deathYear", F.when(future_death | death_first, None)
                    .otherwise(F.col("deathYear")))
        .withColumn("primaryProfession", split_list("primaryProfession"))
        .withColumn("knownForTitles", split_list("knownForTitles"))
    )


def clean_titles(spark):
    df = typed(spark, "title.basics").dropDuplicates(["tconst"])
    bad_runtime = (F.col("runtimeMinutes") <= 0) | (F.col("runtimeMinutes") > MAX_RUNTIME)
    end_first = F.col("endYear") < F.col("startYear")
    df = with_flags(
        df,
        flag(bad_runtime, "invalid_runtime"),
        flag(end_first, "end_before_start"),
    )
    return (
        df
        .withColumn("runtimeMinutes", F.when(bad_runtime, None)
                    .otherwise(F.col("runtimeMinutes")))
        .withColumn("endYear", F.when(end_first, None).otherwise(F.col("endYear")))
        .withColumn("isAdult", F.col("isAdult") == 1)
        .withColumn("genres", split_list("genres"))
    )


def clean_ratings(spark, title_ids):
    return (
        typed(spark, "title.ratings")
        .dropDuplicates(["tconst"])
        .filter(F.col("averageRating").between(1, 10) & (F.col("numVotes") > 0))
        .join(title_ids, "tconst", "left_semi")
    )


def clean_principals(spark, titles, names):
    df = (
        typed(spark, "title.principals")
        .dropDuplicates(["tconst", "ordering"])
        .join(titles.select("tconst"), "tconst", "left_semi")
        .join(names.select("nconst"), "nconst", "left_semi")
    )
    # Temporal anomalies are kept (credits can be wrong but the link is real)
    # and flagged so analyses can exclude them.
    timeline = (
        df.join(titles.select("tconst", "startYear"), "tconst", "left")
        .join(names.select("nconst", "birthYear", "deathYear"), "nconst", "left")
    )
    archive = F.col("category").isin("archive_footage", "archive_sound", "self")
    timeline = with_flags(
        timeline,
        flag(~archive & (F.col("startYear") > F.col("deathYear") + 2),
             "died_before_title"),
        flag(F.col("birthYear") > F.col("startYear"), "born_after_title"),
    )
    return timeline.drop("startYear", "birthYear", "deathYear")


def crew_pairs(spark, role):
    """(tconst, nconst) pairs of one crew role, as listed in title.crew."""
    return typed(spark, "title.crew").select(
        "tconst", F.explode(split_list(role)).alias("nconst")
    )


def crew_bridge(spark, role, titles, names):
    return (
        crew_pairs(spark, role)
        .dropDuplicates()
        .join(titles.select("tconst"), "tconst", "left_semi")
        .join(names.select("nconst"), "nconst", "left_semi")
    )


def clean_episodes(spark, titles):
    series = titles.filter(F.col("titleType").isin("tvSeries", "tvMiniSeries"))
    return (
        typed(spark, "title.episode")
        .dropDuplicates(["tconst"])
        .join(titles.select("tconst"), "tconst", "left_semi")
        .join(series.select(F.col("tconst").alias("parentTconst")),
              "parentTconst", "left_semi")
    )


def clean_akas(spark, titles):
    return (
        typed(spark, "title.akas")
        .dropDuplicates(["titleId", "ordering"])
        .join(titles.select(F.col("tconst").alias("titleId")), "titleId", "left_semi")
        .withColumn("isOriginalTitle", F.col("isOriginalTitle") == 1)
        .withColumn("types", split_list("types", "\u0002"))
        .withColumn("attributes", split_list("attributes", "\u0002"))
    )


def save(df, name):
    path = SILVER_DIR / name
    df.write.mode("overwrite").parquet(str(path))
    return path


def run(spark):
    summary = []

    def persist(df, name, bronze_table=None, before_df=None):
        path = save(df, name)
        out = spark.read.parquet(str(path)).count()
        # bridge tables have no bronze table: count the pairs before cleaning
        if before_df is None:
            before_df = typed(spark, bronze_table)
        before = before_df.count()
        summary.append((name, before, out))
        print(f"{name:<20} {before:>12,} -> {out:,}")

    names_path = save(clean_names(spark), table_dir_name("name.basics"))
    titles_path = save(clean_titles(spark), table_dir_name("title.basics"))
    names = spark.read.parquet(str(names_path))
    titles = spark.read.parquet(str(titles_path))
    for table, df in (("name.basics", names), ("title.basics", titles)):
        summary.append((table_dir_name(table), typed(spark, table).count(), df.count()))

    persist(clean_ratings(spark, titles.select("tconst")),
            table_dir_name("title.ratings"), "title.ratings")
    persist(clean_principals(spark, titles, names),
            table_dir_name("title.principals"), "title.principals")
    persist(clean_episodes(spark, titles),
            table_dir_name("title.episode"), "title.episode")
    persist(clean_akas(spark, titles), table_dir_name("title.akas"), "title.akas")
    for role in ("directors", "writers"):
        persist(crew_bridge(spark, role, titles, names), f"title_{role}",
                before_df=crew_pairs(spark, role))

    result = spark.createDataFrame(
        summary, "table string, rows_before long, rows_after long"
    )
    write_result(result, "preparation", "cleaning_summary")


def main():
    spark = get_spark("imdb_clean_to_silver")
    run(spark)
    spark.stop()


if __name__ == "__main__":
    main()
