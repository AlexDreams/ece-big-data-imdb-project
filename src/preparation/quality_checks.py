"""Data quality checks on the IMDb dataset (beyond missing values).

Each check counts the violating rows. Results:
output/preparation/quality_issues.csv and a few example rows per check.
Usage: python -m src.preparation.quality_checks
"""
from datetime import date

from pyspark.sql import functions as F

from src.common.io import write_result
from src.common.spark import get_spark
from src.preparation.typed import split_list, typed

CURRENT_YEAR = date.today().year
# Categories where appearing after one's death is expected
ARCHIVE_CATEGORIES = ["archive_footage", "archive_sound", "self"]


def not_numeric(c):
    return F.col(f"{c}__raw").isNotNull() & F.col(c).isNull()


def run_checks(spark):
    # Only the columns used by the checks: these tables are too big to cache
    names = typed(spark, "name.basics", keep_raw=True).select(
        "nconst", "primaryName", "birthYear", "deathYear",
        "birthYear__raw", "deathYear__raw",
    )
    titles = typed(spark, "title.basics", keep_raw=True).select(
        "tconst", "titleType", "primaryTitle", "startYear", "endYear",
        "runtimeMinutes", "runtimeMinutes__raw", "isAdult", "genres",
    )
    ratings = typed(spark, "title.ratings", keep_raw=True)
    principals = typed(spark, "title.principals")
    episodes = typed(spark, "title.episode", keep_raw=True)
    crew = typed(spark, "title.crew")
    akas = typed(spark, "title.akas", keep_raw=True)

    title_ids = titles.select("tconst").distinct()
    name_ids = names.select("nconst").distinct()

    checks = []  # (table, check, description, violating DataFrame, base rows)

    def add(table, name, description, bad_df, base_df):
        checks.append((table, name, description, bad_df, base_df))

    # --- name.basics
    add("name.basics", "death_before_birth",
        "deathYear < birthYear",
        names.filter(F.col("deathYear") < F.col("birthYear")), names)
    add("name.basics", "implausible_lifespan",
        "deathYear - birthYear > 120",
        names.filter(F.col("deathYear") - F.col("birthYear") > 120), names)
    add("name.basics", "alive_over_120",
        f"no deathYear and birthYear < {CURRENT_YEAR - 120}",
        names.filter(F.col("deathYear").isNull()
                     & (F.col("birthYear") < CURRENT_YEAR - 120)), names)
    add("name.basics", "year_in_future",
        f"birthYear or deathYear > {CURRENT_YEAR}",
        names.filter((F.col("birthYear") > CURRENT_YEAR)
                     | (F.col("deathYear") > CURRENT_YEAR)), names)
    add("name.basics", "non_numeric_year",
        "birthYear/deathYear present but not an integer",
        names.filter(not_numeric("birthYear") | not_numeric("deathYear")), names)
    add("name.basics", "duplicate_nconst",
        "nconst appearing more than once",
        names.groupBy("nconst").count().filter("count > 1"), names)

    # --- title.basics
    add("title.basics", "duplicate_tconst", "tconst appearing more than once",
        titles.groupBy("tconst").count().filter("count > 1"), titles)
    add("title.basics", "end_before_start", "endYear < startYear",
        titles.filter(F.col("endYear") < F.col("startYear")), titles)
    add("title.basics", "runtime_non_positive", "runtimeMinutes <= 0",
        titles.filter(F.col("runtimeMinutes") <= 0), titles)
    add("title.basics", "runtime_over_24h", "runtimeMinutes > 1440",
        titles.filter(F.col("runtimeMinutes") > 1440), titles)
    add("title.basics", "start_year_future",
        f"startYear > {CURRENT_YEAR} (announced titles)",
        titles.filter(F.col("startYear") > CURRENT_YEAR), titles)
    add("title.basics", "invalid_is_adult", "isAdult not in (0, 1)",
        titles.filter(~F.col("isAdult").isin(0, 1) | F.col("isAdult").isNull()),
        titles)
    add("title.basics", "non_numeric_runtime",
        "runtimeMinutes present but not an integer",
        titles.filter(not_numeric("runtimeMinutes")), titles)
    add("title.basics", "movie_without_genre", "titleType = movie and no genres",
        titles.filter((F.col("titleType") == "movie") & F.col("genres").isNull()),
        titles)

    # --- title.ratings
    add("title.ratings", "rating_out_of_range", "averageRating not in [1, 10]",
        ratings.filter((F.col("averageRating") < 1) | (F.col("averageRating") > 10)),
        ratings)
    add("title.ratings", "non_positive_votes", "numVotes <= 0",
        ratings.filter(F.col("numVotes") <= 0), ratings)
    add("title.ratings", "orphan_tconst", "tconst not in title.basics",
        ratings.join(title_ids, "tconst", "left_anti"), ratings)

    # --- title.principals
    add("title.principals", "orphan_tconst", "tconst not in title.basics",
        principals.join(title_ids, "tconst", "left_anti"), principals)
    add("title.principals", "orphan_nconst", "nconst not in name.basics",
        principals.join(name_ids, "nconst", "left_anti"), principals)
    add("title.principals", "duplicate_tconst_ordering",
        "(tconst, ordering) appearing more than once",
        principals.groupBy("tconst", "ordering").count().filter("count > 1"),
        principals)

    timeline = (
        principals.select("tconst", "nconst", "category")
        .join(titles.select("tconst", "primaryTitle", "startYear"), "tconst")
        .join(names.select("nconst", "primaryName", "birthYear", "deathYear"),
              "nconst")
    )
    add("title.principals", "died_before_title",
        "principal died > 2 years before the title start (non archive roles)",
        timeline.filter(~F.col("category").isin(ARCHIVE_CATEGORIES)
                        & (F.col("startYear") > F.col("deathYear") + 2)),
        principals)
    add("title.principals", "born_after_title",
        "principal born after the title start",
        timeline.filter(F.col("birthYear") > F.col("startYear")), principals)

    # --- title.crew
    for role in ("directors", "writers"):
        exploded = crew.select(
            "tconst", F.explode(split_list(role)).alias("nconst")
        )
        add("title.crew", f"orphan_{role}", f"{role} id not in name.basics",
            exploded.join(name_ids, "nconst", "left_anti"), exploded)
    add("title.crew", "orphan_tconst", "tconst not in title.basics",
        crew.join(title_ids, "tconst", "left_anti"), crew)

    # --- title.episode
    add("title.episode", "orphan_parent", "parentTconst not in title.basics",
        episodes.join(title_ids.withColumnRenamed("tconst", "parentTconst"),
                      "parentTconst", "left_anti"), episodes)
    add("title.episode", "orphan_episode", "tconst not in title.basics",
        episodes.join(title_ids, "tconst", "left_anti"), episodes)
    add("title.episode", "parent_not_series",
        "parent is not a tvSeries / tvMiniSeries",
        episodes.join(
            titles.select(F.col("tconst").alias("parentTconst"), "titleType"),
            "parentTconst")
        .filter(~F.col("titleType").isin("tvSeries", "tvMiniSeries")),
        episodes)
    add("title.episode", "non_numeric_season",
        "seasonNumber present but not an integer",
        episodes.filter(not_numeric("seasonNumber")), episodes)

    # --- title.akas
    add("title.akas", "orphan_title_id", "titleId not in title.basics",
        akas.join(title_ids.withColumnRenamed("tconst", "titleId"),
                  "titleId", "left_anti"), akas)
    add("title.akas", "invalid_is_original", "isOriginalTitle not in (0, 1)",
        akas.filter(~F.col("isOriginalTitle").isin(0, 1)), akas)
    return checks


def run(spark):
    rows = []
    base_counts = {}  # several checks share the same base table
    for table, name, description, bad_df, base_df in run_checks(spark):
        bad = bad_df.count()
        if id(base_df) not in base_counts:
            base_counts[id(base_df)] = base_df.count()
        base = base_counts[id(base_df)]
        pct = round(100.0 * bad / base, 4) if base else 0.0
        rows.append((table, name, description, bad, base, pct))
        print(f"{table:<17} {name:<28} {bad:>12,} ({pct}%)")
        if bad and name in ("death_before_birth", "died_before_title",
                            "born_after_title", "end_before_start"):
            write_result(bad_df.limit(20), "preparation", f"examples_{name}", 5)
    result = spark.createDataFrame(
        rows,
        "table string, check string, description string, violations long, "
        "base_rows long, violation_pct double",
    )
    write_result(result, "preparation", "quality_issues", show=50)


def main():
    spark = get_spark("imdb_quality_checks")
    run(spark)
    spark.stop()


if __name__ == "__main__":
    main()
