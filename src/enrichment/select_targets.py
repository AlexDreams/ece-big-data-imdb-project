"""Select the films and principals to enrich (Spark).

Films: feature films released in [YEAR_MIN, YEAR_MAX], top TOP_N per genre per
year by number of IMDb votes. Candidates: the first N_CANDIDATES credits of
each selected film (IMDb billing order); build_enriched.py then keeps three
principals per film, those with a biography first. Every parameter can be
raised up to the full dataset without code changes.
Output: data/enrichment/targets/{films,credits}.csv
Usage: python -m src.enrichment.select_targets [--top-n 10] [--candidates 5]
"""
import argparse
import shutil

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.io import read_silver
from src.common.spark import DATA_DIR, get_spark
from src.exploration.common import feature_movies, movie_genres

TARGET_DIR = DATA_DIR / "enrichment" / "targets"


def select_films(spark, top_n, year_min, year_max):
    mg = movie_genres(feature_movies(spark)).filter(
        F.col("startYear").between(year_min, year_max)
    )
    w = Window.partitionBy("startYear", "genre").orderBy(F.desc("numVotes"), "tconst")
    ranked = mg.withColumn("rank_in_genre_year", F.row_number().over(w)).filter(
        F.col("rank_in_genre_year") <= top_n
    )
    # a film can be in the top of several genres: keep one row per film
    return ranked.groupBy("tconst", "primaryTitle", "startYear", "numVotes").agg(
        F.concat_ws("|", F.sort_array(F.collect_set("genre"))).alias("selected_genres")
    )


def select_people(spark, films, n_candidates):
    w = Window.partitionBy("tconst").orderBy("ordering")
    return (
        read_silver(spark, "title_principals")
        .join(films.select("tconst"), "tconst")
        .withColumn("r", F.row_number().over(w))
        .filter(F.col("r") <= n_candidates)
        .select("tconst", "nconst", "ordering", "category")
    )


def write_csv(df, name):
    """Write one CSV file with Spark, without collecting the rows on the driver."""
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    tmp_dir = TARGET_DIR / f"_{name}_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp_dir))
    target = TARGET_DIR / f"{name}.csv"
    next(tmp_dir.glob("part-*.csv")).replace(target)
    shutil.rmtree(tmp_dir)
    print(f"{name} -> {target}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--candidates", type=int, default=5)
    parser.add_argument("--year-min", type=int, default=1980)
    parser.add_argument("--year-max", type=int, default=2025)
    args = parser.parse_args()

    spark = get_spark("imdb_enrichment_targets")
    films = select_films(spark, args.top_n, args.year_min, args.year_max).cache()
    credits = select_people(spark, films, args.candidates)
    write_csv(films, "films")
    write_csv(credits, "credits")
    spark.stop()


if __name__ == "__main__":
    main()
