"""Select the films and principals to enrich (Spark).

Films: feature films released in [YEAR_MIN, YEAR_MAX], top TOP_N per genre per
year by number of IMDb votes. Principals: the first N_PRINCIPALS credits of
each selected film (IMDb billing order). Every parameter can be raised up to
the full dataset without code changes.
Output: data/enrichment/targets/{films,people}.csv
Usage: python -m src.enrichment.select_targets [--top-n 10] [--principals 3]
"""
import argparse

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


def select_people(spark, films, n_principals):
    w = Window.partitionBy("tconst").orderBy("ordering")
    return (
        read_silver(spark, "title_principals")
        .join(films.select("tconst"), "tconst")
        .withColumn("r", F.row_number().over(w))
        .filter(F.col("r") <= n_principals)
        .select("tconst", "nconst", "ordering", "category")
    )


def write_csv(df, name):
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(TARGET_DIR / f"{name}.csv", index=False)
    print(f"{name}: {len(pdf):,} rows -> {TARGET_DIR / name}.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--principals", type=int, default=3)
    parser.add_argument("--year-min", type=int, default=1980)
    parser.add_argument("--year-max", type=int, default=2025)
    args = parser.parse_args()

    spark = get_spark("imdb_enrichment_targets")
    films = select_films(spark, args.top_n, args.year_min, args.year_max).cache()
    credits = select_people(spark, films, args.principals)
    write_csv(films, "films")
    write_csv(credits, "credits")
    spark.stop()


if __name__ == "__main__":
    main()
