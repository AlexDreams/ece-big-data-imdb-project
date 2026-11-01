"""Data exploration questions on the silver layer.

Usage: python -m src.exploration.questions [q1 q2 ...]   (default: all)
Results are written to output/exploration/.
"""
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.io import read_silver, write_result
from src.common.spark import get_spark
from src.exploration.common import (
    acting_credits, feature_movies, movie_genres, movies, names,
)
from src.exploration.original_language import (
    EXTERNAL_DIR, IDS_FILE, fetch as fetch_languages, load_languages,
)

SECTION = "exploration"
YEARS = (1980, 2025)
MIN_VOTES = 100            # ignore titles rated by a handful of users
OUTLIER_Z = 2.0            # q1: |runtime - genre mean| > 2 standard deviations
SUCCESS_RATING, SUCCESS_VOTES = 7.0, 1000   # q6: definition of a successful film


def q1_runtime_outliers(spark):
    """Feature films whose runtime differs notably from their genre average."""
    mg = movie_genres(feature_movies(spark)).filter(F.col("runtimeMinutes").isNotNull())
    stats = mg.groupBy("genre").agg(
        F.count("*").alias("n_movies"),
        F.round(F.avg("runtimeMinutes"), 1).alias("avg_runtime"),
        F.round(F.stddev("runtimeMinutes"), 1).alias("std_runtime"),
    )
    scored = mg.join(stats, "genre").withColumn(
        "z_score",
        F.round((F.col("runtimeMinutes") - F.col("avg_runtime")) / F.col("std_runtime"), 2),
    )
    outliers = scored.filter(F.abs("z_score") > OUTLIER_Z)
    per_genre = stats.join(
        outliers.groupBy("genre").agg(
            F.count("*").alias("n_outliers"),
            F.sum((F.col("z_score") > 0).cast("int")).alias("n_longer"),
            F.sum((F.col("z_score") < 0).cast("int")).alias("n_shorter"),
        ),
        "genre", "left",
    ).fillna(0).orderBy(F.desc("n_movies"))
    write_result(per_genre, SECTION, "q1_runtime_outliers_by_genre", show=30)
    write_result(
        outliers.select("genre", "tconst", "primaryTitle", "startYear",
                        "runtimeMinutes", "avg_runtime", "z_score", "numVotes")
        .orderBy("genre", F.desc(F.abs("z_score"))),
        SECTION, "q1_runtime_outliers", show=10,
    )


def genre_year_stats(spark):
    mg = movie_genres(feature_movies(spark)).filter(
        F.col("startYear").between(*YEARS)
    )
    return mg.groupBy(F.col("startYear").alias("year"), "genre").agg(
        F.count("*").alias("n_movies"),
        F.sum("numVotes").alias("total_votes"),
        F.round(F.avg("averageRating"), 2).alias("avg_rating"),
    )


def ranked_genres(spark):
    """Popularity of a genre in a year = total IMDb votes of its feature films."""
    w = Window.partitionBy("year").orderBy(F.desc("total_votes"))
    return genre_year_stats(spark).withColumn("rank", F.row_number().over(w))


def q2_top3_genres(spark):
    top3 = ranked_genres(spark).filter("rank <= 3").orderBy("year", "rank")
    write_result(top3, SECTION, "q2_top3_genres_per_year", show=15)


def q3_top3_vs_rest(spark):
    ranked = ranked_genres(spark).withColumn(
        "group", F.when(F.col("rank") <= 3, "top3").otherwise("rest")
    )
    per_year = ranked.groupBy("year", "group").agg(
        F.sum("n_movies").alias("n_movies"),
        F.sum("total_votes").alias("total_votes"),
        F.countDistinct("genre").alias("n_genres"),
    )
    w = Window.partitionBy("year")
    per_year = per_year.withColumn(
        "vote_share_pct",
        F.round(100 * F.col("total_votes") / F.sum("total_votes").over(w), 1),
    ).withColumn(
        "votes_per_movie", F.round(F.col("total_votes") / F.col("n_movies"), 0)
    ).orderBy("year", "group")
    write_result(per_year, SECTION, "q3_top3_vs_rest_per_year", show=12)

    # Rating is averaged per movie, so it is computed from movie-level data
    mg = movie_genres(feature_movies(spark)).filter(F.col("startYear").between(*YEARS))
    labelled = mg.join(
        ranked.select(F.col("year").alias("startYear"), "genre", "group"),
        ["startYear", "genre"],
    )
    overall = labelled.groupBy("group").agg(
        F.round(F.avg("averageRating"), 2).alias("avg_rating"),
        F.round(F.avg("numVotes"), 0).alias("avg_votes_per_movie"),
        F.round(F.avg("runtimeMinutes"), 1).alias("avg_runtime"),
    )
    write_result(overall, SECTION, "q3_top3_vs_rest_overall")
    frequency = (
        ranked.filter("rank <= 3").groupBy("genre")
        .agg(F.count("*").alias("years_in_top3"))
        .orderBy(F.desc("years_in_top3"))
    )
    write_result(frequency, SECTION, "q3_genre_frequency_in_top3")


def q4_one_hit_directors(spark):
    directed = (
        read_silver(spark, "title_directors")
        .join(movies(spark, MIN_VOTES), "tconst")
    )
    per_director = directed.groupBy("nconst").agg(
        F.sum((F.col("averageRating") >= 8.0).cast("int")).alias("n_hits"),
        F.count("*").alias("n_movies"),
        F.avg(F.when(F.col("averageRating") < 8.0, F.col("averageRating")))
        .alias("avg_other_rating"),
        F.max(F.when(F.col("averageRating") >= 8.0, F.struct(
            "averageRating", "primaryTitle", "startYear"))).alias("hit"),
    )
    result = (
        per_director
        .filter((F.col("n_hits") == 1) & (F.col("n_movies") - 1 >= 2)
                & (F.col("avg_other_rating") <= 6.0))
        .join(names(spark), "nconst")
        .select("nconst", "primaryName", (F.col("n_movies") - 1).alias("n_other_movies"),
                F.round("avg_other_rating", 2).alias("avg_other_rating"),
                F.col("hit.primaryTitle").alias("hit_title"),
                F.col("hit.startYear").alias("hit_year"),
                F.col("hit.averageRating").alias("hit_rating"))
        .orderBy(F.desc("n_other_movies"))
    )
    write_result(result, SECTION, "q4_one_hit_directors")


QUESTIONS = {"q1": q1_runtime_outliers, "q2": q2_top3_genres, "q3": q3_top3_vs_rest, "q4": q4_one_hit_directors}


def main(selected):
    spark = get_spark("imdb_exploration")
    for key in selected or QUESTIONS:
        print(f"\n######## {key}")
        QUESTIONS[key](spark)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
