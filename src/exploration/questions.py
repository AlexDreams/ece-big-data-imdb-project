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


QUESTIONS = {"q1": q1_runtime_outliers}


def main(selected):
    spark = get_spark("imdb_exploration")
    for key in selected or QUESTIONS:
        print(f"\n######## {key}")
        QUESTIONS[key](spark)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
