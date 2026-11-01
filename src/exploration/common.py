"""Shared silver-layer views used by the exploration questions."""
from pyspark.sql import functions as F

from src.common.io import read_silver

FEATURE_MIN_RUNTIME = 40  # AMPAS / BFI: a feature film runs more than 40 minutes
ACTING = ["actor", "actress"]


def movies(spark, min_votes=0):
    """Rated movies (titleType = movie) with their rating, one row per title."""
    titles = read_silver(spark, "title_basics").filter(F.col("titleType") == "movie")
    ratings = read_silver(spark, "title_ratings")
    return (
        titles.join(ratings, "tconst")
        .filter(F.col("numVotes") >= min_votes)
        .select("tconst", "primaryTitle", "originalTitle", "startYear",
                "runtimeMinutes", "genres", "averageRating", "numVotes", "isAdult")
    )


def feature_movies(spark, min_votes=0):
    return movies(spark, min_votes).filter(
        F.col("runtimeMinutes") > FEATURE_MIN_RUNTIME
    )


def movie_genres(df):
    """One row per (movie, genre)."""
    return df.withColumn("genre", F.explode("genres")).drop("genres")


def names(spark):
    return read_silver(spark, "name_basics").select(
        "nconst", "primaryName", "birthYear", "deathYear"
    )


def acting_credits(spark):
    """(tconst, nconst) for actor/actress credits without quality flags."""
    return (
        read_silver(spark, "title_principals")
        .filter(F.col("category").isin(ACTING) & (F.size("dq_flags") == 0))
        .select("tconst", "nconst")
        .dropDuplicates()
    )
