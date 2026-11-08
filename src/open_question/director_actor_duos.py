"""Open question: do recurring director-actor collaborations get better ratings?

A duo = a director and one of the top-billed actors/actresses (billing order
<= LEAD_BILLING) of the same movie. For every film of a duo we know whether it
is their 1st, 2nd, 3rd... collaboration. Ratings are compared both raw and
relative to the director's own average rating, so that a good director
working repeatedly with the same actors does not bias the result.
Usage: python -m src.open_question.director_actor_duos
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.io import read_silver, write_result
from src.common.spark import get_spark
from src.exploration.common import ACTING, movies, names

SECTION = "open_question"
MIN_VOTES = 1000
LEAD_BILLING = 3


def duo_films(spark):
    films = movies(spark, MIN_VOTES).filter(F.col("startYear").isNotNull())
    directors = read_silver(spark, "title_directors").withColumnRenamed("nconst", "director")
    leads = (
        read_silver(spark, "title_principals")
        .filter(F.col("category").isin(ACTING) & (F.col("ordering") <= LEAD_BILLING)
                & (F.size("dq_flags") == 0))
        .select("tconst", F.col("nconst").alias("actor"))
    )
    duos = (
        films.join(directors, "tconst").join(leads, "tconst")
        .filter(F.col("director") != F.col("actor"))      # exclude self-directed
    )
    w_director = Window.partitionBy("director")
    w_duo = Window.partitionBy("director", "actor")
    return (
        duos
        .withColumn("director_avg", F.avg("averageRating").over(w_director))
        .withColumn("director_films", F.size(F.collect_set("tconst").over(w_director)))
        .withColumn("collab_index",
                    F.row_number().over(w_duo.orderBy("startYear", "tconst")))
        .withColumn("duo_films", F.count("*").over(w_duo))
        .withColumn("relative_rating", F.col("averageRating") - F.col("director_avg"))
        .withColumn("collab_bucket",
                    F.when(F.col("collab_index") >= 5, "5+")
                    .otherwise(F.col("collab_index").cast("string")))
        .filter(F.col("director_films") >= 5)   # director average must be meaningful
    )


def main():
    spark = get_spark("imdb_open_question")
    df = duo_films(spark).cache()

    write_result(
        df.groupBy("collab_bucket").agg(
            F.count("*").alias("n_duo_films"),
            F.countDistinct("director", "actor").alias("n_duos"),
            F.round(F.avg("averageRating"), 3).alias("avg_rating"),
            F.round(F.avg("relative_rating"), 3).alias("avg_rating_vs_director_mean"),
            F.round(F.avg("numVotes"), 0).alias("avg_votes"),
        ).orderBy("collab_bucket"),
        SECTION, "rating_by_collaboration_index",
    )
    # recurring = duo with 3+ films together, occasional = 1-2 films
    duo_type = F.when(F.col("duo_films") >= 3, "recurring").otherwise("occasional")
    write_result(
        df.withColumn("genre", F.explode("genres"))
        .groupBy("genre").agg(*[
            agg.alias(f"{kind}_{name}")
            for kind in ("occasional", "recurring")
            for name, agg in (
                ("films", F.sum((duo_type == kind).cast("int"))),
                ("rating_vs_director_mean",
                 F.round(F.avg(F.when(duo_type == kind, F.col("relative_rating"))), 3)),
            )
        ]).orderBy("genre"),
        SECTION, "recurring_vs_occasional_by_genre", show=30,
    )
    # Within the same duo: does the 2nd+ film beat the 1st one?
    first = df.filter("collab_index = 1").select(
        "director", "actor", F.col("averageRating").alias("first_rating"))
    later = df.filter("collab_index > 1").groupBy("director", "actor").agg(
        F.avg("averageRating").alias("later_avg"), F.count("*").alias("n_later"))
    paired = first.join(later, ["director", "actor"])
    write_result(
        paired.agg(
            F.count("*").alias("n_duos"),
            F.round(F.avg(F.col("later_avg") - F.col("first_rating")), 3)
            .alias("mean_later_minus_first"),
            F.round(100 * F.avg((F.col("later_avg") > F.col("first_rating")).cast("int")), 1)
            .alias("pct_duos_improving"),
        ),
        SECTION, "within_duo_evolution",
    )
    people = names(spark)
    write_result(
        df.groupBy("director", "actor").agg(
            F.count("*").alias("n_films"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
            F.round(F.avg("relative_rating"), 2).alias("avg_vs_director_mean"),
            F.min("startYear").alias("first_year"), F.max("startYear").alias("last_year"),
        )
        .filter("n_films >= 5")
        .join(people.select(F.col("nconst").alias("director"),
                            F.col("primaryName").alias("director_name")), "director")
        .join(people.select(F.col("nconst").alias("actor"),
                            F.col("primaryName").alias("actor_name")), "actor")
        .select("director_name", "actor_name", "n_films", "avg_rating",
                "avg_vs_director_mean", "first_year", "last_year")
        .orderBy(F.desc("n_films"), F.desc("avg_rating"),
                 "director_name", "actor_name").limit(30),
        SECTION, "top_recurring_duos",
    )
    spark.stop()


if __name__ == "__main__":
    main()
