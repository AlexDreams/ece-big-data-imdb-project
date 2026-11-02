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
    acting_credits, all_feature_movies, feature_movies, movie_genres, movies,
    names,
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
    """Feature films whose runtime differs notably from their genre average.

    All feature films are used, rated or not: the runtime does not depend on
    the rating (numVotes is null for unrated films).
    """
    mg = movie_genres(all_feature_movies(spark)).filter(
        F.col("runtimeMinutes").isNotNull()
    )
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


def q5_declining_series(spark):
    episodes = read_silver(spark, "title_episode").filter(
        F.col("seasonNumber").isNotNull()
    )
    ratings = read_silver(spark, "title_ratings").select("tconst", "averageRating")
    season_size = episodes.groupBy("parentTconst", "seasonNumber").agg(
        F.count("*").alias("n_episodes")
    )
    eligible = (
        season_size.groupBy("parentTconst").agg(
            F.sum((F.col("n_episodes") >= 10).cast("int")).alias("n_full_seasons"),
            F.max("seasonNumber").alias("last_season"),
        ).filter(F.col("n_full_seasons") >= 3)
    )
    rated = episodes.join(ratings, "tconst").join(eligible, "parentTconst")
    per_series = rated.groupBy("parentTconst", "last_season", "n_full_seasons").agg(
        F.min("averageRating").alias("worst"),
        F.max("averageRating").alias("best"),
        F.min(F.when(F.col("seasonNumber") == F.col("last_season"),
                     F.col("averageRating"))).alias("worst_last_season"),
        F.max(F.when(F.col("seasonNumber") <= 2, F.col("averageRating")))
        .alias("best_first_two"),
    )
    titles = read_silver(spark, "title_basics").select(
        F.col("tconst").alias("parentTconst"), "primaryTitle", "startYear"
    )
    result = (
        per_series
        .filter((F.col("worst_last_season") == F.col("worst"))
                & (F.col("best_first_two") == F.col("best")))
        .join(titles, "parentTconst")
        .select("parentTconst", "primaryTitle", "startYear", "n_full_seasons",
                "last_season", "best", "worst")
        .orderBy(F.desc(F.col("best") - F.col("worst")))
    )
    write_result(result, SECTION, "q5_declining_series")


def q6_multilingual_actors(spark):
    """Original languages come from Wikidata (see original_language.py)."""
    successful = movies(spark, SUCCESS_VOTES).filter(
        F.col("averageRating") >= SUCCESS_RATING
    )
    ids = [r.tconst for r in successful.select("tconst").collect()]
    EXTERNAL_DIR.mkdir(parents=True, exist_ok=True)
    with open(IDS_FILE, "w", encoding="utf-8") as f:
        f.write("tconst\n" + "\n".join(ids) + "\n")
    fetch_languages(ids)   # resumable: only ids not fetched yet are queried
    # Wikidata lists every language spoken in a film; keep films with a single
    # original language so that "a film originally in French" is unambiguous
    languages = load_languages(spark)
    single = languages.groupBy("tconst").count().filter("count = 1").select("tconst")
    write_result(
        languages.groupBy("tconst").count()
        .agg(F.count("*").alias("films_with_language"),
             F.sum((F.col("count") == 1).cast("int")).alias("single_language_films")),
        SECTION, "q6_single_vs_multi_language",
    )
    languages = languages.join(single, "tconst", "left_semi")
    hits = successful.join(languages, "tconst")
    write_result(
        successful.join(languages.select("tconst").distinct(), "tconst", "left_semi")
        .agg(F.count("*").alias("films_with_language"))
        .crossJoin(successful.agg(F.count("*").alias("successful_films"))),
        SECTION, "q6_language_coverage",
    )
    per_actor = (
        acting_credits(spark).join(hits, "tconst")
        .groupBy("nconst").agg(
            F.countDistinct("language").alias("n_languages"),
            F.sort_array(F.collect_set("language")).alias("languages"),
            F.count("*").alias("n_successful_movies"),
        )
        .filter("n_languages >= 2")
        .join(names(spark), "nconst")
        .select("nconst", "primaryName", "n_languages", "languages",
                "n_successful_movies")
        .withColumn("languages", F.concat_ws("|", "languages"))
        .orderBy(F.desc("n_languages"), F.desc("n_successful_movies"))
    )
    write_result(per_actor, SECTION, "q6_actors_2plus_languages", show=10)
    write_result(per_actor.filter("n_languages >= 3"), SECTION,
                 "q6_actors_3plus_languages", show=20)


def q7_early_peak_actors(spark):
    films = acting_credits(spark).join(
        movies(spark, MIN_VOTES).filter(F.col("startYear").isNotNull()), "tconst"
    )
    career = films.groupBy("nconst").agg(
        F.min("startYear").alias("first_year"),
        F.max("startYear").alias("last_year"),
        F.count("*").alias("n_movies"),
        # best rated movie; earliest one on ties
        F.max(F.struct("averageRating", -F.col("startYear"))).alias("peak"),
    ).withColumn("span", F.col("last_year") - F.col("first_year"))
    career = career.filter((F.col("span") >= 20) & (F.col("n_movies") >= 15))
    halves = (
        films.join(career.select("nconst", "first_year", "span"), "nconst")
        .withColumn("first_half",
                    F.col("startYear") <= F.col("first_year") + F.col("span") / 2)
        .groupBy("nconst").agg(
            F.avg(F.when(F.col("first_half"), F.col("averageRating"))).alias("avg_first_half"),
            F.avg(F.when(~F.col("first_half"), F.col("averageRating"))).alias("avg_second_half"),
        )
    )
    result = (
        career.join(halves, "nconst")
        .withColumn("peak_year", -F.col("peak.col2"))
        .withColumn("peak_position",
                    F.round((F.col("peak_year") - F.col("first_year")) / F.col("span"), 2))
        .filter((F.col("peak_position") <= 1 / 3)
                & (F.col("avg_first_half") > F.col("avg_second_half")))
        .join(names(spark), "nconst")
        .select("nconst", "primaryName", "first_year", "last_year", "n_movies",
                "peak_year", F.col("peak.averageRating").alias("peak_rating"),
                F.round("avg_first_half", 2).alias("avg_first_half"),
                F.round("avg_second_half", 2).alias("avg_second_half"))
        .orderBy(F.desc(F.col("avg_first_half") - F.col("avg_second_half")))
    )
    write_result(result, SECTION, "q7_early_peak_actors")


def q8_multi_talented(spark):
    film_ids = movies(spark, MIN_VOTES).select("tconst", "averageRating")
    principals = read_silver(spark, "title_principals").filter(
        F.size("dq_flags") == 0
    ).select(
        "tconst", "nconst",
        F.when(F.col("category").isin("actor", "actress"), "actor")
        .when(F.col("category") == "director", "director")
        .when(F.col("category") == "writer", "writer").alias("role"),
    ).filter(F.col("role").isNotNull())
    crew = (
        read_silver(spark, "title_directors").withColumn("role", F.lit("director"))
        .unionByName(read_silver(spark, "title_writers").withColumn("role", F.lit("writer")))
    )
    roles = principals.unionByName(crew).dropDuplicates().join(film_ids, "tconst")
    per_title = roles.groupBy("nconst", "tconst", "averageRating").agg(
        F.sort_array(F.collect_set("role")).alias("roles")
    ).withColumn("n_roles", F.size("roles"))
    talents = (
        roles.groupBy("nconst", "role").agg(F.countDistinct("tconst").alias("n"))
        .filter("n >= 3")
        .groupBy("nconst").agg(F.sort_array(F.collect_set("role")).alias("career_roles"))
        .filter(F.size("career_roles") >= 2)
    )
    multi = per_title.join(talents, "nconst")
    comparison = multi.groupBy(
        F.concat_ws("+", "career_roles").alias("career_profile"),
        F.when(F.col("n_roles") >= 2, "multiple roles on the title")
        .otherwise("single role on the title").alias("situation"),
    ).agg(
        F.countDistinct("nconst").alias("n_people"),
        F.count("*").alias("n_titles"),
        F.round(F.avg("averageRating"), 3).alias("avg_rating"),
    ).orderBy("career_profile", "situation")
    write_result(comparison, SECTION, "q8_multi_role_vs_single_role")
    per_person = (
        multi.groupBy("nconst").agg(
            F.avg(F.when(F.col("n_roles") >= 2, F.col("averageRating"))).alias("avg_multi"),
            F.avg(F.when(F.col("n_roles") == 1, F.col("averageRating"))).alias("avg_single"),
            F.sum((F.col("n_roles") >= 2).cast("int")).alias("n_multi_titles"),
        )
        .filter(F.col("avg_multi").isNotNull() & F.col("avg_single").isNotNull())
    )
    write_result(
        per_person.agg(
            F.count("*").alias("n_people"),
            F.round(F.avg(F.col("avg_multi") - F.col("avg_single")), 3)
            .alias("mean_rating_gap_multi_minus_single"),
            F.round(100 * F.avg((F.col("avg_multi") > F.col("avg_single")).cast("int")), 1)
            .alias("pct_people_better_in_multi"),
        ),
        SECTION, "q8_per_person_gap",
    )
    write_result(
        per_person.join(names(spark), "nconst")
        .filter("n_multi_titles >= 5")
        .select("primaryName", "n_multi_titles",
                F.round("avg_multi", 2).alias("avg_multi"),
                F.round("avg_single", 2).alias("avg_single"))
        .orderBy(F.desc("n_multi_titles")),
        SECTION, "q8_top_multi_talented",
    )


QUESTIONS = {
    "q1": q1_runtime_outliers, "q2": q2_top3_genres, "q3": q3_top3_vs_rest,
    "q4": q4_one_hit_directors, "q5": q5_declining_series,
    "q6": q6_multilingual_actors, "q7": q7_early_peak_actors,
    "q8": q8_multi_talented,
}


def main(selected):
    spark = get_spark("imdb_exploration")
    for key in selected or QUESTIONS:
        print(f"\n######## {key}")
        QUESTIONS[key](spark)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
