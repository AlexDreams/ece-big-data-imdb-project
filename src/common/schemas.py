"""Explicit schemas of the IMDb non-commercial datasets.

Bronze keeps every column as a string so that no value is silently lost by a
failed cast; TYPED_COLUMNS lists the target types applied in the silver layer.
"""
from pyspark.sql.types import StringType, StructField, StructType

TABLE_COLUMNS = {
    "name.basics": [
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession", "knownForTitles",
    ],
    "title.akas": [
        "titleId", "ordering", "title", "region", "language",
        "types", "attributes", "isOriginalTitle",
    ],
    "title.basics": [
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title.crew": ["tconst", "directors", "writers"],
    "title.episode": ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
    "title.principals": [
        "tconst", "ordering", "nconst", "category", "job", "characters",
    ],
    "title.ratings": ["tconst", "averageRating", "numVotes"],
}

TYPED_COLUMNS = {
    "name.basics": {"birthYear": "int", "deathYear": "int"},
    "title.akas": {"ordering": "int", "isOriginalTitle": "int"},
    "title.basics": {
        "isAdult": "int", "startYear": "int",
        "endYear": "int", "runtimeMinutes": "int",
    },
    "title.crew": {},
    "title.episode": {"seasonNumber": "int", "episodeNumber": "int"},
    "title.principals": {"ordering": "int"},
    "title.ratings": {"averageRating": "double", "numVotes": "int"},
}


def raw_schema(table):
    """All-string schema used to read the raw TSV files."""
    return StructType(
        [StructField(c, StringType(), True) for c in TABLE_COLUMNS[table]]
    )


def table_dir_name(table):
    """'title.basics' -> 'title_basics' (dots are awkward in paths and SQL)."""
    return table.replace(".", "_")
