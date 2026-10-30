"""Bronze tables with their target types applied.

Columns that fail the cast become null; `<col>__raw` keeps the original string
so that quality checks can tell 'missing' from 'not a number'.
"""
from pyspark.sql import functions as F

from src.common.io import read_bronze
from src.common.schemas import TYPED_COLUMNS

LIST_COLUMNS = {
    "name.basics": ["primaryProfession", "knownForTitles"],
    "title.basics": ["genres"],
    "title.crew": ["directors", "writers"],
    "title.akas": ["types", "attributes"],
}


def typed(spark, table, keep_raw=False):
    df = read_bronze(spark, table).drop("_ingested_at", "_source_file")
    for c, t in TYPED_COLUMNS[table].items():
        if keep_raw:
            df = df.withColumn(f"{c}__raw", F.col(c))
        df = df.withColumn(c, F.col(c).try_cast(t))
    # Normalise blank strings to null
    for c, t in df.dtypes:
        if t == "string" and not c.endswith("__raw"):
            df = df.withColumn(
                c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c))
            )
    return df


def split_list(col_name, sep=","):
    """'a,b,c' -> ['a','b','c'], null -> empty array."""
    return F.coalesce(
        F.split(F.col(col_name), sep), F.array().cast("array<string>")
    )
