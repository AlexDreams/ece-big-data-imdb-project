"""Percentage of null, missing or empty values for every column of every table.

A value counts as missing when it is null (IMDb '\\N'), an empty or blank
string. Results: output/preparation/missing_values.csv and top5_missing_columns.csv
Usage: python -m src.preparation.missing_values
"""
from pyspark.sql import functions as F

from src.common.io import read_bronze, write_result
from src.common.schemas import TABLE_COLUMNS
from src.common.spark import get_spark


def missing_profile(spark, table):
    df = read_bronze(spark, table)
    columns = TABLE_COLUMNS[table]
    # One pass over the table: count nulls and blanks for every column at once
    aggs = [F.count(F.lit(1)).alias("_rows")]
    for c in columns:
        aggs.append(F.sum(F.col(c).isNull().cast("long")).alias(f"{c}__null"))
        aggs.append(
            F.sum((F.trim(F.col(c)) == "").cast("long")).alias(f"{c}__empty")
        )
    row = df.agg(*aggs).first()
    total = row["_rows"]
    records = []
    for c in columns:
        nulls = row[f"{c}__null"] or 0
        empties = row[f"{c}__empty"] or 0
        records.append((
            table, c, total, nulls, empties,
            round(100.0 * (nulls + empties) / total, 3) if total else 0.0,
        ))
    return records


def run(spark):
    records = []
    for table in TABLE_COLUMNS:
        records += missing_profile(spark, table)
    result = spark.createDataFrame(
        records,
        "table string, column string, total_rows long, null_count long, "
        "empty_count long, missing_pct double",
    ).orderBy("table", "column")
    write_result(result, "preparation", "missing_values", show=60)
    top5 = result.orderBy(F.desc("missing_pct")).limit(5)
    write_result(top5, "preparation", "top5_missing_columns")


def main():
    spark = get_spark("imdb_missing_values")
    run(spark)
    spark.stop()


if __name__ == "__main__":
    main()
