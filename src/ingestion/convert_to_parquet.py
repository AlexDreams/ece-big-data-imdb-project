"""Convert the raw IMDb TSV.GZ files into bronze Parquet tables.

IMDb files are tab separated, use '\\N' for missing values and contain
unescaped double quotes inside titles, so CSV quoting is disabled.
Usage (inside the spark container):
    python -m src.ingestion.convert_to_parquet
"""
from pyspark.sql import functions as F

from src.common.schemas import TABLE_COLUMNS, raw_schema, table_dir_name
from src.common.spark import BRONZE_DIR, RAW_DIR, get_spark


def convert(spark, table):
    source = RAW_DIR / f"{table}.tsv.gz"
    target = BRONZE_DIR / table_dir_name(table)
    df = (
        spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("quote", "")
        .option("nullValue", "\\N")
        .option("mode", "PERMISSIVE")
        .schema(raw_schema(table))
        .csv(str(source))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source.name))
    )
    # .gz is not splittable: repartition so downstream jobs run in parallel
    df.repartition(16).write.mode("overwrite").parquet(str(target))
    count = spark.read.parquet(str(target)).count()
    print(f"{table:<18} -> {target}  ({count:,} rows)")


def main():
    spark = get_spark("imdb_convert_to_parquet")
    for table in TABLE_COLUMNS:
        convert(spark, table)
    spark.stop()


if __name__ == "__main__":
    main()
