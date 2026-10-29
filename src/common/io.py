"""Helpers to read the medallion layers and export small results."""
from src.common.schemas import table_dir_name
from src.common.spark import BRONZE_DIR, GOLD_DIR, OUTPUT_DIR, SILVER_DIR

# Every saved result, as pandas, keyed by "<section>/<name>" (used by notebooks)
RESULTS = {}
VERBOSE = True


def read_bronze(spark, table):
    return spark.read.parquet(str(BRONZE_DIR / table_dir_name(table)))


def read_silver(spark, table):
    return spark.read.parquet(str(SILVER_DIR / table_dir_name(table)))


def read_gold(spark, name):
    return spark.read.parquet(str(GOLD_DIR / name))


def write_result(df, section, name, show=20):
    """Save a (small) result as CSV under output/<section>/ and print it.

    Only call this on aggregated results: the data is collected to the driver.
    """
    target = OUTPUT_DIR / section
    target.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(target / f"{name}.csv", index=False)
    RESULTS[f"{section}/{name}"] = pdf
    if VERBOSE:
        print(f"\n== {section}/{name} ({len(pdf)} rows) ==")
        print(pdf.head(show).to_string(index=False))
    return pdf
