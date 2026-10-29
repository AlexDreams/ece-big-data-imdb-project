# ece-big-data-imdb-project

Big Data Processing project (ECE ING5, Fall 2026): IMDb data preparation, exploration, Wikipedia enrichment and Kafka + Spark Structured Streaming pipeline with PySpark.

## Group

- Group: gr-04 (Data & IA)
- Project member 1: Alexandre Garreau, AlexDreams
- Project member 2: Nicolas Donier, reinoD15
- Project member 3: Quentin Varet, Caent1v

## Documentation

- [PROJECT.md](PROJECT.md): contributors, execution and operations guide, AI usage declaration
- [REPORT.md](REPORT.md): results and methodology

## Repository layout

```text
compose.yaml            Kafka (KRaft) + Spark/Jupyter containers
requirements-host.txt   host dependencies of the Kafka admin and producer
src/common/             SparkSession factory, paths, IMDb schemas, result export
src/ingestion/          download of the IMDb datasets, TSV -> bronze Parquet
src/preparation/        missing values, data quality checks, bronze -> silver
src/exploration/        exploration questions q1 to q8
src/enrichment/         Wikidata/Wikipedia synopses and biographies -> gold
src/streaming/          Wikimedia -> Kafka producer, Structured Streaming job, report
src/open_question/      recurring director-actor duos
notebooks/              exploration notebook (preparation and exploration results)
data/                   raw, bronze, silver, gold layers (not versioned)
output/                 query results referenced in REPORT.md
```
