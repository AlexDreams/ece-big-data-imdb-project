# ece-big-data-imdb-project

Big Data Processing project (ECE ING5, Fall 2026): IMDb data preparation, exploration, Wikipedia enrichment and Kafka + Spark Structured Streaming pipeline with PySpark.

## Group

- Group: gr-04 (Data & IA)
- Project member 1: Alexandre Garreau, AlexDreams
- Project member 2: Nicolas Donier, reinoD
- Project member 3: Quentin Varet, Caent1

## Documentation

- [PROJECT.md](PROJECT.md): contributors, execution and operations guide, AI usage declaration
- [REPORT.md](REPORT.md): results and methodology

## Repository layout

```text
compose.yaml            Kafka (KRaft) + Spark/Jupyter containers
src/common/             SparkSession factory, paths, IMDb schemas
src/ingestion/          download of the IMDb datasets, TSV -> bronze Parquet
data/                   raw, bronze, silver, gold layers (not versioned)
output/                 query results referenced in REPORT.md
```
