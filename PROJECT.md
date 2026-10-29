# ECE Big Data Processing — IMDb project

## Contributors

| Name | Email | GitHub username |
| ---- | ----- | --------------- |
| Alexandre Garreau | alexandre.garreau@edu.ece.fr | AlexDreams |
| Nicolas Donier | nicolas.donier@edu.ece.fr | reinoD |
| Quentin Varet | TODO | Caent1 |

Group: gr-04 (Data & IA)

## Execution and operations guide

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ on the host (only for the Kafka producer of the streaming part)
- About 15 GB of free disk space and 8 GB of RAM allocated to Docker

### 1. Start the environment

```bash
docker compose up -d
docker logs imdb-spark 2>&1 | grep "token="   # JupyterLab link (optional)
```

The `imdb-spark` container mounts the repository at `/home/jovyan/project`. All Spark jobs run inside it.

### 2. Download and convert the IMDb datasets

```bash
docker exec imdb-spark python -m src.ingestion.download_imdb
docker exec imdb-spark python -m src.ingestion.convert_to_parquet
```

Raw files land in `data/raw/`, bronze Parquet tables in `data/bronze/<table>/`. The `data/` folder is not versioned.

### 3. Data preparation

TODO

### 4. Data exploration

TODO

### 5. Data enrichment

TODO

### 6. Streaming

TODO

### 7. Open question

TODO

### Stop the environment

```bash
docker compose down
```

## AI / LLM usage declaration

TODO — to be completed precisely before submission (tool used and tasks that relied on it).
