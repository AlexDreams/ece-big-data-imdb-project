# ECE Big Data Processing — IMDb project

## Contributors

| Name | Email | GitHub username |
| ---- | ----- | --------------- |
| Alexandre Garreau | alexandre.garreau@edu.ece.fr | AlexDreams |
| Nicolas Donier | nicolas.donier@edu.ece.fr | reinoD15 |
| Quentin Varet | qt1dark@gmail.com | Caent1v |

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

```bash
docker exec imdb-spark python -m src.preparation.missing_values   # % null/missing per column, top 5
docker exec imdb-spark python -m src.preparation.quality_checks   # 31 data quality checks
docker exec imdb-spark python -m src.preparation.clean_to_silver  # handling rules -> data/silver/
```

Results: `output/preparation/*.csv`. The silver layer is required by every following step.

### 4. Data exploration

```bash
docker exec imdb-spark python -m src.exploration.questions         # all questions, or e.g. "q1 q6"
```

Results: `output/exploration/*.csv`. Question 6 looks up the original language of the successful films on
Wikidata (cached in `data/external/`, resumable). The notebook `notebooks/exploration.ipynb` runs the
preparation and exploration functions and displays their results (open it from JupyterLab, link given by
`docker logs imdb-spark 2>&1 | grep token=`).

### 5. Data enrichment

```bash
docker exec imdb-spark python -m src.enrichment.select_targets     # top 10 films per genre and year, 5 candidate principals each
docker exec imdb-spark python -m src.enrichment.fetch_wikipedia    # Wikidata + Wikipedia gathering (about 1h30, resumable)
docker exec imdb-spark python -m src.enrichment.build_enriched     # cleaning, schema enforcement, join -> data/gold/
```

`build_enriched` keeps three principals per film, those with a biography first. `fetch_wikipedia` accepts
`--limit N` for a quick test; if it is interrupted, run it again and it resumes where it stopped. Results:
`data/gold/enriched_films`, `data/gold/enriched_principals` and `output/enrichment/*.csv`.

### 6. Streaming

Host environment for the Kafka producer (once):

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-host.txt   # Linux/macOS: .venv/bin/python
```

Then, in three terminals:

```bash
# terminal 1: create the topic (add --reset to start from an empty topic)
.venv/Scripts/python -m src.streaming.admin
# terminal 2: Spark Structured Streaming job, runs for 20 minutes
docker exec imdb-spark python -m src.streaming.imdb_wiki_stream --minutes 20
# terminal 3: Wikimedia -> Kafka producer, started right after the Spark job
.venv/Scripts/python -m src.streaming.producer --minutes 20
```

Once both have finished:

```bash
docker exec imdb-spark python -m src.streaming.report
```

Results: `data/streaming/` (Parquet sinks and checkpoints) and `output/streaming/*.csv`.

Before a new run, remove the previous outputs and checkpoints and empty the topic, otherwise the job resumes
from the old checkpoints:

```bash
docker exec imdb-spark rm -rf /home/jovyan/project/data/streaming
.venv/Scripts/python -m src.streaming.admin --reset
```

If the Spark job is stopped with Ctrl+C, it can keep running inside the container; stop it with
`docker exec imdb-spark pkill -f imdb_wiki_stream`.

### 7. Open question

```bash
docker exec imdb-spark python -m src.open_question.director_actor_duos
```

Results: `output/open_question/*.csv`.

### Stop the environment

```bash
docker compose down
```

## AI / LLM usage declaration

**Tool used:** Claude Code (Anthropic, Claude Opus 5.5 model), as a coding assistant.

**How it was used:** We worked with Claude Code interactively by proposing our own architecture ideas and suggesting possible implementations. It helped us evaluate different approaches, make technical decisions, and improve our understanding of the code. It also assisted with writing and modifying code based on our requests. We ran the pipeline ourselves, checked the results, and reviewed each other's pull requests before merging.

**Tasks supported by the tool:**

- Assistance with Docker Compose environment setup and Spark configuration, including memory tuning for `title.principals`.
- Support in developing and improving the pipeline modules under `src/` and the notebook `notebooks/exploration.ipynb`.
- Help with debugging issues, including out-of-memory errors, Wikidata rate limiting, and original language lookup.

