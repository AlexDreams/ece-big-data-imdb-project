"""Original language of films, from Wikidata (property P364).

The IMDb datasets do not store the original language of a title: the
'original' aka has no language/region, and many countries keep the original
title, so akas cannot be used reliably. Wikidata links films to IMDb ids
(P345) and to their original language (P364, ISO 639 code via P218/P220).

Fetching runs outside Spark with polite rate limiting; Spark reads the JSONL
with an explicit schema (see load_languages).
Usage: python -m src.exploration.original_language   (ids from data/external/film_ids.csv)
"""
import csv
import json
import sys
import time

import requests
from pyspark.sql.types import StringType, StructField, StructType

from src.common.spark import DATA_DIR

EXTERNAL_DIR = DATA_DIR / "external"
IDS_FILE = EXTERNAL_DIR / "film_ids.csv"
OUT_FILE = EXTERNAL_DIR / "wikidata_film_languages.jsonl"
SPARQL_URL = "https://query.wikidata.org/sparql"
USER_AGENT = (
    "ECE-BigData-IMDb-student-project/1.0 "
    "(https://github.com/AlexDreams/ece-big-data-imdb-project)"
)
BATCH = 100
MIN_INTERVAL = 3.0

SCHEMA = StructType([
    StructField("imdb_id", StringType(), False),
    StructField("language", StringType()),
    StructField("language_label", StringType()),
    StructField("wikidata_url", StringType()),
])


def query(session, ids):
    values = " ".join(f'"{i}"' for i in ids)
    sparql = f"""
SELECT ?imdb ?item ?code ?label WHERE {{
  VALUES ?imdb {{ {values} }}
  ?item wdt:P345 ?imdb ; wdt:P364 ?lang .
  OPTIONAL {{ ?lang wdt:P218 ?iso1 . }}
  OPTIONAL {{ ?lang wdt:P220 ?iso3 . }}
  BIND(COALESCE(?iso1, ?iso3) AS ?code)
  OPTIONAL {{ ?lang rdfs:label ?label . FILTER(LANG(?label) = "en") }}
}}"""
    for attempt in range(8):
        try:
            resp = session.post(SPARQL_URL, data={"query": sparql, "format": "json"},
                                headers={"Accept": "application/sparql-results+json"},
                                timeout=90)
        except requests.RequestException as exc:
            delay = 2 ** (attempt + 2)
            print(f"  network error ({type(exc).__name__}), retry in {delay}s")
            time.sleep(delay)
            continue
        if resp.status_code == 200:
            return resp.json()["results"]["bindings"]
        delay = int(resp.headers.get("Retry-After", 2 ** (attempt + 2)))
        print(f"  HTTP {resp.status_code}, retry in {delay}s")
        time.sleep(delay)
    raise RuntimeError("Wikidata query failed repeatedly; rerun to resume")


def fetch(ids):
    done = set()
    if OUT_FILE.exists():
        done = {json.loads(line)["imdb_id"]
                for line in open(OUT_FILE, encoding="utf-8")}
    todo = [i for i in ids if i not in done]
    print(f"{len(todo):,} films to look up on Wikidata")
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    with open(OUT_FILE, "a", encoding="utf-8") as out:
        for start in range(0, len(todo), BATCH):
            batch = todo[start:start + BATCH]
            t0 = time.monotonic()
            found = set()
            for b in query(session, batch):
                if "code" not in b:
                    continue
                found.add(b["imdb"]["value"])
                out.write(json.dumps({
                    "imdb_id": b["imdb"]["value"],
                    "language": b["code"]["value"],
                    "language_label": b.get("label", {}).get("value"),
                    "wikidata_url": b["item"]["value"],
                }, ensure_ascii=False) + "\n")
            for missing in set(batch) - found:  # remember misses to resume cleanly
                out.write(json.dumps({"imdb_id": missing, "language": None,
                                      "language_label": None,
                                      "wikidata_url": None}) + "\n")
            out.flush()
            print(f"  {min(start + BATCH, len(todo)):,}/{len(todo):,}")
            time.sleep(max(0.0, MIN_INTERVAL - (time.monotonic() - t0)))


def load_languages(spark):
    """(tconst, language) for films with a known original language.

    A film with several original languages (co-productions) keeps one row
    per language.
    """
    return (
        spark.read.schema(SCHEMA).json(str(OUT_FILE))
        .filter("language IS NOT NULL")
        .selectExpr("imdb_id AS tconst", "language", "language_label")
        .dropDuplicates(["tconst", "language"])
    )


def main():
    with open(IDS_FILE, newline="", encoding="utf-8") as f:
        ids = sorted({row["tconst"] for row in csv.DictReader(f)})
    fetch(ids)


if __name__ == "__main__":
    sys.exit(main())
