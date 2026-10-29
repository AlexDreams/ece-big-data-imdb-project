"""Gather human-authored film synopses and biographies from Wikidata/Wikipedia.

Data gathering runs outside Spark (allowed by the instructions); all cleaning
and schema enforcement happen in build_enriched.py.

1. Wikidata SPARQL maps each IMDb id (property P345) to its Wikidata item and
   English Wikipedia article. People also get structured facts (dates, places).
2. Wikipedia API: plot section of each film article, lead section of each
   person article.

Politeness: one request at a time, a minimum delay per host, a descriptive
User-Agent, `maxlag` on the MediaWiki API, and back-off on 429/5xx honouring
Retry-After. Results are appended to JSONL files and already fetched ids are
skipped, so the job can be stopped and resumed.
Usage: python -m src.enrichment.fetch_wikipedia [--limit N]
"""
import argparse
import csv
import json
import re
import time
from datetime import datetime, timezone

import requests

from src.common.spark import DATA_DIR

TARGET_DIR = DATA_DIR / "enrichment" / "targets"
RAW_DIR = DATA_DIR / "enrichment" / "raw"
USER_AGENT = (
    "ECE-BigData-IMDb-student-project/1.0 "
    "(https://github.com/AlexDreams/ece-big-data-imdb-project)"
)
SPARQL_URL = "https://query.wikidata.org/sparql"
WIKI_API = "https://en.wikipedia.org/w/api.php"
SPARQL_BATCH = 100
BIO_BATCH = 20                      # MediaWiki limit for intro extracts
MIN_INTERVAL = {"sparql": 3.0, "wiki": 0.5}  # seconds between requests
PLOT_HEADINGS = ("Plot", "Synopsis", "Plot summary", "Premise", "Story")
LICENSE = {"wikidata": "CC0 1.0", "wikipedia": "CC BY-SA 4.0"}


class PoliteClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.last_call = {}

    def request(self, kind, method, url, retries=6, **kwargs):
        for attempt in range(retries):
            wait = MIN_INTERVAL[kind] - (time.monotonic() - self.last_call.get(kind, 0))
            if wait > 0:
                time.sleep(wait)
            self.last_call[kind] = time.monotonic()
            try:
                resp = self.session.request(method, url, timeout=90, **kwargs)
            except requests.RequestException as exc:
                delay = 2 ** (attempt + 2)
                print(f"  {kind} network error ({type(exc).__name__}), retry in {delay}s")
                time.sleep(delay)
                continue
            if resp.status_code == 200:
                data = resp.json()
                if "error" in data and data["error"].get("code") == "maxlag":
                    time.sleep(int(resp.headers.get("Retry-After", 5)))
                    continue
                return data
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = int(resp.headers.get("Retry-After", 2 ** (attempt + 1)))
                print(f"  {kind} HTTP {resp.status_code}, retry in {delay}s")
                time.sleep(delay)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"{kind} request failed after {retries} attempts: {url}")


def now():
    return datetime.now(timezone.utc).isoformat()


def read_ids(path, column):
    with open(path, newline="", encoding="utf-8") as f:
        return sorted({row[column] for row in csv.DictReader(f)})


def done_ids(path):
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        return {json.loads(line)["imdb_id"] for line in f if line.strip()}


def append(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ---------------------------------------------------------------- Wikidata
PERSON_FIELDS = """
  OPTIONAL { ?item wdt:P569 ?birth_date . }
  OPTIONAL { ?item wdt:P570 ?death_date . }
  OPTIONAL { ?item wdt:P19 ?bp . ?bp rdfs:label ?birth_place . FILTER(LANG(?birth_place) = "en") }
  OPTIONAL { ?item wdt:P27 ?ct . ?ct rdfs:label ?citizenship . FILTER(LANG(?citizenship) = "en") }
"""


def resolve_wikidata(client, ids, people):
    values = " ".join(f'"{i}"' for i in ids)
    query = f"""
SELECT ?imdb ?item ?article ?birth_date ?death_date ?birth_place ?citizenship WHERE {{
  VALUES ?imdb {{ {values} }}
  ?item wdt:P345 ?imdb .
  OPTIONAL {{ ?article schema:about ?item ;
                       schema:isPartOf <https://en.wikipedia.org/> . }}
  {PERSON_FIELDS if people else ""}
}}"""
    data = client.request(
        "sparql", "POST", SPARQL_URL,
        data={"query": query, "format": "json"},
        headers={"Accept": "application/sparql-results+json"},
    )
    found = {}
    for b in data["results"]["bindings"]:
        imdb = b["imdb"]["value"]
        rec = found.setdefault(imdb, {
            "imdb_id": imdb,
            "wikidata_url": b["item"]["value"],
            "wikipedia_url": b.get("article", {}).get("value"),
            "birth_date": None, "death_date": None,
            "birth_place": None, "citizenship": set(),
        })
        if not rec["wikipedia_url"] and "article" in b:
            rec["wikipedia_url"] = b["article"]["value"]
        for key in ("birth_date", "death_date", "birth_place"):
            if key in b and not rec[key]:
                rec[key] = b[key]["value"]
        if "citizenship" in b:
            rec["citizenship"].add(b["citizenship"]["value"])
    records = []
    for imdb in ids:
        rec = found.get(imdb)
        if rec is None:
            rec = {"imdb_id": imdb, "wikidata_url": None, "wikipedia_url": None,
                   "birth_date": None, "death_date": None, "birth_place": None,
                   "citizenship": set(), "note": "no Wikidata item with this IMDb id"}
        rec["citizenship"] = sorted(rec["citizenship"])
        rec.setdefault("note", None if rec["wikipedia_url"]
                       else "no English Wikipedia article linked in Wikidata")
        rec["retrieved_at"] = now()
        if not people:
            for key in ("birth_date", "death_date", "birth_place", "citizenship"):
                rec.pop(key)
        records.append(rec)
    return records


def resolve_all(client, kind, ids, limit):
    out = RAW_DIR / f"wikidata_{kind}.jsonl"
    todo = [i for i in ids if i not in done_ids(out)][:limit]
    print(f"wikidata {kind}: {len(todo):,} ids to resolve")
    for n, batch in enumerate(chunks(todo, SPARQL_BATCH), 1):
        append(out, resolve_wikidata(client, batch, kind == "people"))
        print(f"  batch {n}: {min(n * SPARQL_BATCH, len(todo)):,}/{len(todo):,}")
    return out


# ---------------------------------------------------------------- Wikipedia
def page_title(url):
    return requests.utils.unquote(url.rsplit("/wiki/", 1)[1]).replace("_", " ")


def extract_section(text, headings):
    """Return the body of the first level-2 section whose title matches."""
    parts = re.split(r"^==\s*([^=].*?)\s*==\s*$", text, flags=re.M)
    # parts = [lead, title1, body1, title2, body2, ...]
    for title, body in zip(parts[1::2], parts[2::2]):
        if title.strip() in headings:
            body = re.split(r"^==\s*[^=]", body, maxsplit=1, flags=re.M)[0]
            return body.strip(), title.strip()
    return None, None


def query_pages(client, titles, intro):
    params = {
        "action": "query", "format": "json", "formatversion": 2,
        "prop": "extracts|info", "inprop": "url", "explaintext": 1,
        "exsectionformat": "wiki", "redirects": 1, "maxlag": 5,
        "titles": "|".join(titles),
    }
    if intro:
        params.update({"exintro": 1, "exlimit": "max"})
    data = client.request("wiki", "GET", WIKI_API, params=params)
    query = data.get("query", {})
    alias = {t: t for t in titles}
    for step in query.get("normalized", []) + query.get("redirects", []):
        for original, target in alias.items():
            if target == step["from"]:
                alias[original] = step["to"]
    pages = {p["title"]: p for p in query.get("pages", [])}
    return {t: pages.get(alias[t]) for t in titles}


def wiki_record(imdb_id, wikidata, page):
    return {
        "imdb_id": imdb_id,
        "wikidata_url": wikidata["wikidata_url"],
        "source_url": page.get("fullurl") if page else wikidata["wikipedia_url"],
        "source_revision_id": page.get("lastrevid") if page else None,
        "source_license": LICENSE["wikipedia"],
        "retrieved_at": now(),
    }


def fetch_films(client, limit):
    resolved = [json.loads(l) for l in open(RAW_DIR / "wikidata_films.jsonl", encoding="utf-8")]
    out = RAW_DIR / "wikipedia_films.jsonl"
    seen = done_ids(out)
    todo = [r for r in resolved if r["wikipedia_url"] and r["imdb_id"] not in seen][:limit]
    print(f"wikipedia films: {len(todo):,} articles to fetch")
    for n, wd in enumerate(todo, 1):
        title = page_title(wd["wikipedia_url"])
        page = query_pages(client, [title], intro=False)[title]
        rec = wiki_record(wd["imdb_id"], wd, page)
        text = (page or {}).get("extract") or ""
        rec["synopsis"], rec["synopsis_section"] = extract_section(text, PLOT_HEADINGS)
        rec["note"] = None if rec["synopsis"] else (
            "article not found" if not page or page.get("missing")
            else "no plot/synopsis section in the article")
        append(out, [rec])
        if n % 100 == 0:
            print(f"  {n:,}/{len(todo):,}")


def fetch_people(client, limit):
    resolved = [json.loads(l) for l in open(RAW_DIR / "wikidata_people.jsonl", encoding="utf-8")]
    out = RAW_DIR / "wikipedia_people.jsonl"
    seen = done_ids(out)
    todo = [r for r in resolved if r["wikipedia_url"] and r["imdb_id"] not in seen][:limit]
    print(f"wikipedia people: {len(todo):,} articles to fetch")
    for n, batch in enumerate(chunks(todo, BIO_BATCH), 1):
        titles = [page_title(wd["wikipedia_url"]) for wd in batch]
        pages = query_pages(client, titles, intro=True)
        records = []
        for wd, title in zip(batch, titles):
            page = pages.get(title)
            rec = wiki_record(wd["imdb_id"], wd, page)
            rec["biography"] = (page or {}).get("extract") or None
            rec["note"] = None if rec["biography"] else "empty lead section"
            records.append(rec)
        append(out, records)
        if n % 20 == 0:
            print(f"  {min(n * BIO_BATCH, len(todo)):,}/{len(todo):,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="max ids per step (for a quick test run)")
    args = parser.parse_args()
    client = PoliteClient()
    films = read_ids(TARGET_DIR / "films.csv", "tconst")
    people = read_ids(TARGET_DIR / "credits.csv", "nconst")
    resolve_all(client, "films", films, args.limit)
    resolve_all(client, "people", people, args.limit)
    fetch_films(client, args.limit)
    fetch_people(client, args.limit)


if __name__ == "__main__":
    main()
