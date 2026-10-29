"""Download the IMDb non-commercial datasets into data/raw.

Files already present are skipped. Standard library only, so it runs on the
host or inside the spark container:
    python -m src.ingestion.download_imdb
"""
import shutil
import urllib.request
from pathlib import Path

BASE_URL = "https://datasets.imdbws.com"
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
TABLES = [
    "name.basics", "title.akas", "title.basics", "title.crew",
    "title.episode", "title.principals", "title.ratings",
]


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        target = RAW_DIR / f"{table}.tsv.gz"
        if target.exists() and target.stat().st_size > 0:
            print(f"skip {target.name} (already downloaded)")
            continue
        url = f"{BASE_URL}/{table}.tsv.gz"
        print(f"download {url}")
        tmp = target.with_suffix(".part")
        with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:
            shutil.copyfileobj(resp, out)
        tmp.rename(target)


if __name__ == "__main__":
    main()
