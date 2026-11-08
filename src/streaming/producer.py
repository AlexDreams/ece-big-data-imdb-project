"""Publish live Wikipedia change events to Kafka (runs on the host).

Reads the Wikimedia EventStreams 'recentchange' SSE feed, keeps article
(namespace 0) edits and page creations of the selected wikis, and publishes
them as JSON to the `wikistreams` topic, keyed by page title.
Usage: python -m src.streaming.producer --minutes 20 [--wikis enwiki frwiki ...]
"""
import argparse
import json
import socket
import time
from datetime import datetime, timedelta

import requests
from confluent_kafka import Producer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = (
    "ECE-BigData-IMDb-student-project/1.0 "
    "(https://github.com/AlexDreams/ece-big-data-imdb-project)"
)
TOPIC = "wikistreams"


def sse_events(last_event_id=None):
    """Yield (event_id, payload) from the SSE stream."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        event_id, data = None, []
        for raw in resp.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            if raw == "":
                if data:
                    yield event_id, "\n".join(data)
                event_id, data = None, []
            elif raw.startswith("id:"):
                event_id = raw[3:].strip()
            elif raw.startswith("data:"):
                data.append(raw[5:].strip())


def keep(event, wikis):
    return (
        event.get("wiki") in wikis
        and event.get("namespace") == 0
        and event.get("type") in ("edit", "new")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=20)
    parser.add_argument("--wikis", nargs="+",
                        default=["enwiki", "frwiki", "dewiki", "eswiki", "itwiki"])
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": socket.gethostname(),
        "compression.type": "lz4",
        "linger.ms": 50,
    })
    stop_at = datetime.now() + timedelta(minutes=args.minutes)
    last_id, sent, seen = None, 0, 0
    print(f"Streaming {args.wikis} until {stop_at:%H:%M:%S}")
    while datetime.now() < stop_at:
        try:
            for event_id, payload in sse_events(last_id):
                last_id = event_id or last_id
                if datetime.now() >= stop_at:
                    break
                try:
                    event = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                seen += 1
                if not keep(event, args.wikis):
                    continue
                while True:
                    try:
                        producer.produce(TOPIC, key=event["title"].encode("utf-8"),
                                         value=payload.encode("utf-8"))
                        break
                    except BufferError:
                        producer.poll(0.5)
                producer.poll(0)
                sent += 1
                if sent % 500 == 0:
                    print(f"  {sent:,} events sent ({seen:,} read)")
        except (requests.RequestException, ConnectionError) as exc:
            # the stream drops connections regularly: resume from the last id
            print(f"  stream interrupted ({exc}), reconnecting...")
            time.sleep(2)
    producer.flush()
    print(f"Done: {sent:,} events sent out of {seen:,} read")


if __name__ == "__main__":
    main()
