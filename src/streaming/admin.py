"""Create (or recreate with --reset) the Kafka topic used by the pipeline.

Usage: python -m src.streaming.admin [--reset]
"""
import argparse
import time

from confluent_kafka.admin import AdminClient, NewTopic

TOPIC = "wikistreams"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    if args.reset and TOPIC in admin.list_topics(timeout=10).topics:
        admin.delete_topics([TOPIC])[TOPIC].result()
        time.sleep(3)
    if TOPIC not in admin.list_topics(timeout=10).topics:
        topic = NewTopic(TOPIC, num_partitions=args.partitions, replication_factor=1)
        admin.create_topics([topic])[TOPIC].result()
    print("topics:", sorted(admin.list_topics(timeout=10).topics))


if __name__ == "__main__":
    main()
