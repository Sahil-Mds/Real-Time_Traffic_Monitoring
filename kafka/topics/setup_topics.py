"""
Topic Setup Script

Creates all required Kafka topics for the Real-Time Traffic Monitoring pipeline.
Run this once before starting the producer or Flink jobs.

Usage:
    python setup_topics.py [--bootstrap-servers localhost:9092]
"""

import argparse
import logging
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Topic definitions
# ---------------------------------------------------------------------------
TOPICS = [
    NewTopic("taxi-raw-data",      num_partitions=3, replication_factor=1),
    NewTopic("taxi-locations",     num_partitions=3, replication_factor=1),
    NewTopic("violation-events",   num_partitions=3, replication_factor=1),
    NewTopic("fleet-metrics",      num_partitions=1, replication_factor=1),
    NewTopic("state-updates",      num_partitions=3, replication_factor=1),
]


def create_topics(bootstrap_servers: str):
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [t for t in TOPICS if t.topic not in existing]

    if not to_create:
        log.info("All topics already exist — nothing to create.")
        return

    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            log.info("Created topic: %s", topic)
        except Exception as exc:
            log.error("Failed to create topic %s: %s", topic, exc)


def main():
    parser = argparse.ArgumentParser(description="Create Kafka topics for the taxi pipeline.")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    args = parser.parse_args()
    create_topics(args.bootstrap_servers)


if __name__ == "__main__":
    main()
