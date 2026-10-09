"""
Data Provider (Kafka Producer) for T-Drive Taxi Dataset.

Reads all taxi trajectory files, merges them into a global chronological
stream, and replays them into a Kafka topic at a configurable speed.

Dataset format per line:
    taxiId, timestamp, longitude, latitude
    e.g.: 1,2008-02-02 15:36:08,116.51172,39.92123

Usage:
    python producer.py --data-dir ../../data/taxi_log_2008_by_id --speed 300 --topic taxi-raw-data
"""

import argparse
import csv
import json
import time
import logging
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def delivery_report(err, msg):
    """Callback invoked by Kafka once a message has been delivered (or failed)."""
    if err is not None:
        log.error("Message delivery failed: %s", err)


def parse_trajectory_file(filepath: Path):
    """
    Parse a single T-drive trajectory file.

    Returns a list of dicts:
        {"taxi_id": int, "timestamp": datetime, "longitude": float, "latitude": float}
    """
    records = []
    taxi_id = int(filepath.stem)   # filename without extension == taxi ID

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line_no, row in enumerate(reader, start=1):
            if len(row) < 4:
                log.warning("Skipping malformed row %d in %s", line_no, filepath.name)
                continue
            try:
                ts = datetime.strptime(row[1].strip(), "%Y-%m-%d %H:%M:%S")
                records.append({
                    "taxi_id":   taxi_id,
                    "timestamp": ts,
                    "longitude": float(row[2]),
                    "latitude":  float(row[3]),
                })
            except (ValueError, IndexError) as exc:
                log.warning("Skipping invalid row %d in %s: %s", line_no, filepath.name, exc)

    return records


def load_all_trajectories(data_dir: Path, max_files: int = None):
    """
    Load *.txt trajectory files in data_dir and merge them into a single
    list sorted by timestamp (global chronological order).

    :param max_files: If set, only load the first N files (useful for testing).
    """
    txt_files = sorted(data_dir.glob("*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in: {data_dir}")

    if max_files is not None:
        txt_files = txt_files[:max_files]
        log.info("Loading first %d of %d trajectory files from %s …", len(txt_files), len(sorted(data_dir.glob("*.txt"))), data_dir)
    else:
        log.info("Loading %d trajectory files from %s …", len(txt_files), data_dir)

    all_records = []
    for fp in txt_files:
        records = parse_trajectory_file(fp)
        log.info("  %-20s → %6d records", fp.name, len(records))
        all_records.extend(records)

    all_records.sort(key=lambda r: r["timestamp"])
    log.info("Total records loaded: %d", len(all_records))
    return all_records


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

def stream_to_kafka(records: list, producer: Producer, topic: str, speed: float):
    """
    Replay records to Kafka in chronological order at the requested speed
    multiplier.

    :param records: Sorted list of GPS records.
    :param producer: Confluent Kafka Producer instance.
    :param topic:    Kafka topic name.
    :param speed:    Replay speed multiplier (1 = real-time, 10 = 10× faster).
    """
    if not records:
        log.warning("No records to stream.")
        return

    first_event_ts  = records[0]["timestamp"]
    playback_start  = time.monotonic()

    sent = 0
    taxi_ids_seen = set()

    for record in records:
        taxi_id = record["taxi_id"]
        taxi_ids_seen.add(taxi_id)

        # ----------------------------------------------------------------
        # Time-alignment: wait until the right wall-clock moment
        # ----------------------------------------------------------------
        event_offset_s   = (record["timestamp"] - first_event_ts).total_seconds()
        target_wall_time = playback_start + (event_offset_s / speed)
        now              = time.monotonic()

        if target_wall_time > now:
            time.sleep(target_wall_time - now)

        # ----------------------------------------------------------------
        # Build and send the Kafka message
        # ----------------------------------------------------------------
        payload = {
            "taxi_id":   taxi_id,
            "timestamp": record["timestamp"].isoformat(),
            "longitude": record["longitude"],
            "latitude":  record["latitude"],
            "type":      "location",
        }

        producer.produce(
            topic,
            key=str(taxi_id),
            value=json.dumps(payload),
            callback=delivery_report,
        )
        producer.poll(0)   # trigger delivery callbacks without blocking

        sent += 1
        if sent % 1000 == 0:
            log.info("Sent %6d messages so far …", sent)

    # --------------------------------------------------------------------
    # Send an END token for every taxi that was streamed
    # --------------------------------------------------------------------
    log.info("Sending END tokens for %d taxis …", len(taxi_ids_seen))
    for taxi_id in taxi_ids_seen:
        end_payload = {
            "taxi_id": taxi_id,
            "type":    "END",
        }
        producer.produce(
            topic,
            key=str(taxi_id),
            value=json.dumps(end_payload),
            callback=delivery_report,
        )

    # Wait until all messages have been delivered
    log.info("Flushing remaining messages …")
    producer.flush()
    log.info("Done! Streamed %d location records + %d END tokens.", sent, len(taxi_ids_seen))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="T-Drive Kafka Producer – streams taxi GPS data into Kafka."
    )
    parser.add_argument(
        "--data-dir",
        default="../../data/taxi_log_2008_by_id",
        help="Directory containing T-drive .txt files (default: ../../data/taxi_log_2008_by_id)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="taxi-raw-data",
        help="Kafka topic name (default: taxi-raw-data)",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay speed multiplier: 1=real-time, 10=10× faster (default: 1.0)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Limit to the first N trajectory files (useful for testing, default: all files)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        raise SystemExit(f"ERROR: data directory not found: {data_dir}")

    # Load dataset
    records = load_all_trajectories(data_dir, max_files=args.max_files)

    # Create Kafka producer
    producer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "client.id":         "tdrive-data-provider",
        "linger.ms":         5,
        "batch.size":        65536,
    }
    producer = Producer(producer_conf)
    log.info(
        "Streaming %d records to topic '%s' at %.1f× speed …",
        len(records),
        args.topic,
        args.speed,
    )

    stream_to_kafka(records, producer, args.topic, args.speed)


if __name__ == "__main__":
    main()
