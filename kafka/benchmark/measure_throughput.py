import argparse
import time
from collections import defaultdict
from uuid import uuid4

from confluent_kafka import Consumer


def print_interval_report(
    start_time,
    last_report_time,
    interval_count,
    interval_topic_counts,
    topics,
):
    now = time.time()
    interval_seconds = now - last_report_time

    if interval_seconds <= 0:
        return 0.0

    interval_throughput = interval_count / interval_seconds

    print("")
    print(f"Time elapsed: {now - start_time:.1f}s")
    print(f"Interval throughput: {interval_throughput:.2f} msg/s")
    print(f"Interval messages: {interval_count}")

    for topic_name in topics:
        topic_rate = interval_topic_counts[topic_name] / interval_seconds
        print(f"  {topic_name}: {topic_rate:.2f} msg/s")

    return interval_throughput


def main():
    parser = argparse.ArgumentParser(
        description="Measure Kafka output throughput for the taxi monitoring pipeline."
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers."
    )

    parser.add_argument(
        "--topics",
        nargs="+",
        default=["taxi-locations"],
        help="Kafka topics to measure."
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Benchmark duration in seconds."
    )

    parser.add_argument(
        "--report-interval",
        type=int,
        default=5,
        help="Print throughput every N seconds."
    )

    parser.add_argument(
        "--offset-reset",
        default="latest",
        choices=["latest", "earliest"],
        help="Start from latest messages or earliest messages."
    )

    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": f"throughput-benchmark-{uuid4()}",
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": False,
    })

    consumer.subscribe(args.topics)

    total_count = 0
    interval_count = 0
    topic_counts = defaultdict(int)
    interval_topic_counts = defaultdict(int)
    peak_interval_throughput = 0.0

    start_time = time.time()
    last_report_time = start_time

    print("Kafka throughput benchmark")
    print(f"Topics: {', '.join(args.topics)}")
    print(f"Duration: {args.duration} seconds")
    print(f"Offset reset: {args.offset_reset}")
    print("Waiting for messages...")

    try:
        while time.time() - start_time < args.duration:
            msg = consumer.poll(0.2)

            if msg is not None:
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                else:
                    topic = msg.topic()

                    total_count += 1
                    interval_count += 1
                    topic_counts[topic] += 1
                    interval_topic_counts[topic] += 1

            now = time.time()

            if now - last_report_time >= args.report_interval:
                interval_throughput = print_interval_report(
                    start_time,
                    last_report_time,
                    interval_count,
                    interval_topic_counts,
                    args.topics,
                )

                peak_interval_throughput = max(
                    peak_interval_throughput,
                    interval_throughput
                )

                interval_count = 0
                interval_topic_counts.clear()
                last_report_time = now

    finally:
        consumer.close()

    elapsed_seconds = time.time() - start_time
    average_throughput = total_count / elapsed_seconds if elapsed_seconds > 0 else 0.0

    print("")
    print("Final benchmark result")
    print(f"Duration seconds: {elapsed_seconds:.2f}")
    print(f"Total messages: {total_count}")
    print(f"Average throughput: {average_throughput:.2f} msg/s")
    print(f"Peak interval throughput: {peak_interval_throughput:.2f} msg/s")

    for topic_name in args.topics:
        print(f"{topic_name} total messages: {topic_counts[topic_name]}")


if __name__ == "__main__":
    main()