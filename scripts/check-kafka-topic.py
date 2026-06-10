#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "ecommerce.raw-events"


def topic_message_count(bootstrap_servers: str, topic: str) -> tuple[int, int]:
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as exc:
        raise RuntimeError(
            "confluent-kafka is required. Install dependencies with: pip install -r requirements.txt"
        ) from exc

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "kafka-topic-count-checker",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Topic not found or unhealthy: {topic}")

        total = 0
        partitions = 0
        for partition_id in topic_metadata.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition_id),
                timeout=10,
            )
            total += high - low
            partitions += 1
        return partitions, total
    finally:
        consumer.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check Kafka topic partition and message count.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--expected", type=int)
    parser.add_argument("--min-expected", type=int)
    parser.add_argument("--expected-partitions", type=int)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    partitions, count = topic_message_count(args.bootstrap_servers, args.topic)
    print(f"topic={args.topic}")
    print(f"partitions={partitions}")
    print(f"message_count={count}")

    if args.expected_partitions is not None and partitions != args.expected_partitions:
        print(f"expected_partitions={args.expected_partitions}")
        return 1
    if args.expected is not None and count != args.expected:
        print(f"expected_count={args.expected}")
        return 1
    if args.min_expected is not None and count < args.min_expected:
        print(f"min_expected_count={args.min_expected}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
