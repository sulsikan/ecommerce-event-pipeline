from __future__ import annotations

import argparse
import json
import os
import time

from event_replay.generator import (
    DEFAULT_DATABASE_URL,
    DEFAULT_KAFKA_TOPIC,
    PostgresEventSink,
    raw_event_from_payload,
)


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_GROUP_ID = "ecommerce-postgres-writer"


def consume_kafka_to_postgres(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    database_url: str,
    max_messages: int | None,
    replay_run_id: str | None,
    idle_timeout_seconds: float,
) -> int:
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError as exc:
        raise RuntimeError(
            "confluent-kafka is required. Install dependencies with: pip install -r requirements.txt"
        ) from exc

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    sink = PostgresEventSink(database_url)
    processed_count = 0
    last_seen_at = time.monotonic()
    consumer.subscribe([topic])

    try:
        while max_messages is None or processed_count < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_seen_at > idle_timeout_seconds:
                    raise TimeoutError(
                        f"No matching Kafka messages received for {idle_timeout_seconds} seconds."
                    )
                continue
            last_seen_at = time.monotonic()
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            payload = json.loads(message.value().decode("utf-8"))
            event = raw_event_from_payload(payload)
            if replay_run_id is not None and event.replay_run_id != replay_run_id:
                consumer.commit(message=message, asynchronous=False)
                continue

            sink.ensure_replay_run(
                replay_run_id=event.replay_run_id,
                source_file=event.source_file,
                schema_version=event.schema_version,
                replay_speed="kafka",
            )
            sink.insert_event(event)
            sink.commit()
            consumer.commit(message=message, asynchronous=False)
            processed_count += 1
    finally:
        consumer.close()
        sink.close()

    return processed_count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume Kafka raw events and store them in PostgreSQL.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC))
    parser.add_argument("--group-id", default=os.environ.get("KAFKA_GROUP_ID", DEFAULT_GROUP_ID))
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
    )
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--replay-run-id")
    parser.add_argument("--idle-timeout-seconds", type=float, default=30)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.max_messages is not None and args.max_messages <= 0:
        parser.error("--max-messages must be positive when provided")
    if args.idle_timeout_seconds <= 0:
        parser.error("--idle-timeout-seconds must be positive")

    processed_count = consume_kafka_to_postgres(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        database_url=args.database_url,
        max_messages=args.max_messages,
        replay_run_id=args.replay_run_id,
        idle_timeout_seconds=args.idle_timeout_seconds,
    )
    print(f"topic={args.topic}")
    print(f"group_id={args.group_id}")
    print(f"processed_count={processed_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
