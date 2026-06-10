from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from event_replay.generator import (
    DEFAULT_KAFKA_TOPIC,
    DEFAULT_SCHEMA_VERSION,
    iter_raw_events,
    parse_speed,
    raw_event_to_json,
    wait_for_replay_timing,
)


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required. Install dependencies with: pip install -r requirements.txt"
            ) from exc
        self._errors: list[str] = []
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "event-replay-generator",
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def _on_delivery(self, error, message) -> None:
        if error is not None:
            self._errors.append(str(error))

    def publish(self, *, topic: str, key: str, value: str) -> None:
        while True:
            try:
                self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
                self._producer.poll(0)
                return
            except BufferError:
                self._producer.poll(1.0)

    def flush(self) -> None:
        self._producer.flush()
        if self._errors:
            raise RuntimeError("; ".join(self._errors))


def replay_csv_to_kafka(
    *,
    csv_path: Path,
    bootstrap_servers: str,
    topic: str,
    replay_run_id: str,
    schema_version: str,
    replay_speed: float | None,
    limit: int | None,
) -> int:
    producer = KafkaEventProducer(bootstrap_servers)
    emitted_count = 0
    previous_event_time = None

    for event in iter_raw_events(
        csv_path,
        replay_run_id=replay_run_id,
        schema_version=schema_version,
        limit=limit,
    ):
        wait_for_replay_timing(previous_event_time, event.event_time, replay_speed)
        previous_event_time = event.event_time
        producer.publish(topic=topic, key=str(event.user_id), value=raw_event_to_json(event))
        emitted_count += 1

    producer.flush()
    return emitted_count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay Kaggle ecommerce CSV events into Kafka.")
    parser.add_argument("--csv", required=True, type=Path, help="Path to 2019-Oct.csv or a compatible CSV.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC))
    parser.add_argument("--speed", default="max")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--replay-run-id", default=f"kafka-replay-{uuid.uuid4()}")
    parser.add_argument("--schema-version", default=DEFAULT_SCHEMA_VERSION)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.csv.exists():
        parser.error(f"CSV file does not exist: {args.csv}")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive when provided")

    started_at = datetime.now(timezone.utc)
    emitted_count = replay_csv_to_kafka(
        csv_path=args.csv,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        replay_run_id=args.replay_run_id,
        schema_version=args.schema_version,
        replay_speed=parse_speed(args.speed),
        limit=args.limit,
    )
    print(f"replay_run_id={args.replay_run_id}")
    print(f"topic={args.topic}")
    print(f"emitted_count={emitted_count}")
    print(f"started_at={started_at.isoformat()}")
    print(f"completed_at={datetime.now(timezone.utc).isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
