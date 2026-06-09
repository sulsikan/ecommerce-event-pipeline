from __future__ import annotations

import argparse
import csv
import hashlib
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator


KST = timezone(timedelta(hours=9))
DEFAULT_SCHEMA_VERSION = "raw-event-v1"
DEFAULT_DATABASE_URL = "postgresql://ecommerce:ecommerce@localhost:5432/ecommerce"
VALID_EVENT_TYPES = {"view", "cart", "purchase"}


@dataclass(frozen=True)
class RawEvent:
    replay_sequence: int
    event_id: str
    event_time: datetime
    event_time_kst: datetime
    event_date_kst: str
    event_hour_kst: int
    event_type: str
    product_id: int
    category_id: int | None
    category_code: str | None
    brand: str | None
    price: Decimal | None
    user_id: int
    user_session: str | None
    source_file: str
    source_row_number: int
    replay_run_id: str
    schema_version: str
    ingested_at: datetime


def parse_event_time(value: str) -> datetime:
    raw_value = value.strip()
    if raw_value.endswith(" UTC"):
        parsed = datetime.strptime(raw_value[:-4], "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=timezone.utc)

    parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def parse_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def parse_optional_decimal(value: str | None) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"invalid price: {value}") from exc


def build_event_id(
    *,
    schema_version: str,
    source_file: str,
    source_row_number: int,
    event_time: datetime,
    event_type: str,
    product_id: int,
    user_id: int,
    user_session: str | None,
    price: Decimal | None,
) -> str:
    parts = [
        schema_version,
        source_file,
        str(source_row_number),
        event_time.isoformat(),
        event_type,
        str(product_id),
        str(user_id),
        user_session or "",
        str(price) if price is not None else "",
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def raw_event_from_row(
    row: dict[str, str],
    *,
    replay_sequence: int,
    source_file: str,
    source_row_number: int,
    replay_run_id: str,
    schema_version: str,
) -> RawEvent:
    event_time = parse_event_time(row["event_time"])
    event_time_kst = event_time.astimezone(KST)
    event_type = row["event_type"].strip()
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"invalid event_type: {event_type}")

    product_id = int(row["product_id"])
    user_id = int(row["user_id"])
    user_session = parse_optional_text(row.get("user_session"))
    price = parse_optional_decimal(row.get("price"))
    event_id = build_event_id(
        schema_version=schema_version,
        source_file=source_file,
        source_row_number=source_row_number,
        event_time=event_time,
        event_type=event_type,
        product_id=product_id,
        user_id=user_id,
        user_session=user_session,
        price=price,
    )

    return RawEvent(
        replay_sequence=replay_sequence,
        event_id=event_id,
        event_time=event_time,
        event_time_kst=event_time_kst,
        event_date_kst=event_time_kst.date().isoformat(),
        event_hour_kst=event_time_kst.hour,
        event_type=event_type,
        product_id=product_id,
        category_id=parse_optional_int(row.get("category_id")),
        category_code=parse_optional_text(row.get("category_code")),
        brand=parse_optional_text(row.get("brand")),
        price=price,
        user_id=user_id,
        user_session=user_session,
        source_file=source_file,
        source_row_number=source_row_number,
        replay_run_id=replay_run_id,
        schema_version=schema_version,
        ingested_at=datetime.now(timezone.utc),
    )


def iter_raw_events(
    csv_path: Path,
    *,
    replay_run_id: str,
    schema_version: str,
    limit: int | None,
) -> Iterator[RawEvent]:
    source_file = csv_path.name
    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        required = {
            "event_time",
            "event_type",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "price",
            "user_id",
            "user_session",
        }
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(sorted(missing))}")

        emitted = 0
        previous_event_time: datetime | None = None
        for source_row_number, row in enumerate(reader, start=1):
            if limit is not None and emitted >= limit:
                break

            event = raw_event_from_row(
                row,
                replay_sequence=emitted + 1,
                source_file=source_file,
                source_row_number=source_row_number,
                replay_run_id=replay_run_id,
                schema_version=schema_version,
            )
            if previous_event_time and event.event_time < previous_event_time:
                raise ValueError(
                    "CSV is not ordered by event_time. "
                    f"row={source_row_number}, event_time={event.event_time.isoformat()}, "
                    f"previous_event_time={previous_event_time.isoformat()}"
                )
            previous_event_time = event.event_time
            emitted += 1
            yield event


def parse_speed(value: str) -> float | None:
    if value == "max":
        return None
    speed = float(value)
    if speed <= 0:
        raise argparse.ArgumentTypeError("--speed must be positive or 'max'")
    return speed


def wait_for_replay_timing(
    previous_event_time: datetime | None,
    current_event_time: datetime,
    replay_speed: float | None,
) -> None:
    if previous_event_time is None or replay_speed is None:
        return
    gap_seconds = max(0.0, (current_event_time - previous_event_time).total_seconds())
    delay_seconds = gap_seconds / replay_speed
    if delay_seconds > 0:
        time.sleep(delay_seconds)


class PostgresEventSink:
    def __init__(self, database_url: str) -> None:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "psycopg is required. Install dependencies with: pip install -r requirements.txt"
            ) from exc
        self._psycopg = psycopg
        self._connection = psycopg.connect(database_url)

    def close(self) -> None:
        self._connection.close()

    def create_replay_run(
        self,
        *,
        replay_run_id: str,
        source_file: str,
        schema_version: str,
        replay_speed: str,
        max_events: int | None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ecommerce.replay_runs (
                    replay_run_id,
                    source_file,
                    schema_version,
                    replay_speed,
                    max_events,
                    started_at,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'running')
                """,
                (
                    replay_run_id,
                    source_file,
                    schema_version,
                    replay_speed,
                    max_events,
                    datetime.now(timezone.utc),
                ),
            )
        self._connection.commit()

    def insert_event(self, event: RawEvent) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ecommerce.raw_events (
                    event_id,
                    replay_sequence,
                    event_time,
                    event_time_kst,
                    event_date_kst,
                    event_hour_kst,
                    event_type,
                    product_id,
                    category_id,
                    category_code,
                    brand,
                    price,
                    user_id,
                    user_session,
                    source_file,
                    source_row_number,
                    replay_run_id,
                    schema_version,
                    ingested_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    event.event_id,
                    event.replay_sequence,
                    event.event_time,
                    event.event_time_kst,
                    event.event_date_kst,
                    event.event_hour_kst,
                    event.event_type,
                    event.product_id,
                    event.category_id,
                    event.category_code,
                    event.brand,
                    event.price,
                    event.user_id,
                    event.user_session,
                    event.source_file,
                    event.source_row_number,
                    event.replay_run_id,
                    event.schema_version,
                    event.ingested_at,
                ),
            )

    def complete_replay_run(self, replay_run_id: str, emitted_count: int) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ecommerce.replay_runs
                SET completed_at = %s,
                    emitted_count = %s,
                    status = 'completed'
                WHERE replay_run_id = %s
                """,
                (datetime.now(timezone.utc), emitted_count, replay_run_id),
            )
        self._connection.commit()

    def fail_replay_run(self, replay_run_id: str, error_message: str, emitted_count: int) -> None:
        self._connection.rollback()
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ecommerce.replay_runs
                SET completed_at = %s,
                    emitted_count = %s,
                    status = 'failed',
                    error_message = %s
                WHERE replay_run_id = %s
                """,
                (datetime.now(timezone.utc), emitted_count, error_message, replay_run_id),
            )
        self._connection.commit()

    def commit(self) -> None:
        self._connection.commit()


def replay_events(
    *,
    csv_path: Path,
    database_url: str,
    replay_run_id: str,
    schema_version: str,
    speed_label: str,
    replay_speed: float | None,
    limit: int | None,
    commit_every: int,
) -> int:
    sink = PostgresEventSink(database_url)
    emitted_count = 0
    previous_event_time: datetime | None = None

    sink.create_replay_run(
        replay_run_id=replay_run_id,
        source_file=csv_path.name,
        schema_version=schema_version,
        replay_speed=speed_label,
        max_events=limit,
    )

    try:
        for event in iter_raw_events(
            csv_path,
            replay_run_id=replay_run_id,
            schema_version=schema_version,
            limit=limit,
        ):
            wait_for_replay_timing(previous_event_time, event.event_time, replay_speed)
            previous_event_time = event.event_time
            sink.insert_event(event)
            emitted_count += 1
            if emitted_count % commit_every == 0:
                sink.commit()

        sink.complete_replay_run(replay_run_id, emitted_count)
        return emitted_count
    except Exception as exc:
        sink.fail_replay_run(replay_run_id, str(exc), emitted_count)
        raise
    finally:
        sink.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay Kaggle ecommerce CSV events into PostgreSQL.")
    parser.add_argument("--csv", required=True, type=Path, help="Path to 2019-Oct.csv or a compatible CSV.")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        help="PostgreSQL connection URL.",
    )
    parser.add_argument("--speed", default="max", help="Replay speed multiplier, e.g. 1, 10, 100, 1000, or max.")
    parser.add_argument("--limit", type=int, help="Maximum number of events to replay.")
    parser.add_argument("--replay-run-id", default=f"replay-{uuid.uuid4()}")
    parser.add_argument("--schema-version", default=DEFAULT_SCHEMA_VERSION)
    parser.add_argument("--commit-every", type=int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.csv.exists():
        parser.error(f"CSV file does not exist: {args.csv}")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive when provided")
    if args.commit_every <= 0:
        parser.error("--commit-every must be positive")

    speed = parse_speed(args.speed)
    emitted_count = replay_events(
        csv_path=args.csv,
        database_url=args.database_url,
        replay_run_id=args.replay_run_id,
        schema_version=args.schema_version,
        speed_label=args.speed,
        replay_speed=speed,
        limit=args.limit,
        commit_every=args.commit_every,
    )
    print(f"replay_run_id={args.replay_run_id}")
    print(f"emitted_count={emitted_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
