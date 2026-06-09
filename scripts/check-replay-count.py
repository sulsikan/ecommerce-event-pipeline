#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from typing import Any


DEFAULT_DATABASE_URL = "postgresql://ecommerce:ecommerce@localhost:5432/ecommerce"


def connect(database_url: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is required. Install dependencies with: pip install -r requirements.txt"
        ) from exc
    return psycopg.connect(database_url)


def latest_replay_run_id(connection: Any) -> str:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT replay_run_id
            FROM ecommerce.replay_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError("No replay runs found.")
    return row[0]


def fetch_validation_summary(connection: Any, replay_run_id: str) -> dict[str, int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                count(*) AS total_count,
                count(DISTINCT event_id) AS distinct_event_ids,
                count(DISTINCT replay_run_id) AS replay_run_ids,
                count(*) FILTER (
                    WHERE event_id IS NULL
                       OR event_time IS NULL
                       OR event_time_kst IS NULL
                       OR event_date_kst IS NULL
                       OR event_hour_kst IS NULL
                       OR event_type IS NULL
                       OR product_id IS NULL
                       OR user_id IS NULL
                       OR source_file IS NULL
                       OR source_row_number IS NULL
                       OR replay_run_id IS NULL
                       OR schema_version IS NULL
                ) AS required_null_count,
                count(*) FILTER (WHERE event_type = 'view') AS view_count,
                count(*) FILTER (WHERE event_type = 'cart') AS cart_count,
                count(*) FILTER (WHERE event_type = 'purchase') AS purchase_count,
                count(*) FILTER (
                    WHERE event_date_kst <> (event_time_kst AT TIME ZONE 'Asia/Seoul')::date
                       OR event_hour_kst <> EXTRACT(HOUR FROM event_time_kst AT TIME ZONE 'Asia/Seoul')::int
                ) AS kst_mismatch_count,
                count(*) FILTER (
                    WHERE replay_sequence IS NULL OR replay_sequence <= 0
                ) AS invalid_sequence_count
            FROM ecommerce.raw_events
            WHERE replay_run_id = %s
            """,
            (replay_run_id,),
        )
        row = cursor.fetchone()
    labels = [
        "total_count",
        "distinct_event_ids",
        "replay_run_ids",
        "required_null_count",
        "view_count",
        "cart_count",
        "purchase_count",
        "kst_mismatch_count",
        "invalid_sequence_count",
    ]
    return {label: int(value) for label, value in zip(labels, row)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check replayed raw event count in PostgreSQL.")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        help="PostgreSQL connection URL.",
    )
    parser.add_argument("--replay-run-id", help="Replay run id. Defaults to the latest replay run.")
    parser.add_argument("--expected", type=int, help="Expected raw event count.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    connection = connect(args.database_url)
    try:
        replay_run_id = args.replay_run_id or latest_replay_run_id(connection)
        summary = fetch_validation_summary(connection, replay_run_id)
        actual = summary["total_count"]
    finally:
        connection.close()

    print(f"replay_run_id={replay_run_id}")
    for key, value in summary.items():
        print(f"{key}={value}")

    if args.expected is not None and actual != args.expected:
        print(f"expected_count={args.expected}")
        return 1
    if summary["required_null_count"] != 0:
        return 1
    if summary["distinct_event_ids"] != actual:
        return 1
    if summary["view_count"] + summary["cart_count"] + summary["purchase_count"] != actual:
        return 1
    if summary["kst_mismatch_count"] != 0:
        return 1
    if summary["invalid_sequence_count"] != 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
