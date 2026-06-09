#!/usr/bin/env python3
"""Validate that the schema harness documents contain required contracts."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_GUIDE = ROOT / "docs" / "data-pipeline" / "schema-guide.md"

REQUIRED_FIELDS = [
    "event_id",
    "event_time",
    "event_time_kst",
    "ingested_at",
    "event_date_kst",
    "event_hour_kst",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    "schema_version",
]

REQUIRED_SECTIONS = [
    "## Source CSV Schema",
    "## Canonical Event Schema",
    "## Primary Key Candidates",
    "## Partition Keys",
    "## Schema Evolution",
    "## Layer Schemas",
]


def main() -> int:
    if not SCHEMA_GUIDE.exists():
        print(f"FAIL: missing {SCHEMA_GUIDE.relative_to(ROOT)}")
        return 1

    text = SCHEMA_GUIDE.read_text(encoding="utf-8")
    failures = []

    for section in REQUIRED_SECTIONS:
        if section not in text:
            failures.append(f"missing section: {section}")

    for field in REQUIRED_FIELDS:
        if not re.search(rf"`{re.escape(field)}`", text):
            failures.append(f"missing field contract: {field}")

    for token in ["user_id", "event_date_kst", "event_id", "KST", "UTC"]:
        if token not in text:
            failures.append(f"missing required schema concept: {token}")

    if failures:
        print("Schema validation failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Schema validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

