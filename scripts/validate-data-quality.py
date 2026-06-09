#!/usr/bin/env python3
"""Validate that data quality documentation covers required rule families."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
QUALITY_DOC = ROOT / "docs" / "data-pipeline" / "data-quality-rules.md"

REQUIRED_TERMS = [
    "DQ_SCHEMA_REQUIRED",
    "DQ_SCHEMA_TYPE",
    "DQ_EVENT_TYPE_ENUM",
    "DQ_NULL_EVENT_TIME",
    "DQ_NULL_USER_ID",
    "DQ_DUP_EVENT_ID",
    "DQ_PRICE_NEGATIVE",
    "DQ_EVENT_TOO_LATE",
    "DQ_SESSION_USER_CONSISTENCY",
    "Cart conversion rate",
    "Purchase conversion rate",
    "DLQ",
    "quarantine",
    "dq_duplicate_event_id_total",
]

REQUIRED_SECTIONS = [
    "## Severity Levels",
    "## Schema Rules",
    "## Null Rules",
    "## Duplicate Rules",
    "## Range Rules",
    "## Referential Integrity Rules",
    "## Conversion Metric Checks",
    "## Failure Handling",
]


def main() -> int:
    if not QUALITY_DOC.exists():
        print(f"FAIL: missing {QUALITY_DOC.relative_to(ROOT)}")
        return 1

    text = QUALITY_DOC.read_text(encoding="utf-8")
    failures = []

    for section in REQUIRED_SECTIONS:
        if section not in text:
            failures.append(f"missing section: {section}")

    for term in REQUIRED_TERMS:
        if term not in text:
            failures.append(f"missing required quality contract: {term}")

    if failures:
        print("Data quality validation failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Data quality validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

