#!/usr/bin/env python3
"""스키마 하네스 문서가 필수 계약을 포함하는지 검증한다."""

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
    "## 원천 CSV 스키마",
    "## 표준 이벤트 스키마",
    "## Primary Key 후보",
    "## Partition Key",
    "## 스키마 진화",
    "## 계층별 스키마",
]


def main() -> int:
    if not SCHEMA_GUIDE.exists():
        print(f"실패: {SCHEMA_GUIDE.relative_to(ROOT)} 파일이 없습니다.")
        return 1

    text = SCHEMA_GUIDE.read_text(encoding="utf-8")
    failures = []

    for section in REQUIRED_SECTIONS:
        if section not in text:
            failures.append(f"섹션 누락: {section}")

    for field in REQUIRED_FIELDS:
        if not re.search(rf"`{re.escape(field)}`", text):
            failures.append(f"필드 계약 누락: {field}")

    for token in ["user_id", "event_date_kst", "event_id", "KST", "UTC"]:
        if token not in text:
            failures.append(f"필수 스키마 개념 누락: {token}")

    if failures:
        print("스키마 검증 실패:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("스키마 검증 통과.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
