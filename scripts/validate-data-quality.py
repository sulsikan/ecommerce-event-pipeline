#!/usr/bin/env python3
"""데이터 품질 문서가 필수 규칙군을 포함하는지 검증한다."""

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
    "장바구니 전환율",
    "구매 전환율",
    "DLQ",
    "quarantine",
    "dq_duplicate_event_id_total",
]

REQUIRED_SECTIONS = [
    "## Severity 수준",
    "## 스키마 규칙",
    "## Null 규칙",
    "## 중복 규칙",
    "## 범위 규칙",
    "## 참조 무결성 규칙",
    "## 전환율 지표 검증",
    "## 실패 처리",
]


def main() -> int:
    if not QUALITY_DOC.exists():
        print(f"실패: {QUALITY_DOC.relative_to(ROOT)} 파일이 없습니다.")
        return 1

    text = QUALITY_DOC.read_text(encoding="utf-8")
    failures = []

    for section in REQUIRED_SECTIONS:
        if section not in text:
            failures.append(f"섹션 누락: {section}")

    for term in REQUIRED_TERMS:
        if term not in text:
            failures.append(f"필수 품질 계약 누락: {term}")

    if failures:
        print("데이터 품질 검증 실패:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("데이터 품질 검증 통과.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
