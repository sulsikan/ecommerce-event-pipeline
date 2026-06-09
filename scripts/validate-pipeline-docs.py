#!/usr/bin/env python3
"""파이프라인 하네스 파일 구조와 필수 제목을 검증한다."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "AGENTS.md",
    "skills/data-pipeline-design.md",
    "skills/schema-design.md",
    "skills/event-replay.md",
    "skills/kafka-streaming.md",
    "skills/spark-processing.md",
    "skills/data-quality.md",
    "skills/monitoring.md",
    "skills/review.md",
    "docs/data-pipeline/architecture.md",
    "docs/data-pipeline/roadmap.md",
    "docs/data-pipeline/schema-guide.md",
    "docs/data-pipeline/event-replay-guide.md",
    "docs/data-pipeline/kafka-guide.md",
    "docs/data-pipeline/spark-guide.md",
    "docs/data-pipeline/data-quality-rules.md",
    "docs/data-pipeline/monitoring-guide.md",
    "docs/data-pipeline/review-checklist.md",
    "exec-plans/templates/data-pipeline-exec-plan.md",
    "scripts/validate-schema.py",
    "scripts/validate-data-quality.py",
    "scripts/validate-pipeline-docs.py",
    "scripts/generate-pipeline-report.py",
]

SKILL_HEADINGS = [
    "## 목적",
    "## 사용 시점",
    "## 필수 입력",
    "## 절차",
    "## 산출물",
    "## 검증 체크리스트",
    "## 안티패턴",
    "## 예시 프롬프트",
]

AGENT_NAMES = [
    "Schema Designer Agent",
    "Event Replay Agent",
    "Kafka Streaming Agent",
    "Spark Processing Agent",
    "Data Quality Agent",
    "Monitoring Agent",
    "Review Agent",
]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> int:
    failures = []

    for relative in REQUIRED_FILES:
        path = ROOT / relative
        if not path.exists():
            failures.append(f"파일 누락: {relative}")

    skill_files = [
    "skills/data-pipeline-design.md",
    "skills/schema-design.md",
        "skills/event-replay.md",
        "skills/kafka-streaming.md",
        "skills/spark-processing.md",
        "skills/data-quality.md",
        "skills/monitoring.md",
        "skills/review.md",
    ]

    for relative in skill_files:
        path = ROOT / relative
        if not path.exists():
            continue
        text = read_text(path)
        for heading in SKILL_HEADINGS:
            if heading not in text:
                failures.append(f"{relative}: 제목 누락 {heading}")

    agents_path = ROOT / "AGENTS.md"
    if agents_path.exists():
        agents_text = read_text(agents_path)
        for agent in AGENT_NAMES:
            if agent not in agents_text:
                failures.append(f"AGENTS.md: 에이전트 누락 {agent}")
        for rule in ["main", "develop", "feature/*", "커밋을 진행할까요?"]:
            if rule not in agents_text:
                failures.append(f"AGENTS.md: Git 규칙 토큰 누락 {rule}")

    if failures:
        print("파이프라인 문서 검증 실패:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("파이프라인 문서 검증 통과.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
