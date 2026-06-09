#!/usr/bin/env python3
"""Validate the pipeline harness file structure and required headings."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "AGENTS.md",
    "skills/data-pipeline-design.md",
    "skills/data-pipeline-design/SKILL.md",
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
    "## Purpose",
    "## When to Use",
    "## Required Inputs",
    "## Step-by-Step Procedure",
    "## Output Artifacts",
    "## Validation Checklist",
    "## Anti-Patterns",
    "## Example Prompt",
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
            failures.append(f"missing file: {relative}")

    skill_files = [
        "skills/data-pipeline-design.md",
        "skills/data-pipeline-design/SKILL.md",
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
                failures.append(f"{relative}: missing heading {heading}")

    agents_path = ROOT / "AGENTS.md"
    if agents_path.exists():
        agents_text = read_text(agents_path)
        for agent in AGENT_NAMES:
            if agent not in agents_text:
                failures.append(f"AGENTS.md: missing {agent}")
        for rule in ["main", "feature/*", "커밋을 진행할까요?", "main으로 merge할까요?"]:
            if rule not in agents_text:
                failures.append(f"AGENTS.md: missing Git rule token {rule}")

    if failures:
        print("Pipeline docs validation failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Pipeline docs validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

