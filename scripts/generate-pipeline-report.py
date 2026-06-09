#!/usr/bin/env python3
"""Generate a lightweight Markdown report for the pipeline harness."""

from datetime import datetime, timezone
from pathlib import Path
import argparse
import sys


ROOT = Path(__file__).resolve().parents[1]

REPORT_FILES = [
    "AGENTS.md",
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
]


def count_checkboxes(text: str) -> tuple[int, int]:
    total = text.count("- [ ]") + text.count("- [x]") + text.count("- [X]")
    done = text.count("- [x]") + text.count("- [X]")
    return total, done


def build_report() -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    lines = [
        "# Pipeline Harness Report",
        "",
        f"- Generated at: `{generated_at}`",
        f"- Repository: `{ROOT.name}`",
        "",
        "## Artifact Inventory",
        "",
        "| Artifact | Status | Checklist Progress |",
        "| --- | --- | --- |",
    ]

    for relative in REPORT_FILES:
        path = ROOT / relative
        if not path.exists():
            lines.append(f"| `{relative}` | missing | n/a |")
            continue
        text = path.read_text(encoding="utf-8")
        total, done = count_checkboxes(text)
        progress = "n/a" if total == 0 else f"{done}/{total}"
        lines.append(f"| `{relative}` | present | {progress} |")

    lines.extend(
        [
            "",
            "## Required Validation Commands",
            "",
            "```bash",
            "python3 scripts/validate-schema.py",
            "python3 scripts/validate-data-quality.py",
            "python3 scripts/validate-pipeline-docs.py",
            "python3 scripts/generate-pipeline-report.py",
            "```",
            "",
            "## Commit Gate",
            "",
            "Do not commit until the user approves after `커밋을 진행할까요?`.",
            "Do not merge until the user approves after `main으로 merge할까요?`.",
        ]
    )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", help="Optional path to write the report.")
    args = parser.parse_args()

    report = build_report()
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = ROOT / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
        print(f"Wrote {output_path.relative_to(ROOT)}")
    else:
        sys.stdout.write(report)

    return 0


if __name__ == "__main__":
    sys.exit(main())

