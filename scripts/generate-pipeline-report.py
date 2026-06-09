#!/usr/bin/env python3
"""파이프라인 하네스의 가벼운 Markdown 리포트를 생성한다."""

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
        "# 파이프라인 하네스 리포트",
        "",
        f"- 생성 시각: `{generated_at}`",
        f"- 리포지터리: `{ROOT.name}`",
        "",
        "## 산출물 목록",
        "",
        "| 산출물 | 상태 | 체크리스트 진행률 |",
        "| --- | --- | --- |",
    ]

    for relative in REPORT_FILES:
        path = ROOT / relative
        if not path.exists():
            lines.append(f"| `{relative}` | 없음 | 해당 없음 |")
            continue
        text = path.read_text(encoding="utf-8")
        total, done = count_checkboxes(text)
        progress = "해당 없음" if total == 0 else f"{done}/{total}"
        lines.append(f"| `{relative}` | 있음 | {progress} |")

    lines.extend(
        [
            "",
            "## 필수 검증 명령",
            "",
            "```bash",
            "python3 scripts/validate-schema.py",
            "python3 scripts/validate-data-quality.py",
            "python3 scripts/validate-pipeline-docs.py",
            "python3 scripts/generate-pipeline-report.py",
            "```",
            "",
            "## 커밋 게이트",
            "",
            "`커밋을 진행할까요?` 이후 사용자 승인을 받기 전까지 커밋하지 않는다.",
            "대상 브랜치를 명시하고 사용자 승인을 받기 전까지 머지하지 않는다.",
        ]
    )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", help="리포트를 저장할 선택 경로")
    args = parser.parse_args()

    report = build_report()
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = ROOT / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
        print(f"{output_path.relative_to(ROOT)} 파일을 작성했습니다.")
    else:
        sys.stdout.write(report)

    return 0


if __name__ == "__main__":
    sys.exit(main())
