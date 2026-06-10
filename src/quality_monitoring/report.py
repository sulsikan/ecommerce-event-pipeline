from __future__ import annotations

import argparse
import json
from pathlib import Path

from quality_monitoring.quality import build_spark, has_parquet


DEFAULT_OUTPUT_DIR = "data/quality-monitoring"
DEFAULT_REPORT_PATH = "data/quality-monitoring/monitoring/phase5-report.json"
DEFAULT_PROMETHEUS_PATH = "data/quality-monitoring/monitoring/prometheus-metrics.prom"


def read_rows(spark, path: Path) -> list[dict[str, object]]:
    if not has_parquet(path):
        return []
    return [row.asDict(recursive=True) for row in spark.read.parquet(str(path)).collect()]


def evaluate_alerts(rule_rows: list[dict[str, object]], metric_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    metric_by_name: dict[str, float] = {}
    for row in metric_rows:
        metric_name = str(row["metric_name"])
        metric_by_name[metric_name] = max(metric_by_name.get(metric_name, 0.0), float(row["metric_value"]))
    reject_failures = sum(
        int(row["failure_count"]) for row in rule_rows if row["severity"] == "reject" and int(row["failure_count"]) > 0
    )
    quarantine_failures = sum(
        int(row["failure_count"])
        for row in rule_rows
        if row["severity"] == "quarantine" and int(row["failure_count"]) > 0
    )
    duplicate_rate = metric_by_name.get("dq_duplicate_event_rate", 0.0)
    purchase_count = metric_by_name.get("business_purchase_count", 0.0)

    return [
        {
            "alert": "DataQualityRejectFailures",
            "severity": "critical",
            "firing": reject_failures > 0,
            "observed_value": reject_failures,
            "runbook": "docs/data-pipeline/data-quality-rules.md",
        },
        {
            "alert": "QuarantineGrowthHigh",
            "severity": "warning",
            "firing": quarantine_failures > 0,
            "observed_value": quarantine_failures,
            "runbook": "docs/data-pipeline/data-quality-rules.md",
        },
        {
            "alert": "DuplicateRateHigh",
            "severity": "warning",
            "firing": duplicate_rate > 0.01,
            "observed_value": duplicate_rate,
            "runbook": "docs/data-pipeline/monitoring-guide.md",
        },
        {
            "alert": "PurchaseSpikeDetected",
            "severity": "warning",
            "firing": purchase_count >= 1000,
            "observed_value": purchase_count,
            "runbook": "docs/data-pipeline/monitoring-guide.md",
        },
    ]


def prometheus_lines(metric_rows: list[dict[str, object]]) -> list[str]:
    lines: list[str] = []
    for row in metric_rows:
        metric_name = str(row["metric_name"])
        metric_type = str(row["metric_type"])
        stage = str(row["stage"])
        label_name = str(row["label_name"])
        label_value = str(row["label_value"])
        value = float(row["metric_value"])
        lines.append(f"# TYPE {metric_name} {metric_type}")
        lines.append(f'{metric_name}{{stage="{stage}",{label_name}="{label_value}"}} {value}')
    return lines


def generate_report(*, output_dir: Path, report_path: Path, prometheus_path: Path) -> None:
    spark = build_spark(app_name="ecommerce-monitoring-report")
    try:
        rule_rows = read_rows(spark, output_dir / "data_quality" / "rule_results")
        quarantine_rows = read_rows(spark, output_dir / "data_quality" / "quarantine_events")
        metric_rows = read_rows(spark, output_dir / "monitoring" / "metric_events")
    finally:
        spark.stop()

    report = {
        "summary": {
            "rule_count": len(rule_rows),
            "failed_rule_count": sum(1 for row in rule_rows if int(row["failure_count"]) > 0),
            "quarantine_event_count": len(quarantine_rows),
            "metric_count": len(metric_rows),
        },
        "rule_results": rule_rows,
        "alerts": evaluate_alerts(rule_rows, metric_rows),
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    prometheus_path.parent.mkdir(parents=True, exist_ok=True)
    prometheus_path.write_text("\n".join(prometheus_lines(metric_rows)) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Phase 5 monitoring report.")
    parser.add_argument("--output-dir", type=Path, default=Path(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--report-path", type=Path, default=Path(DEFAULT_REPORT_PATH))
    parser.add_argument("--prometheus-path", type=Path, default=Path(DEFAULT_PROMETHEUS_PATH))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    generate_report(output_dir=args.output_dir, report_path=args.report_path, prometheus_path=args.prometheus_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
