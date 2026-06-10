from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_WAREHOUSE_DIR = "data/spark-warehouse"
DEFAULT_OUTPUT_DIR = "data/quality-monitoring"


@dataclass(frozen=True)
class RuleResult:
    rule_id: str
    severity: str
    failed_stage: str
    metric_name: str
    failure_count: int
    total_count: int
    status: str
    detected_at: str


def build_spark(app_name: str = "ecommerce-data-quality"):
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC").getOrCreate()


def has_parquet(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.parquet"))


def read_required_parquet(spark, path: Path, name: str):
    if not has_parquet(path):
        raise FileNotFoundError(f"{name} parquet output does not exist: {path}")
    return spark.read.parquet(str(path))


def status_for(severity: str, failure_count: int) -> str:
    if failure_count == 0:
        return "pass"
    if severity in {"reject", "quarantine"}:
        return "fail"
    return "warn"


def add_rule(
    results: list[RuleResult],
    *,
    rule_id: str,
    severity: str,
    failed_stage: str,
    metric_name: str,
    failure_count: int,
    total_count: int,
    detected_at: str,
) -> None:
    results.append(
        RuleResult(
            rule_id=rule_id,
            severity=severity,
            failed_stage=failed_stage,
            metric_name=metric_name,
            failure_count=int(failure_count),
            total_count=int(total_count),
            status=status_for(severity, int(failure_count)),
            detected_at=detected_at,
        )
    )


def write_rule_results(spark, results: list[RuleResult], output_dir: Path) -> None:
    rows = [result.__dict__ for result in results]
    spark.createDataFrame(rows).coalesce(1).write.mode("overwrite").parquet(str(output_dir / "data_quality" / "rule_results"))


def write_quarantine_events(bronze_df, output_dir: Path) -> None:
    from pyspark.sql import functions as F

    parse_failures = (
        bronze_df.filter(F.col("parse_status") != "parsed")
        .withColumn("rule_id", F.coalesce(F.col("parse_error_code"), F.lit("DQ_SCHEMA_REQUIRED")))
        .withColumn("severity", F.lit("reject"))
        .withColumn("failed_stage", F.lit("bronze"))
        .withColumn("failure_reason", F.coalesce(F.col("parse_error_message"), F.lit("schema validation failed")))
    )
    negative_prices = (
        bronze_df.filter((F.col("parse_status") == "parsed") & (F.col("price") < F.lit(0)))
        .withColumn("rule_id", F.lit("DQ_PRICE_NEGATIVE"))
        .withColumn("severity", F.lit("quarantine"))
        .withColumn("failed_stage", F.lit("silver"))
        .withColumn("failure_reason", F.lit("price must be greater than or equal to 0"))
    )
    quarantine_df = parse_failures.unionByName(negative_prices, allowMissingColumns=True).select(
        "rule_id",
        "severity",
        "failed_stage",
        "event_id",
        "event_time",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "raw_payload",
        "failure_reason",
        F.current_timestamp().alias("detected_at"),
    )
    quarantine_df.write.mode("overwrite").parquet(str(output_dir / "data_quality" / "quarantine_events"))


def write_metric_events(spark, *, warehouse_dir: Path, output_dir: Path, results: list[RuleResult]) -> None:
    from pyspark.sql import functions as F

    bronze_df = read_required_parquet(spark, warehouse_dir / "bronze" / "raw_events", "Bronze")
    silver_df = read_required_parquet(spark, warehouse_dir / "silver" / "events", "Silver")

    metric_rows = [
        {
            "metric_name": "pipeline_events_in_total",
            "metric_type": "counter",
            "stage": "bronze",
            "label_name": "all",
            "label_value": "all",
            "metric_value": float(bronze_df.count()),
        },
        {
            "metric_name": "pipeline_events_out_total",
            "metric_type": "counter",
            "stage": "silver",
            "label_name": "all",
            "label_value": "all",
            "metric_value": float(silver_df.count()),
        },
    ]
    bronze_count = metric_rows[0]["metric_value"]

    for result in results:
        metric_rows.append(
            {
                "metric_name": result.metric_name,
                "metric_type": "counter",
                "stage": result.failed_stage,
                "label_name": "rule_id",
                "label_value": result.rule_id,
                "metric_value": float(result.failure_count),
            }
        )

    duplicate_failures = next(
        (result.failure_count for result in results if result.rule_id == "DQ_DUP_EVENT_ID"),
        0,
    )
    metric_rows.append(
        {
            "metric_name": "dq_duplicate_event_rate",
            "metric_type": "gauge",
            "stage": "silver",
            "label_name": "rule_id",
            "label_value": "DQ_DUP_EVENT_ID",
            "metric_value": float(duplicate_failures) / float(bronze_count) if bronze_count else 0.0,
        }
    )

    order_volume_path = warehouse_dir / "gold" / "order_volume_hourly"
    if has_parquet(order_volume_path):
        order_df = spark.read.parquet(str(order_volume_path))
        purchase_count = order_df.agg(F.coalesce(F.sum("order_count"), F.lit(0)).alias("value")).collect()[0]["value"]
        metric_rows.append(
            {
                "metric_name": "business_purchase_count",
                "metric_type": "counter",
                "stage": "gold",
                "label_name": "metric",
                "label_value": "order_volume_hourly",
                "metric_value": float(purchase_count or 0),
            }
        )

    funnel_path = warehouse_dir / "gold" / "conversion_funnel_hourly"
    if has_parquet(funnel_path):
        funnel_df = spark.read.parquet(str(funnel_path))
        conversion = funnel_df.agg(F.avg("purchase_conversion_rate").alias("value")).collect()[0]["value"]
        metric_rows.append(
            {
                "metric_name": "business_conversion_rate",
                "metric_type": "gauge",
                "stage": "gold",
                "label_name": "funnel_step",
                "label_value": "view_to_purchase",
                "metric_value": float(conversion or 0),
            }
        )

    duplicate_path = warehouse_dir / "gold" / "duplicate_event_summary"
    if has_parquet(duplicate_path):
        duplicate_df = spark.read.parquet(str(duplicate_path))
        duplicate_rate = duplicate_df.agg(F.max("duplicate_event_rate").alias("value")).collect()[0]["value"]
        metric_rows.append(
            {
                "metric_name": "dq_duplicate_event_rate",
                "metric_type": "gauge",
                "stage": "gold",
                "label_name": "window",
                "label_value": "5m",
                "metric_value": float(duplicate_rate or 0),
            }
        )

    metric_df = spark.createDataFrame(metric_rows).withColumn("observed_at", F.current_timestamp())
    metric_df.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "monitoring" / "metric_events"))


def run_quality_checks(*, warehouse_dir: Path, output_dir: Path) -> None:
    from pyspark.sql import functions as F

    spark = build_spark()
    try:
        bronze_df = read_required_parquet(spark, warehouse_dir / "bronze" / "raw_events", "Bronze")
        silver_df = read_required_parquet(spark, warehouse_dir / "silver" / "events", "Silver")
        detected_at = datetime.now(timezone.utc).isoformat()
        total_bronze = bronze_df.count()
        total_silver = silver_df.count()

        results: list[RuleResult] = []
        add_rule(
            results,
            rule_id="DQ_SCHEMA_REQUIRED",
            severity="reject",
            failed_stage="bronze",
            metric_name="dq_schema_required_failures_total",
            failure_count=bronze_df.filter(F.col("parse_status") != "parsed").count(),
            total_count=total_bronze,
            detected_at=detected_at,
        )
        add_rule(
            results,
            rule_id="DQ_EVENT_TYPE_ENUM",
            severity="reject",
            failed_stage="bronze",
            metric_name="dq_event_type_invalid_total",
            failure_count=bronze_df.filter(F.col("parse_error_code") == "invalid_event_type").count(),
            total_count=total_bronze,
            detected_at=detected_at,
        )
        add_rule(
            results,
            rule_id="DQ_NULL_EVENT_TIME",
            severity="reject",
            failed_stage="bronze",
            metric_name="dq_null_event_time_total",
            failure_count=bronze_df.filter(F.col("event_time").isNull()).count(),
            total_count=total_bronze,
            detected_at=detected_at,
        )
        add_rule(
            results,
            rule_id="DQ_NULL_USER_ID",
            severity="reject",
            failed_stage="bronze",
            metric_name="dq_null_user_id_total",
            failure_count=bronze_df.filter(F.col("user_id").isNull()).count(),
            total_count=total_bronze,
            detected_at=detected_at,
        )
        add_rule(
            results,
            rule_id="DQ_NULL_PRODUCT_ID",
            severity="reject",
            failed_stage="bronze",
            metric_name="dq_null_product_id_total",
            failure_count=bronze_df.filter(F.col("product_id").isNull()).count(),
            total_count=total_bronze,
            detected_at=detected_at,
        )
        add_rule(
            results,
            rule_id="DQ_NULL_CATEGORY_CODE",
            severity="observe",
            failed_stage="silver",
            metric_name="dq_null_category_code_total",
            failure_count=silver_df.filter(F.col("category_code").isNull()).count(),
            total_count=total_silver,
            detected_at=detected_at,
        )
        duplicate_count = (
            bronze_df.filter(F.col("event_id").isNotNull())
            .groupBy("event_id")
            .agg(F.count("*").alias("event_count"))
            .filter(F.col("event_count") > 1)
            .agg(F.coalesce(F.sum(F.col("event_count") - F.lit(1)), F.lit(0)).alias("duplicate_count"))
            .collect()[0]["duplicate_count"]
        )
        add_rule(
            results,
            rule_id="DQ_DUP_EVENT_ID",
            severity="warn",
            failed_stage="silver",
            metric_name="dq_duplicate_event_id_total",
            failure_count=duplicate_count,
            total_count=total_bronze,
            detected_at=detected_at,
        )
        add_rule(
            results,
            rule_id="DQ_PRICE_NEGATIVE",
            severity="quarantine",
            failed_stage="silver",
            metric_name="dq_price_negative_total",
            failure_count=bronze_df.filter((F.col("parse_status") == "parsed") & (F.col("price") < F.lit(0))).count(),
            total_count=total_bronze,
            detected_at=detected_at,
        )
        add_rule(
            results,
            rule_id="DQ_PURCHASE_PRICE_NULL",
            severity="warn",
            failed_stage="silver",
            metric_name="dq_purchase_price_null_total",
            failure_count=silver_df.filter((F.col("event_type") == "purchase") & F.col("price").isNull()).count(),
            total_count=total_silver,
            detected_at=detected_at,
        )

        funnel_path = warehouse_dir / "gold" / "conversion_funnel_hourly"
        if has_parquet(funnel_path):
            funnel_df = spark.read.parquet(str(funnel_path))
            funnel_total = funnel_df.count()
            denominator_zero = funnel_df.filter(F.col("unique_view_users") == 0).count()
            conversion_out_of_range = funnel_df.filter(
                (F.col("cart_conversion_rate") < 0)
                | (F.col("cart_conversion_rate") > 1)
                | (F.col("purchase_conversion_rate") < 0)
                | (F.col("purchase_conversion_rate") > 1)
                | (F.col("cart_to_purchase_conversion_rate") < 0)
                | (F.col("cart_to_purchase_conversion_rate") > 1)
            ).count()
            add_rule(
                results,
                rule_id="DQ_CONVERSION_DENOMINATOR_ZERO",
                severity="observe",
                failed_stage="gold",
                metric_name="dq_conversion_denominator_zero_total",
                failure_count=denominator_zero,
                total_count=funnel_total,
                detected_at=detected_at,
            )
            add_rule(
                results,
                rule_id="DQ_CONVERSION_RATE_RANGE",
                severity="quarantine",
                failed_stage="gold",
                metric_name="dq_conversion_rate_range_total",
                failure_count=conversion_out_of_range,
                total_count=funnel_total,
                detected_at=detected_at,
            )

        write_rule_results(spark, results, output_dir)
        write_quarantine_events(bronze_df, output_dir)
        write_metric_events(spark, warehouse_dir=warehouse_dir, output_dir=output_dir, results=results)
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 5 data quality checks.")
    parser.add_argument("--warehouse-dir", type=Path, default=Path(DEFAULT_WAREHOUSE_DIR))
    parser.add_argument("--output-dir", type=Path, default=Path(DEFAULT_OUTPUT_DIR))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_quality_checks(warehouse_dir=args.warehouse_dir, output_dir=args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
