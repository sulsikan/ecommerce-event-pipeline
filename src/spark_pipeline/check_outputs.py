from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_WAREHOUSE_DIR = "data/spark-warehouse"


def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("ecommerce-spark-output-check")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def count_parquet(spark, path: Path) -> int:
    if not path.exists():
        return 0
    if not list(path.rglob("*.parquet")):
        return 0
    return spark.read.parquet(str(path)).count()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check Spark Bronze/Silver/Gold parquet output counts.")
    parser.add_argument("--warehouse-dir", type=Path, default=Path(DEFAULT_WAREHOUSE_DIR))
    parser.add_argument("--expected-bronze", type=int)
    parser.add_argument("--expected-silver", type=int)
    parser.add_argument("--min-gold-order-volume", type=int, default=1)
    parser.add_argument("--min-gold-funnel", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spark = build_spark()
    try:
        counts = {
            "bronze_raw_events": count_parquet(spark, args.warehouse_dir / "bronze" / "raw_events"),
            "silver_events": count_parquet(spark, args.warehouse_dir / "silver" / "events"),
            "gold_order_volume_hourly": count_parquet(
                spark, args.warehouse_dir / "gold" / "order_volume_hourly"
            ),
            "gold_conversion_funnel_hourly": count_parquet(
                spark, args.warehouse_dir / "gold" / "conversion_funnel_hourly"
            ),
        }
    finally:
        spark.stop()

    for name, count in counts.items():
        print(f"{name}={count}")

    if args.expected_bronze is not None and counts["bronze_raw_events"] != args.expected_bronze:
        print(f"expected_bronze={args.expected_bronze}")
        return 1
    if args.expected_silver is not None and counts["silver_events"] != args.expected_silver:
        print(f"expected_silver={args.expected_silver}")
        return 1
    if counts["gold_order_volume_hourly"] < args.min_gold_order_volume:
        print(f"min_gold_order_volume={args.min_gold_order_volume}")
        return 1
    if counts["gold_conversion_funnel_hourly"] < args.min_gold_funnel:
        print(f"min_gold_funnel={args.min_gold_funnel}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
