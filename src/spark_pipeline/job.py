from __future__ import annotations

import argparse
import os
from pathlib import Path

from spark_pipeline.transforms import (
    bronze_to_silver,
    gold_conversion_funnel_hourly,
    gold_duplicate_event_summary,
    gold_order_volume_by_category,
    gold_order_volume_hourly,
    gold_user_purchase_burst_features,
    kafka_to_bronze,
)


DEFAULT_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_TOPIC = "ecommerce.raw-events"
DEFAULT_WAREHOUSE_DIR = "data/spark-warehouse"
DEFAULT_CHECKPOINT_DIR = "data/spark-checkpoints"
DEFAULT_WATERMARK = "30 minutes"
DEFAULT_SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"


def build_spark(*, app_name: str, spark_packages: str):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
    )
    if spark_packages:
        builder = builder.config("spark.jars.packages", spark_packages)
    return builder.getOrCreate()


def read_kafka_stream(spark, *, bootstrap_servers: str, topic: str, starting_offsets: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parquet_sink(
    df,
    *,
    output_path: Path,
    checkpoint_path: Path,
    output_mode: str,
    trigger: str,
    query_name: str,
    partition_by: list[str] | None = None,
):
    writer = (
        df.writeStream.format("parquet")
        .queryName(query_name)
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode(output_mode)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if trigger == "available-now":
        writer = writer.trigger(availableNow=True)
    elif trigger == "processing-time":
        writer = writer.trigger(processingTime="10 seconds")
    else:
        raise ValueError(f"Unsupported trigger: {trigger}")
    return writer.start()


def write_batch_parquet(df, *, output_path: Path, partition_by: list[str] | None = None) -> None:
    writer = df.write.format("parquet").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(output_path))


def materialize_gold_batch(*, spark, warehouse_dir: Path, watermark: str) -> None:
    silver_path = warehouse_dir / "silver" / "events"
    silver_df = spark.read.parquet(str(silver_path))

    write_batch_parquet(
        gold_order_volume_hourly(silver_df, watermark=watermark),
        output_path=warehouse_dir / "gold" / "order_volume_hourly",
        partition_by=["metric_date_kst"],
    )
    write_batch_parquet(
        gold_order_volume_by_category(silver_df, watermark=watermark),
        output_path=warehouse_dir / "gold" / "order_volume_by_category",
        partition_by=["metric_date_kst"],
    )
    write_batch_parquet(
        gold_conversion_funnel_hourly(silver_df, watermark=watermark),
        output_path=warehouse_dir / "gold" / "conversion_funnel_hourly",
        partition_by=["metric_date_kst"],
    )
    write_batch_parquet(
        gold_user_purchase_burst_features(silver_df, watermark=watermark),
        output_path=warehouse_dir / "gold" / "user_purchase_burst_features",
        partition_by=["metric_date_kst"],
    )
    bronze_df = spark.read.parquet(str(warehouse_dir / "bronze" / "raw_events"))
    write_batch_parquet(
        gold_duplicate_event_summary(bronze_df, watermark=watermark),
        output_path=warehouse_dir / "gold" / "duplicate_event_summary",
        partition_by=["metric_date_kst"],
    )


def run_pipeline(
    *,
    bootstrap_servers: str,
    topic: str,
    warehouse_dir: Path,
    checkpoint_dir: Path,
    watermark: str,
    starting_offsets: str,
    trigger: str,
    spark_packages: str,
) -> None:
    spark = build_spark(app_name="ecommerce-spark-pipeline", spark_packages=spark_packages)
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        starting_offsets=starting_offsets,
    )
    bronze_df = kafka_to_bronze(kafka_df)
    silver_df = bronze_to_silver(bronze_df, watermark=watermark)

    queries = [
        parquet_sink(
            bronze_df,
            output_path=warehouse_dir / "bronze" / "raw_events",
            checkpoint_path=checkpoint_dir / "bronze_raw_events",
            output_mode="append",
            trigger=trigger,
            query_name="bronze_raw_events",
            partition_by=["bronze_ingest_date_kst"],
        ),
        parquet_sink(
            silver_df,
            output_path=warehouse_dir / "silver" / "events",
            checkpoint_path=checkpoint_dir / "silver_events",
            output_mode="append",
            trigger=trigger,
            query_name="silver_events",
            partition_by=["event_date_kst"],
        ),
    ]

    try:
        for query in queries:
            query.awaitTermination()
        if trigger == "available-now":
            materialize_gold_batch(spark=spark, warehouse_dir=warehouse_dir, watermark=watermark)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Spark Structured Streaming Bronze/Silver/Gold pipeline.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--warehouse-dir", type=Path, default=Path(DEFAULT_WAREHOUSE_DIR))
    parser.add_argument("--checkpoint-dir", type=Path, default=Path(DEFAULT_CHECKPOINT_DIR))
    parser.add_argument("--watermark", default=DEFAULT_WATERMARK)
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--trigger", default="available-now", choices=["available-now", "processing-time"])
    parser.add_argument("--spark-packages", default=os.environ.get("SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_pipeline(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        warehouse_dir=args.warehouse_dir,
        checkpoint_dir=args.checkpoint_dir,
        watermark=args.watermark,
        starting_offsets=args.starting_offsets,
        trigger=args.trigger,
        spark_packages=args.spark_packages,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
