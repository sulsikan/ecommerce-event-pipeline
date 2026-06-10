from __future__ import annotations

from spark_pipeline.schemas import raw_event_json_schema


KST_TIMEZONE = "Asia/Seoul"
VALID_EVENT_TYPES = ["view", "cart", "purchase"]


def kafka_to_bronze(kafka_df):
    from pyspark.sql import functions as F

    parsed = F.from_json(F.col("value").cast("string"), raw_event_json_schema())
    event_time = F.to_timestamp(parsed["event_time"])
    event_time_kst = F.to_timestamp(parsed["event_time_kst"])
    ingested_at = F.current_timestamp()

    required_for_parse = [
        parsed["event_id"],
        event_time,
        parsed["event_type"],
        parsed["product_id"],
        parsed["user_id"],
        parsed["source_file"],
        parsed["source_row_number"],
        parsed["replay_run_id"],
        parsed["schema_version"],
    ]
    parsed_ok = required_for_parse[0].isNotNull()
    for column in required_for_parse[1:]:
        parsed_ok = parsed_ok & column.isNotNull()

    parse_error_code = (
        F.when(parsed["event_id"].isNull(), F.lit("invalid_json_or_missing_event_id"))
        .when(event_time.isNull(), F.lit("invalid_event_time"))
        .when(~parsed["event_type"].isin(VALID_EVENT_TYPES), F.lit("invalid_event_type"))
        .when(parsed["product_id"].isNull(), F.lit("missing_product_id"))
        .when(parsed["user_id"].isNull(), F.lit("missing_user_id"))
        .when(parsed["source_file"].isNull(), F.lit("missing_source_file"))
        .when(parsed["source_row_number"].isNull(), F.lit("missing_source_row_number"))
        .when(parsed["replay_run_id"].isNull(), F.lit("missing_replay_run_id"))
        .when(parsed["schema_version"].isNull(), F.lit("missing_schema_version"))
    )

    return kafka_df.select(
        F.sha2(
            F.concat_ws(
                "|",
                F.col("topic"),
                F.col("partition").cast("string"),
                F.col("offset").cast("string"),
            ),
            256,
        ).alias("bronze_record_id"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_payload"),
        parsed["event_id"].alias("event_id"),
        event_time.alias("event_time"),
        event_time_kst.alias("event_time_kst"),
        F.to_date(parsed["event_date_kst"]).alias("event_date_kst"),
        parsed["event_hour_kst"].alias("event_hour_kst"),
        parsed["event_type"].alias("event_type"),
        parsed["product_id"].alias("product_id"),
        parsed["category_id"].alias("category_id"),
        parsed["category_code"].alias("category_code"),
        parsed["brand"].alias("brand"),
        parsed["price"].cast("decimal(18,2)").alias("price"),
        parsed["user_id"].alias("user_id"),
        parsed["user_session"].alias("user_session"),
        parsed["source_file"].alias("source_file"),
        parsed["source_row_number"].alias("source_row_number"),
        parsed["replay_run_id"].alias("replay_run_id"),
        parsed["schema_version"].alias("schema_version"),
        F.when(parsed_ok, F.lit("parsed")).otherwise(F.lit("failed")).alias("parse_status"),
        F.when(parsed_ok, F.lit(None).cast("string")).otherwise(parse_error_code).alias("parse_error_code"),
        F.when(parsed_ok, F.lit(None).cast("string"))
        .otherwise(F.lit("Kafka value does not satisfy raw event schema."))
        .alias("parse_error_message"),
        ingested_at.alias("ingested_at"),
        F.to_date(F.from_utc_timestamp(ingested_at, KST_TIMEZONE)).alias("bronze_ingest_date_kst"),
    )


def bronze_to_silver(bronze_df, *, watermark: str):
    from pyspark.sql import functions as F

    category_parts = F.split(F.coalesce(F.col("category_code"), F.lit("")), "\\.")
    valid = (
        (F.col("parse_status") == "parsed")
        & F.col("event_type").isin(VALID_EVENT_TYPES)
        & F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("event_date_kst").isNotNull()
        & F.col("event_hour_kst").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("source_file").isNotNull()
        & F.col("source_row_number").isNotNull()
        & F.col("replay_run_id").isNotNull()
        & F.col("schema_version").isNotNull()
        & (F.col("price").isNull() | (F.col("price") >= F.lit(0)))
    )

    return (
        bronze_df.filter(valid)
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("category_level_1", F.when(F.col("category_code").isNotNull(), category_parts.getItem(0)))
        .withColumn("category_level_2", F.when(F.size(category_parts) > 1, category_parts.getItem(1)))
        .withColumn("category_level_3", F.when(F.size(category_parts) > 2, category_parts.getItem(2)))
        .withColumn("quality_status", F.lit("valid"))
        .withColumn("quality_rule_ids", F.array().cast("array<string>"))
        .withColumn("is_duplicate", F.lit(False))
        .withColumn("duplicate_of_event_id", F.lit(None).cast("string"))
        .select(
            "event_id",
            "event_time",
            "event_time_kst",
            "event_date_kst",
            "event_hour_kst",
            "event_type",
            "product_id",
            "category_id",
            "category_code",
            "category_level_1",
            "category_level_2",
            "category_level_3",
            "brand",
            "price",
            "user_id",
            "user_session",
            "source_file",
            "source_row_number",
            "replay_run_id",
            "schema_version",
            "ingested_at",
            "processed_at",
            "quality_status",
            "quality_rule_ids",
            "is_duplicate",
            "duplicate_of_event_id",
        )
        .withWatermark("event_time", watermark)
        .dropDuplicates(["event_id"])
    )


def gold_order_volume_hourly(silver_df, *, watermark: str):
    from pyspark.sql import functions as F

    _ = watermark
    return _with_window_metric_fields(
        silver_df.filter(F.col("event_type") == "purchase")
        .groupBy(F.window("event_time", "1 hour"), "replay_run_id")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("price").cast("decimal(18,2)").alias("gross_revenue"),
            F.count("*").alias("purchase_event_count"),
            F.count("*").alias("input_event_count"),
        ),
        metric_name="order_volume_hourly",
        window_grain="1h",
    )


def gold_order_volume_by_category(silver_df, *, watermark: str):
    from pyspark.sql import functions as F

    _ = watermark
    return _with_window_metric_fields(
        silver_df.filter(F.col("event_type") == "purchase")
        .groupBy(
            F.window("event_time", "1 hour"),
            "category_id",
            "category_code",
            "category_level_1",
            "replay_run_id",
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum("price").cast("decimal(18,2)").alias("gross_revenue"),
            F.count("*").alias("input_event_count"),
        ),
        metric_name="order_volume_by_category",
        window_grain="1h",
    )


def gold_conversion_funnel_hourly(silver_df, *, watermark: str):
    from pyspark.sql import functions as F

    _ = watermark
    grouped = (
        silver_df.groupBy(F.window("event_time", "1 hour"), "replay_run_id")
        .agg(
            F.count(F.when(F.col("event_type") == "view", True)).alias("view_event_count"),
            F.count(F.when(F.col("event_type") == "cart", True)).alias("cart_event_count"),
            F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchase_event_count"),
            F.approx_count_distinct(F.when(F.col("event_type") == "view", F.col("user_id"))).alias(
                "unique_view_users"
            ),
            F.approx_count_distinct(F.when(F.col("event_type") == "cart", F.col("user_id"))).alias(
                "unique_cart_users"
            ),
            F.approx_count_distinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias(
                "unique_purchase_users"
            ),
            F.count("*").alias("input_event_count"),
        )
        .withColumn(
            "cart_conversion_rate",
            F.when(F.col("unique_view_users") > 0, F.col("unique_cart_users") / F.col("unique_view_users")),
        )
        .withColumn(
            "purchase_conversion_rate",
            F.when(F.col("unique_view_users") > 0, F.col("unique_purchase_users") / F.col("unique_view_users")),
        )
        .withColumn(
            "cart_to_purchase_conversion_rate",
            F.when(F.col("unique_cart_users") > 0, F.col("unique_purchase_users") / F.col("unique_cart_users")),
        )
    )

    return _with_window_metric_fields(grouped, metric_name="conversion_funnel_hourly", window_grain="1h")


def gold_user_purchase_burst_features(silver_df, *, watermark: str):
    from pyspark.sql import functions as F

    _ = watermark
    grouped = (
        silver_df.filter(F.col("event_type") == "purchase")
        .groupBy(F.window("event_time", "5 minutes", "1 minute"), "user_id", "replay_run_id")
        .agg(
            F.count("*").alias("user_purchase_count"),
            F.sum("price").cast("decimal(18,2)").alias("user_purchase_amount"),
            F.count("*").alias("input_event_count"),
        )
        .withColumn("burst_score", F.col("user_purchase_count").cast("decimal(18,6)"))
        .withColumn("abnormal_purchase_flag", F.col("user_purchase_count") >= F.lit(5))
    )

    return _with_window_metric_fields(grouped, metric_name="user_purchase_burst", window_grain="5m")


def gold_duplicate_event_summary(bronze_df, *, watermark: str):
    from pyspark.sql import functions as F

    _ = watermark
    duplicate_events = (
        bronze_df.filter((F.col("parse_status") == "parsed") & F.col("event_id").isNotNull())
        .groupBy(F.window("event_time", "5 minutes"), "event_id", "replay_run_id")
        .agg(F.count("*").alias("event_occurrence_count"))
    )

    grouped = duplicate_events.groupBy(F.col("window"), "replay_run_id").agg(
        F.count("*").alias("unique_event_count"),
        F.sum("event_occurrence_count").alias("raw_event_count"),
        F.sum(
            F.when(F.col("event_occurrence_count") > 1, F.col("event_occurrence_count") - F.lit(1)).otherwise(
                F.lit(0)
            )
        ).alias("duplicate_event_count"),
        F.count(F.when(F.col("event_occurrence_count") > 1, True)).alias("duplicated_event_id_count"),
        F.count("*").alias("input_event_count"),
    )

    return _with_window_metric_fields(
        grouped.withColumn(
            "duplicate_event_rate",
            F.when(F.col("raw_event_count") > 0, F.col("duplicate_event_count") / F.col("raw_event_count")),
        ),
        metric_name="duplicate_event_summary",
        window_grain="5m",
    )


def _with_window_metric_fields(df, *, metric_name: str, window_grain: str):
    from pyspark.sql import functions as F

    for optional_column in ["category_code", "user_id"]:
        if optional_column not in df.columns:
            df = df.withColumn(optional_column, F.lit(None).cast("string"))

    window_start_kst = F.from_utc_timestamp(F.col("window.start"), KST_TIMEZONE)
    window_end_kst = F.from_utc_timestamp(F.col("window.end"), KST_TIMEZONE)

    return (
        df.withColumn("metric_name", F.lit(metric_name))
        .withColumn("window_start_kst", window_start_kst)
        .withColumn("window_end_kst", window_end_kst)
        .withColumn("metric_date_kst", F.to_date(F.col("window_start_kst")))
        .withColumn("metric_hour_kst", F.hour(F.col("window_start_kst")))
        .withColumn("window_grain", F.lit(window_grain))
        .withColumn("metric_quality_status", F.lit("complete"))
        .withColumn("schema_version", F.lit("gold-metric-v1"))
        .withColumn("computed_at", F.current_timestamp())
        .withColumn(
            "metric_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("metric_name"),
                    F.col("window_start_kst").cast("string"),
                    F.col("window_end_kst").cast("string"),
                    F.coalesce(F.col("replay_run_id"), F.lit("")),
                    F.coalesce(F.col("category_code"), F.lit("")),
                    F.coalesce(F.col("user_id").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
        .drop("window")
    )
