from __future__ import annotations


def raw_event_json_schema():
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("replay_sequence", LongType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("event_time_kst", StringType(), True),
            StructField("event_date_kst", StringType(), True),
            StructField("event_hour_kst", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", StringType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("source_row_number", LongType(), True),
            StructField("replay_run_id", StringType(), True),
            StructField("schema_version", StringType(), True),
            StructField("ingested_at", StringType(), True),
        ]
    )


def silver_event_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DateType,
        DecimalType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("event_time_kst", TimestampType(), False),
            StructField("event_date_kst", DateType(), False),
            StructField("event_hour_kst", IntegerType(), False),
            StructField("event_type", StringType(), False),
            StructField("product_id", LongType(), False),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("category_level_1", StringType(), True),
            StructField("category_level_2", StringType(), True),
            StructField("category_level_3", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DecimalType(18, 2), True),
            StructField("user_id", LongType(), False),
            StructField("user_session", StringType(), True),
            StructField("source_file", StringType(), False),
            StructField("source_row_number", LongType(), False),
            StructField("replay_run_id", StringType(), False),
            StructField("schema_version", StringType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("processed_at", TimestampType(), False),
            StructField("quality_status", StringType(), False),
            StructField("quality_rule_ids", ArrayType(StringType()), False),
            StructField("is_duplicate", BooleanType(), False),
            StructField("duplicate_of_event_id", StringType(), True),
        ]
    )
