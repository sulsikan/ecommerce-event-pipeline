# Schema Guide

## Source CSV Schema

Expected Kaggle source columns:

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `event_time` | timestamp string | No | Source event timestamp, parsed as UTC |
| `event_type` | string enum | No | One of `view`, `cart`, `purchase` |
| `product_id` | long | No | Product identifier |
| `category_id` | long | Yes | Product category identifier |
| `category_code` | string | Yes | Hierarchical category text |
| `brand` | string | Yes | Product brand |
| `price` | decimal | Conditionally | Required for `purchase`, expected non-negative for all events |
| `user_id` | long | No | User identifier |
| `user_session` | string | Yes | Session identifier from source |

## Canonical Event Schema

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `event_id` | string | No | Deterministic hash of stable source attributes |
| `event_time` | timestamp | No | Source event timestamp in UTC |
| `event_time_kst` | timestamp | No | Source event timestamp converted to KST |
| `ingested_at` | timestamp | No | Platform ingestion timestamp in UTC |
| `event_date_kst` | date | No | KST date partition |
| `event_hour_kst` | integer | No | KST hour for hourly aggregates |
| `event_type` | string | No | `view`, `cart`, or `purchase` |
| `product_id` | long | No | Product identifier |
| `category_id` | long | Yes | Category identifier |
| `category_code` | string | Yes | Category path |
| `brand` | string | Yes | Brand name |
| `price` | decimal(18, 2) | Yes | Product price |
| `user_id` | long | No | User identifier |
| `user_session` | string | Yes | User session identifier |
| `source_file` | string | Yes | Replay source file name |
| `source_row_number` | long | Yes | Source row number within file |
| `schema_version` | string | No | Canonical schema version |

## Primary Key Candidates

- Primary: `event_id`.
- Fallback source fingerprint: hash of `event_time`, `event_type`, `product_id`, `user_id`, `user_session`, `price`, and `source_row_number`.
- Do not use `source_row_number` alone because replay can combine multiple files.

## Partition Keys

- Kafka partition key: `user_id`.
- Bronze storage partition: `event_date_kst` when parseable, else ingestion date.
- Silver storage partition: `event_date_kst`.
- Gold storage partitions: `event_date_kst` and metric-specific dimensions such as `event_hour_kst` or `category_code`.

## Schema Evolution

| Change Type | Compatibility | Required Action |
| --- | --- | --- |
| Add nullable field | Compatible | Update schema docs and quality warnings |
| Add required field | Breaking | Version schema and update replay, Kafka, Spark, quality, monitoring |
| Rename field | Breaking | Add compatibility mapping or new schema version |
| Change type width safely | Conditional | Verify Spark casts and downstream storage |
| Remove field | Breaking | Deprecate first, then remove in a major version |

## Layer Schemas

Bronze keeps raw Kafka payload, Kafka metadata, parse result, and ingestion metadata. Silver keeps only validated canonical events plus quality status fields. Gold contains aggregate tables:

- `gold_order_volume_hourly`
- `gold_order_volume_by_category`
- `gold_conversion_funnel_hourly`
- `gold_user_purchase_burst_features`
- `gold_duplicate_event_summary`

