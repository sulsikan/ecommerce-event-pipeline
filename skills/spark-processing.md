# Spark Processing

## Purpose

Design Spark Structured Streaming jobs for Bronze, Silver, and Gold processing layers with reliable event-time behavior.

## When to Use

Use this skill when designing streaming ingestion, deduplication, watermarking, checkpointing, window aggregation, KST partitioning, or storage layer contracts.

## Required Inputs

- Kafka topic contracts.
- Canonical event schema.
- Data quality rules.
- Monitoring metrics.
- Storage format and table naming conventions.

## Step-by-Step Procedure

1. Read canonical events from Kafka into the Bronze layer.
2. Persist raw payload, Kafka metadata, ingestion metadata, and parsing status.
3. Validate and normalize events into the Silver layer.
4. Deduplicate with `event_id` and a bounded watermark.
5. Convert event timestamps into KST partition fields.
6. Build Gold aggregates for order volume, category volume, conversion funnel, and anomaly features.
7. Configure checkpoint locations per streaming query.
8. Define recovery, backfill, and replay behavior.

## Output Artifacts

- `docs/data-pipeline/spark-guide.md`
- Bronze, Silver, and Gold layer contracts.
- Aggregation definitions for dashboards.

## Validation Checklist

- Every streaming query has a checkpoint location.
- Watermark duration is documented.
- Deduplication key matches schema and quality rules.
- KST partitioning is consistent across layers.
- Gold outputs map to the analysis goals.

## Anti-Patterns

- Using processing time for all business metrics.
- Sharing checkpoint directories between unrelated queries.
- Deduplicating without a bounded state policy.
- Mixing raw invalid events into trusted Gold tables.

## Example Prompt

`Spark Structured Streaming 기준으로 Bronze/Silver/Gold 처리 흐름과 watermark, checkpoint, window aggregation 설계를 작성해줘.`

