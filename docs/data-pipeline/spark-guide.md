# Spark Guide

## Processing Model

Use Spark Structured Streaming for event-time processing from Kafka into Bronze, Silver, and Gold layers.

## Bronze Layer

Bronze stores raw ingestion records:

- Kafka topic, partition, offset, key, timestamp.
- Raw payload.
- Parse status.
- Ingestion timestamp.
- `event_date_kst` when parseable.

Failure behavior:

- Payload parse failures are written to quarantine or DLQ.
- Kafka offsets are committed only after durable write.

## Silver Layer

Silver stores validated canonical events:

- Apply schema validation.
- Normalize timestamp fields.
- Generate KST partition fields.
- Deduplicate by `event_id`.
- Apply watermark based on `event_time`.
- Preserve quality status fields for warnings.

Recommended defaults:

- Watermark: start with `30 minutes`, adjust after profiling replay disorder.
- Deduplication key: `event_id`.
- Partition: `event_date_kst`.
- Checkpoint: one checkpoint directory per query.

## Gold Layer

Gold tables support analysis goals:

| Gold Table | Purpose | Window |
| --- | --- | --- |
| `gold_order_volume_hourly` | Purchases by hour | 1 hour |
| `gold_order_volume_by_category` | Purchases by category and hour | 1 hour |
| `gold_conversion_funnel_hourly` | `view -> cart -> purchase` conversion | 1 hour |
| `gold_user_purchase_burst_features` | User purchase burst features | 5 minutes, 1 hour |
| `gold_duplicate_event_summary` | Duplicate event metrics | 5 minutes |

## KST Partitioning

Source `event_time` is UTC. Convert it to `event_time_kst`, then derive:

- `event_date_kst`
- `event_hour_kst`

All storage partitions and dashboard groupings that refer to business day or business hour should use KST fields.

## Checkpointing

- Each streaming query has a dedicated checkpoint location.
- Do not reuse checkpoint directories across query names.
- Include query name and layer in checkpoint path.
- Document checkpoint reset procedure before running destructive replay tests.

## Recovery

- Reprocessing raw events must be possible from Kafka retention or Bronze storage.
- Late events beyond watermark are tracked by quality metrics.
- DLQ records can be triaged and replayed into retry or raw topics after correction.

