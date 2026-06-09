# Kafka Guide

## Topic Naming Convention

Use `<domain>.<entity>.<stage>.<version>`.

## Topics

| Topic | Purpose | Key | Value | Retention |
| --- | --- | --- | --- | --- |
| `ecommerce.events.raw.v1` | Canonical events from replay producer | `user_id` | Canonical event JSON or Avro | 7 days |
| `ecommerce.events.retry.v1` | Transient processing retry events | `user_id` | DLQ-compatible retry envelope | 3 days |
| `ecommerce.events.dlq.v1` | Invalid or unrecoverable events | `event_id` when available | DLQ envelope | 30 days |
| `ecommerce.quality.metrics.v1` | Quality metrics and rule outcomes | rule name | Quality metric event | 30 days |

## Partition Key Strategy

Default to `user_id` for raw and retry topics. This keeps each user's `view`, `cart`, and `purchase` behavior flow ordered within a partition and supports user purchase burst detection.

Use `event_id` for DLQ when available because DLQ consumers usually triage failed records independently. If `event_id` is missing, use a hash of the original payload.

## Producer Contract

- Validate required schema fields before publish.
- Generate deterministic `event_id`.
- Enable idempotent producer behavior when the implementation stack supports it.
- Use acknowledgements that confirm durable broker write.
- Include `schema_version` in each message.
- Log `replay_run_id`, topic, partition, offset, and publish result.

## Consumer Groups

| Consumer Group | Purpose | Offset Strategy |
| --- | --- | --- |
| `spark-bronze-ingest` | Bronze ingestion from raw topic | Commit after durable Bronze write |
| `spark-quality-validator` | Optional quality side stream | Commit after quality result write |
| `monitoring-lag-exporter` | Lag and throughput metrics | Commit after metric export |
| `dlq-triage-worker` | DLQ inspection and replay | Commit after triage state is persisted |

## Retry and DLQ

Transient failures go to `ecommerce.events.retry.v1` with retry count and original metadata. Unrecoverable schema or quality failures go to `ecommerce.events.dlq.v1`.

DLQ envelope fields:

- `dlq_id`
- `event_id`
- `original_topic`
- `original_partition`
- `original_offset`
- `failed_stage`
- `error_code`
- `error_message`
- `original_payload`
- `failed_at`
- `schema_version`

## Operational Rules

- Never discard invalid events without DLQ or quarantine.
- Offsets are committed only after durable processing.
- Replay from DLQ must preserve original payload and failure metadata.
- Topic changes require updates to Spark, quality, monitoring, and review documents.

