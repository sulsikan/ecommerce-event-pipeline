# Kafka Streaming

## Purpose

Design reliable Kafka topics, partitioning, producers, consumers, retries, DLQ handling, and replay behavior for the e-commerce event stream.

## When to Use

Use this skill when creating or reviewing Kafka topic contracts, partition keys, producer and consumer settings, offset strategy, retry policy, or DLQ design.

## Required Inputs

- Canonical event schema.
- Event replay behavior.
- Expected throughput and replay speed.
- Consumer applications and Spark jobs.
- Retention and recovery requirements.

## Step-by-Step Procedure

1. Define topic naming convention: `<domain>.<entity>.<stage>.<version>`.
2. Create raw replay topic for canonical events, retry topic for transient failures, and DLQ topic for invalid events.
3. Choose partition key. Use `user_id` for behavior flow consistency and user anomaly detection.
4. Define producer serialization, idempotence, acknowledgement, retry, compression, and schema validation behavior.
5. Define consumer groups for Spark Bronze ingestion, quality validation, monitoring, and ad hoc backfills.
6. Define offset commit strategy and recovery behavior.
7. Define DLQ envelope with original payload, error code, error message, failed stage, and timestamps.
8. Document retention, compaction, and replay policy.

## Output Artifacts

- `docs/data-pipeline/kafka-guide.md`
- Kafka-related architecture diagram or text flow.
- Topic contract entries referenced by Spark, data quality, and monitoring docs.

## Validation Checklist

- Every topic has purpose, key, value schema, partition count guidance, retention, and owner.
- Partition key aligns with anomaly and conversion analysis.
- Consumer group names are unique and meaningful.
- DLQ policy includes triage and replay path.
- Offset handling is documented for failure and restart.

## Anti-Patterns

- Partitioning purchase events by product while conversion logic requires user ordering.
- Using one catch-all topic for raw, retry, and DLQ events.
- Committing offsets before durable processing.
- Making DLQ payloads impossible to replay.

## Example Prompt

`이 이벤트 파이프라인의 Kafka topic, partition key, consumer group, retry, DLQ 전략을 설계해줘.`

