# Data Pipeline Architecture

## Scope

This document describes the target platform design for replaying Kaggle e-commerce behavior CSV data as real-time events. The first harness phase documents the platform and validation contracts only; application implementation is intentionally out of scope.

## Analysis Goals

- Real-time order volume monitoring by time window and category.
- Spike detection for sudden purchase increases.
- User behavior funnel analysis: `view -> cart -> purchase`.
- Cart conversion and purchase conversion measurement.
- Anomaly detection for purchase bursts, duplicate events, and abnormal purchase patterns.

## High-Level Flow

```text
Kaggle CSV
  -> Event Replay Producer
  -> Kafka raw event topic
  -> Spark Bronze ingestion
  -> Spark Silver validation and normalization
  -> Spark Gold aggregates and anomaly features
  -> Storage tables
  -> Grafana dashboards and alerts
```

## Core Event Contract

The canonical event is centered on these stable fields:

- `event_id`: deterministic event identifier.
- `event_time`: original event timestamp from the Kaggle row, parsed as UTC.
- `ingested_at`: platform ingestion timestamp.
- `event_type`: one of `view`, `cart`, or `purchase`.
- `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`: source business fields.
- `event_date_kst`, `event_hour_kst`: KST partition and aggregation fields.

## Storage Layers

| Layer | Purpose | Trust Level | Partitioning |
| --- | --- | --- | --- |
| Bronze | Raw Kafka messages, Kafka metadata, parse status | Low | `event_date_kst` when parseable, else ingestion date |
| Silver | Validated and normalized canonical events | Medium | `event_date_kst` |
| Gold | Aggregates for dashboards, funnel metrics, anomaly features | High | `event_date_kst`, plus metric-specific dimensions |

## Reliability Design

- Event replay uses `event_time` to simulate original timing.
- Kafka partitions by `user_id` to preserve user behavior ordering within a partition.
- Spark deduplicates by `event_id` with a bounded watermark.
- Invalid events go to DLQ or quarantine based on rule severity.
- Metrics track lag, latency, duplicate rate, null rate, DLQ count, and processing failures.

## Agent Ownership

| Agent | Main Artifact | Cross-Checks |
| --- | --- | --- |
| Schema Designer Agent | `schema-guide.md` | Kafka payloads, Spark layers, quality rules |
| Event Replay Agent | `event-replay-guide.md` | Schema payload, Kafka producer, fault scenarios |
| Kafka Streaming Agent | `kafka-guide.md` | Replay producer, Spark consumer, DLQ |
| Spark Processing Agent | `spark-guide.md` | Schema, quality rules, monitoring |
| Data Quality Agent | `data-quality-rules.md` | DLQ, quarantine, metrics |
| Monitoring Agent | `monitoring-guide.md` | Kafka, Spark, quality, business goals |
| Review Agent | `review-checklist.md` | End-to-end consistency |

## Open Decisions

- Storage engine and table format.
- Kafka partition count sizing.
- Exact watermark duration after sample data profiling.
- Dashboard deployment path.
- Model-serving or anomaly-scoring implementation details.

