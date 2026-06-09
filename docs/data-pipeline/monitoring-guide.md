# Monitoring Guide

## Monitoring Goals

- Detect ingestion failures and replay drift.
- Detect Kafka consumer lag and DLQ growth.
- Detect Spark query failures, checkpoint recovery events, and processing latency.
- Detect data latency, duplicates, missing patterns, and quality rule failures.
- Display order volume, category volume, conversion funnel, and anomaly signals.

## Structured Logs

Each stage should log:

- `stage`
- `run_id` or `query_id`
- `event_id` when applicable
- `topic`, `partition`, `offset` when applicable
- `event_time`
- `ingested_at`
- `severity`
- `error_code`
- `message`

## Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `pipeline_events_in_total` | counter | stage, event_type | Input throughput |
| `pipeline_events_out_total` | counter | stage, event_type | Output throughput |
| `pipeline_processing_latency_seconds` | histogram | stage | Processing latency |
| `pipeline_event_lag_seconds` | histogram | stage | `ingested_at - event_time` |
| `kafka_consumer_lag` | gauge | consumer_group, topic, partition | Kafka lag |
| `kafka_dlq_events_total` | counter | error_code, failed_stage | DLQ growth |
| `spark_query_failures_total` | counter | query_name | Spark failures |
| `spark_checkpoint_recoveries_total` | counter | query_name | Recovery events |
| `dq_duplicate_event_id_total` | counter | stage | Duplicate events |
| `dq_schema_required_failures_total` | counter | stage | Required field failures |
| `business_purchase_count` | counter | category_code | Purchase count |
| `business_conversion_rate` | gauge | funnel_step, window | Funnel conversion |

## Alert Rules

| Alert | Condition | Severity | Runbook |
| --- | --- | --- | --- |
| KafkaConsumerLagHigh | `kafka_consumer_lag` remains high for 10 minutes | critical | Check Spark query health and broker throughput |
| DLQGrowthHigh | DLQ event rate exceeds baseline for 5 minutes | critical | Inspect `error_code` and replay source |
| SparkQueryFailed | Any production query fails | critical | Check checkpoint and latest deployment |
| DataLatencyHigh | p95 `pipeline_event_lag_seconds` exceeds target | warning | Check replay speed and downstream lag |
| DuplicateRateHigh | Duplicate rate exceeds threshold | warning | Check replay fault scenario and `event_id` generation |
| PurchaseSpikeDetected | Purchase count exceeds rolling baseline | warning | Check anomaly dashboard and source replay |

## Grafana Dashboards

Dashboard sections:

- Replay producer health: events emitted, replay speed, producer failures.
- Kafka health: broker throughput, consumer lag, retry topic size, DLQ count.
- Spark health: input rows per second, processed rows per second, query failures, checkpoint recovery.
- Data quality: null counts, duplicate counts, schema failures, quarantine counts.
- Business metrics: hourly purchase count, category purchase count, conversion funnel, purchase burst users.

## Review Requirements

- Every critical alert must have an owner and runbook.
- Every reject or quarantine data quality rule must emit a metric.
- Dashboards must distinguish event-time metrics from processing-time metrics.
- Alert thresholds must be revisited after replay load testing.

