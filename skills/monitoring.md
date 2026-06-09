# Monitoring

## Purpose

Design logs, metrics, alerts, and Grafana dashboards for operating the streaming data platform.

## When to Use

Use this skill when defining pipeline observability, Kafka consumer lag alerts, DLQ monitoring, processing failure detection, throughput dashboards, or data delay alerts.

## Required Inputs

- Kafka topic and consumer group definitions.
- Spark streaming query names.
- Data quality rule names and severity.
- Analysis goals and dashboard consumers.
- Operational service-level targets.

## Step-by-Step Procedure

1. Define structured logs for replay, producer, Kafka consumer, Spark job, quality validation, and storage writes.
2. Define platform metrics for throughput, latency, lag, failure count, retry count, and DLQ count.
3. Define data metrics for event counts, duplicate rate, null rate, conversion rates, and purchase spikes.
4. Define alert thresholds and routing.
5. Define Grafana dashboard panels for replay, Kafka, Spark, data quality, and business metrics.
6. Define runbook links for each critical alert.
7. Review whether every data quality failure has a visible metric.

## Output Artifacts

- `docs/data-pipeline/monitoring-guide.md`
- Alert rule table.
- Grafana dashboard panel inventory.

## Validation Checklist

- Every critical pipeline stage emits logs and metrics.
- Kafka consumer lag and DLQ growth are alertable.
- Spark query failures and checkpoint recovery events are alertable.
- Data latency is measured using `event_time` and `ingested_at`.
- Dashboard panels map to project analysis goals.

## Anti-Patterns

- Monitoring only infrastructure metrics while ignoring data correctness.
- Alerting on raw counts without time windows.
- Using dashboards as a substitute for alerts.
- Omitting runbook owners for critical alerts.

## Example Prompt

`Kafka consumer lag, DLQ 수, Spark 처리 실패, 데이터 지연, 주문 급증을 감지하는 Grafana 대시보드와 알림 룰을 설계해줘.`

