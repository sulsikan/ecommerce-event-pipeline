# E-commerce Event Pipeline Agents

This harness defines the working contract for a real-time data pipeline project based on Kaggle `E-commerce behavior data from multi category store`. The project goal is to replay CSV rows as streaming events and build a reliable platform for ingestion, preprocessing, modeling hooks, storage, and visualization.

The first project phase is documentation and harness setup only. Do not add application runtime code until a later implementation task explicitly asks for it.

## Project Goals

- Monitor real-time order volume by time window and category.
- Detect order spikes and abnormal purchase patterns.
- Analyze user behavior conversion from `view` to `cart` to `purchase`.
- Validate duplicate, late, missing, malformed, and referentially inconsistent events.
- Design a dependable platform around Kafka, Spark Structured Streaming, data quality checks, storage layers, metrics, alerts, and dashboards.

## Agent Workflow

1. Schema Designer Agent drafts source and event schemas.
2. Event Replay Agent designs CSV-based event replay.
3. Kafka Streaming Agent designs topics, partitions, producer, consumer groups, retry, and DLQ behavior.
4. Spark Processing Agent designs Bronze, Silver, and Gold streaming processing.
5. Data Quality Agent defines validation rules and failure handling.
6. Monitoring Agent defines logs, metrics, alerts, and dashboards.
7. Review Agent checks cross-document consistency.
8. Run `scripts/validate-schema.py`, `scripts/validate-data-quality.py`, and `scripts/validate-pipeline-docs.py`.
9. Update the execution plan in `exec-plans/templates/data-pipeline-exec-plan.md` or a copied plan.
10. Complete the PR readiness checklist in `docs/data-pipeline/review-checklist.md`.

## Schema Designer Agent

Responsibilities:

- Design source, canonical event, Bronze, Silver, and Gold schemas.
- Define column names, types, nullable rules, descriptions, partition keys, and primary key candidates.
- Define compatibility rules for schema changes.
- Keep schema names aligned with Kafka message fields, Spark processing fields, data quality rules, and monitoring metrics.

Expected outputs:

- `docs/data-pipeline/schema-guide.md`
- Schema-related sections in `docs/data-pipeline/architecture.md`
- Validation updates in `scripts/validate-schema.py`

## Event Replay Agent

Responsibilities:

- Design replay of Kaggle CSV data as real-time events.
- Preserve `event_time` ordering semantics while supporting configurable replay speed.
- Preserve the behavior flow across `view`, `cart`, and `purchase`.
- Define duplicate, late, missing, out-of-order, and malformed event test scenarios.

Expected outputs:

- `docs/data-pipeline/event-replay-guide.md`
- Replay assumptions in `docs/data-pipeline/architecture.md`

## Kafka Streaming Agent

Responsibilities:

- Design Kafka topics and naming conventions.
- Define partition key strategy.
- Define producer and consumer behavior.
- Define consumer groups, offset handling, retry policy, dead-letter queue, and replay policy.

Expected outputs:

- `docs/data-pipeline/kafka-guide.md`
- Topic and DLQ references in quality and monitoring documents.

## Spark Processing Agent

Responsibilities:

- Design Spark Structured Streaming jobs.
- Define deduplication, watermarking, checkpointing, window aggregation, and joins.
- Define KST-based partitioning.
- Define Bronze, Silver, and Gold storage layers.

Expected outputs:

- `docs/data-pipeline/spark-guide.md`
- Layer descriptions in `docs/data-pipeline/architecture.md`

## Data Quality Agent

Responsibilities:

- Define null, duplicate, range, referential integrity, and schema validation rules.
- Define conversion-rate calculation checks.
- Define failure handling, quarantine, DLQ, and alert behavior.
- Align rule names with monitoring metrics and review checklist items.

Expected outputs:

- `docs/data-pipeline/data-quality-rules.md`
- Updates to `scripts/validate-data-quality.py`

## Monitoring Agent

Responsibilities:

- Define logs, metrics, alert thresholds, and dashboard panels.
- Detect data latency, missing events, duplicate events, processing failures, Kafka consumer lag, DLQ growth, and throughput anomalies.
- Document Grafana dashboard structure and alert rules.

Expected outputs:

- `docs/data-pipeline/monitoring-guide.md`
- Monitoring references in `docs/data-pipeline/review-checklist.md`

## Review Agent

Responsibilities:

- Review the outputs from all other agents.
- Check schema, replay, Kafka, Spark, quality, and monitoring consistency.
- Verify that PR readiness is documented before commit or merge.

Expected outputs:

- `docs/data-pipeline/review-checklist.md`
- Findings added to the active execution plan.

## Git Rules

- Do not commit directly to `main`.
- Create a `feature/*` branch for feature work.
- Do not commit before user approval.
- Do not merge before user approval.
- After implementation, summarize changed files.
- Perform a self review before asking for commit approval.
- Ask: `커밋을 진행할까요?`
- Commit only after approval.
- Ask: `main으로 merge할까요?`
- Merge only after approval.

## Required Validation

Run these before requesting commit approval:

```bash
python3 scripts/validate-schema.py
python3 scripts/validate-data-quality.py
python3 scripts/validate-pipeline-docs.py
python3 scripts/generate-pipeline-report.py
```

