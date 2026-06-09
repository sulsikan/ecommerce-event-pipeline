# Data Pipeline Design

## Purpose

Coordinate the end-to-end design of the real-time e-commerce behavior data pipeline harness, from schema design through PR readiness review.

## When to Use

Use this skill when planning or reviewing the full pipeline across schema, event replay, Kafka, Spark Structured Streaming, data quality, monitoring, and execution planning.

## Required Inputs

- Project goal and analysis objectives.
- Kaggle CSV source assumptions.
- Target runtime stack decisions, if already known.
- Existing docs under `docs/data-pipeline/`.
- Existing execution plan under `exec-plans/`.

## Step-by-Step Procedure

1. Read `AGENTS.md` for role boundaries and Git rules.
2. Run Schema Designer Agent to draft canonical event and storage schemas.
3. Run Event Replay Agent to define CSV replay semantics and failure scenarios.
4. Run Kafka Streaming Agent to define topic, partition, consumer group, offset, DLQ, and retry strategy.
5. Run Spark Processing Agent to define Bronze, Silver, Gold transformations.
6. Run Data Quality Agent to define validation rules and failure handling.
7. Run Monitoring Agent to define metrics, logs, alerts, and dashboards.
8. Run Review Agent to check consistency across all artifacts.
9. Execute validation scripts.
10. Update the execution plan and PR checklist.

## Output Artifacts

- `docs/data-pipeline/architecture.md`
- `docs/data-pipeline/roadmap.md`
- `docs/data-pipeline/schema-guide.md`
- `docs/data-pipeline/event-replay-guide.md`
- `docs/data-pipeline/kafka-guide.md`
- `docs/data-pipeline/spark-guide.md`
- `docs/data-pipeline/data-quality-rules.md`
- `docs/data-pipeline/monitoring-guide.md`
- `docs/data-pipeline/review-checklist.md`
- `exec-plans/templates/data-pipeline-exec-plan.md`

## Validation Checklist

- Each agent has a named owner, input, output, and validation step.
- Field names are consistent across schema, Kafka messages, Spark outputs, quality checks, and metrics.
- KST partitioning is consistently documented.
- DLQ, retry, watermark, checkpoint, and deduplication policies are documented.
- Git rules are included and followed.

## Anti-Patterns

- Designing analytics without defining data reliability contracts.
- Changing schema fields in one document without updating Kafka, Spark, quality, and monitoring docs.
- Treating duplicate, delayed, or missing events as edge cases with no test scenario.
- Committing or merging before user approval.

## Example Prompt

`데이터 파이프라인 설계를 전체 검토하고, 스키마/이벤트 재생/Kafka/Spark/품질/모니터링 문서의 불일치를 찾아줘.`

