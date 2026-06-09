# Data Pipeline Roadmap

## Phase 1: Harness and Design

- Create agent role definitions.
- Create skill documents.
- Create architecture, schema, replay, Kafka, Spark, quality, monitoring, and review documents.
- Create validation scripts.
- Create execution plan template.
- Run harness validation.

## Phase 2: Local Replay Prototype

- Add CSV fixture discovery.
- Implement event replay producer.
- Generate deterministic `event_id`.
- Add configurable replay speed.
- Add fault injection scenarios.

## Phase 3: Kafka Streaming Foundation

- Provision local Kafka environment.
- Create raw, retry, and DLQ topics.
- Add producer schema validation.
- Add consumer group and offset recovery tests.

## Phase 4: Spark Processing

- Implement Bronze ingestion.
- Implement Silver validation, normalization, watermarking, and deduplication.
- Implement Gold aggregates for order volume, category counts, funnel metrics, and anomaly features.
- Add checkpoint and replay recovery tests.

## Phase 5: Quality and Observability

- Implement data quality checks.
- Add quarantine and DLQ triage flow.
- Add metrics and structured logs.
- Add Grafana dashboards and alert rules.

## Phase 6: Review and Hardening

- Run Review Agent consistency checks.
- Run load and replay tests.
- Document operational runbooks.
- Prepare PR checklist.

## Git Workflow Milestones

- Work on `feature/*` branches only.
- Summarize changed files after each implementation slice.
- Perform self review before commit approval.
- Ask `커밋을 진행할까요?` before committing.
- Ask `main으로 merge할까요?` before merging.

