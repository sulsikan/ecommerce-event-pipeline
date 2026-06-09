---
name: data-pipeline-design
description: Orchestrate schema, event replay, Kafka, Spark, data quality, monitoring, review, validation scripts, execution plans, and Git workflow for the e-commerce event pipeline harness.
---

# Data Pipeline Design

## Purpose

Guide the complete harness workflow for a real-time data pipeline that replays Kaggle e-commerce behavior CSV rows as streaming events.

## When to Use

Use this skill when coordinating multiple pipeline agents, creating or updating data pipeline documentation, running validation scripts, preparing an execution plan, or performing a pre-PR review.

## Required Inputs

- User request and current project phase.
- `AGENTS.md`
- Role skill docs in `skills/`
- Pipeline docs in `docs/data-pipeline/`
- Execution plan template in `exec-plans/templates/`
- Validation scripts in `scripts/`

## Step-by-Step Procedure

1. Confirm scope. For the first harness task, create documentation and validation scripts only; do not implement application runtime code.
2. Read `AGENTS.md` to confirm agent responsibilities and Git rules.
3. Ask Schema Designer Agent to define source, canonical event, Bronze, Silver, and Gold schemas.
4. Ask Event Replay Agent to define CSV replay semantics, replay speed, ordering, and fault scenarios.
5. Ask Kafka Streaming Agent to define topics, partition keys, producer and consumer contracts, offsets, retries, and DLQ.
6. Ask Spark Processing Agent to define Structured Streaming jobs, watermarking, deduplication, checkpointing, KST partitioning, and layer outputs.
7. Ask Data Quality Agent to define validation rules and failure handling.
8. Ask Monitoring Agent to define logs, metrics, alerts, and Grafana dashboards.
9. Ask Review Agent to compare all outputs and record inconsistencies.
10. Run validation scripts:

```bash
python3 scripts/validate-schema.py
python3 scripts/validate-data-quality.py
python3 scripts/validate-pipeline-docs.py
python3 scripts/generate-pipeline-report.py
```

11. Update the execution plan with decisions, risks, validation results, and next tasks.
12. Prepare the PR checklist.
13. Summarize changed files and perform self review.
14. Ask `커밋을 진행할까요?` before committing.
15. After approved commit, ask `main으로 merge할까요?` before merge.

## Output Artifacts

- Updated pipeline docs under `docs/data-pipeline/`
- Updated execution plan or copied plan under `exec-plans/`
- Validation script output
- PR readiness checklist
- Changed-file summary

## Validation Checklist

- Orchestration order matches `AGENTS.md`.
- No application code is introduced during harness-only tasks.
- All agent outputs have corresponding documents.
- All validation scripts pass.
- Git approval gates are preserved.

## Anti-Patterns

- Skipping Review Agent after local document edits.
- Using different event identity fields in schema, Kafka, Spark, and quality rules.
- Running implementation work when the user asked only for harness documentation.
- Committing or merging without explicit approval.

## Example Prompt

`data-pipeline-design skill을 사용해서 전체 파이프라인 문서와 검증 결과를 점검하고 exec-plan을 업데이트해줘.`

