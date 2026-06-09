# Review

## Purpose

Review all pipeline design artifacts for consistency, completeness, operational reliability, and PR readiness.

## When to Use

Use this skill before opening a PR, before asking for commit approval, or after changing schema, replay, Kafka, Spark, quality, or monitoring documents.

## Required Inputs

- `AGENTS.md`
- All documents under `docs/data-pipeline/`
- Skill docs under `skills/`
- Validation script output.
- Active execution plan.

## Step-by-Step Procedure

1. Check that agent outputs exist and match their responsibilities.
2. Compare field names across schema, Kafka, Spark, quality rules, and monitoring metrics.
3. Confirm that `event_id`, `event_time`, `ingested_at`, `event_date_kst`, and `user_id` semantics are consistent.
4. Confirm that replay failure scenarios map to quality rules and monitoring alerts.
5. Confirm that Kafka DLQ and Spark quarantine behavior are aligned.
6. Confirm that Gold tables satisfy order monitoring, conversion, and anomaly goals.
7. Run validation scripts.
8. Update the review checklist with findings.
9. Summarize changed files.
10. Ask for commit approval only after self review.

## Output Artifacts

- `docs/data-pipeline/review-checklist.md`
- Review notes in the execution plan.
- Validation command results.

## Validation Checklist

- No unresolved contradiction exists between pipeline documents.
- Required validation scripts pass.
- PR checklist has owners and status.
- Git approval gates are preserved.
- Remaining risks are documented.

## Anti-Patterns

- Reviewing each document in isolation only.
- Treating validation script success as a substitute for design review.
- Asking for commit approval without changed-file summary.
- Merging immediately after commit approval.

## Example Prompt

`PR 전에 전체 데이터 파이프라인 하네스를 리뷰하고 불일치, 누락된 테스트, Git 승인 절차 위반 여부를 확인해줘.`

