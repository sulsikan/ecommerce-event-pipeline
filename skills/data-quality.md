# Data Quality

## Purpose

Define validation rules and failure handling for streaming e-commerce events and derived metrics.

## When to Use

Use this skill when writing or reviewing null, duplicate, range, referential integrity, schema, conversion-rate, or anomaly validation rules.

## Required Inputs

- Canonical event schema.
- Kafka DLQ contract.
- Spark processing design.
- Monitoring alert requirements.
- Business metric definitions.

## Step-by-Step Procedure

1. Classify validation rules by severity: reject, quarantine, warn, or observe.
2. Define schema validation for required fields, types, enum values, and timestamp parsing.
3. Define null checks for identifiers, event type, event time, and price rules.
4. Define duplicate checks based on `event_id` and source fingerprint.
5. Define range checks for `price`, timestamp lateness, and replay skew.
6. Define referential checks for user-session and product-category consistency.
7. Define metric checks for conversion rates and aggregate counts.
8. Define failure handling to DLQ, quarantine tables, alerts, or quality reports.

## Output Artifacts

- `docs/data-pipeline/data-quality-rules.md`
- Updated validation script expectations.
- Quality metric names referenced in monitoring docs.

## Validation Checklist

- Every rule has severity, owner, failure action, and metric name.
- Reject rules map to DLQ or quarantine behavior.
- Warn rules map to dashboards or alerts.
- Conversion-rate checks define denominator behavior.
- Duplicate checks match Spark deduplication logic.

## Anti-Patterns

- Logging invalid events without a durable failed-record location.
- Treating quality rules as only batch checks.
- Alerting on noisy rules without severity and threshold.
- Computing conversion rates with inconsistent denominators.

## Example Prompt

`이벤트 스키마 검증, 중복 탐지, 가격 범위 검증, 전환율 계산 검증 규칙과 실패 처리 방식을 작성해줘.`

