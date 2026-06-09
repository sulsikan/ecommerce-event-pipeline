# Event Replay

## Purpose

Design CSV-to-stream replay that makes historical Kaggle events behave like real-time events while preserving event-time semantics.

## When to Use

Use this skill when designing replay speed, event ordering, synthetic ingestion timestamps, producer behavior, and fault injection scenarios.

## Required Inputs

- Kaggle CSV path and sample row count.
- Desired replay speed multiplier.
- Kafka topic contract.
- Event-time range and timezone policy.
- Fault scenario requirements.

## Step-by-Step Procedure

1. Read CSV rows using a streaming or chunked reader.
2. Parse `event_time` as UTC source event time.
3. Sort or partition replay by `event_time` while preserving stable order for identical timestamps.
4. Compute replay delay from adjacent event timestamps divided by `replay_speed`.
5. Emit canonical event payloads with `ingested_at` set at publish time.
6. Preserve `view`, `cart`, and `purchase` event types without collapsing the behavior path.
7. Add configurable fault injection for duplicate, late, missing, malformed, and out-of-order events.
8. Record replay run metadata for reproducibility.

## Output Artifacts

- `docs/data-pipeline/event-replay-guide.md`
- Replay test scenario table.
- Producer contract updates in `docs/data-pipeline/kafka-guide.md`.

## Validation Checklist

- Replay uses `event_time`, not file read time, for event-time semantics.
- Replay speed is configurable and documented.
- Duplicate and late event scenarios are reproducible.
- Replayed payloads match the canonical schema.
- Replay can be resumed or restarted with known offsets.

## Anti-Patterns

- Sleeping a fixed interval between all rows regardless of source timestamps.
- Dropping `view` or `cart` events because only purchases are analyzed.
- Generating random fault scenarios without a seed.
- Treating producer success as end-to-end pipeline success.

## Example Prompt

`Kaggle CSV를 100배속 실시간 이벤트처럼 Kafka에 재생하는 설계를 작성하고, 지연/중복/누락 테스트 시나리오도 포함해줘.`

