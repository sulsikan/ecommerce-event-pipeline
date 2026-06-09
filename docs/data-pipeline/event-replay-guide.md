# Event Replay Guide

## Purpose

Replay Kaggle CSV rows as if they are real-time user behavior events while preserving source event-time semantics.

## Replay Inputs

- Kaggle CSV file path.
- Replay speed multiplier, for example `1`, `10`, `100`, or `1000`.
- Optional start and end event-time range.
- Optional deterministic fault injection seed.
- Target Kafka raw topic.

## Replay Semantics

- Parse source `event_time` as UTC.
- Emit payloads matching the canonical event schema.
- Preserve `event_type` values: `view`, `cart`, `purchase`.
- Use source event-time gaps divided by `replay_speed` to schedule events.
- Set `ingested_at` when the event is published.
- Generate `event_id` deterministically before publishing.
- Preserve stable ordering for identical timestamps by `source_file` and `source_row_number`.

## Replay Metadata

Each replay run should record:

- `replay_run_id`
- source file names
- row range
- event-time range
- replay speed
- fault scenario name
- random seed, if fault injection is enabled
- target topic
- start and end wall-clock timestamps

## Fault Injection Scenarios

| Scenario | Description | Expected Downstream Behavior |
| --- | --- | --- |
| duplicate_event | Re-emit selected events with identical `event_id` | Spark dedup removes duplicate; quality metric increments |
| delayed_event | Emit selected events after watermark threshold | Late-event rule records warning or quarantine |
| missing_event | Drop selected rows during replay | Missing-rate scenario is visible in replay report |
| malformed_event | Corrupt required fields or types | Kafka validation rejects or Spark sends to DLQ |
| out_of_order_event | Shuffle bounded event windows | Spark watermark handles tolerated disorder |
| purchase_burst | Emit many purchases for one `user_id` in a short window | Gold anomaly feature and alert fire |

## Validation

- Sample emitted payloads validate against `docs/data-pipeline/schema-guide.md`.
- Replay speed behavior is tested with a short fixture.
- Fault injection is deterministic when a seed is provided.
- Producer failures are logged with replay run metadata.

