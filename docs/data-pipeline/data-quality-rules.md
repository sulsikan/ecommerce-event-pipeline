# Data Quality Rules

## Severity Levels

| Severity | Meaning | Action |
| --- | --- | --- |
| reject | Event cannot be trusted | Send to DLQ |
| quarantine | Event may be useful after inspection | Write to quarantine table |
| warn | Event is usable but suspicious | Write metric and alert if threshold is breached |
| observe | Track trend only | Write metric |

## Schema Rules

| Rule ID | Rule | Severity | Metric |
| --- | --- | --- | --- |
| DQ_SCHEMA_REQUIRED | Required fields exist: `event_id`, `event_time`, `event_type`, `product_id`, `user_id`, `schema_version` | reject | `dq_schema_required_failures_total` |
| DQ_SCHEMA_TYPE | Field types match canonical schema | reject | `dq_schema_type_failures_total` |
| DQ_EVENT_TYPE_ENUM | `event_type` is one of `view`, `cart`, `purchase` | reject | `dq_event_type_invalid_total` |

## Null Rules

| Rule ID | Rule | Severity | Metric |
| --- | --- | --- | --- |
| DQ_NULL_EVENT_TIME | `event_time` must not be null | reject | `dq_null_event_time_total` |
| DQ_NULL_USER_ID | `user_id` must not be null | reject | `dq_null_user_id_total` |
| DQ_NULL_PRODUCT_ID | `product_id` must not be null | reject | `dq_null_product_id_total` |
| DQ_NULL_CATEGORY_CODE | `category_code` may be null but should be tracked | observe | `dq_null_category_code_total` |

## Duplicate Rules

| Rule ID | Rule | Severity | Metric |
| --- | --- | --- | --- |
| DQ_DUP_EVENT_ID | More than one event has the same `event_id` inside watermark horizon | warn | `dq_duplicate_event_id_total` |
| DQ_DUP_SOURCE_FINGERPRINT | Different `event_id` values share the same source fingerprint | quarantine | `dq_duplicate_source_fingerprint_total` |

## Range Rules

| Rule ID | Rule | Severity | Metric |
| --- | --- | --- | --- |
| DQ_PRICE_NEGATIVE | `price` must be greater than or equal to 0 when present | quarantine | `dq_price_negative_total` |
| DQ_PURCHASE_PRICE_NULL | `purchase` events should have non-null `price` | warn | `dq_purchase_price_null_total` |
| DQ_EVENT_TOO_LATE | `event_time` arrives later than watermark tolerance | warn | `dq_event_too_late_total` |

## Referential Integrity Rules

| Rule ID | Rule | Severity | Metric |
| --- | --- | --- | --- |
| DQ_SESSION_USER_CONSISTENCY | One `user_session` should not map to many unrelated `user_id` values in a short window | warn | `dq_session_user_conflict_total` |
| DQ_PRODUCT_CATEGORY_CONSISTENCY | One `product_id` should not rapidly switch across unrelated `category_id` values | observe | `dq_product_category_shift_total` |

## Conversion Metric Checks

- Cart conversion rate: distinct users with `cart` divided by distinct users with `view` in the same window.
- Purchase conversion rate: distinct users with `purchase` divided by distinct users with `view` in the same window.
- Denominator zero behavior: emit null conversion rate and increment `dq_conversion_denominator_zero_total`.
- Conversion rate must be between `0` and `1`; otherwise quarantine the aggregate row for investigation.

## Failure Handling

- Reject failures go to Kafka DLQ.
- Quarantine failures go to a durable quarantine table with rule metadata.
- Warning failures remain in Silver but emit quality metrics.
- Observe rules emit metrics only.
- All failures include `rule_id`, `severity`, `event_id`, `detected_at`, and `failed_stage`.

