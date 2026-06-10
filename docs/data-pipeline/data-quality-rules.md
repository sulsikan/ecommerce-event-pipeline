# 데이터 품질 규칙

## Severity 수준

| Severity | 의미 | 조치 |
| --- | --- | --- |
| reject | 이벤트를 신뢰할 수 없음 | DLQ 전송 |
| quarantine | 점검 후 활용 가능성이 있음 | Quarantine table 저장 |
| warn | 사용 가능하지만 의심스러움 | Metric 기록, threshold 초과 시 alert |
| observe | 추세만 추적 | Metric 기록 |

## 스키마 규칙

| Rule ID | 규칙 | Severity | Metric |
| --- | --- | --- | --- |
| DQ_SCHEMA_REQUIRED | `event_id`, `event_time`, `event_type`, `product_id`, `user_id`, `schema_version` 필수 | reject | `dq_schema_required_failures_total` |
| DQ_SCHEMA_TYPE | 필드 타입이 표준 스키마와 일치 | reject | `dq_schema_type_failures_total` |
| DQ_EVENT_TYPE_ENUM | `event_type`은 `view`, `cart`, `purchase` 중 하나 | reject | `dq_event_type_invalid_total` |

## Null 규칙

| Rule ID | 규칙 | Severity | Metric |
| --- | --- | --- | --- |
| DQ_NULL_EVENT_TIME | `event_time`은 null일 수 없음 | reject | `dq_null_event_time_total` |
| DQ_NULL_USER_ID | `user_id`는 null일 수 없음 | reject | `dq_null_user_id_total` |
| DQ_NULL_PRODUCT_ID | `product_id`는 null일 수 없음 | reject | `dq_null_product_id_total` |
| DQ_NULL_CATEGORY_CODE | `category_code` null은 허용하지만 추적 | observe | `dq_null_category_code_total` |

## 중복 규칙

| Rule ID | 규칙 | Severity | Metric |
| --- | --- | --- | --- |
| DQ_DUP_EVENT_ID | Watermark 범위 안에서 동일한 `event_id`가 2개 이상 존재 | warn | `dq_duplicate_event_id_total` |
| DQ_DUP_SOURCE_FINGERPRINT | 서로 다른 `event_id`가 같은 source fingerprint를 공유 | quarantine | `dq_duplicate_source_fingerprint_total` |

## 범위 규칙

| Rule ID | 규칙 | Severity | Metric |
| --- | --- | --- | --- |
| DQ_PRICE_NEGATIVE | `price`가 존재하면 0 이상이어야 함 | quarantine | `dq_price_negative_total` |
| DQ_PURCHASE_PRICE_NULL | `purchase` 이벤트의 `price`는 null이 아니어야 함 | warn | `dq_purchase_price_null_total` |
| DQ_EVENT_TOO_LATE | `event_time`이 watermark 허용 범위를 초과해 도착 | warn | `dq_event_too_late_total` |

## 참조 무결성 규칙

| Rule ID | 규칙 | Severity | Metric |
| --- | --- | --- | --- |
| DQ_SESSION_USER_CONSISTENCY | 짧은 window에서 하나의 `user_session`이 여러 무관한 `user_id`에 매핑되지 않아야 함 | warn | `dq_session_user_conflict_total` |
| DQ_PRODUCT_CATEGORY_CONSISTENCY | 하나의 `product_id`가 짧은 시간에 무관한 `category_id`로 급격히 이동하지 않아야 함 | observe | `dq_product_category_shift_total` |

## 전환율 지표 검증

- 장바구니 전환율: 같은 window에서 `cart`가 있는 distinct user 수를 `view`가 있는 distinct user 수로 나눈 값
- 구매 전환율: 같은 window에서 `purchase`가 있는 distinct user 수를 `view`가 있는 distinct user 수로 나눈 값
- Denominator가 0이면 conversion rate는 null로 방출하고 `dq_conversion_denominator_zero_total`을 증가시킨다.
- 전환율은 0 이상 1 이하이어야 하며, 범위를 벗어나면 aggregate row를 조사 대상으로 quarantine한다.

## 실패 처리

- reject 실패는 Kafka DLQ로 보낸다.
- quarantine 실패는 rule metadata와 함께 durable quarantine table에 저장한다.
- warn 실패는 Silver에는 남기되 quality metric을 방출한다.
- observe 규칙은 metric만 방출한다.
- 모든 실패 record에는 `rule_id`, `severity`, `event_id`, `detected_at`, `failed_stage`를 포함한다.

## Phase 5 구현

Phase 5 로컬 구현은 Spark warehouse를 읽어 다음 산출물을 생성한다.

```bash
docker compose exec spark \
  /opt/spark/bin/spark-submit \
  /workspace/scripts/run-data-quality-checks.py \
  --warehouse-dir /workspace/data/spark-warehouse \
  --output-dir /workspace/data/quality-monitoring
```

| 산출물 | 설명 |
| --- | --- |
| `data_quality/rule_results` | `rule_id`, `severity`, `failed_stage`, `metric_name`, `failure_count`, `status` |
| `data_quality/quarantine_events` | reject/quarantine 대상 record, Kafka metadata, 원본 payload, 실패 사유 |
| `monitoring/metric_events` | 품질/비즈니스/파이프라인 metric event |

초기 구현 규칙:

- `DQ_SCHEMA_REQUIRED`, `DQ_EVENT_TYPE_ENUM`, `DQ_NULL_EVENT_TIME`, `DQ_NULL_USER_ID`, `DQ_NULL_PRODUCT_ID`
- `DQ_NULL_CATEGORY_CODE`, `DQ_DUP_EVENT_ID`, `DQ_PRICE_NEGATIVE`, `DQ_PURCHASE_PRICE_NULL`
- `DQ_CONVERSION_DENOMINATOR_ZERO`, `DQ_CONVERSION_RATE_RANGE`
