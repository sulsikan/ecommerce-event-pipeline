# Spark 가이드

## 설계 목표

Spark Structured Streaming은 Kafka raw event를 읽어 Bronze, Silver, Gold 계층으로 처리한다. 목표는 replay 속도나 처리 지연에 흔들리지 않는 event-time 기반 지표와, 장애 발생 시 재처리 가능한 저장 계층을 만드는 것이다.

## 핵심 설계 결정

| 결정 | 내용 | 이유 |
| --- | --- | --- |
| Structured Streaming 사용 | Kafka source에서 micro-batch streaming query로 처리한다. | Kafka offset, checkpoint, watermark를 일관된 모델로 관리할 수 있다. |
| Bronze/Silver/Gold 분리 | raw 보존, 검증된 이벤트, 분석 집계를 분리한다. | 품질 실패와 재처리 요구가 Gold dashboard를 오염시키지 않게 한다. |
| Event-time watermark | `event_time` 기준 watermark를 적용한다. | replay 지연과 out-of-order 이벤트를 허용하면서 state 크기를 제한한다. |
| `event_id` deduplication | Silver에서 `event_id` 기준 중복 제거를 수행한다. | replay retry와 duplicate fault injection에 대응한다. |
| KST partitioning | `event_date_kst`, `event_hour_kst`를 저장과 집계 기준으로 사용한다. | 한국 시간 기준 주문량 dashboard와 partition pruning을 맞춘다. |

## 처리 모델

```text
Kafka ecommerce.events.raw.v1
  -> Bronze query: raw payload + Kafka metadata 저장
  -> Silver query: parse, schema validation, normalization, deduplication
  -> Gold queries:
       - hourly order volume
       - category order volume
       - conversion funnel
       - purchase burst features
       - duplicate summary
```

## Bronze 계층

Bronze는 raw ingestion record를 저장한다.

필드:

- `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`
- `message_key`
- `raw_payload`
- `parse_status`, `parse_error`
- `ingested_at`
- `replay_run_id`
- 파싱 가능 시 `event_id`, `event_time`, `event_date_kst`

실패 처리:

- Payload parse failure는 parse 실패 상태로 Bronze에 남긴다.
- 구조적으로 처리 불가능한 record는 DLQ 또는 quarantine 대상으로 표시한다.
- Kafka offset은 durable write 이후에만 처리 완료로 간주한다.

결정 이유:

- Bronze는 원본 보존 계층이므로 잘못된 이벤트도 분석 가능한 형태로 남겨야 한다.
- Kafka metadata를 보존하면 중복 수집, offset 오류, replay 문제를 추적할 수 있다.

## Silver 계층

Silver는 검증된 표준 이벤트를 저장한다.

처리:

- 표준 이벤트 schema 적용
- `event_time` UTC parsing과 `event_time_kst` 변환
- `event_date_kst`, `event_hour_kst` 생성
- required field, enum, range 기본 검증
- `event_id` 기준 deduplication
- `event_time` 기준 watermark 적용
- warning 품질 상태 보존

권장 기본값:

- Watermark: `30 minutes`
- Deduplication key: `event_id`
- Partition: `event_date_kst`
- Checkpoint: query별 독립 directory

결정 이유:

- 30분 watermark는 초기 설계값이며, Kaggle replay disorder profiling 후 조정한다.
- Silver에서 deduplication해야 모든 Gold query가 같은 중복 제거 결과를 공유한다.
- Warning 이벤트를 완전히 버리지 않으면 conversion funnel 왜곡을 줄일 수 있다.

## Gold 계층

Gold table은 dashboard와 anomaly feature를 위한 목적별 aggregate로 둔다.

| Gold Table | 목적 | Window | 주요 Metric | 결정 이유 |
| --- | --- | --- | --- | --- |
| `gold_order_volume_hourly` | 시간대별 주문량 | 1시간 tumbling | purchase count, amount sum | 실시간 주문량 dashboard의 기본 table이다. |
| `gold_order_volume_by_category` | 카테고리별 주문량 | 1시간 tumbling | category purchase count | 카테고리별 급증과 인기 변화를 본다. |
| `gold_conversion_funnel_hourly` | `view -> cart -> purchase` 전환율 | 1시간 tumbling | view users, cart users, purchase users, conversion rates | 사용자 행동 로그 분석 목표와 직접 연결된다. |
| `gold_user_purchase_burst_features` | 사용자 구매 폭증 탐지 feature | 5분 sliding, 1시간 tumbling | purchase count, amount sum, distinct session count | rule 기반 이상 탐지와 추후 모델 feature에 모두 쓸 수 있다. |
| `gold_duplicate_event_summary` | 중복 이벤트 지표 | 5분 tumbling | duplicate count, duplicate rate | replay와 Kafka retry 품질을 운영 지표로 확인한다. |

## Window Aggregation

- 주문량 집계는 `event_type = 'purchase'`만 포함한다.
- 카테고리 주문량은 `category_code`를 우선 사용하고, 없으면 `category_id`를 fallback dimension으로 둔다.
- Funnel은 같은 window의 distinct `user_id` 기준으로 계산한다.
- 장바구니 전환율은 `cart_users / view_users`다.
- 구매 전환율은 `purchase_users / view_users`다.
- denominator가 0이면 전환율은 null로 둔다.

결정 이유:

- purchase count와 amount는 주문량 metric에 직접 대응한다.
- distinct user 기준 funnel은 중복 event와 여러 session에 덜 민감하다.
- denominator 0을 0%로 처리하면 데이터 없음과 실제 전환 없음이 섞인다.

## Deduplication

- Silver에서 `event_id` 기준으로 deduplication한다.
- Dedup state는 watermark 기간 안에서 유지한다.
- 중복으로 제거된 건수는 quality metric과 Gold duplicate summary로 보낸다.

결정 이유:

- deduplication이 Gold마다 따로 있으면 지표가 서로 달라질 수 있다.
- bounded state가 없으면 streaming query의 state store가 계속 증가한다.

## Watermark

- 초기 watermark는 `event_time` 기준 30분이다.
- `delayed_event`와 `out_of_order_event` fault scenario로 적정값을 조정한다.
- Watermark를 초과한 late event는 정책에 따라 quarantine 또는 late metric으로 기록한다.

결정 이유:

- Watermark는 정확도와 상태 크기 사이의 trade-off다.
- 초기값은 문서상 계약일 뿐이며 Phase 5에서 부하와 지연 분포를 보고 조정해야 한다.

## KST 파티셔닝

원천 `event_time`은 UTC로 보존하고, KST 파생 필드를 만든다.

- `event_time_kst`
- `event_date_kst`
- `event_hour_kst`

저장 partition과 dashboard grouping은 `event_date_kst`와 `event_hour_kst`를 사용한다.

결정 이유:

- UTC를 유지해야 원천 의미와 국제적 처리 기준이 보존된다.
- KST field를 별도로 두면 dashboard query에서 매번 timezone 변환을 반복하지 않는다.

## Checkpoint

- Streaming query별 checkpoint directory를 분리한다.
- checkpoint path에는 layer와 query name을 포함한다.
- checkpoint reset은 replay/backfill 절차와 함께만 수행한다.
- checkpoint와 output table을 같은 lifecycle로 관리한다.

결정 이유:

- checkpoint 공유는 offset/state 충돌을 일으킬 수 있다.
- reset 절차 없이 checkpoint를 삭제하면 중복 처리나 데이터 누락이 발생할 수 있다.

## 복구와 재처리

- 단기 재처리는 Kafka raw topic retention 안에서 수행한다.
- 장기 재처리는 Bronze storage를 기준으로 수행한다.
- DLQ record는 triage 후 retry topic 또는 raw topic으로 replay한다.
- Gold aggregate는 source range와 window 기준으로 재생성 가능해야 한다.

결정 이유:

- Kafka retention만 장기 복구 수단으로 쓰면 비용과 운영 부담이 크다.
- Bronze를 보존하면 parser나 schema 변경 후에도 재처리가 가능하다.

## Phase별 Spark 설계

| Phase | 적용 내용 | 결정 이유 |
| --- | --- | --- |
| Phase 1 | Bronze/Silver/Gold 계약, watermark, checkpoint 정책 문서화 | 구현 전 처리 계층 책임을 고정한다. |
| Phase 2 | replay fixture와 fault scenario로 Spark 입력 기대값 정의 | Spark job 구현 전 테스트 입력을 명확히 한다. |
| Phase 3 | Kafka consumer group과 offset/checkpoint 정책 연결 | Spark 장애 복구가 Kafka 설계와 충돌하지 않게 한다. |
| Phase 4 | Structured Streaming job과 Gold aggregate 구현 설계 확정 | 실제 분석 목표를 지원하는 streaming 처리 흐름을 만든다. |
| Phase 5 | 처리량, 지연, duplicate, late event metric을 monitoring에 연결 | 운영 가능한 데이터 플랫폼인지 검증한다. |

