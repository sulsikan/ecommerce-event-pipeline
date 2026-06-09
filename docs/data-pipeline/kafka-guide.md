# Kafka 가이드

## 설계 목표

Kafka 계층은 replay producer와 Spark processing 사이에서 내구성 있는 이벤트 버퍼, 소비자 분리, 장애 격리, 재처리 경로를 제공한다. 이 프로젝트의 핵심은 분석 모델보다 신뢰 가능한 실시간 플랫폼이므로 Topic, Partition, Offset, Retry, DLQ 계약을 명확히 둔다.

## Topic 이름 규칙

`<domain>.<entity>.<stage>.<version>` 형식을 사용한다.

결정 이유:

- domain과 entity가 있어 topic 목적을 빠르게 알 수 있다.
- stage와 version이 있어 raw/retry/DLQ와 schema 변경을 분리할 수 있다.
- consumer가 topic 이름만 보고 신뢰 수준을 추론할 수 있다.

## Topic 설계

| Topic | 목적 | Key | Value | Retention | 결정 이유 |
| --- | --- | --- | --- | --- | --- |
| `ecommerce.events.raw.v1` | Replay Producer가 발행하는 표준 이벤트 | `user_id` | 표준 이벤트 | 7일 | Spark 장애 시 Kafka에서 단기 재처리가 가능해야 한다. |
| `ecommerce.events.retry.v1` | 일시적 처리 실패 재시도 이벤트 | `user_id` | retry envelope | 3일 | raw와 재시도를 분리해 정상 stream 오염을 막는다. |
| `ecommerce.events.dlq.v1` | 잘못되었거나 복구 불가능한 이벤트 | 가능하면 `event_id` | DLQ envelope | 30일 | 장애 분석은 시간이 걸리므로 raw보다 긴 보존이 필요하다. |
| `ecommerce.quality.metrics.v1` | 품질 규칙 결과와 metric event | `rule_id` | quality metric event | 30일 | 품질 상태를 streaming으로 dashboard에 연결한다. |
| `ecommerce.replay.audit.v1` | replay run 시작/종료/audit event | `replay_run_id` | replay audit event | 30일 | replay 재현성과 누락률 검증에 필요하다. |

## Partition Key 전략

Raw와 retry topic은 기본적으로 `user_id`를 사용한다.

결정 이유:

- `view -> cart -> purchase` funnel은 사용자별 순서가 중요하다.
- 특정 유저의 짧은 시간 내 purchase burst를 탐지하려면 같은 사용자의 이벤트가 같은 partition에 모이는 것이 유리하다.
- `product_id`나 `category_code`로 partition하면 카테고리 집계에는 유리하지만 사용자 행동 흐름이 분산된다.

예외:

- DLQ는 가능한 경우 `event_id`를 key로 사용한다.
- `event_id`가 없으면 원본 payload hash를 사용한다.
- quality metric topic은 `rule_id`를 key로 사용해 규칙별 처리와 dashboard grouping을 쉽게 한다.

## Producer 계약

Replay Producer는 다음 계약을 따른다.

- Kafka key는 `user_id` 문자열이다.
- Value는 표준 이벤트 스키마를 따른다.
- 모든 메시지에 `schema_version`과 `replay_run_id`를 포함한다.
- publish 전 required field와 enum을 검증한다.
- producer가 지원하면 idempotence를 켠다.
- acknowledgement는 broker durable write를 확인할 수 있는 수준으로 둔다.
- publish 실패는 retry 가능한 오류와 불가능한 오류로 분류해 log와 metric으로 남긴다.

결정 이유:

- key/value 계약이 명확해야 Spark가 안정적으로 schema를 적용할 수 있다.
- idempotent producer와 deterministic `event_id`를 함께 사용하면 중복 발행이 발생해도 downstream deduplication이 가능하다.
- `replay_run_id`는 실험성 replay와 운영 replay를 구분하는 핵심 metadata다.

## Consumer Group

| Consumer Group | 목적 | Offset 전략 | 결정 이유 |
| --- | --- | --- | --- |
| `spark-bronze-ingest` | Raw topic에서 Bronze 수집 | Bronze durable write 후 commit | Kafka message 손실 없이 재시작해야 한다. |
| `spark-silver-quality` | Silver validation과 품질 상태 생성 | Silver/quarantine write 후 commit | 품질 실패도 처리 결과로 남겨야 한다. |
| `spark-gold-aggregation` | Gold 집계 생성 | Gold write/checkpoint 완료 후 commit | dashboard 지표 중복과 누락을 줄인다. |
| `monitoring-lag-exporter` | Lag와 throughput metric 수집 | metric export 후 commit | 운영 metric 수집을 processing job과 분리한다. |
| `dlq-triage-worker` | DLQ 점검과 replay 준비 | triage state 저장 후 commit | DLQ record 처리 이력을 잃지 않는다. |

## Offset Commit 정책

- Consumer는 처리 결과가 durable storage 또는 checkpoint에 기록된 뒤 offset을 commit한다.
- Spark Structured Streaming은 checkpoint를 offset 관리의 기준으로 둔다.
- 장애 복구 시 마지막 committed offset 또는 checkpoint부터 재처리한다.
- 재처리로 인한 중복은 Silver `event_id` deduplication에서 제거한다.

결정 이유:

- At-least-once 처리를 기본으로 두고, idempotent/dedup 설계로 결과 중복을 제어하는 편이 데이터 손실보다 안전하다.
- Exactly-once를 모든 외부 시스템에 강제하면 설계가 과도하게 복잡해진다.

## Retry와 DLQ

일시적 실패는 retry topic으로 보내고, 구조적으로 복구 불가능한 실패는 DLQ로 보낸다.

Retry envelope:

- `retry_id`
- `event_id`
- `original_topic`
- `original_partition`
- `original_offset`
- `retry_count`
- `first_failed_at`
- `last_failed_at`
- `error_code`
- `original_payload`

DLQ envelope:

- `dlq_id`
- `event_id`
- `original_topic`
- `original_partition`
- `original_offset`
- `failed_stage`
- `error_code`
- `error_message`
- `original_payload`
- `failed_at`
- `schema_version`
- `replay_run_id`

결정 이유:

- Retry와 DLQ를 분리해야 일시 장애와 데이터 결함을 다른 운영 절차로 처리할 수 있다.
- 원본 payload와 offset metadata가 있어야 수정 후 replay하거나 원인 분석할 수 있다.

## Retention과 Backfill

- Raw topic retention 기본값은 7일로 둔다.
- DLQ와 quality metric은 30일로 둔다.
- 장기 재처리는 Kafka가 아니라 Bronze storage를 기준으로 한다.
- `max` replay 또는 source manifest 기반 replay는 backfill 절차로 분리해 기록한다.

결정 이유:

- Kafka는 streaming buffer이지 장기 lake storage가 아니다.
- Bronze를 장기 재처리 기준으로 두면 Kafka retention 비용과 운영 부담을 줄일 수 있다.

## Phase별 Kafka 설계

| Phase | 적용 내용 | 결정 이유 |
| --- | --- | --- |
| Phase 1 | Topic, key, consumer group, DLQ 계약 문서화 | 구현 전 producer와 consumer 사이의 계약을 고정한다. |
| Phase 2 | Replay Producer가 raw topic 계약을 따르도록 설계 | CSV replay를 실제 streaming input으로 전환한다. |
| Phase 3 | Kafka topic 생성, producer/consumer 설정, retry/DLQ 설계 확정 | 스트리밍 기반의 신뢰성을 검증한다. |
| Phase 4 | Spark consumer group과 checkpoint/offset 정책 연결 | 처리 장애 후 재시작과 재처리를 안정화한다. |
| Phase 5 | consumer lag, DLQ count, throughput metric을 dashboard와 alert로 연결 | 운영 가능한 플랫폼인지 판단할 수 있다. |

## 운영 규칙

- 잘못된 이벤트를 DLQ 또는 quarantine 없이 버리지 않는다.
- Topic 변경은 schema, replay, Spark, data quality, monitoring 문서 변경과 함께 수행한다.
- Partition count 변경은 ordering과 key skew 영향을 검토한 뒤 진행한다.
- DLQ replay는 원본 payload와 실패 metadata를 유지해야 한다.

