# Kafka 가이드

## Topic 이름 규칙

`<domain>.<entity>.<stage>.<version>` 형식을 사용한다.

## Topic

| Topic | 목적 | Key | Value | Retention |
| --- | --- | --- | --- | --- |
| `ecommerce.events.raw.v1` | Replay Producer가 발행하는 표준 이벤트 | `user_id` | 표준 이벤트 JSON 또는 Avro | 7일 |
| `ecommerce.events.retry.v1` | 일시적 처리 실패 재시도 이벤트 | `user_id` | DLQ 호환 retry envelope | 3일 |
| `ecommerce.events.dlq.v1` | 잘못되었거나 복구 불가능한 이벤트 | 가능하면 `event_id` | DLQ envelope | 30일 |
| `ecommerce.quality.metrics.v1` | 품질 지표와 규칙 결과 | rule name | 품질 metric event | 30일 |

## Partition Key 전략

Raw와 retry topic은 기본적으로 `user_id`를 사용한다. 이렇게 하면 각 사용자의 `view`, `cart`, `purchase` 행동 흐름이 같은 partition 안에서 순서를 유지하고, 사용자 구매 폭증 탐지에도 유리하다.

DLQ는 가능한 경우 `event_id`를 사용한다. DLQ consumer는 실패 record를 독립적으로 triage하는 경우가 많기 때문이다. `event_id`가 없으면 원본 payload hash를 사용한다.

## Producer 계약

- Publish 전 필수 스키마 필드를 검증한다.
- 결정적 `event_id`를 생성한다.
- 구현 stack이 지원하면 idempotent producer 동작을 활성화한다.
- Broker durable write를 확인할 수 있는 acknowledgement를 사용한다.
- 모든 메시지에 `schema_version`을 포함한다.
- `replay_run_id`, topic, partition, offset, publish result를 로그로 남긴다.

## Consumer Group

| Consumer Group | 목적 | Offset 전략 |
| --- | --- | --- |
| `spark-bronze-ingest` | Raw topic에서 Bronze 수집 | Bronze durable write 후 commit |
| `spark-quality-validator` | 선택적 품질 side stream | 품질 결과 write 후 commit |
| `monitoring-lag-exporter` | Lag와 throughput metric 수집 | metric export 후 commit |
| `dlq-triage-worker` | DLQ 점검과 replay | triage state 저장 후 commit |

## Retry와 DLQ

일시적 실패는 retry count와 원본 metadata를 포함해 `ecommerce.events.retry.v1`로 보낸다. 복구 불가능한 schema 또는 품질 실패는 `ecommerce.events.dlq.v1`로 보낸다.

DLQ envelope 필드:

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

## 운영 규칙

- 잘못된 이벤트를 DLQ 또는 quarantine 없이 버리지 않는다.
- Offset은 durable processing 이후에만 commit한다.
- DLQ replay는 원본 payload와 실패 metadata를 보존해야 한다.
- Topic 변경 시 Spark, 품질, 모니터링, 리뷰 문서를 함께 갱신한다.

