# Kafka 스트리밍

## 목적

이커머스 이벤트 스트림을 위한 Kafka Topic, Partition, Producer, Consumer, Retry, DLQ, 재처리 방식을 신뢰성 있게 설계한다.

## 사용 시점

Kafka Topic 계약, Partition Key, Producer/Consumer 설정, Offset 전략, Retry 정책, DLQ 설계를 만들거나 검토할 때 사용한다.

## 필수 입력

- 표준 이벤트 스키마
- 이벤트 재생 방식
- 예상 처리량과 재생 속도
- Consumer 애플리케이션과 Spark 작업
- 보존 기간과 복구 요구사항

## 절차

1. Topic 이름 규칙 `<domain>.<entity>.<stage>.<version>`을 정의한다.
2. raw replay topic, retry topic, DLQ topic을 구분한다.
3. Partition Key를 정한다. 사용자 행동 순서와 이상 탐지를 위해 기본값은 `user_id`다.
4. Producer serialization, idempotence, acknowledgement, retry, compression, schema validation 동작을 정의한다.
5. Spark Bronze ingestion, 품질 검증, 모니터링, backfill용 Consumer Group을 정의한다.
6. Offset commit 전략과 복구 방식을 정의한다.
7. DLQ envelope에 원본 payload, error code, error message, failed stage, timestamp를 포함한다.
8. retention, compaction, replay 정책을 문서화한다.

## 산출물

- `docs/data-pipeline/kafka-guide.md`
- Kafka 관련 아키텍처 흐름
- Spark, 품질, 모니터링 문서에서 참조하는 Topic 계약

## 검증 체크리스트

- 모든 Topic에 목적, key, value schema, partition count 기준, retention, owner가 있다.
- Partition Key가 이상 탐지와 전환 분석에 맞다.
- Consumer Group 이름이 고유하고 의미가 있다.
- DLQ 정책에 triage와 replay 경로가 있다.
- 장애와 재시작 시 Offset 처리가 문서화되어 있다.

## 안티패턴

- 전환 분석이 사용자 순서를 필요로 하는데 purchase 이벤트를 product 기준으로 partition한다.
- raw, retry, DLQ를 하나의 Topic에 섞는다.
- durable processing 전에 Offset을 commit한다.
- replay할 수 없는 DLQ payload를 만든다.

## 예시 프롬프트

`이 이벤트 파이프라인의 Kafka Topic, Partition Key, Consumer Group, Retry, DLQ 전략을 설계해줘.`

