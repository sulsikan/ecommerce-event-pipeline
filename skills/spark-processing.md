# Spark 처리

## 목적

Spark Structured Streaming을 사용해 Bronze, Silver, Gold 처리 계층과 신뢰할 수 있는 event-time 처리를 설계한다.

## 사용 시점

Streaming ingestion, deduplication, watermark, checkpoint, window aggregation, KST 파티셔닝, 저장 계층 계약을 설계할 때 사용한다.

## 필수 입력

- Kafka Topic 계약
- 표준 이벤트 스키마
- 데이터 품질 규칙
- 모니터링 지표
- 저장 포맷과 테이블 이름 규칙

## 절차

1. Kafka에서 표준 이벤트를 읽어 Bronze 계층에 적재한다.
2. raw payload, Kafka metadata, ingestion metadata, parsing status를 저장한다.
3. 이벤트를 검증하고 정규화하여 Silver 계층에 저장한다.
4. `event_id`와 bounded watermark로 deduplication을 수행한다.
5. 이벤트 timestamp를 KST 파티션 필드로 변환한다.
6. 주문량, 카테고리 주문량, 전환 funnel, 이상 탐지 feature를 Gold 집계로 만든다.
7. Streaming query별 checkpoint 위치를 설정한다.
8. recovery, backfill, replay 동작을 정의한다.

## 산출물

- `docs/data-pipeline/spark-guide.md`
- Bronze, Silver, Gold 계층 계약
- 대시보드용 집계 정의

## 검증 체크리스트

- 모든 Streaming query에 checkpoint 위치가 있다.
- Watermark 기간이 문서화되어 있다.
- Deduplication key가 스키마와 품질 규칙과 일치한다.
- KST 파티셔닝이 계층 전체에서 일관된다.
- Gold 출력이 분석 목표와 연결된다.

## 안티패턴

- 모든 비즈니스 지표에 processing time을 사용한다.
- 서로 다른 query가 checkpoint directory를 공유한다.
- bounded state 정책 없이 deduplication한다.
- 신뢰할 수 없는 raw invalid event를 Gold 테이블에 섞는다.

## 예시 프롬프트

`Spark Structured Streaming 기준으로 Bronze, Silver, Gold 처리 흐름과 watermark, checkpoint, window aggregation 설계를 작성해줘.`

