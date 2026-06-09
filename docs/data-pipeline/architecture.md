# 데이터 파이프라인 아키텍처

## 범위

이 문서는 Kaggle 이커머스 행동 CSV 데이터를 실시간 이벤트처럼 재생하는 목표 플랫폼 설계를 설명한다. 현재 하네스 단계에서는 플랫폼 문서와 검증 계약만 다루며, 애플리케이션 구현은 범위에서 제외한다.

## 분석 목표

- 시간대별 주문량과 카테고리별 주문량을 실시간으로 모니터링한다.
- 갑작스러운 구매 증가를 감지한다.
- 사용자 행동 funnel `view -> cart -> purchase`를 분석한다.
- 장바구니 전환율과 구매 전환율을 측정한다.
- 특정 유저의 구매 폭증, 중복 이벤트, 비정상 구매 패턴을 탐지한다.

## 전체 흐름

```text
Kaggle CSV
  -> 이벤트 재생 Producer
  -> Kafka Raw Event Topic
  -> Spark Bronze 수집
  -> Spark Silver 검증 및 정규화
  -> Spark Gold 집계 및 이상 탐지 feature
  -> 저장 테이블
  -> Grafana 대시보드와 알림
```

## 핵심 이벤트 계약

표준 이벤트는 다음 안정 필드를 중심으로 설계한다.

- `event_id`: 결정적 이벤트 식별자
- `event_time`: Kaggle 원천 행의 이벤트 timestamp, UTC로 파싱
- `ingested_at`: 플랫폼 수집 timestamp
- `event_type`: `view`, `cart`, `purchase` 중 하나
- `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`: 원천 비즈니스 필드
- `event_date_kst`, `event_hour_kst`: KST 파티션 및 집계 필드

## 저장 계층

| 계층 | 목적 | 신뢰 수준 | 파티셔닝 |
| --- | --- | --- | --- |
| Bronze | Raw Kafka 메시지, Kafka metadata, parse status 보관 | 낮음 | 파싱 가능 시 `event_date_kst`, 아니면 ingestion date |
| Silver | 검증 및 정규화된 표준 이벤트 | 중간 | `event_date_kst` |
| Gold | 대시보드, funnel 지표, 이상 탐지 feature용 집계 | 높음 | `event_date_kst`와 지표별 dimension |

## 신뢰성 설계

- 이벤트 재생은 `event_time`으로 원천 시간 의미를 시뮬레이션한다.
- Kafka는 사용자 행동 순서를 보존하기 위해 `user_id`로 partition한다.
- Spark는 bounded watermark와 `event_id`로 deduplication한다.
- 잘못된 이벤트는 규칙 severity에 따라 DLQ 또는 quarantine으로 보낸다.
- 메트릭은 lag, latency, duplicate rate, null rate, DLQ count, processing failure를 추적한다.

## 에이전트 소유권

| 에이전트 | 주요 산출물 | 교차 검토 대상 |
| --- | --- | --- |
| Schema Designer Agent | `schema-guide.md` | Kafka payload, Spark layer, 품질 규칙 |
| Event Replay Agent | `event-replay-guide.md` | Schema payload, Kafka Producer, 장애 시나리오 |
| Kafka Streaming Agent | `kafka-guide.md` | Replay Producer, Spark Consumer, DLQ |
| Spark Processing Agent | `spark-guide.md` | Schema, 품질 규칙, 모니터링 |
| Data Quality Agent | `data-quality-rules.md` | DLQ, quarantine, metric |
| Monitoring Agent | `monitoring-guide.md` | Kafka, Spark, 품질, 비즈니스 목표 |
| Review Agent | `review-checklist.md` | 전체 일관성 |

## 미결정 사항

- 저장 엔진과 테이블 포맷
- Kafka partition count 산정
- 샘플 데이터 profiling 이후 정확한 watermark 기간
- Dashboard 배포 방식
- 모델 서빙 또는 anomaly scoring 구현 방식

