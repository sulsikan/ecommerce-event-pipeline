# 데이터 파이프라인 아키텍처

## 범위

이 문서는 Kaggle `E-commerce behavior data from multi category store` CSV 데이터를 실시간 이벤트처럼 재생하여 수집, 전처리, 저장, 집계, 시각화까지 이어지는 데이터 플랫폼의 Phase 1~5 설계를 정의한다.

현재 작업 범위는 설계 문서 작성이다. 애플리케이션 코드, 인프라 코드, Spark job, Kafka producer/consumer 구현은 포함하지 않는다.

## 분석 목표

- 실시간 주문량 모니터링: 시간대별 주문량, 카테고리별 주문량, 주문 급증 감지
- 사용자 행동 로그 분석: `view -> cart -> purchase` funnel, 장바구니 전환율, 구매 전환율
- 이상 거래 탐지: 특정 유저의 짧은 시간 내 구매 폭증, 중복 이벤트, 비정상 구매 패턴

## Phase 정의

| Phase | 목표 | 주요 산출물 |
| --- | --- | --- |
| Phase 1 | 하네스와 설계 확정 | 문서, 스키마 계약, 검증 기준 |
| Phase 2 | CSV 이벤트 재생 설계와 로컬 재생 준비 | Replay contract, fault scenario, replay metadata |
| Phase 3 | Kafka 스트리밍 기반 설계 | Topic, partition key, consumer group, retry, DLQ |
| Phase 4 | Spark Structured Streaming 처리 설계 | Bronze/Silver/Gold, watermark, checkpoint, window aggregation |
| Phase 5 | 데이터 품질과 관측성 설계 | 품질 규칙, 지표, 알림, 대시보드 연결 |

## 전체 흐름

```text
Kaggle CSV
  -> Event Replay Producer
  -> ecommerce.events.raw.v1
  -> Spark Bronze: raw event 보존
  -> Spark Silver: schema validation, normalization, deduplication
  -> Spark Gold: order volume, category volume, funnel, anomaly feature
  -> Storage tables
  -> Grafana dashboards and alerts
```

## 핵심 설계 결정

| 결정 | 내용 | 이유 |
| --- | --- | --- |
| Event-time 중심 처리 | 모든 비즈니스 지표는 `event_time` 기준으로 계산한다. | Kaggle 데이터는 과거 로그이며 replay 속도와 처리 지연이 바뀌어도 분석 결과가 안정적이어야 한다. |
| KST 비즈니스 파티션 | UTC `event_time`에서 `event_time_kst`, `event_date_kst`, `event_hour_kst`를 파생한다. | 사용자는 Asia/Seoul 기준 시간대별 주문량을 보게 되며, 저장 partition과 dashboard grouping 기준을 맞춰야 한다. |
| 사용자 기준 Kafka partition | Raw event topic의 partition key는 `user_id`로 둔다. | `view -> cart -> purchase` 순서와 사용자 구매 폭증 탐지는 같은 사용자의 이벤트 순서 보존이 중요하다. |
| 결정적 `event_id` | 원천 필드 기반 hash로 `event_id`를 생성한다. | Kafka 재시도, Spark 재처리, fault injection 중에도 동일 이벤트를 안정적으로 deduplication해야 한다. |
| Bronze/Silver/Gold 분리 | Raw 보존, 검증된 이벤트, 분석용 집계를 계층화한다. | 장애 분석, 재처리, 품질 격리, dashboard 안정성을 동시에 만족하기 위해 신뢰 수준을 분리한다. |
| DLQ와 quarantine 병행 | 구조적으로 처리 불가능한 이벤트는 DLQ, 조사 가능한 이벤트는 quarantine으로 보낸다. | 운영자는 손실 없이 실패를 추적해야 하며, 복구 가능한 데이터와 불가능한 데이터를 구분해야 한다. |

## 표준 이벤트 계약

표준 이벤트는 다음 필드를 모든 하위 설계의 공통 계약으로 사용한다.

- `event_id`: 결정적 이벤트 식별자
- `event_time`: 원천 이벤트 timestamp, UTC
- `event_time_kst`: KST로 변환한 이벤트 timestamp
- `ingested_at`: 플랫폼 수집 timestamp, UTC
- `event_date_kst`, `event_hour_kst`: KST partition 및 집계 필드
- `event_type`: `view`, `cart`, `purchase`
- `product_id`, `category_id`, `category_code`, `brand`, `price`
- `user_id`, `user_session`
- `source_file`, `source_row_number`, `schema_version`, `replay_run_id`

## 저장 계층

| 계층 | 목적 | 저장 내용 | 신뢰 수준 | Partition |
| --- | --- | --- | --- | --- |
| Bronze | 원본 보존과 재처리 | Kafka metadata, raw payload, parse status, replay metadata | 낮음 | `event_date_kst` 가능 시 사용, 아니면 ingestion date |
| Silver | 검증 및 정규화 | 표준 이벤트, 품질 상태, dedup 결과 | 중간 | `event_date_kst` |
| Gold | 분석과 시각화 | 주문량, funnel, 이상 탐지 feature aggregate | 높음 | `event_date_kst`, `event_hour_kst`, 지표별 dimension |

## Phase별 아키텍처 적용

| Phase | 적용 범위 | 설계 이유 |
| --- | --- | --- |
| Phase 1 | 문서와 검증 스크립트로 계약을 고정한다. | 구현 전에 schema, replay, Kafka, Spark 계약을 맞춰야 나중에 재작업을 줄인다. |
| Phase 2 | CSV replay는 Kafka producer 역할만 수행하고 분석 계산을 하지 않는다. | replay와 processing 책임을 분리해야 fault injection과 downstream 검증이 명확하다. |
| Phase 3 | Kafka는 raw, retry, DLQ topic을 분리한다. | 실패 유형별 보존 기간과 소비자를 다르게 운영할 수 있다. |
| Phase 4 | Spark는 Bronze부터 Gold까지 event-time streaming query로 처리한다. | processing-time 기반 지표 왜곡을 피하고 replay 속도와 무관한 결과를 얻는다. |
| Phase 5 | 데이터 품질 지표와 운영 지표를 dashboard와 alert로 연결한다. | 파이프라인의 핵심은 모델보다 신뢰성 있는 운영이므로 이상 징후를 가시화해야 한다. |

## 에이전트별 설계 책임

| 에이전트 | 책임 | 대상 문서 |
| --- | --- | --- |
| Schema Designer Agent | 원천/표준/계층별 스키마, key, partition, evolution | `schema-guide.md` |
| Event Replay Agent | CSV replay semantics, speed, ordering, fault scenario | `event-replay-guide.md` |
| Kafka Streaming Agent | Topic, key, producer/consumer, offset, retry, DLQ | `kafka-guide.md` |
| Spark Processing Agent | Bronze/Silver/Gold, watermark, checkpoint, aggregation | `spark-guide.md` |

## 미결정 사항

- Kafka partition count는 실제 replay throughput profiling 이후 결정한다.
- Watermark 기본값은 30분으로 시작하되 out-of-order 분포 측정 후 조정한다.
- 저장 포맷은 Delta Lake 또는 Iceberg 중 local/dev 환경과 운영 목표를 비교해 결정한다.
- Grafana dashboard와 alert threshold는 Phase 5 load test 이후 baseline 기반으로 확정한다.

