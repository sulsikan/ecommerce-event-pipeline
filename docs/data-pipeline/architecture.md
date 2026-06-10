# 데이터 파이프라인 아키텍처

## 범위

이 문서는 Kaggle `E-commerce behavior data from multi category store` CSV 데이터를 실시간 이벤트처럼 재생하여 수집, 전처리, 저장, 집계, 시각화까지 이어지는 데이터 플랫폼의 Phase 1~5 설계를 정의한다.

현재 구현 범위는 Phase 5 데이터 품질과 모니터링이다. 로컬 개발 환경에서는 `2019-Oct.csv`를 replay generator로 읽어 Kafka raw topic에 발행하고, Spark Structured Streaming이 Bronze/Silver/Gold Parquet 계층을 생성한 뒤, 품질 규칙과 모니터링 리포트를 생성한다.

## 분석 목표

- 실시간 주문량 모니터링: 시간대별 주문량, 카테고리별 주문량, 주문 급증 감지
- 사용자 행동 로그 분석: `view -> cart -> purchase` funnel, 장바구니 전환율, 구매 전환율
- 이상 거래 탐지: 특정 유저의 짧은 시간 내 구매 폭증, 중복 이벤트, 비정상 구매 패턴

## Phase 정의

| Phase | 목표 | 주요 산출물 |
| --- | --- | --- |
| Phase 1 | CSV replay와 PostgreSQL 로컬 적재 | Replay generator, raw event table, 100건 적재 검증 |
| Phase 2 | Kafka ingestion 경로 도입 | KRaft Kafka, raw topic, producer, consumer, PostgreSQL 적재 검증 |
| Phase 3 | Kafka ingestion 안정화 | KRaft Kafka, raw topic, producer, consumer, PostgreSQL 적재 검증 |
| Phase 4 | Spark Structured Streaming 처리 | Bronze/Silver/Gold, watermark, checkpoint, window aggregation |
| Phase 5 | 운영 관측성과 대시보드 | 지표, 알림, Grafana dashboard, 운영 runbook |

## 전체 흐름

```text
Kaggle CSV
  -> Event Replay Producer
  -> Kafka ecommerce.raw-events
  -> Spark Bronze: raw event 보존
  -> Spark Silver: schema validation, normalization, deduplication
  -> Spark Gold: order volume, category volume, funnel, anomaly feature
  -> Data Quality: rule_results, quarantine_events
  -> Monitoring: metric_events, report, Grafana dashboard and alerts
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
| Phase 1 | CSV replay 결과를 PostgreSQL에 직접 저장해 스키마와 event-time 변환을 검증한다. | Kafka 도입 전 원천 파싱, KST 파생 필드, `event_id` 생성 규칙을 작게 고정한다. |
| Phase 2 | CSV replay producer는 Kafka에만 발행하고, consumer가 PostgreSQL에 저장한다. | replay, broker, storage writer 책임을 분리해 이후 Spark consumer로 자연스럽게 확장한다. |
| Phase 3 | Kafka ingestion 경로를 안정화하고 PostgreSQL 적재 smoke test를 수행한다. | replay, broker, storage writer 책임을 검증한다. |
| Phase 4 | Spark는 Kafka raw topic에서 Bronze부터 Gold까지 event-time streaming query로 처리한다. | processing-time 기반 지표 왜곡을 피하고 replay 속도와 무관한 결과를 얻는다. |
| Phase 5 | 데이터 품질 지표와 운영 지표를 dashboard와 alert로 연결한다. | 파이프라인의 핵심은 모델보다 신뢰성 있는 운영이므로 이상 징후를 가시화해야 한다. |

## Phase 5 산출물

| 산출물 | 경로 | 목적 |
| --- | --- | --- |
| 품질 규칙 결과 | `data/quality-monitoring/data_quality/rule_results` | rule별 failure count, severity, status 보존 |
| Quarantine 후보 | `data/quality-monitoring/data_quality/quarantine_events` | reject/quarantine 대상 원본 payload와 실패 metadata 보존 |
| Metric event | `data/quality-monitoring/monitoring/metric_events` | Prometheus 변환 가능한 운영 지표 저장 |
| Monitoring report | `data/quality-monitoring/monitoring/phase5-report.json` | alert firing 상태와 품질 요약 제공 |
| Grafana dashboard | `configs/grafana/ecommerce-pipeline-dashboard.json` | dashboard import 계약 |
| Alert rule | `configs/alerts/pipeline-alert-rules.yml` | severity, owner, runbook 포함 alert 계약 |

## 에이전트별 설계 책임

| 에이전트 | 책임 | 대상 문서 |
| --- | --- | --- |
| Schema Designer Agent | 원천/표준/계층별 스키마, key, partition, evolution | `schema-guide.md` |
| Event Replay Agent | CSV replay semantics, speed, ordering, fault scenario | `event-replay-guide.md` |
| Kafka Streaming Agent | Topic, key, producer/consumer, offset, retry, DLQ | `kafka-guide.md` |
| Spark Processing Agent | Bronze/Silver/Gold, watermark, checkpoint, aggregation | `spark-guide.md` |

## 미결정 사항

- Phase 2 로컬 Kafka raw topic partition count는 3으로 둔다. 운영 partition count는 실제 replay throughput profiling 이후 재검토한다.
- Watermark 기본값은 30분으로 시작하되 out-of-order 분포 측정 후 조정한다.
- 저장 포맷은 Delta Lake 또는 Iceberg 중 local/dev 환경과 운영 목표를 비교해 결정한다.
- Grafana dashboard와 alert threshold는 Phase 5 load test 이후 baseline 기반으로 확정한다.
