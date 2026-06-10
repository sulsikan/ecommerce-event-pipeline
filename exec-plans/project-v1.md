# Project V1 실행 계획

## 목표

Kaggle `E-commerce behavior data from multi category store`의 `2019-Oct.csv`를 실시간 이벤트처럼 재생하고, Kafka와 Spark Structured Streaming을 통해 수집, 검증, 집계, 저장, 모니터링까지 이어지는 신뢰 가능한 데이터 플랫폼을 구현한다.

이 계획은 현재 문서 하네스를 기준으로 한 V1 구현 순서다. 구현 전제는 다음과 같다.

- 원본 `event_time`은 UTC로 보존한다.
- 비즈니스 날짜와 시간대는 KST 파생 필드인 `event_date_kst`, `event_hour_kst`, `metric_date_kst`, `metric_hour_kst`를 사용한다.
- Kafka raw topic의 partition key는 `user_id`다.
- Silver deduplication 기준은 `event_id`다.
- 장기 재처리 기준은 Kafka가 아니라 Bronze 저장 계층이다.

## 참조 문서

- `docs/data-pipeline/architecture.md`
- `docs/data-pipeline/schema-guide.md`
- `docs/data-pipeline/event-replay-guide.md`
- `docs/data-pipeline/kafka-guide.md`
- `docs/data-pipeline/spark-guide.md`
- `docs/data-pipeline/data-quality-rules.md`
- `docs/data-pipeline/monitoring-guide.md`
- `docs/data-pipeline/review-checklist.md`

## 핵심 계약

- 원천 `event_time`은 UTC로 보존한다.
- 비즈니스 partition과 dashboard grouping은 KST 파생 필드인 `event_time_kst`, `event_date_kst`, `event_hour_kst`, `metric_date_kst`, `metric_hour_kst`를 사용한다.
- 결정적 `event_id`를 replay, Kafka retry, Spark deduplication, 품질 규칙의 공통 식별자로 사용한다.
- Kafka raw topic partition key는 `user_id`로 둔다.
- Phase 2~3 로컬 raw topic은 `ecommerce.raw-events`를 사용한다. 운영 전환 시 versioned topic 이름 후보는 `ecommerce.events.raw.v1`이다.
- Spark는 Bronze, Silver, Gold 계층을 분리한다.
- Silver는 `event_id` 기준 deduplication과 `event_time` 기준 초기 30분 watermark를 사용한다.
- 품질 실패는 severity에 따라 reject, quarantine, warn, observe로 분리한다.
- 구조적으로 복구 불가능한 이벤트는 DLQ로 보내고, 조사 가능한 이벤트는 quarantine table에 저장한다.
- Gold 지표는 시간대별 주문량, 카테고리별 주문량, funnel 전환율, 사용자 구매 폭증, 중복 이벤트 요약을 지원한다.

## 구현 순서

### Phase 1: 설계 계약 고정

작업:

1. `schema-guide.md`의 Raw, Bronze, Silver, Gold Metrics Schema를 최종 기준으로 확정한다.
2. `event-replay-guide.md`의 `2019-Oct.csv` replay speed, accelerated replay, duplicate, late, failure scenario 계약을 확정한다.
3. `kafka-guide.md`의 raw, retry, DLQ, quality metric, replay audit topic 계약을 확정한다.
4. `spark-guide.md`의 Bronze/Silver/Gold 처리 흐름, watermark, checkpoint, aggregation contract를 확정한다.
5. `data-quality-rules.md`와 `monitoring-guide.md`가 schema, replay, Kafka, Spark 문서의 필드명을 동일하게 사용하는지 검토한다.

예상 산출물:

- 확정된 설계 문서
- 문서 검증 스크립트 통과 로그
- PR 전 리뷰 체크리스트 초안

완료 기준:

- `event_id`, `replay_run_id`, `event_time`, `event_date_kst`, `user_id`, DLQ envelope 필드명이 문서 간 일치한다.
- Phase 2~5 구현자가 문서만 보고 입력, 출력, 실패 처리 방식을 알 수 있다.
- 모든 하네스 검증 스크립트가 통과한다.

검증 방법:

```bash
python3 scripts/validate-schema.py
python3 scripts/validate-data-quality.py
python3 scripts/validate-pipeline-docs.py
python3 scripts/generate-pipeline-report.py
```

### Phase 2: `2019-Oct.csv` 이벤트 재생 구현

작업:

1. `2019-Oct.csv` source manifest를 생성한다.
2. CSV row를 Raw Event Schema로 변환하는 replay producer를 구현한다.
3. 원본 `event_time` 기준 정렬과 동일 timestamp 안정 정렬을 구현한다.
4. `1x`, `10x`, `100x`, `1000x`, `max` replay profile을 구현한다.
5. accelerated replay delay 계산과 throughput cap을 구현한다.
6. duplicate, late, malformed, missing, out-of-order, producer failure, purchase burst scenario를 seed 기반으로 생성한다.
7. replay audit와 replay report를 생성한다.

예상 산출물:

- Replay producer
- Source manifest
- Replay configuration
- Fault scenario configuration
- Replay audit/report output

완료 기준:

- 같은 `2019-Oct.csv`, 같은 manifest, 같은 seed로 replay 결과가 재현된다.
- duplicate event는 동일 `event_id`로 재발행된다.
- late event는 `event_time`을 변경하지 않고 발행 시점만 지연된다.
- replay report에 emitted, skipped, failed, duplicate injected, late injected, malformed injected count가 기록된다.

검증 방법:

- 샘플 CSV fixture로 replay order를 검증한다.
- `max` replay에서 producer failure와 backpressure metric이 기록되는지 확인한다.
- duplicate/late/failure scenario별 downstream 기대 동작을 테스트 케이스로 기록한다.
- replay payload가 Raw Event Schema 필수 필드를 만족하는지 확인한다.

### Phase 3: Spark Structured Streaming 처리 구현

작업:

1. Spark Structured Streaming job을 구현한다.
2. Kafka `ecommerce.raw-events`를 읽어 Bronze raw event 계층을 생성한다.
3. Bronze에서 Silver 표준 이벤트를 파싱, 검증, 정규화한다.
4. Silver에서 `event_id` 기준 deduplication과 `event_time` watermark를 적용한다.
5. Gold 주문량, 카테고리 주문량, funnel, 사용자 구매 폭증 feature 집계를 생성한다.
6. query별 checkpoint와 로컬 Parquet warehouse 경로를 분리한다.

예상 산출물:

- Spark Structured Streaming job
- Bronze/Silver/Gold Parquet output
- Query별 checkpoint directory
- Spark 실행 가이드
- Spark output 검증 스크립트

완료 기준:

- Bronze가 Kafka metadata와 raw payload를 보존한다.
- Silver가 필수 schema validation, KST 파생, `event_id` deduplication을 수행한다.
- Gold가 시간대별 주문량, 카테고리 주문량, funnel 전환율, 사용자 구매 폭증 feature를 생성한다.
- 모든 query가 독립 checkpoint를 가진다.
- Spark job은 `available-now` trigger로 100건 smoke test를 재현할 수 있다.

검증 방법:

- Replay producer로 raw topic에 100건 sample event를 publish한다.
- Spark `available-now` trigger로 Bronze/Silver/Gold output을 생성한다.
- Bronze/Silver row count와 Gold metric row 존재 여부를 확인한다.
- checkpoint 삭제 없이 같은 query를 재실행했을 때 이미 처리한 Kafka offset이 중복 처리되지 않는지 확인한다.

### Phase 4: 데이터 품질과 장애 격리 구현

작업:

1. 스키마, null, duplicate, range, referential integrity, conversion metric 검증 규칙을 구현한다.
2. reject, quarantine, warn, observe severity별 처리 경로를 구현한다.
3. retry topic과 DLQ topic envelope를 구현한다.
4. malformed, late, duplicate event 처리 결과를 품질 지표와 연결한다.
5. backfill과 replay recovery 절차를 정의한다.

예상 산출물:

- Data quality validation jobs 또는 rules
- Quarantine table
- Retry/DLQ envelope contract
- DLQ triage 절차
- Backfill/recovery runbook

완료 기준:

- 모든 reject/quarantine 품질 규칙이 durable failure location으로 연결된다.
- Retry와 DLQ record가 원본 payload와 실패 metadata를 보존한다.
- malformed event가 DLQ 또는 quarantine 경로로 이동한다.
- watermark 초과 late event 처리 정책이 문서와 일치한다.

검증 방법:

- duplicate fault scenario에서 Gold 중복 지표가 증가하고 핵심 metric은 중복 제거 결과를 사용한다.
- late event scenario에서 watermark 이내/초과 동작을 구분해 확인한다.
- DLQ와 quarantine record가 원본 payload와 rule metadata를 포함하는지 확인한다.
- checkpoint 삭제 없이 restart가 가능한지 확인한다.

### Phase 5: 모니터링 구현

작업:

1. replay, Kafka, Spark, data quality, Gold metric 지표를 수집한다.
2. Grafana dashboard와 alert rule을 구성한다.
3. purchase spike, consumer lag, DLQ growth, duplicate rate, data latency alert를 검증한다.

예상 산출물:

- Data quality validation jobs 또는 rules
- Quarantine table
- DLQ triage 절차
- Metrics exporter 또는 metric event stream
- Grafana dashboard
- Alert rules
- 운영 runbook

완료 기준:

- 모든 reject/quarantine 품질 규칙이 durable failure location으로 연결된다.
- Kafka Consumer Lag, DLQ count, processing latency, duplicate rate, late event count가 dashboard에 표시된다.
- purchase spike와 user purchase burst alert가 테스트 scenario에서 발생한다.
- alert마다 owner와 runbook이 있다.

검증 방법:

- fault scenario replay로 품질 규칙별 metric 증가를 확인한다.
- DLQ와 quarantine record가 원본 payload와 rule metadata를 포함하는지 확인한다.
- Grafana dashboard가 event-time metric과 processing-time metric을 구분하는지 확인한다.
- alert threshold를 load test baseline 기준으로 조정한다.

## 예상 산출물 요약

| 구분 | 산출물 |
| --- | --- |
| 설계 | Architecture, Schema, Replay, Kafka, Spark, Quality, Monitoring 문서 |
| Replay | `2019-Oct.csv` source manifest, replay producer, replay report, fault scenario config |
| Kafka | Topic, producer/consumer contract, consumer groups, retry/DLQ envelope |
| Spark | Bronze/Silver/Gold tables, streaming queries, checkpoints, recovery 절차 |
| Quality | 품질 규칙, quarantine, DLQ routing, conversion 검증 |
| Monitoring | Metrics, logs, Grafana dashboard, alert rules, runbooks |
| Review | 검증 로그, 리뷰 체크리스트, PR 준비 요약 |

## 완료 기준

V1은 다음 조건을 모두 만족하면 완료로 본다.

- `2019-Oct.csv`를 재현 가능한 방식으로 replay할 수 있다.
- Kafka raw topic에서 Spark Bronze까지 이벤트가 손실 없이 수집된다.
- Silver가 schema validation, KST 파생, deduplication, 품질 상태를 처리한다.
- Gold가 주문량, 카테고리 주문량, conversion funnel, anomaly feature, duplicate summary metric을 생성한다.
- duplicate, late, malformed, missing, purchase burst scenario가 각각 기대한 품질/모니터링 결과로 이어진다.
- 주요 운영 지표와 alert가 dashboard에서 확인된다.
- 검증 스크립트와 PR 체크리스트가 통과한다.

## 리스크

| 리스크 | 영향 | 완화 | 검증 |
| --- | --- | --- | --- |
| `event_id` 생성 기준 불안정 | replay마다 dedup 결과가 달라짐 | source manifest와 `source_file`, `source_row_number` 고정 | 동일 seed replay에서 `event_id` set 비교 |
| `2019-Oct.csv` 대용량 처리 병목 | replay 또는 Spark 처리 지연 | chunk read, throughput cap, `max` replay와 backpressure metric | replay throughput test |
| Kafka partition skew | 특정 `user_id` 집중으로 consumer lag 증가 | key skew metric 수집, partition count 재검토 | consumer lag와 key distribution 분석 |
| Watermark가 너무 짧음 | 정상 late event가 drop 또는 quarantine됨 | late scenario로 분포 측정 후 조정 | watermark 이내/초과 late test |
| Storage format 미확정 | 재처리, upsert, partition 관리 방식 지연 | Delta Lake/Iceberg 후보를 Phase 4 진입 전 결정 | storage PoC와 backfill dry run |
| Gold metric 정의 불일치 | dashboard 지표 신뢰도 저하 | Gold schema와 Spark aggregation contract 고정 | fixture 기반 metric expected value 비교 |
| DLQ replay 불가 | 실패 데이터 복구 불가 | 원본 payload, offset, error metadata 보존 | DLQ replay dry run |
| Alert noise | 운영자가 알림을 신뢰하지 않음 | Phase 5 baseline 기반 threshold 조정 | load test와 alert firing 검토 |
| Replay producer가 집계 책임을 침범 | downstream 검증 의미 약화 | producer 책임을 표준 이벤트 생성, publish, audit, fault injection으로 제한 | replay output에 aggregate 필드가 없는지 확인 |
| 문서와 구현 불일치 | Review와 운영 혼선 | PR 전 Review Agent 체크리스트 수행 | 문서/코드 계약 비교 |

## 검증 방법

### 문서 하네스 검증

```bash
python3 scripts/validate-schema.py
python3 scripts/validate-data-quality.py
python3 scripts/validate-pipeline-docs.py
python3 scripts/generate-pipeline-report.py
```

### 구현 검증

- Schema validation: Raw, Bronze, Silver, Gold Metrics Schema 필수 필드 확인
- Replay validation: `2019-Oct.csv` sample replay, speed profile, fault scenario 재현성 확인
- Kafka validation: topic publish/consume, offset recovery, retry/DLQ routing 확인
- Spark validation: Bronze raw 보존, Silver dedup, Gold aggregation expected value 확인
- Quality validation: null, duplicate, range, referential integrity, conversion rate rule 확인
- Monitoring validation: consumer lag, DLQ count, processing latency, duplicate rate, purchase spike alert 확인

## Review Agent 게이트

- 스키마 게이트: required field, nullable, datatype, partition field가 문서 간 충돌하지 않아야 한다.
- Replay 게이트: `event_time`, `source_file`, `source_row_number` 안정 정렬과 deterministic fault injection이 정의되어야 한다.
- Kafka 게이트: topic, key, retention, consumer group, retry/DLQ envelope가 downstream 처리와 맞아야 한다.
- Spark 게이트: Bronze/Silver/Gold 책임이 섞이지 않고 watermark, checkpoint, deduplication 정책이 명확해야 한다.
- 품질 게이트: 모든 reject/quarantine 규칙은 실패 record와 metric을 남겨야 한다.
- 모니터링 게이트: event-time metric과 processing-time metric을 dashboard와 alert에서 구분해야 한다.
- Git 게이트: `main`에 직접 커밋하지 않고, 사용자 승인 전 커밋/머지를 하지 않는다.

## PR 전 리뷰 게이트

- [ ] 현재 브랜치가 `feature/*` 또는 `develop` 기반 작업 브랜치다.
- [ ] 변경 파일 요약을 작성했다.
- [ ] 자체 리뷰를 완료했다.
- [ ] 검증 스크립트가 통과했다.
- [ ] `docs/data-pipeline/review-checklist.md`를 확인했다.
- [ ] 사용자 승인 전 커밋하지 않았다.
- [ ] feature 작업은 `develop`으로 머지한다.
