# 모니터링 가이드

## 모니터링 목표

- 수집 실패와 replay drift를 감지한다.
- Kafka Consumer Lag와 DLQ 증가를 감지한다.
- Spark query failure, checkpoint recovery, processing latency를 감지한다.
- 데이터 지연, 중복, 누락 패턴, 품질 규칙 실패를 감지한다.
- 주문량, 카테고리 주문량, 전환 funnel, 이상 신호를 표시한다.

## 구조화 로그

각 단계는 다음 필드를 로그로 남겨야 한다.

- `stage`
- `run_id` 또는 `query_id`
- 적용 가능한 경우 `event_id`
- 적용 가능한 경우 `topic`, `partition`, `offset`
- `event_time`
- `ingested_at`
- `severity`
- `error_code`
- `message`

## 메트릭

| Metric | Type | Labels | 목적 |
| --- | --- | --- | --- |
| `pipeline_events_in_total` | counter | stage, event_type | 입력 처리량 |
| `pipeline_events_out_total` | counter | stage, event_type | 출력 처리량 |
| `pipeline_processing_latency_seconds` | histogram | stage | 처리 latency |
| `pipeline_event_lag_seconds` | histogram | stage | `ingested_at - event_time` |
| `kafka_consumer_lag` | gauge | consumer_group, topic, partition | Kafka lag |
| `kafka_dlq_events_total` | counter | error_code, failed_stage | DLQ 증가 |
| `spark_query_failures_total` | counter | query_name | Spark failure |
| `spark_checkpoint_recoveries_total` | counter | query_name | Recovery event |
| `dq_duplicate_event_id_total` | counter | stage | 중복 이벤트 |
| `dq_schema_required_failures_total` | counter | stage | 필수 필드 실패 |
| `business_purchase_count` | counter | category_code | 구매 수 |
| `business_conversion_rate` | gauge | funnel_step, window | Funnel 전환율 |

## 알림 룰

| 알림 | 조건 | Severity | Runbook |
| --- | --- | --- | --- |
| KafkaConsumerLagHigh | `kafka_consumer_lag`가 10분 동안 높게 유지 | critical | Spark query health와 broker throughput 확인 |
| DLQGrowthHigh | DLQ event rate가 5분 동안 baseline 초과 | critical | `error_code`와 replay source 확인 |
| SparkQueryFailed | Production query 실패 | critical | Checkpoint와 최신 배포 확인 |
| DataLatencyHigh | p95 `pipeline_event_lag_seconds`가 목표 초과 | warning | Replay speed와 downstream lag 확인 |
| DuplicateRateHigh | Duplicate rate가 threshold 초과 | warning | Replay fault scenario와 `event_id` 생성 확인 |
| PurchaseSpikeDetected | Purchase count가 rolling baseline 초과 | warning | Anomaly dashboard와 source replay 확인 |

## Grafana 대시보드

Dashboard section:

- Replay Producer 상태: event emitted, replay speed, producer failure
- Kafka 상태: broker throughput, consumer lag, retry topic size, DLQ count
- Spark 상태: input rows per second, processed rows per second, query failure, checkpoint recovery
- 데이터 품질: null count, duplicate count, schema failure, quarantine count
- 비즈니스 지표: 시간대별 purchase count, 카테고리별 purchase count, conversion funnel, purchase burst user

## 리뷰 요구사항

- 모든 critical alert에는 owner와 runbook이 있어야 한다.
- 모든 reject 또는 quarantine 품질 규칙은 metric을 방출해야 한다.
- Dashboard는 event-time metric과 processing-time metric을 구분해야 한다.
- Alert threshold는 replay load test 이후 재검토해야 한다.

