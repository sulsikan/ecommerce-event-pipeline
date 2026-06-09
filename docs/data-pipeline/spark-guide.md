# Spark 가이드

## 처리 모델

Kafka에서 Bronze, Silver, Gold 계층으로 이어지는 event-time 처리는 Spark Structured Streaming을 사용한다.

## Bronze 계층

Bronze는 raw ingestion record를 저장한다.

- Kafka topic, partition, offset, key, timestamp
- Raw payload
- Parse status
- Ingestion timestamp
- 파싱 가능 시 `event_date_kst`

실패 처리:

- Payload parse failure는 quarantine 또는 DLQ로 보낸다.
- Kafka offset은 durable write 이후에만 commit한다.

## Silver 계층

Silver는 검증된 표준 이벤트를 저장한다.

- Schema validation 적용
- Timestamp field 정규화
- KST partition field 생성
- `event_id` 기준 deduplication
- `event_time` 기준 watermark 적용
- Warning용 quality status field 보존

권장 기본값:

- Watermark: 우선 `30 minutes`, replay disorder profiling 후 조정
- Deduplication key: `event_id`
- Partition: `event_date_kst`
- Checkpoint: query별 독립 checkpoint directory

## Gold 계층

Gold table은 분석 목표를 지원한다.

| Gold Table | 목적 | Window |
| --- | --- | --- |
| `gold_order_volume_hourly` | 시간대별 purchase 수 | 1시간 |
| `gold_order_volume_by_category` | 카테고리와 시간대별 purchase 수 | 1시간 |
| `gold_conversion_funnel_hourly` | `view -> cart -> purchase` 전환율 | 1시간 |
| `gold_user_purchase_burst_features` | 사용자 구매 폭증 feature | 5분, 1시간 |
| `gold_duplicate_event_summary` | 중복 이벤트 지표 | 5분 |

## KST 파티셔닝

원천 `event_time`은 UTC다. 이를 `event_time_kst`로 변환한 뒤 다음 필드를 만든다.

- `event_date_kst`
- `event_hour_kst`

비즈니스 날짜 또는 시간대 기준 저장 partition과 dashboard grouping은 KST 필드를 사용한다.

## Checkpoint

- 각 Streaming query는 독립 checkpoint 위치를 가진다.
- 서로 다른 query가 checkpoint directory를 공유하지 않는다.
- checkpoint path에는 query name과 layer를 포함한다.
- 파괴적인 replay test 전 checkpoint reset 절차를 문서화한다.

## 복구

- Raw 이벤트 재처리는 Kafka retention 또는 Bronze storage에서 가능해야 한다.
- Watermark를 초과한 late event는 품질 metric으로 추적한다.
- DLQ record는 triage 후 correction을 거쳐 retry 또는 raw topic으로 replay할 수 있다.

