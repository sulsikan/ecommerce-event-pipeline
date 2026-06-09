# 스키마 가이드

## 설계 원칙

Kaggle ecommerce CSV는 분석용 과거 로그이므로 원천 row 자체에는 안정적인 event id가 없다. 따라서 이 프로젝트는 원천 필드를 보존하면서 streaming platform에서 사용할 표준 이벤트 스키마를 별도로 둔다.

| 결정 | 이유 |
| --- | --- |
| 원천 스키마와 표준 이벤트 스키마를 분리한다. | 원천 CSV의 결측과 타입 흔들림을 보존하면서 downstream은 안정적인 계약으로 처리해야 한다. |
| `event_time`은 UTC, 비즈니스 partition은 KST로 둔다. | 원천 timestamp 의미를 잃지 않으면서 한국 시간 기준 dashboard와 partition pruning을 지원한다. |
| `event_id`는 결정적 hash로 만든다. | replay, retry, Spark 재처리, duplicate fault injection에서 같은 이벤트를 같은 id로 식별해야 한다. |
| `user_id`를 Kafka partition key 후보로 둔다. | 전환 funnel과 사용자 구매 폭증 분석은 사용자별 이벤트 순서가 중요하다. |

## 원천 CSV 스키마 / Raw Event Schema

Raw Event Schema는 Kaggle `E-commerce behavior data from multi category store` CSV row를 replay producer가 Kafka에 발행하기 직전의 이벤트 계약으로 정의한다. 원천의 비즈니스 필드는 보존하고, replay 재현성과 downstream 추적을 위한 운영 필드를 추가한다.

| 컬럼 | Datatype | Nullable | Business meaning | Partition strategy |
| --- | --- | --- | --- | --- |
| `event_id` | string | 아니오 | 원천 row와 핵심 이벤트 속성으로 생성한 결정적 이벤트 식별자. replay, retry, duplicate injection, Silver deduplication의 공통 key다. | Kafka message id와 Silver primary key 후보. 저장 partition으로 쓰지 않는다. |
| `event_time` | timestamp | 아니오 | CSV의 원천 이벤트 시각을 UTC로 파싱한 값. 모든 주문량, funnel, 이상 탐지 지표의 event-time 기준이다. | `event_time_kst`, `event_date_kst`, `event_hour_kst` 파생 기준. 직접 partition key로 쓰지 않는다. |
| `event_time_kst` | timestamp | 아니오 | `event_time`을 Asia/Seoul 기준으로 변환한 값. 한국 시간 기준 운영 화면에서 표시할 이벤트 시각이다. | KST business partition 파생 기준. |
| `event_date_kst` | date | 아니오 | KST 기준 이벤트 날짜. 일별 backfill, dashboard filter, 저장 partition pruning 기준이다. | Raw replay manifest와 downstream storage의 기본 날짜 partition 후보. |
| `event_hour_kst` | integer | 아니오 | KST 기준 이벤트 시간대, `0`부터 `23`까지. 시간대별 주문량과 funnel 지표의 기본 grouping이다. | hourly metric partition 또는 bucket 후보. 단독 Kafka key로 쓰지 않는다. |
| `event_type` | string enum | 아니오 | 사용자 행동 유형. 허용값은 `view`, `cart`, `purchase`다. | metric dimension. partition key로 쓰면 사용자 행동 순서가 깨질 수 있어 사용하지 않는다. |
| `product_id` | long | 아니오 | 상품 식별자. 상품 단위 중복/참조 검증과 category join의 기준이다. | 고카디널리티 dimension이므로 storage partition으로 쓰지 않는다. |
| `category_id` | long | 예 | 상품 카테고리 식별자. 일부 원천 row에서 결측 가능하므로 nullable이다. | category metric dimension. partition으로 쓰기보다 Gold 집계 dimension으로 사용한다. |
| `category_code` | string | 예 | `electronics.smartphone` 같은 계층형 카테고리 문자열. 카테고리별 주문량과 dashboard label에 사용한다. | category metric dimension. Gold category 집계에서 선택적 clustering 후보. |
| `brand` | string | 예 | 상품 브랜드명. 브랜드별 품질 조사와 비정상 구매 패턴 분석의 보조 dimension이다. | 보조 dimension. 기본 partition으로 사용하지 않는다. |
| `price` | decimal(18, 2) | 예 | 이벤트 시점의 상품 가격. purchase 매출, 비정상 고액 구매, 가격 range 검증에 사용한다. | metric value. partition으로 사용하지 않는다. |
| `user_id` | long | 아니오 | 사용자 식별자. `view -> cart -> purchase` 순서, 장바구니/구매 전환율, 특정 유저 구매 폭증 탐지의 핵심 기준이다. | Kafka partition key 기본값. storage partition으로는 사용하지 않고 Gold user feature dimension으로 사용한다. |
| `user_session` | string | 예 | 원천 세션 식별자. 세션 funnel과 동일 세션 구매 흐름 검증에 사용한다. | session-level ordering 보조 key. 고카디널리티라 partition으로 쓰지 않는다. |
| `source_file` | string | 아니오 | replay 입력 CSV 파일명 또는 manifest path. 장애 triage와 backfill 재현에 필요하다. | replay audit partition 후보. Kafka/storage business partition으로 쓰지 않는다. |
| `source_row_number` | long | 아니오 | `source_file` 안의 원천 row 번호. 동일 timestamp row의 안정 정렬과 event id 생성에 사용한다. | partition으로 쓰지 않는다. `source_file`과 함께 lineage key로 사용한다. |
| `replay_run_id` | string | 아니오 | replay 실행 식별자. 같은 원천 데이터를 여러 속도나 fault scenario로 재생한 실험을 구분한다. | replay run 단위 audit/report partition 후보. business metric partition으로 쓰지 않는다. |
| `schema_version` | string | 아니오 | Raw event contract version. producer와 consumer의 호환성 검토 기준이다. | schema migration filter로 사용. partition으로 쓰지 않는다. |

## 표준 이벤트 스키마

표준 이벤트 스키마는 Silver Schema의 비즈니스 필드 하위 집합이며 Kafka value, Spark Silver, DLQ envelope, 품질 규칙, 모니터링 지표가 공유하는 이름 계약이다. 필드 의미와 partition 정책은 아래 Silver Schema를 기준으로 한다.

필수 표준 필드는 `event_id`, `event_time`, `event_time_kst`, `ingested_at`, `event_date_kst`, `event_hour_kst`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`, `source_file`, `source_row_number`, `replay_run_id`, `schema_version`이다.

## Event ID 생성

기본 후보:

```text
sha256(
  schema_version,
  source_file,
  source_row_number,
  event_time,
  event_type,
  product_id,
  user_id,
  user_session,
  price
)
```

결정 이유:

- Kaggle CSV에는 고유 event id가 없으므로 플랫폼이 생성해야 한다.
- `source_file`과 `source_row_number`를 포함하면 같은 속성을 가진 반복 행동도 구분할 수 있다.
- replay가 여러 번 실행되어도 같은 원천 row는 같은 `event_id`를 갖는다.
- duplicate fault injection은 동일 `event_id` 재발행으로 명확히 테스트할 수 있다.

주의:

- `source_row_number`만 primary key로 사용하지 않는다.
- `ingested_at`과 `replay_run_id`는 replay마다 달라질 수 있으므로 `event_id` 기본 hash에는 넣지 않는다.
- 원천 파일을 분할하거나 정렬 방식이 바뀌면 `source_row_number` 안정성이 깨질 수 있으므로 Phase 2에서 source manifest를 고정한다.

## Primary Key 후보

- 표준 이벤트 primary key: `event_id`
- Bronze 보조 key: `kafka_topic`, `kafka_partition`, `kafka_offset`
- 원천 추적 key: `source_file`, `source_row_number`
- Gold aggregate key: `metric_name`, `metric_date_kst`, window start/end, metric dimension

결정 이유:

- Kafka offset은 ingestion 순서 식별에는 좋지만 replay source identity가 아니다.
- `event_id`는 Silver deduplication과 DLQ triage에 가장 적합하다.
- Gold는 event 단위가 아니라 window aggregate이므로 별도 복합 key가 필요하다.

## Partition Key

- Kafka partition key: `user_id`
- Bronze storage partition: `event_date_kst`가 파싱 가능하면 사용, 아니면 ingestion date
- Silver storage partition: `event_date_kst`
- Gold storage partition: `metric_date_kst`, `metric_hour_kst`, metric dimension

결정 이유:

- Kafka에서 `user_id`를 key로 두면 사용자별 행동 순서를 보존할 가능성이 높다.
- 저장소 partition은 dashboard와 backfill에서 가장 자주 사용하는 날짜 기준이어야 한다.
- KST 기준 partition을 사용하면 한국 시간 기준 운영 리포트와 저장 경계가 일치한다.

## 계층별 스키마

### Bronze Schema

Bronze는 Kafka에서 수집한 메시지와 파싱 결과를 손실 없이 보존한다. 구조적으로 잘못된 이벤트도 `raw_payload`, Kafka offset, parse 상태를 남겨 재처리와 장애 조사를 가능하게 한다.

| 컬럼 | Datatype | Nullable | Business meaning | Partition strategy |
| --- | --- | --- | --- | --- |
| `bronze_record_id` | string | 아니오 | `kafka_topic`, `kafka_partition`, `kafka_offset` 기반 Bronze 수집 row 식별자. | Bronze table primary key 후보. partition으로 쓰지 않는다. |
| `kafka_topic` | string | 아니오 | 수신 Kafka topic 이름. raw, retry, DLQ 경로 추적에 사용한다. | 운영 filter dimension. partition으로 쓰지 않는다. |
| `kafka_partition` | integer | 아니오 | Kafka partition 번호. offset commit, ordering, lag 조사에 사용한다. | `kafka_offset`과 함께 ingestion uniqueness key. storage partition으로 쓰지 않는다. |
| `kafka_offset` | long | 아니오 | Kafka partition 내 offset. 중복 수집과 재처리 범위 확인 기준이다. | `kafka_partition`과 함께 Bronze uniqueness key. |
| `kafka_timestamp` | timestamp | 아니오 | Kafka broker 또는 producer timestamp, UTC. replay 지연과 broker 수신 시각 분석에 사용한다. | ingestion latency metric 기준. partition으로 쓰지 않는다. |
| `message_key` | string | 예 | Kafka message key. 기본적으로 `user_id` 문자열이며 사용자별 ordering 검증에 사용한다. | Kafka partition key 관측 필드. storage partition으로 쓰지 않는다. |
| `raw_payload` | string | 아니오 | Kafka value 원문 JSON 또는 직렬화된 payload. parser 변경 후 재처리의 원본이다. | partition으로 쓰지 않는다. 원문 보존 필드다. |
| `event_id` | string | 예 | Raw payload에서 파싱한 결정적 이벤트 식별자. 파싱 실패 시 null일 수 있다. | Silver dedup key 후보. Bronze partition으로 쓰지 않는다. |
| `event_time` | timestamp | 예 | Raw payload에서 파싱한 UTC event time. 파싱 실패 또는 형식 오류 시 null이다. | 파싱 성공 시 `event_date_kst` 산출 기준. |
| `event_time_kst` | timestamp | 예 | `event_time`의 KST 변환 값. event time 파싱 실패 시 null이다. | `event_date_kst`, `event_hour_kst` 산출 기준. |
| `event_date_kst` | date | 예 | KST 기준 이벤트 날짜. parse success row의 business date다. | Bronze storage 1차 partition. null이면 `bronze_ingest_date_kst`로 fallback한다. |
| `event_hour_kst` | integer | 예 | KST 기준 이벤트 시간대. 시간대별 ordering/volume 예비 분석에 사용한다. | optional bucket 또는 secondary clustering 후보. |
| `event_type` | string | 예 | 파싱된 행동 유형. enum 검증 전이므로 null 또는 허용 외 값이 존재할 수 있다. | 품질 검증 dimension. partition으로 쓰지 않는다. |
| `product_id` | long | 예 | 파싱된 상품 식별자. type cast 실패 시 null이다. | 품질 검증과 Silver 필드 후보. partition으로 쓰지 않는다. |
| `category_id` | long | 예 | 파싱된 카테고리 식별자. 원천 결측과 cast 실패를 모두 보존한다. | category 품질 조사 dimension. |
| `category_code` | string | 예 | 파싱된 카테고리 경로 문자열. | category metric 후보. 기본 partition으로 쓰지 않는다. |
| `brand` | string | 예 | 파싱된 브랜드명. | 보조 dimension. partition으로 쓰지 않는다. |
| `price` | decimal(18, 2) | 예 | 파싱된 가격. cast 실패, 결측, 음수 여부는 품질 규칙에서 판정한다. | metric value. partition으로 쓰지 않는다. |
| `user_id` | long | 예 | 파싱된 사용자 식별자. cast 실패 시 null이며 Silver 승격 전 필수 검증 대상이다. | Kafka key 검증 기준. Bronze storage partition으로 쓰지 않는다. |
| `user_session` | string | 예 | 파싱된 세션 식별자. | session funnel 후보. 고카디널리티라 partition으로 쓰지 않는다. |
| `source_file` | string | 예 | replay 원천 CSV 파일명. payload 누락 또는 파싱 실패 시 null 가능하다. | replay audit filter. business partition으로 쓰지 않는다. |
| `source_row_number` | long | 예 | 원천 CSV row 번호. | `source_file`과 lineage key. partition으로 쓰지 않는다. |
| `replay_run_id` | string | 예 | replay 실행 식별자. | replay run별 audit/report filter. partition 후보지만 기본 storage partition은 아니다. |
| `schema_version` | string | 예 | payload schema version. 누락 또는 파싱 실패 시 null 가능하다. | schema compatibility filter. |
| `parse_status` | string enum | 아니오 | `parsed`, `failed`, `partial` 중 하나. Silver 승격 가능 여부를 결정한다. | Bronze 품질 partition 보조 후보. 기본은 `event_date_kst`/fallback date를 사용한다. |
| `parse_error_code` | string | 예 | 파싱 실패 유형 코드. 예: `invalid_timestamp`, `invalid_json`, `type_cast_failed`. | DLQ/quarantine 집계 dimension. |
| `parse_error_message` | string | 예 | 장애 조사용 축약 오류 메시지. | partition으로 쓰지 않는다. |
| `ingested_at` | timestamp | 아니오 | Spark Bronze 수집 시각, UTC. 처리 지연과 replay drift 계산에 사용한다. | `bronze_ingest_date_kst` 파생 기준. |
| `bronze_ingest_date_kst` | date | 아니오 | KST 기준 Bronze 수집 날짜. event time 파싱 실패 row의 fallback partition이다. | Bronze storage fallback partition. |

### Silver Schema

Silver는 파싱, 표준화, 필수 품질 검증, deduplication 판정을 통과한 이벤트 계약이다. Gold 집계는 기본적으로 `quality_status = 'valid'`이고 `is_duplicate = false`인 row를 사용하되, 품질/중복 모니터링 지표는 warning과 duplicate row도 참조할 수 있다.

| 컬럼 | Datatype | Nullable | Business meaning | Partition strategy |
| --- | --- | --- | --- | --- |
| `event_id` | string | 아니오 | 결정적 이벤트 식별자. Silver primary key와 deduplication 기준이다. | Silver primary key 후보. partition으로 쓰지 않는다. |
| `event_time` | timestamp | 아니오 | UTC 기준 원천 이벤트 시각. 모든 event-time window 집계의 기준이다. | watermark와 KST partition 파생 기준. |
| `event_time_kst` | timestamp | 아니오 | KST 기준 이벤트 시각. dashboard 표시와 운영 분석 기준이다. | KST date/hour 산출 기준. |
| `event_date_kst` | date | 아니오 | KST 기준 이벤트 날짜. | Silver storage 1차 partition. Gold metric date 파생 기준. |
| `event_hour_kst` | integer | 아니오 | KST 기준 이벤트 시간대. | 시간대별 주문량/funnel 집계 key. secondary clustering 후보. |
| `event_type` | string enum | 아니오 | `view`, `cart`, `purchase` 중 하나로 검증된 행동 유형. | Gold metric dimension. partition으로 쓰지 않는다. |
| `product_id` | long | 아니오 | 검증된 상품 식별자. | 참조 검증과 product-level 분석 key. storage partition으로 쓰지 않는다. |
| `category_id` | long | 예 | 상품 카테고리 식별자. 원천 결측 가능성을 보존한다. | category aggregate dimension. 기본 partition으로 쓰지 않는다. |
| `category_code` | string | 예 | 사람이 읽을 수 있는 계층형 카테고리 경로. | category dashboard dimension. Gold category 집계 key. |
| `category_level_1` | string | 예 | `category_code`의 최상위 카테고리. 카테고리 대분류 주문량 비교에 사용한다. | Gold category rollup dimension. |
| `category_level_2` | string | 예 | `category_code`의 두 번째 계층. | Gold category rollup dimension. |
| `category_level_3` | string | 예 | `category_code`의 세 번째 계층 이하를 필요 시 축약한 값. | 세부 category drill-down dimension. |
| `brand` | string | 예 | 브랜드명. 브랜드 결측률과 특정 브랜드 이상 구매 조사에 사용한다. | 보조 dimension. partition으로 쓰지 않는다. |
| `price` | decimal(18, 2) | 예 | 상품 가격. purchase에서는 주문 금액과 이상 구매 검증의 기준이다. | metric value. partition으로 쓰지 않는다. |
| `user_id` | long | 아니오 | 사용자 식별자. funnel, cart/purchase conversion, user purchase burst feature의 핵심 key다. | Kafka ordering 검증 key와 Gold user feature dimension. Silver storage partition으로 쓰지 않는다. |
| `user_session` | string | 예 | 사용자 세션 식별자. 세션 내 행동 흐름 분석에 사용한다. | session funnel grouping key. 고카디널리티라 partition으로 쓰지 않는다. |
| `source_file` | string | 아니오 | 원천 CSV 파일명. lineage와 재처리 범위 지정에 사용한다. | lineage filter. business partition으로 쓰지 않는다. |
| `source_row_number` | long | 아니오 | 원천 CSV row 번호. 동일 원천 row 추적과 event id 검증에 사용한다. | `source_file`과 lineage key. partition으로 쓰지 않는다. |
| `replay_run_id` | string | 아니오 | replay 실행 식별자. 동일 데이터의 반복 replay, fault scenario, backfill 결과를 분리한다. | replay audit filter. metric partition으로 쓰지 않는다. |
| `schema_version` | string | 아니오 | Silver contract version. | schema compatibility filter. |
| `ingested_at` | timestamp | 아니오 | Bronze 수집 시각, UTC. source delay와 processing delay 계산에 사용한다. | latency metric 파생 기준. |
| `processed_at` | timestamp | 아니오 | Silver 처리 완료 시각, UTC. Spark 처리 지연과 SLA 계산에 사용한다. | operational metric 기준. partition으로 쓰지 않는다. |
| `quality_status` | string enum | 아니오 | `valid`, `warning`, `quarantined` 중 하나. Gold 포함/제외 정책과 품질 dashboard 기준이다. | 품질 집계 dimension. 기본 storage partition으로 쓰지 않는다. |
| `quality_rule_ids` | array<string> | 아니오 | 적용 또는 실패한 품질 규칙 id 목록. 빈 배열 가능하다. | 품질 metric explode dimension. |
| `is_duplicate` | boolean | 아니오 | 같은 `event_id`가 watermark 범위 안에서 재등장했는지 여부. | duplicate summary metric dimension. |
| `duplicate_of_event_id` | string | 예 | duplicate로 판정된 경우 기준이 되는 원본 `event_id`. | 장애 조사 key. partition으로 쓰지 않는다. |

### Gold Metrics Schema

Gold는 event row가 아니라 dashboard, alert, report가 읽는 metric row다. 목적별 물리 table은 나눌 수 있지만 공통 metric contract는 아래 wide schema를 따른다. metric별로 의미 없는 dimension과 metric 값은 null을 허용한다.

| 컬럼 | Datatype | Nullable | Business meaning | Partition strategy |
| --- | --- | --- | --- | --- |
| `metric_id` | string | 아니오 | metric row의 결정적 식별자. `metric_name`, window, dimension 조합으로 생성한다. | Gold primary key 후보. partition으로 쓰지 않는다. |
| `metric_name` | string enum | 아니오 | 지표 이름. 예: `order_volume_hourly`, `order_volume_by_category`, `conversion_funnel_hourly`, `user_purchase_burst`, `duplicate_event_summary`, `abnormal_purchase_pattern`. | metric table 또는 dashboard panel routing key. |
| `metric_date_kst` | date | 아니오 | metric window의 KST 날짜. | Gold storage 1차 partition. |
| `metric_hour_kst` | integer | 예 | hourly metric의 KST hour. 일 단위 또는 긴 window metric에서는 null이다. | Gold hourly partition 또는 clustering 후보. |
| `window_start_kst` | timestamp | 아니오 | KST 기준 metric window 시작 시각. | Gold aggregate key. |
| `window_end_kst` | timestamp | 아니오 | KST 기준 metric window 종료 시각. | Gold aggregate key. |
| `window_grain` | string enum | 아니오 | `5m`, `15m`, `1h`, `1d` 같은 집계 단위. | metric key와 dashboard filter dimension. |
| `category_id` | long | 예 | 카테고리별 주문량 metric의 category dimension. 전체 metric에서는 null이다. | category metric clustering 후보. 기본 partition은 `metric_date_kst`다. |
| `category_code` | string | 예 | 카테고리별 주문량과 drill-down dashboard label. | category metric dimension. |
| `category_level_1` | string | 예 | 대분류 카테고리 rollup. | category rollup dimension. |
| `brand` | string | 예 | 브랜드별 보조 분석 dimension. 기본 주문량 metric에서는 null일 수 있다. | optional clustering dimension. |
| `user_id` | long | 예 | 특정 유저 구매 폭증과 비정상 구매 패턴 metric의 사용자 dimension. | user anomaly metric key. storage partition으로 쓰지 않는다. |
| `event_type` | string | 예 | event type별 count metric에서 사용하는 dimension. funnel row에서는 null일 수 있다. | metric dimension. |
| `view_event_count` | long | 예 | window 내 view 이벤트 수. funnel denominator와 행동량 모니터링에 사용한다. | metric value. partition으로 쓰지 않는다. |
| `cart_event_count` | long | 예 | window 내 cart 이벤트 수. 장바구니 전환율 계산에 사용한다. | metric value. |
| `purchase_event_count` | long | 예 | window 내 purchase 이벤트 수. 주문량과 구매 전환율 계산에 사용한다. | metric value. |
| `unique_view_users` | long | 예 | window 내 view 사용자 수. user-level funnel denominator다. | metric value. |
| `unique_cart_users` | long | 예 | window 내 cart 사용자 수. 장바구니 전환 사용자 수다. | metric value. |
| `unique_purchase_users` | long | 예 | window 내 purchase 사용자 수. 구매 전환 사용자 수다. | metric value. |
| `cart_conversion_rate` | decimal(18, 6) | 예 | `unique_cart_users / unique_view_users`. view에서 cart로 이동한 비율이다. | metric value. |
| `purchase_conversion_rate` | decimal(18, 6) | 예 | `unique_purchase_users / unique_view_users`. view에서 purchase로 이어진 비율이다. | metric value. |
| `cart_to_purchase_conversion_rate` | decimal(18, 6) | 예 | `unique_purchase_users / unique_cart_users`. 장바구니 대비 구매 전환율이다. | metric value. |
| `order_count` | long | 예 | purchase 이벤트 수 기반 주문량. 시간대별/카테고리별 주문량의 핵심 metric이다. | metric value. |
| `gross_revenue` | decimal(18, 2) | 예 | purchase 이벤트의 `price` 합계. 고액 구매와 매출 급증 탐지 보조 metric이다. | metric value. |
| `duplicate_event_count` | long | 예 | duplicate로 판정된 이벤트 수. | duplicate monitoring metric value. |
| `duplicate_event_rate` | decimal(18, 6) | 예 | duplicate 이벤트 비율. | alert threshold metric value. |
| `late_event_count` | long | 예 | watermark 기준 지연 이벤트 수. | latency/quality monitoring metric value. |
| `dq_failed_event_count` | long | 예 | 품질 규칙 실패 또는 quarantine 이벤트 수. | data quality monitoring metric value. |
| `input_event_count` | long | 예 | 해당 metric 계산에 사용된 Silver 입력 이벤트 수. | metric completeness 검증 value. |
| `user_purchase_count` | long | 예 | user window 내 purchase 횟수. 특정 유저 구매 폭증 탐지 기준이다. | user anomaly metric value. |
| `user_purchase_amount` | decimal(18, 2) | 예 | user window 내 purchase 금액 합계. | user anomaly metric value. |
| `burst_score` | decimal(18, 6) | 예 | baseline 대비 구매 폭증 정도를 나타내는 rule 기반 score 후보. | alert threshold metric value. |
| `abnormal_purchase_flag` | boolean | 예 | 비정상 구매 패턴 후보 여부. Phase 1에서는 rule 결과 자리만 정의한다. | alert routing dimension. |
| `metric_quality_status` | string enum | 아니오 | `complete`, `partial`, `delayed` 중 하나. window 지연이나 입력 부족 여부를 표시한다. | dashboard filter와 alert suppression dimension. |
| `replay_run_id` | string | 예 | 특정 replay run 기준으로 계산한 metric임을 표시한다. 운영 통합 metric에서는 null 가능하다. | replay report filter. |
| `schema_version` | string | 아니오 | Gold metric schema version. | schema compatibility filter. |
| `computed_at` | timestamp | 아니오 | metric row 계산 완료 시각, UTC. | freshness metric 기준. partition으로 쓰지 않는다. |

Gold 목적별 table 후보는 다음과 같다.

| Table | Key | 주요 metric |
| --- | --- | --- |
| `gold_order_volume_hourly` | `metric_date_kst`, `metric_hour_kst`, `window_start_kst`, `window_end_kst` | `order_count`, `gross_revenue`, `purchase_event_count` |
| `gold_order_volume_by_category` | `metric_date_kst`, window, `category_code` | `order_count`, `gross_revenue` |
| `gold_conversion_funnel_hourly` | `metric_date_kst`, `metric_hour_kst`, window | `unique_view_users`, `unique_cart_users`, `unique_purchase_users`, conversion rates |
| `gold_user_purchase_burst_features` | window, `user_id` | `user_purchase_count`, `user_purchase_amount`, `burst_score`, `abnormal_purchase_flag` |
| `gold_duplicate_event_summary` | `metric_date_kst`, window | `duplicate_event_count`, `duplicate_event_rate`, `input_event_count` |

결정 이유:

- Dashboard는 event table scan보다 목적별 aggregate table을 읽는 편이 안정적이다.
- anomaly feature는 모델 구현 전에도 rule 기반 탐지와 모니터링에 사용할 수 있다.
- Gold metric partition은 이벤트 발생일과 같은 KST business date를 기준으로 하되, 필드명은 metric row임을 명확히 하기 위해 `metric_date_kst`, `metric_hour_kst`를 사용한다.

## 스키마 진화

| 변경 유형 | 호환성 | 필수 조치 | 이유 |
| --- | --- | --- | --- |
| Nullable 필드 추가 | 호환 | 스키마 문서, Kafka schema, quality observe rule 갱신 | 기존 consumer를 깨지 않는다. |
| Required 필드 추가 | Breaking | schema major version 변경, replay/Kafka/Spark/품질 문서 동시 갱신 | 기존 메시지가 새 필드를 보장하지 않는다. |
| 필드명 변경 | Breaking | 새 필드 추가 후 deprecate 기간 운영 | streaming consumer의 즉시 실패를 방지한다. |
| 타입 확장 | 조건부 | Spark cast와 storage type 검증 | decimal precision 또는 long 범위 확장이 안전한지 확인해야 한다. |
| 필드 삭제 | Breaking | 사용처 제거 확인 후 major version에서 삭제 | Gold 지표와 dashboard가 깨질 수 있다. |

## Phase별 스키마 적용

| Phase | 적용 내용 | 결정 이유 |
| --- | --- | --- |
| Phase 1 | 문서상 schema contract와 validation 기준 확정 | 구현 전 공통 언어를 고정한다. |
| Phase 2 | replay payload가 표준 이벤트 스키마를 따르게 설계 | producer와 downstream 연결 실패를 줄인다. |
| Phase 3 | Kafka key/value schema와 DLQ envelope에 스키마 버전 포함 | consumer 호환성과 장애 triage가 쉬워진다. |
| Phase 4 | Spark Bronze/Silver/Gold schema를 분리 | raw 보존, 검증, 집계 책임을 분리한다. |
| Phase 5 | 품질 지표와 dashboard dimension을 schema field와 연결 | 운영 관측성과 분석 지표가 같은 계약을 사용한다. |
