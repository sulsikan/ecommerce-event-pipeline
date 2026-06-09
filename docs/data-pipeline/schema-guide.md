# 스키마 가이드

## 원천 CSV 스키마

예상 Kaggle 원천 컬럼은 다음과 같다.

| 컬럼 | 타입 | Nullable | 설명 |
| --- | --- | --- | --- |
| `event_time` | timestamp string | 아니오 | 원천 이벤트 timestamp, UTC로 파싱 |
| `event_type` | string enum | 아니오 | `view`, `cart`, `purchase` 중 하나 |
| `product_id` | long | 아니오 | 상품 식별자 |
| `category_id` | long | 예 | 상품 카테고리 식별자 |
| `category_code` | string | 예 | 계층형 카테고리 텍스트 |
| `brand` | string | 예 | 상품 브랜드 |
| `price` | decimal | 조건부 | `purchase`에서는 필수, 모든 이벤트에서 0 이상 기대 |
| `user_id` | long | 아니오 | 사용자 식별자 |
| `user_session` | string | 예 | 원천 세션 식별자 |

## 표준 이벤트 스키마

| 컬럼 | 타입 | Nullable | 설명 |
| --- | --- | --- | --- |
| `event_id` | string | 아니오 | 안정적인 원천 속성 기반 결정적 hash |
| `event_time` | timestamp | 아니오 | UTC 기준 원천 이벤트 timestamp |
| `event_time_kst` | timestamp | 아니오 | KST로 변환한 이벤트 timestamp |
| `ingested_at` | timestamp | 아니오 | UTC 기준 플랫폼 수집 timestamp |
| `event_date_kst` | date | 아니오 | KST 날짜 partition |
| `event_hour_kst` | integer | 아니오 | 시간대별 집계용 KST hour |
| `event_type` | string | 아니오 | `view`, `cart`, `purchase` 중 하나 |
| `product_id` | long | 아니오 | 상품 식별자 |
| `category_id` | long | 예 | 카테고리 식별자 |
| `category_code` | string | 예 | 카테고리 경로 |
| `brand` | string | 예 | 브랜드명 |
| `price` | decimal(18, 2) | 예 | 상품 가격 |
| `user_id` | long | 아니오 | 사용자 식별자 |
| `user_session` | string | 예 | 사용자 세션 식별자 |
| `source_file` | string | 예 | 재생 원천 파일명 |
| `source_row_number` | long | 예 | 파일 내 원천 row 번호 |
| `schema_version` | string | 아니오 | 표준 스키마 버전 |

## Primary Key 후보

- 기본값: `event_id`
- 대안 source fingerprint: `event_time`, `event_type`, `product_id`, `user_id`, `user_session`, `price`, `source_row_number`의 hash
- 여러 파일을 합칠 수 있으므로 `source_row_number`만 단독 key로 사용하지 않는다.

## Partition Key

- Kafka partition key: `user_id`
- Bronze storage partition: 파싱 가능 시 `event_date_kst`, 아니면 ingestion date
- Silver storage partition: `event_date_kst`
- Gold storage partition: `event_date_kst`와 `event_hour_kst`, `category_code` 같은 지표별 dimension

## 스키마 진화

| 변경 유형 | 호환성 | 필수 조치 |
| --- | --- | --- |
| Nullable 필드 추가 | 호환 | 스키마 문서와 품질 warning 갱신 |
| Required 필드 추가 | Breaking | 스키마 version 변경 및 replay, Kafka, Spark, 품질, 모니터링 갱신 |
| 필드명 변경 | Breaking | 호환 mapping 또는 새 schema version 추가 |
| 안전한 타입 확장 | 조건부 | Spark cast와 downstream storage 검증 |
| 필드 삭제 | Breaking | 먼저 deprecate 후 major version에서 삭제 |

## 계층별 스키마

Bronze는 raw Kafka payload, Kafka metadata, parse result, ingestion metadata를 보관한다. Silver는 검증된 표준 이벤트와 품질 상태 필드를 보관한다. Gold는 다음 aggregate table을 포함한다.

- `gold_order_volume_hourly`
- `gold_order_volume_by_category`
- `gold_conversion_funnel_hourly`
- `gold_user_purchase_burst_features`
- `gold_duplicate_event_summary`

