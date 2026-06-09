# 스키마 설계

## 목적

원천 CSV 수집, 표준 이벤트, 스트리밍 계층, 집계 출력, 스키마 진화 규칙을 일관되게 설계한다.

## 사용 시점

컬럼, 데이터 타입, nullable 규칙, partition key, primary key 후보, 호환성 규칙, 스키마 검증 로직을 정의하거나 검토할 때 사용한다.

## 필수 입력

- Kaggle 이커머스 CSV 컬럼 목록
- 주문량, 전환율, 이상 탐지 분석 요구사항
- Bronze, Silver, Gold 저장 계층 목표
- Kafka 메시지 계약 요구사항
- 기존 스키마 가이드

## 절차

1. 원천 컬럼 `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`을 확인한다.
2. 표준 필드 `event_id`, `ingested_at`, `event_date_kst`, `event_hour_kst`, `source_file`, `source_row_number`를 추가한다.
3. 각 필드의 타입, nullable 여부, 설명, 검증 규칙을 정의한다.
4. primary key 후보를 정한다. 기본값은 `event_id`이고, 대안은 원천 이벤트 속성 기반 결정적 해시다.
5. partition key를 정한다. 저장소는 `event_date_kst`, Kafka는 사용자 행동 흐름 보존을 위해 기본적으로 `user_id`를 사용한다.
6. 추가, 변경, 삭제, 폐기 필드에 대한 호환성 규칙을 정의한다.
7. 스키마 문서와 검증 스크립트를 갱신한다.

## 산출물

- `docs/data-pipeline/schema-guide.md`
- `docs/data-pipeline/architecture.md`의 스키마 섹션
- 규칙 변경 시 `scripts/validate-schema.py`

## 검증 체크리스트

- 모든 필드에 타입, nullable 규칙, 설명, 소유자가 있다.
- `event_id` 생성 방식이 결정적이다.
- 타임존 처리 방식이 명시되어 있다.
- KST 파티션 필드가 문서화되어 있다.
- 스키마 변경에는 호환성 검토가 포함되어 있다.

## 안티패턴

- 행 번호만 primary key로 사용한다.
- downstream 동작을 정의하지 않은 nullable 필드를 허용한다.
- 로컬 시스템 타임존을 암묵적으로 사용한다.
- 스키마 필드를 추가하고 Kafka, Spark, 품질, 모니터링 문서를 갱신하지 않는다.

## 예시 프롬프트

`Kaggle 이커머스 CSV를 기준으로 표준 이벤트 스키마와 Bronze, Silver, Gold 스키마를 설계해줘.`

