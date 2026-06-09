# 데이터 품질

## 목적

스트리밍 이커머스 이벤트와 파생 지표의 검증 규칙과 실패 처리 방식을 정의한다.

## 사용 시점

null, duplicate, range, referential integrity, schema, conversion rate, anomaly validation 규칙을 작성하거나 검토할 때 사용한다.

## 필수 입력

- 표준 이벤트 스키마
- Kafka DLQ 계약
- Spark 처리 설계
- 모니터링 알림 요구사항
- 비즈니스 지표 정의

## 절차

1. 검증 규칙을 reject, quarantine, warn, observe 심각도로 분류한다.
2. 필수 필드, 타입, enum, timestamp parsing 스키마 검증을 정의한다.
3. identifier, event type, event time, price에 대한 null 검사를 정의한다.
4. `event_id`와 source fingerprint 기반 duplicate 검사를 정의한다.
5. `price`, timestamp lateness, replay skew에 대한 range 검사를 정의한다.
6. user-session, product-category 일관성에 대한 referential 검사를 정의한다.
7. 전환율과 aggregate count 검증을 정의한다.
8. DLQ, quarantine table, alert, quality report로 실패 처리를 정의한다.

## 산출물

- `docs/data-pipeline/data-quality-rules.md`
- 갱신된 검증 스크립트 기대값
- 모니터링 문서에서 참조하는 품질 지표 이름

## 검증 체크리스트

- 모든 규칙에 severity, owner, failure action, metric name이 있다.
- reject 규칙은 DLQ 또는 quarantine 동작과 연결된다.
- warn 규칙은 dashboard 또는 alert와 연결된다.
- 전환율 검증은 denominator 처리 방식을 정의한다.
- duplicate 검사가 Spark deduplication 로직과 일치한다.

## 안티패턴

- invalid event를 durable failed-record 위치 없이 로그로만 남긴다.
- 품질 규칙을 batch check로만 다룬다.
- severity와 threshold 없이 noisy rule에 alert를 건다.
- 일관되지 않은 denominator로 전환율을 계산한다.

## 예시 프롬프트

`이벤트 스키마 검증, 중복 탐지, 가격 범위 검증, 전환율 계산 검증 규칙과 실패 처리 방식을 작성해줘.`

