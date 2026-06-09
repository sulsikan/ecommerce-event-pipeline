# 데이터 파이프라인 설계

## 목적

스키마 설계부터 PR 준비 검토까지 이커머스 행동 데이터 실시간 파이프라인 하네스의 전체 설계를 조율한다.

## 사용 시점

스키마, 이벤트 재생, Kafka, Spark Structured Streaming, 데이터 품질, 모니터링, 실행 계획을 함께 설계하거나 검토할 때 사용한다.

## 필수 입력

- 프로젝트 목표와 분석 요구사항
- Kaggle CSV 원천 데이터 가정
- 이미 결정된 런타임 스택
- `docs/data-pipeline/` 하위 문서
- `exec-plans/` 하위 실행 계획

## 절차

1. `AGENTS.md`에서 역할 경계와 Git 규칙을 확인한다.
2. Schema Designer Agent로 표준 이벤트와 저장 계층 스키마를 작성한다.
3. Event Replay Agent로 CSV 재생 의미와 장애 주입 시나리오를 정의한다.
4. Kafka Streaming Agent로 Topic, Partition, Consumer Group, Offset, Retry, DLQ를 정의한다.
5. Spark Processing Agent로 Bronze, Silver, Gold 변환을 정의한다.
6. Data Quality Agent로 검증 규칙과 실패 처리를 정의한다.
7. Monitoring Agent로 로그, 메트릭, 알림, 대시보드를 정의한다.
8. Review Agent로 전체 산출물의 일관성을 점검한다.
9. 검증 스크립트를 실행한다.
10. 실행 계획과 PR 체크리스트를 업데이트한다.

## 산출물

- `docs/data-pipeline/architecture.md`
- `docs/data-pipeline/roadmap.md`
- `docs/data-pipeline/schema-guide.md`
- `docs/data-pipeline/event-replay-guide.md`
- `docs/data-pipeline/kafka-guide.md`
- `docs/data-pipeline/spark-guide.md`
- `docs/data-pipeline/data-quality-rules.md`
- `docs/data-pipeline/monitoring-guide.md`
- `docs/data-pipeline/review-checklist.md`
- `exec-plans/templates/data-pipeline-exec-plan.md`

## 검증 체크리스트

- 각 에이전트에 역할, 입력, 출력, 검증 단계가 있다.
- 필드명이 스키마, Kafka 메시지, Spark 출력, 품질 규칙, 메트릭에서 일관된다.
- KST 파티셔닝 정책이 일관되게 문서화되어 있다.
- DLQ, Retry, Watermark, Checkpoint, Deduplication 정책이 문서화되어 있다.
- Gitflow와 승인 규칙이 포함되어 있다.

## 안티패턴

- 데이터 신뢰성 계약 없이 분석 지표부터 설계한다.
- 한 문서에서 스키마 필드를 바꾸고 Kafka, Spark, 품질, 모니터링 문서를 갱신하지 않는다.
- 중복, 지연, 누락 이벤트를 테스트 시나리오 없이 예외 상황으로만 둔다.
- 사용자 승인 없이 커밋하거나 머지한다.

## 예시 프롬프트

`데이터 파이프라인 설계를 전체 검토하고 스키마, 이벤트 재생, Kafka, Spark, 품질, 모니터링 문서의 불일치를 찾아줘.`

