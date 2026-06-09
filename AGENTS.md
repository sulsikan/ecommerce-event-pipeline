# 이커머스 이벤트 파이프라인 에이전트

이 하네스는 Kaggle `E-commerce behavior data from multi category store` 데이터를 실시간 이벤트처럼 재생하는 데이터 파이프라인 프로젝트의 작업 규칙과 역할 경계를 정의한다. 프로젝트의 핵심 목표는 분석 모델 자체가 아니라, 수집, 전처리, 스트리밍 처리, 저장, 품질 검증, 모니터링까지 신뢰할 수 있게 운영되는 실시간 데이터 플랫폼을 설계하고 구현하는 것이다.

첫 번째 프로젝트 단계에서는 문서와 검증 하네스만 만든다. 별도 요청이 있기 전까지 애플리케이션 런타임 코드는 작성하지 않는다.

## 프로젝트 목표

- 시간대별 주문량과 카테고리별 주문량을 모니터링한다.
- 주문 급증과 비정상 구매 패턴을 감지한다.
- 사용자 행동 흐름 `view -> cart -> purchase` 전환율을 분석한다.
- 중복, 지연, 누락, 잘못된 형식, 참조 불일치 이벤트를 검증한다.
- Kafka, Spark Structured Streaming, 데이터 품질, 저장 계층, 메트릭, 알림, 대시보드를 중심으로 운영 가능한 플랫폼을 설계한다.

## 에이전트 작업 순서

1. Schema Designer Agent가 스키마 초안을 작성한다.
2. Event Replay Agent가 CSV 기반 이벤트 재생 방식을 설계한다.
3. Kafka Streaming Agent가 Topic, Partition, Consumer Group, Offset, Retry, DLQ 전략을 설계한다.
4. Spark Processing Agent가 Bronze, Silver, Gold 스트리밍 처리 흐름을 설계한다.
5. Data Quality Agent가 검증 규칙과 실패 처리 방식을 정의한다.
6. Monitoring Agent가 로그, 메트릭, 알림, 대시보드를 설계한다.
7. Review Agent가 전체 산출물의 일관성을 검토한다.
8. `scripts/validate-schema.py`, `scripts/validate-data-quality.py`, `scripts/validate-pipeline-docs.py`를 실행한다.
9. `exec-plans/templates/data-pipeline-exec-plan.md` 또는 복사한 실행 계획을 업데이트한다.
10. `docs/data-pipeline/review-checklist.md`의 PR 전 체크리스트를 완료한다.

## Schema Designer Agent

책임:

- 원천 CSV, 표준 이벤트, Bronze, Silver, Gold 스키마를 설계한다.
- 컬럼명, 타입, nullable 여부, 설명, partition key, primary key 후보를 정의한다.
- 스키마 변경 시 호환성 규칙을 검토한다.
- 스키마 필드명이 Kafka 메시지, Spark 처리, 데이터 품질 규칙, 모니터링 지표와 일치하도록 관리한다.

주요 산출물:

- `docs/data-pipeline/schema-guide.md`
- `docs/data-pipeline/architecture.md`의 스키마 관련 섹션
- 필요 시 `scripts/validate-schema.py` 갱신

## Event Replay Agent

책임:

- Kaggle CSV 데이터를 실시간 이벤트처럼 재생하는 방식을 설계한다.
- `event_time` 기준 재생 순서와 재생 속도를 정의한다.
- `view`, `cart`, `purchase` 행동 흐름을 보존한다.
- 중복, 지연, 누락, 순서 뒤섞임, 잘못된 형식 이벤트 테스트 시나리오를 만든다.

주요 산출물:

- `docs/data-pipeline/event-replay-guide.md`
- `docs/data-pipeline/architecture.md`의 재생 가정

## Kafka Streaming Agent

책임:

- Kafka Topic과 이름 규칙을 설계한다.
- Partition Key 전략을 정의한다.
- Producer와 Consumer 동작을 정의한다.
- Consumer Group, Offset 처리, Retry, DLQ, 재처리 정책을 정의한다.

주요 산출물:

- `docs/data-pipeline/kafka-guide.md`
- 품질 검증 및 모니터링 문서에서 참조하는 Topic과 DLQ 계약

## Spark Processing Agent

책임:

- Spark Structured Streaming 작업을 설계한다.
- Deduplication, Watermark, Checkpoint, Window Aggregation, Join 전략을 정의한다.
- KST 기준 파티셔닝을 정의한다.
- Bronze, Silver, Gold 저장 계층을 설계한다.

주요 산출물:

- `docs/data-pipeline/spark-guide.md`
- `docs/data-pipeline/architecture.md`의 계층 설명

## Data Quality Agent

책임:

- null, duplicate, range, referential integrity, schema validation 규칙을 정의한다.
- 전환율 계산 검증 규칙을 정의한다.
- 실패 시 DLQ, quarantine, alert, report 처리 방식을 정의한다.
- 품질 규칙 이름을 모니터링 지표 및 리뷰 체크리스트와 맞춘다.

주요 산출물:

- `docs/data-pipeline/data-quality-rules.md`
- 필요 시 `scripts/validate-data-quality.py` 갱신

## Monitoring Agent

책임:

- 로그, 메트릭, 알림 기준, 대시보드 패널을 정의한다.
- 데이터 지연, 누락, 중복, 처리 실패, Kafka Consumer Lag, DLQ 증가, 처리량 이상을 감지한다.
- Grafana 대시보드와 알림 룰을 문서화한다.

주요 산출물:

- `docs/data-pipeline/monitoring-guide.md`
- `docs/data-pipeline/review-checklist.md`의 모니터링 검토 항목

## Review Agent

책임:

- 다른 에이전트의 산출물을 모두 검토한다.
- 스키마, 이벤트 재생, Kafka, Spark, 품질 검증, 모니터링이 서로 일관되는지 확인한다.
- 커밋 또는 PR 전 체크리스트를 작성한다.

주요 산출물:

- `docs/data-pipeline/review-checklist.md`
- 활성 실행 계획의 리뷰 결과

## Git 작업 규칙

- `main` 브랜치에 직접 커밋하지 않는다.
- Gitflow를 따른다.
- `main` 아래에 `develop` 브랜치를 둔다.
- 기능 개발은 `feature/*` 브랜치에서 진행한다.
- 기능 브랜치는 `develop`으로 머지한다.
- 사용자 승인 전 커밋하지 않는다.
- 사용자 승인 전 머지하지 않는다.
- 구현 완료 후 변경 파일을 요약한다.
- 자체 코드 리뷰를 수행한다.
- 커밋 전 사용자에게 `커밋을 진행할까요?`라고 질문한다.
- 승인 후 커밋한다.
- 머지 전 사용자에게 대상 브랜치를 명시하고 질문한다.
- 승인 후 머지한다.

## 필수 검증

커밋 승인 요청 전 다음 명령을 실행한다.

```bash
python3 scripts/validate-schema.py
python3 scripts/validate-data-quality.py
python3 scripts/validate-pipeline-docs.py
python3 scripts/generate-pipeline-report.py
```

