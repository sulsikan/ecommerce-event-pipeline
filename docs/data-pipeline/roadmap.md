# 데이터 파이프라인 로드맵

## 1단계: 하네스와 설계

- 에이전트 역할 정의를 만든다.
- Skill 문서를 만든다.
- 아키텍처, 스키마, 재생, Kafka, Spark, 품질, 모니터링, 리뷰 문서를 만든다.
- 검증 스크립트를 만든다.
- 실행 계획 템플릿을 만든다.
- 하네스 검증을 실행한다.

## 2단계: 로컬 재생 프로토타입

- CSV fixture 탐색을 추가한다.
- 이벤트 재생 Producer를 구현한다.
- 결정적 `event_id`를 생성한다.
- 설정 가능한 재생 속도를 추가한다.
- 장애 주입 시나리오를 추가한다.

## 3단계: Kafka 스트리밍 기반

- 로컬 Kafka 환경을 구성한다.
- Raw, Retry, DLQ Topic을 만든다.
- Producer schema validation을 추가한다.
- Consumer Group과 Offset 복구 테스트를 추가한다.

## 4단계: Spark 처리

- Bronze 수집을 구현한다.
- Silver 검증, 정규화, watermark, deduplication을 구현한다.
- 주문량, 카테고리 count, funnel 지표, anomaly feature용 Gold 집계를 구현한다.
- Checkpoint와 replay 복구 테스트를 추가한다.

## 5단계: 품질과 관측성

- 데이터 품질 검사를 구현한다.
- Quarantine과 DLQ triage 흐름을 추가한다.
- 메트릭과 구조화 로그를 추가한다.
- Grafana 대시보드와 알림 룰을 추가한다.

## 6단계: 리뷰와 강화

- Review Agent 일관성 검토를 실행한다.
- Load test와 replay test를 실행한다.
- 운영 runbook을 문서화한다.
- PR 체크리스트를 준비한다.

## Gitflow 마일스톤

- `main` 아래에 `develop` 브랜치를 둔다.
- 기능 작업은 `feature/*` 브랜치에서 수행한다.
- 기능 브랜치는 `develop`으로 머지한다.
- 각 작업 단위 완료 후 변경 파일을 요약한다.
- 커밋 승인 전 자체 리뷰를 수행한다.
- 커밋 전 `커밋을 진행할까요?`라고 질문한다.
- 머지 전 대상 브랜치를 명시하고 승인받는다.

