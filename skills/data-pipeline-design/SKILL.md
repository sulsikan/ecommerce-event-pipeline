---
name: data-pipeline-design
description: 이커머스 이벤트 파이프라인 하네스의 스키마, 이벤트 재생, Kafka, Spark, 데이터 품질, 모니터링, 리뷰, 검증 스크립트, 실행 계획, Gitflow 작업 절차를 조율한다.
---

# 데이터 파이프라인 설계

## 목적

Kaggle 이커머스 행동 CSV 행을 스트리밍 이벤트처럼 재생하는 실시간 데이터 파이프라인의 전체 하네스 작업을 안내한다.

## 사용 시점

여러 파이프라인 에이전트를 조율하거나, 데이터 파이프라인 문서를 생성 또는 수정하거나, 검증 스크립트를 실행하거나, 실행 계획과 PR 전 리뷰를 준비할 때 사용한다.

## 필수 입력

- 사용자 요청과 현재 프로젝트 단계
- `AGENTS.md`
- `skills/` 하위 역할별 Skill 문서
- `docs/data-pipeline/` 하위 파이프라인 문서
- `exec-plans/templates/` 하위 실행 계획 템플릿
- `scripts/` 하위 검증 스크립트

## 절차

1. 범위를 확인한다. 첫 하네스 작업에서는 문서와 검증 스크립트만 만들고 애플리케이션 런타임 코드는 작성하지 않는다.
2. `AGENTS.md`에서 에이전트 책임과 Gitflow 규칙을 확인한다.
3. Schema Designer Agent로 원천, 표준 이벤트, Bronze, Silver, Gold 스키마를 정의한다.
4. Event Replay Agent로 CSV 재생 의미, 재생 속도, 순서, 장애 주입 시나리오를 정의한다.
5. Kafka Streaming Agent로 Topic, Partition Key, Producer, Consumer, Offset, Retry, DLQ를 정의한다.
6. Spark Processing Agent로 Structured Streaming 작업, Watermark, Deduplication, Checkpoint, KST 파티셔닝, 계층별 출력을 정의한다.
7. Data Quality Agent로 검증 규칙과 실패 처리를 정의한다.
8. Monitoring Agent로 로그, 메트릭, 알림, Grafana 대시보드를 정의한다.
9. Review Agent로 모든 산출물을 비교하고 불일치를 기록한다.
10. 다음 검증 스크립트를 실행한다.

```bash
python3 scripts/validate-schema.py
python3 scripts/validate-data-quality.py
python3 scripts/validate-pipeline-docs.py
python3 scripts/generate-pipeline-report.py
```

11. 결정 사항, 위험, 검증 결과, 다음 작업을 실행 계획에 반영한다.
12. PR 체크리스트를 준비한다.
13. 변경 파일을 요약하고 자체 리뷰를 수행한다.
14. 커밋 전 `커밋을 진행할까요?`라고 질문한다.
15. 승인된 커밋 후 대상 브랜치를 명시하고 머지 승인을 다시 요청한다.

## 산출물

- `docs/data-pipeline/` 하위 파이프라인 문서
- `exec-plans/` 하위 실행 계획 또는 복사한 계획
- 검증 스크립트 실행 결과
- PR 준비 체크리스트
- 변경 파일 요약

## 검증 체크리스트

- 오케스트레이션 순서가 `AGENTS.md`와 일치한다.
- 하네스 전용 작업에서 애플리케이션 코드가 추가되지 않았다.
- 모든 에이전트 산출물에 대응 문서가 있다.
- 모든 검증 스크립트가 통과한다.
- Gitflow와 승인 게이트가 보존된다.

## 안티패턴

- 문서 수정 후 Review Agent 검토를 생략한다.
- 스키마, Kafka, Spark, 품질 규칙에서 서로 다른 이벤트 식별자를 사용한다.
- 사용자가 하네스 문서만 요청했는데 구현 작업을 진행한다.
- 명시적 승인 없이 커밋하거나 머지한다.

## 예시 프롬프트

`data-pipeline-design skill을 사용해서 전체 파이프라인 문서와 검증 결과를 점검하고 실행 계획을 업데이트해줘.`

