# 데이터 파이프라인 실행 계획

## 목표

Kaggle 이커머스 행동 데이터를 스트리밍 이벤트처럼 재생하고, 주문 모니터링, 전환 분석, 이상 탐지를 지원하는 신뢰 가능한 실시간 데이터 플랫폼을 구축한다.

## 현재 단계

- 단계:
- 브랜치:
- 담당자:
- 마지막 업데이트:

## 작업 순서

1. Schema Designer Agent가 스키마를 작성한다.
2. Event Replay Agent가 CSV 재생 방식을 설계한다.
3. Kafka Streaming Agent가 Topic, Partition, Consumer Group, Retry, DLQ를 설계한다.
4. Spark Processing Agent가 Bronze, Silver, Gold 처리를 설계한다.
5. Data Quality Agent가 검증 규칙을 정의한다.
6. Monitoring Agent가 로그, 메트릭, 알림, 대시보드를 정의한다.
7. Review Agent가 전체 일관성을 검토한다.
8. 검증 스크립트를 실행한다.
9. 실행 계획을 업데이트한다.
10. PR 체크리스트를 완료한다.

## 결정 사항

| 날짜 | 결정 | 근거 | 담당 |
| --- | --- | --- | --- |
| | | | |

## 작업 목록

| 상태 | 작업 | 에이전트 | 산출물 |
| --- | --- | --- | --- |
| pending | 표준 스키마 정의 | Schema Designer Agent | `docs/data-pipeline/schema-guide.md` |
| pending | 재생 의미 정의 | Event Replay Agent | `docs/data-pipeline/event-replay-guide.md` |
| pending | Kafka 전략 정의 | Kafka Streaming Agent | `docs/data-pipeline/kafka-guide.md` |
| pending | Spark 처리 정의 | Spark Processing Agent | `docs/data-pipeline/spark-guide.md` |
| pending | 품질 규칙 정의 | Data Quality Agent | `docs/data-pipeline/data-quality-rules.md` |
| pending | 모니터링 정의 | Monitoring Agent | `docs/data-pipeline/monitoring-guide.md` |
| pending | 일관성 리뷰 | Review Agent | `docs/data-pipeline/review-checklist.md` |

## 검증 로그

| 명령 | 결과 | 메모 |
| --- | --- | --- |
| `python3 scripts/validate-schema.py` | | |
| `python3 scripts/validate-data-quality.py` | | |
| `python3 scripts/validate-pipeline-docs.py` | | |
| `python3 scripts/generate-pipeline-report.py` | | |

## 위험

| 위험 | 영향 | 완화 | 담당 |
| --- | --- | --- | --- |
| Watermark가 너무 짧음 | 정상 late event drop | Replay disorder profiling 후 조정 | Spark Processing Agent |
| Event identity가 약함 | Dedup false positive 또는 miss | 결정적 hash 계약과 테스트 | Schema Designer Agent |
| DLQ replay 불가 | 실패 데이터 복구 불가 | 원본 payload와 metadata 보존 | Kafka Streaming Agent |
| Alert noise | 운영자가 alert를 무시 | Load test 후 threshold 조정 | Monitoring Agent |

## PR 준비 상태

- [ ] 변경 파일 요약
- [ ] 자체 리뷰 완료
- [ ] 검증 스크립트 통과
- [ ] 리뷰 체크리스트 갱신
- [ ] `커밋을 진행할까요?` 질문
- [ ] 승인 후 커밋
- [ ] 대상 브랜치 명시 후 머지 승인 요청
- [ ] 승인 후 `develop`으로 머지

