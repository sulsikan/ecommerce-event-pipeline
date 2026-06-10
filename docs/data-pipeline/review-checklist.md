# 리뷰 체크리스트

## 범위

커밋 승인, PR 생성, 머지 승인 전에 이 체크리스트를 사용한다.

## 설계 일관성

- [ ] `event_id` 정의가 스키마, 재생, Kafka, Spark, 품질 문서에서 일관된다.
- [ ] `event_time`, `event_time_kst`, `event_date_kst`, `event_hour_kst` 의미가 일관된다.
- [ ] Kafka Partition Key가 사용자 행동과 이상 탐지 목표에 맞다.
- [ ] Spark deduplication key가 데이터 품질 duplicate 규칙과 일치한다.
- [ ] DLQ envelope 필드가 Kafka, 품질, 모니터링 문서와 일치한다.
- [ ] Gold table이 주문 모니터링, 전환 funnel, 이상 탐지를 지원한다.

## 에이전트 산출물 리뷰

- [ ] Schema Designer Agent 산출물 검토
- [ ] Event Replay Agent 산출물 검토
- [ ] Kafka Streaming Agent 산출물 검토
- [ ] Spark Processing Agent 산출물 검토
- [ ] Data Quality Agent 산출물 검토
- [ ] Monitoring Agent 산출물 검토
- [ ] Review Agent 발견 사항 기록

## 검증 명령

```bash
python3 scripts/validate-schema.py
python3 scripts/validate-data-quality.py
python3 scripts/validate-pipeline-docs.py
python3 scripts/generate-pipeline-report.py
```

결과 기록:

- `validate-schema.py`:
- `validate-data-quality.py`:
- `validate-pipeline-docs.py`:
- `generate-pipeline-report.py`:

## Git 준비 상태

- [ ] 현재 브랜치가 `main`이 아니다.
- [ ] 기능 작업은 `feature/*` 브랜치에서 수행했다.
- [ ] `develop` 브랜치가 `main` 기준으로 존재한다.
- [ ] 기능 브랜치는 `develop`으로 머지한다.
- [ ] 변경 파일을 요약했다.
- [ ] 자체 리뷰를 완료했다.
- [ ] `커밋을 진행할까요?` 이후 사용자 승인을 받았다.
- [ ] 대상 브랜치를 명시하고 머지 승인을 받았다.

## 남은 위험

- [ ] Kafka partition count 미확정
- [ ] Watermark 기간 미확정
- [ ] Storage format 미확정
- [ ] Grafana 배포 경로 미확정
- [ ] Phase 5 alert threshold는 smoke test 기준이며 운영 baseline 조정 전이다.
