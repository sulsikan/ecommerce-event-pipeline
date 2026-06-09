# Event Replay Generator

## 목적

`2019-Oct.csv`를 event-time 순서로 재생하고 PostgreSQL `ecommerce.raw_events` 테이블에 적재한다. 이 구현은 Phase 1 로컬 검증 슬라이스이며 Kafka, Spark, Airflow, S3, Grafana는 포함하지 않는다.

## 실행 준비

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d postgres
```

PostgreSQL 기본 접속 정보:

```text
postgresql://ecommerce:ecommerce@localhost:5432/ecommerce
```

## 100건 재생

`2019-Oct.csv` 파일을 `data/2019-Oct.csv`에 둔 경우:

```bash
python3 scripts/replay-events.py \
  --csv data/2019-Oct.csv \
  --speed max \
  --limit 100 \
  --replay-run-id phase1-verify-100
```

`--speed max`는 sleep 없이 가능한 속도로 적재한다. 실제 event-time 간격을 반영하려면 `--speed 1`, `--speed 10`, `--speed 100`처럼 배수를 지정한다.

## 적재 건수 확인

```bash
python3 scripts/check-replay-count.py \
  --replay-run-id phase1-verify-100 \
  --expected 100
```

## 구현 범위

- CSV 표준 컬럼을 읽는다.
- 원본 `event_time`을 UTC로 파싱한다.
- KST 파생 필드 `event_time_kst`, `event_date_kst`, `event_hour_kst`를 생성한다.
- `event_time` 순서가 깨지면 오류로 중단한다.
- `view`, `cart`, `purchase` 이벤트를 그대로 유지한다.
- 결정적 `event_id`를 생성한다.
- 적재 순서 검증을 위해 `replay_sequence`를 저장한다.
- replay speed에 따라 인접 이벤트 간 delay를 조절한다.
- PostgreSQL `ecommerce.raw_events`에 적재한다.

## 제외 범위

- Kafka
- Spark
- Airflow
- S3
- Grafana
