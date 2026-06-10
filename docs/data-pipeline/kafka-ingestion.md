# Kafka Ingestion 실행 가이드

## 목적

Phase 2는 Phase 1의 `Replay Generator -> PostgreSQL` 경로를 `Replay Generator -> Kafka -> Consumer -> PostgreSQL` 경로로 바꾼다. Replay Generator는 Kafka `ecommerce.raw-events` topic에 JSON 이벤트를 발행하고, consumer group `ecommerce-postgres-writer`가 메시지를 읽어 PostgreSQL `ecommerce.raw_events`에 저장한다.

## 실행 준비

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d postgres kafka kafka-init
```

Kafka 로컬 계약:

| 항목 | 값 |
| --- | --- |
| Bootstrap servers | `localhost:9092` |
| Topic | `ecommerce.raw-events` |
| Partitions | `3` |
| Replication factor | `1` |
| Consumer group | `ecommerce-postgres-writer` |

## 100건 발행

`2019-Oct.csv` 파일을 `data/2019-Oct.csv`에 둔 경우:

```bash
python3 scripts/replay-events-to-kafka.py \
  --csv data/2019-Oct.csv \
  --speed max \
  --limit 100 \
  --replay-run-id phase2-kafka-verify-100
```

## Kafka 적재 확인

```bash
python3 scripts/check-kafka-topic.py \
  --topic ecommerce.raw-events \
  --min-expected 100 \
  --expected-partitions 3
```

깨끗한 topic에서 최초 실행했다면 `--expected 100`으로 정확히 확인할 수 있다. 이미 같은 topic에 이전 메시지가 남아 있으면 `message_count`는 100보다 클 수 있으므로 `--min-expected 100`과 `replay_run_id` 기준 PostgreSQL 검증을 함께 사용한다.

## PostgreSQL 저장

```bash
python3 scripts/consume-kafka-events.py \
  --topic ecommerce.raw-events \
  --group-id ecommerce-postgres-writer \
  --replay-run-id phase2-kafka-verify-100 \
  --max-messages 100
```

## PostgreSQL 적재 건수 확인

```bash
python3 scripts/check-replay-count.py \
  --replay-run-id phase2-kafka-verify-100 \
  --expected 100
```

## 구현 제외 범위

- DLQ
- Retry
- Schema Registry
- Spark
- Airflow
- Grafana
