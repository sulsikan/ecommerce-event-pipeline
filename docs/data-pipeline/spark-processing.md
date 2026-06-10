# Spark Processing 실행 가이드

## 목적

Phase 3는 Kafka `ecommerce.raw-events` topic을 Spark Structured Streaming으로 읽어 Bronze, Silver, Gold 계층을 로컬 Parquet warehouse에 생성한다. 이 단계의 목표는 모델링이 아니라 Kafka 이후 처리 계층의 책임 경계, checkpoint, event-time 집계, KST 파티셔닝을 검증하는 것이다.

## 실행 준비

```bash
docker compose up -d postgres kafka kafka-init spark
```

Spark 컨테이너는 repository root를 `/workspace`로 마운트한다. 로컬 macOS에 Java가 없어도 다음 명령은 컨테이너 안에서 실행된다.

## 100건 입력 생성

Kafka topic에 이벤트가 없다면 Phase 2 producer로 먼저 100건을 발행한다.

```bash
python3 scripts/replay-events-to-kafka.py \
  --csv data/2019-Oct.csv \
  --speed max \
  --limit 100 \
  --replay-run-id phase3-spark-verify-100
```

## Spark 처리 실행

```bash
docker compose exec spark \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  /workspace/scripts/run-spark-pipeline.py \
  --bootstrap-servers kafka:29092 \
  --topic ecommerce.raw-events \
  --warehouse-dir /workspace/data/spark-warehouse \
  --checkpoint-dir /workspace/data/spark-checkpoints \
  --trigger available-now
```

`available-now` trigger는 현재 Kafka에 있는 데이터를 처리한 뒤 종료한다. 이 모드에서는 Bronze/Silver를 Structured Streaming으로 적재한 뒤, 종료 가능한 로컬 검증을 위해 Silver parquet에서 Gold 지표를 배치로 물질화한다. 장시간 개발 관찰이 필요하면 `--trigger processing-time`을 사용한다.

## 산출물 확인

```bash
docker compose exec spark \
  /opt/spark/bin/spark-submit /workspace/scripts/check-spark-output.py \
  --warehouse-dir /workspace/data/spark-warehouse \
  --expected-bronze 100 \
  --expected-silver 100
```

기본 산출물:

| 계층 | 경로 | 내용 |
| --- | --- | --- |
| Bronze | `data/spark-warehouse/bronze/raw_events` | Kafka metadata, raw payload, parse status |
| Silver | `data/spark-warehouse/silver/events` | 검증된 표준 이벤트, KST 파생 필드, dedup 결과 |
| Gold | `data/spark-warehouse/gold/order_volume_hourly` | 시간대별 주문량 |
| Gold | `data/spark-warehouse/gold/order_volume_by_category` | 카테고리별 주문량 |
| Gold | `data/spark-warehouse/gold/conversion_funnel_hourly` | view/cart/purchase funnel |
| Gold | `data/spark-warehouse/gold/user_purchase_burst_features` | 사용자별 5분 구매 폭증 feature |

## 구현 제외 범위

- DLQ
- Retry
- Schema Registry
- Airflow
- Grafana
