# Phase 5 데이터 품질과 모니터링 실행 가이드

## 목적

Phase 5는 Phase 4에서 생성한 Spark Bronze/Silver/Gold warehouse를 기준으로 데이터 품질 규칙, quarantine 후보, 운영 메트릭, alert 상태를 생성한다. 이 단계는 Grafana 서버 운영 자체보다 모니터링에 필요한 계약과 산출물이 일관되게 만들어지는지를 검증한다.

## 입력

- Bronze: `data/spark-warehouse/bronze/raw_events`
- Silver: `data/spark-warehouse/silver/events`
- Gold: `data/spark-warehouse/gold/*`

## 데이터 품질 실행

```bash
docker compose exec spark \
  /opt/spark/bin/spark-submit \
  /workspace/scripts/run-data-quality-checks.py \
  --warehouse-dir /workspace/data/spark-warehouse \
  --output-dir /workspace/data/quality-monitoring
```

생성 산출물:

| 경로 | 내용 |
| --- | --- |
| `data/quality-monitoring/data_quality/rule_results` | rule별 failure count, severity, status |
| `data/quality-monitoring/data_quality/quarantine_events` | reject/quarantine 대상 원본 payload와 실패 사유 |
| `data/quality-monitoring/monitoring/metric_events` | Prometheus 변환 가능한 metric event |

## 모니터링 리포트 생성

```bash
docker compose exec spark \
  /opt/spark/bin/spark-submit \
  /workspace/scripts/generate-monitoring-report.py \
  --output-dir /workspace/data/quality-monitoring \
  --report-path /workspace/data/quality-monitoring/monitoring/phase5-report.json \
  --prometheus-path /workspace/data/quality-monitoring/monitoring/prometheus-metrics.prom
```

생성 산출물:

- `phase5-report.json`: rule summary, quarantine count, alert firing 상태
- `prometheus-metrics.prom`: Prometheus textfile 형식 metric

## Grafana와 Alert Rule

- Dashboard 정의: `configs/grafana/ecommerce-pipeline-dashboard.json`
- Alert rule 정의: `configs/alerts/pipeline-alert-rules.yml`

로컬 smoke test에서는 Prometheus/Grafana 서버를 필수로 띄우지 않는다. 대신 `prometheus-metrics.prom`을 Prometheus textfile collector 또는 후속 exporter에 연결할 수 있는 형식으로 생성한다.

## 완료 기준

- reject/quarantine/warn/observe rule 결과가 `rule_results`에 기록된다.
- reject/quarantine 대상 record가 원본 payload와 실패 metadata를 포함한다.
- duplicate rate, purchase count, conversion rate, DQ failure metric이 생성된다.
- alert마다 severity, owner, runbook이 정의된다.
