# Review Checklist

## Scope

Use this checklist before commit approval, PR opening, or merge approval.

## Design Consistency

- [ ] `event_id` definition is consistent across schema, replay, Kafka, Spark, and quality docs.
- [ ] `event_time`, `event_time_kst`, `event_date_kst`, and `event_hour_kst` semantics are consistent.
- [ ] Kafka partition key aligns with user behavior and anomaly detection goals.
- [ ] Spark deduplication key matches data quality duplicate rules.
- [ ] DLQ envelope fields match Kafka, quality, and monitoring docs.
- [ ] Gold tables support order monitoring, conversion funnel, and anomaly analysis.

## Agent Artifact Review

- [ ] Schema Designer Agent output reviewed.
- [ ] Event Replay Agent output reviewed.
- [ ] Kafka Streaming Agent output reviewed.
- [ ] Spark Processing Agent output reviewed.
- [ ] Data Quality Agent output reviewed.
- [ ] Monitoring Agent output reviewed.
- [ ] Review Agent findings recorded.

## Validation Commands

```bash
python3 scripts/validate-schema.py
python3 scripts/validate-data-quality.py
python3 scripts/validate-pipeline-docs.py
python3 scripts/generate-pipeline-report.py
```

Record results:

- `validate-schema.py`:
- `validate-data-quality.py`:
- `validate-pipeline-docs.py`:
- `generate-pipeline-report.py`:

## Git Readiness

- [ ] Current branch is not `main`.
- [ ] Branch name follows `feature/*`.
- [ ] Changed files summarized.
- [ ] Self review completed.
- [ ] User approved commit after `커밋을 진행할까요?`.
- [ ] User approved merge after `main으로 merge할까요?`.

## Open Risks

- [ ] Kafka partition count not finalized.
- [ ] Watermark duration not finalized.
- [ ] Storage format not finalized.
- [ ] Grafana deployment path not finalized.

