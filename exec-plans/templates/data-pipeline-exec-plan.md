# Data Pipeline Execution Plan

## Objective

Build a reliable real-time data platform that replays Kaggle e-commerce behavior data as streaming events and supports order monitoring, conversion analysis, and anomaly detection.

## Current Phase

- Phase:
- Branch:
- Owner:
- Last updated:

## Work Sequence

1. Schema Designer Agent drafts schema.
2. Event Replay Agent designs CSV replay.
3. Kafka Streaming Agent designs topics, partitions, consumer groups, retry, and DLQ.
4. Spark Processing Agent designs Bronze, Silver, and Gold processing.
5. Data Quality Agent defines validation rules.
6. Monitoring Agent defines logs, metrics, alerts, and dashboards.
7. Review Agent checks full consistency.
8. Run validation scripts.
9. Update this execution plan.
10. Complete PR checklist.

## Decisions

| Date | Decision | Rationale | Owner |
| --- | --- | --- | --- |
| | | | |

## Tasks

| Status | Task | Agent | Artifact |
| --- | --- | --- | --- |
| pending | Define canonical schema | Schema Designer Agent | `docs/data-pipeline/schema-guide.md` |
| pending | Define replay semantics | Event Replay Agent | `docs/data-pipeline/event-replay-guide.md` |
| pending | Define Kafka strategy | Kafka Streaming Agent | `docs/data-pipeline/kafka-guide.md` |
| pending | Define Spark processing | Spark Processing Agent | `docs/data-pipeline/spark-guide.md` |
| pending | Define quality rules | Data Quality Agent | `docs/data-pipeline/data-quality-rules.md` |
| pending | Define monitoring | Monitoring Agent | `docs/data-pipeline/monitoring-guide.md` |
| pending | Review consistency | Review Agent | `docs/data-pipeline/review-checklist.md` |

## Validation Log

| Command | Result | Notes |
| --- | --- | --- |
| `python3 scripts/validate-schema.py` | | |
| `python3 scripts/validate-data-quality.py` | | |
| `python3 scripts/validate-pipeline-docs.py` | | |
| `python3 scripts/generate-pipeline-report.py` | | |

## Risks

| Risk | Impact | Mitigation | Owner |
| --- | --- | --- | --- |
| Watermark too short | Valid late events dropped | Profile replay disorder and tune | Spark Processing Agent |
| Weak event identity | Dedup false positives or misses | Deterministic hash contract and tests | Schema Designer Agent |
| DLQ not replayable | Failed data cannot recover | Preserve original payload and metadata | Kafka Streaming Agent |
| Alert noise | Operators ignore alerts | Tune thresholds after load tests | Monitoring Agent |

## PR Readiness

- [ ] Changed files summarized.
- [ ] Self review completed.
- [ ] Validation scripts passed.
- [ ] Review checklist updated.
- [ ] Asked `커밋을 진행할까요?`.
- [ ] Commit completed only after approval.
- [ ] Asked `main으로 merge할까요?`.
- [ ] Merge completed only after approval.

