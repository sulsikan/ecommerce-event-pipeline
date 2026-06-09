# 이벤트 재생 가이드

## 목적

Kaggle `E-commerce behavior data from multi category store`의 `2019-Oct.csv` 행을 실시간 사용자 행동 이벤트처럼 재생하면서 원천 `event_time` 의미를 보존한다. Event Replay Agent의 책임은 데이터를 분석하거나 집계하는 것이 아니라, downstream Kafka/Spark 파이프라인을 검증할 수 있는 신뢰 가능한 이벤트 입력을 만드는 것이다.

현재 단계는 문서 설계만 수행한다. CSV producer, Kafka client, Spark job 같은 애플리케이션 런타임 코드는 별도 요청 전까지 작성하지 않는다.

## 핵심 설계 결정

| 결정 | 내용 | 이유 |
| --- | --- | --- |
| `event_time` 기반 재생 | 원천 timestamp 차이를 wall-clock delay로 변환해 발행한다. | replay 속도와 처리 지연이 달라도 시간대별 주문량, funnel, 이상 탐지 결과는 원천 event-time 기준으로 유지되어야 한다. |
| UTC 원천 보존, KST 파생 | CSV `event_time`은 UTC로 파싱하고, KST 필드는 표준 이벤트 스키마에서 파생한다. | Kaggle 원천 timestamp 의미를 훼손하지 않으면서 한국 시간 기준 partition과 dashboard를 지원한다. |
| Producer는 계산하지 않음 | replay producer는 표준 이벤트 생성, fault injection, publish, audit만 수행한다. | 집계, dedup, watermark, 품질 판정은 Spark/Data Quality 책임으로 남겨야 downstream 검증이 명확하다. |
| `view`, `cart`, `purchase` 보존 | 모든 event type을 필터링하지 않고 발행한다. | 전환율은 구매 이벤트만으로 계산할 수 없고 사용자 행동 흐름 전체가 필요하다. |
| 안정 정렬 | `event_time`, `source_file`, `source_row_number` 순으로 발행한다. | 같은 timestamp가 많아도 재현 가능한 replay와 deterministic fault injection이 가능하다. |
| Fault injection 내장 | 중복, 지연, 누락, 순서 뒤섞임, malformed, producer failure, purchase burst를 시나리오로 정의한다. | 플랫폼 신뢰성 검증이 프로젝트 핵심이므로 정상 데이터만 재생하면 부족하다. |

## 재생 입력

- Source: `2019-Oct.csv` 파일 경로 또는 source manifest
- Target topic: `ecommerce.events.raw.v1`
- Audit topic: `ecommerce.replay.audit.v1`
- Replay profile: `1x`, `10x`, `100x`, `1000x`, `max`
- 선택 가능한 시작/종료 `event_time`
- 선택 가능한 source row range
- Fault scenario name
- Fault injection ratio
- Deterministic fault injection seed
- `schema_version`
- `replay_run_id`

결정 이유:

- 입력을 manifest와 parameter로 고정하면 같은 원천 파일을 여러 속도와 fault scenario로 반복 검증할 수 있다.
- `replay_run_id`는 동일한 `2019-Oct.csv`를 정상 replay, duplicate replay, late replay, 부하 replay로 분리해 추적하는 기준이다.

## Source Manifest

Phase 2부터 replay 재현성을 위해 source manifest를 둔다.

| 필드 | 설명 | 이유 |
| --- | --- | --- |
| `source_file` | 재생 대상 CSV 파일명. 기본값은 `2019-Oct.csv` | event_id 생성과 장애 triage 기준이다. |
| `file_checksum` | 파일 checksum | 같은 이름의 다른 파일을 잘못 재생하는 위험을 줄인다. |
| `row_count` | 총 row 수 | 누락 시나리오와 replay 완료율 계산에 필요하다. |
| `event_time_min`, `event_time_max` | 파일 내 event-time 범위 | replay range와 예상 wall-clock 소요 시간을 검증한다. |
| `schema_version` | 발행할 표준 schema version | consumer 호환성 검증 기준이다. |
| `created_at` | manifest 생성 시각, UTC | source audit 이력을 남긴다. |

## 원본 `event_time` 처리

| 항목 | 설계 결정 | 결정 이유 |
| --- | --- | --- |
| Timezone | `2019-Oct.csv`의 `event_time`은 UTC timestamp로 파싱한다. 원천 값에 `UTC` suffix가 있으면 timezone-aware 값으로 읽고, suffix가 없으면 UTC로 간주하되 parser warning을 남긴다. | 원천 데이터셋의 시간 기준을 표준화해야 Kafka, Spark watermark, Gold window 집계가 같은 기준을 사용한다. |
| 원천 보존 | 표준 이벤트의 `event_time`은 UTC 원천 시각으로 유지한다. `event_time_kst`, `event_date_kst`, `event_hour_kst`는 파생 필드로만 만든다. | 비즈니스 화면은 KST가 필요하지만 event-time ordering과 watermark는 원천 UTC 기준으로 일관되어야 한다. |
| Ordering | 기본 발행 순서는 `event_time ASC`, `source_file ASC`, `source_row_number ASC` 안정 정렬이다. | CSV 물리 순서가 바뀌거나 같은 timestamp가 반복되어도 replay 결과가 재현 가능해야 한다. |
| 동일 timestamp | 같은 `event_time`을 가진 row 사이의 event-time gap은 `0`으로 처리한다. 이 그룹 안에서는 `source_row_number` 순서를 유지하며 의도적인 sleep을 넣지 않는다. | 동일 초에 발생한 행동을 임의로 벌리면 source event-time 분포가 왜곡된다. |
| 역전 timestamp | 정렬 전 CSV에서 이전 row보다 과거 timestamp가 나오면 source disorder로 audit metric에 기록한다. 기본 정상 replay는 정렬 후 발행하고, disorder 검증은 `out_of_order_event` scenario에서만 만든다. | 정상 replay와 장애 주입 replay의 의미를 분리해야 테스트 결과를 해석할 수 있다. |
| `ingested_at` 관계 | `ingested_at`은 producer가 Kafka publish를 시도하는 wall-clock UTC 시각이다. retry로 재발행하면 재발행 시점의 새 `ingested_at`을 갖고, `event_time`은 절대 바꾸지 않는다. | `ingested_at - event_time`으로 replay lag와 processing latency를 측정할 수 있어야 한다. |
| Kafka timestamp 관계 | Kafka broker 또는 producer timestamp는 Bronze metadata의 `kafka_timestamp`로 보존한다. 표준 payload의 event-time 기준은 항상 `event_time`이다. | broker 수신 지연 분석과 비즈니스 window 기준을 혼동하지 않기 위해서다. |

## 재생 의미

1. Source manifest와 replay parameter를 audit topic에 시작 이벤트로 기록한다.
2. CSV row를 chunk 단위로 읽는다.
3. `event_time`을 UTC timestamp로 파싱한다.
4. `event_time`, `source_file`, `source_row_number` 기준으로 안정 정렬한다.
5. 인접 row의 source event-time gap을 계산한다.
6. Wall-clock delay를 `source_event_time_gap / replay_speed`로 계산한다.
7. Replay profile의 delay cap, throughput cap, backpressure 정책을 적용한다.
8. 표준 이벤트 payload를 만들고 `ingested_at`을 publish 시점으로 설정한다.
9. Kafka key는 `user_id`로 설정하고 raw topic에 발행한다.
10. 종료 시 emitted, skipped, failed, injected count를 audit topic과 report에 기록한다.

결정 이유:

- chunk 단위 처리는 대용량 CSV에서도 메모리 사용을 제어하기 위한 설계다.
- 안정 정렬은 replay 재현성과 event_id 생성 안정성을 보장한다.
- replay audit을 시작/종료에 남기면 downstream count 불일치가 source 누락인지 처리 누락인지 구분할 수 있다.

## Replay Speed 프로파일

| 프로파일 | Delay 계산 | 사용 시점 | 결정 이유 |
| --- | --- | --- | --- |
| `1x` | `source_event_time_gap` 그대로 sleep | 작은 샘플 데모, event-time 의미 검토, dashboard 수동 확인 | 실제 시간 흐름을 보존해 사람이 결과를 이해하기 쉽다. |
| `10x` | gap을 10으로 나눈다 | 로컬 Kafka/Spark 통합 안정성 확인 | 너무 빠르지 않아 consumer lag, watermark, checkpoint 움직임을 관찰하기 좋다. |
| `100x` | gap을 100으로 나눈다 | 일반 개발 검증, Phase 2~4 기능 테스트 | 하루 단위 샘플을 짧은 시간에 처리하면서 event-time 분포를 유지한다. |
| `1000x` | gap을 1000으로 나눈다 | 부하 테스트, lag baseline 수집, alert threshold 조정 | 운영 지표와 backpressure 한계를 빠르게 드러낸다. |
| `max` | 의도적 sleep 없이 producer가 가능한 만큼 발행 | backfill 처리량 검증, broker/Spark 최대 처리량 측정 | 실시간 시뮬레이션이 아니라 복구/재처리 처리량을 검증하는 모드다. |

공통 정책:

- `1x`, `10x`, `100x`, `1000x`는 같은 delay 계산식을 사용한다.
- `max`는 source event-time ordering은 지키지만 wall-clock delay를 적용하지 않는다.
- 모든 프로파일은 같은 payload schema, Kafka key, audit metadata를 사용한다.
- 프로파일 값은 metric label과 replay report에 기록한다.

## Accelerated Replay 설계

Source event-time gap을 wall-clock delay로 바꾸는 기본 식은 다음과 같다.

```text
wall_clock_delay_seconds = max(0, event_time[i] - event_time[i-1]) / replay_speed
```

| 항목 | 설계 결정 | 결정 이유 |
| --- | --- | --- |
| Gap 기준 | 정렬된 row의 인접 `event_time` 차이를 사용한다. | 원천 데이터의 활동 밀도와 조용한 구간을 replay에 반영하기 위해서다. |
| 음수 gap | 정상 replay에서는 안정 정렬 후 계산하므로 음수 gap은 발생하지 않아야 한다. 발생 시 `0`으로 clamp하고 audit warning을 남긴다. | producer가 멈추는 장애를 피하면서 source disorder를 추적한다. |
| 긴 gap cap | 개발 profile에서는 너무 긴 sleep을 막기 위해 configurable `max_delay_seconds`를 둔다. cap 적용 여부와 횟수는 report에 기록한다. | `2019-Oct.csv`의 긴 비활성 구간 때문에 테스트가 불필요하게 느려지는 일을 방지한다. |
| 동일 timestamp burst | 같은 timestamp 그룹은 delay `0`으로 연속 발행한다. | 원천의 순간 traffic spike를 downstream burst 처리 검증에 그대로 사용한다. |
| Backpressure | producer in-flight request, broker ack latency, local send queue depth를 기준으로 읽기 속도를 늦춘다. queue가 임계치를 넘으면 CSV read를 pause하고, 임의 drop은 하지 않는다. | 부하 상황에서도 source count와 fault injection 의도가 보존되어야 한다. |
| Throughput cap | profile별 `max_events_per_second`를 선택적으로 둔다. `max`에서도 broker/Spark 보호를 위해 cap을 설정할 수 있다. | replay producer가 테스트 대상 시스템을 완전히 압도하면 품질 검증보다 인프라 장애만 보게 된다. |
| Clock drift | replay start wall-clock과 현재 wall-clock의 차이를 기준으로 누적 drift를 기록한다. | 가속 replay가 의도한 속도보다 느려지는지 확인하고 capacity baseline으로 사용한다. |

기대 downstream 동작:

- Spark Gold 집계는 replay profile과 무관하게 같은 event-time window 결과를 내야 한다.
- Monitoring은 profile별 throughput, consumer lag, producer publish latency 차이를 보여야 한다.
- `max` replay 결과는 실시간 알림 threshold 산정에 직접 사용하지 않고 backfill capacity 검증에만 사용한다.

## 행동 흐름 보존

- `view`, `cart`, `purchase`를 필터링하지 않는다.
- 같은 `user_id` 이벤트는 Kafka partition key를 통해 가능한 한 같은 partition으로 보낸다.
- `user_session`은 결측 가능성이 있으므로 session 단위 funnel은 보조 지표로 둔다.
- Funnel 계산은 Spark Gold에서 window 기준 distinct user로 수행한다.
- Fault injection이 아닌 정상 replay에서는 특정 event type을 oversampling하지 않는다.

결정 이유:

- 원천 데이터는 완전한 세션 흐름을 항상 보장하지 않을 수 있으므로 user 단위와 session 단위를 분리해야 한다.
- replay 단계에서 이벤트를 축약하면 downstream 품질 검증과 funnel 계산을 검증할 수 없다.

## Duplicate Event 생성

| 항목 | 설계 결정 | 결정 이유 |
| --- | --- | --- |
| 중복 정의 | 동일한 `event_id`를 가진 이벤트를 raw topic에 다시 발행한다. `event_time`, `product_id`, `user_id`, `source_file`, `source_row_number`는 원본과 동일하게 둔다. | Spark Silver deduplication과 Data Quality `DQ_DUP_EVENT_ID`가 같은 key를 사용해야 한다. |
| `ingested_at` | duplicate 재발행 시점의 새 wall-clock UTC 값을 사용한다. | 같은 원천 이벤트가 다른 수집 시각에 다시 들어온 상황을 모사한다. |
| 선택 방식 | canonical `event_id`와 seed를 입력으로 deterministic hash를 계산해 하위 `ratio`만 선택한다. | seed와 ratio가 같으면 매번 같은 row가 duplicate되어 테스트 재현성이 생긴다. |
| 비율 | 기본 smoke test는 `0.1%`, 통합 테스트는 `1%`, 부하/품질 한계 테스트는 `5%`까지 사용한다. | 낮은 비율은 정상 운영 잡음을, 높은 비율은 dedup state와 metric 한계를 검증한다. |
| 대상 제한 | 기본은 모든 event type 대상이다. 필요 시 `purchase` only, 특정 `user_id`, 특정 event-time range로 제한할 수 있다. | 주문 급증, funnel 왜곡, 사용자 단위 중복 같은 목적별 검증이 필요하다. |
| 발행 위치 | 기본 duplicate는 원본 발행 직후 또는 configurable delay 후 발행한다. late duplicate 검증은 Late Event scenario와 조합한다. | 단순 retry 중복과 watermark 이후 도착 중복은 downstream 기대 동작이 다르다. |

기대 downstream 동작:

- Bronze는 원본과 duplicate를 모두 보존한다.
- Silver는 watermark 범위 내 동일 `event_id`를 duplicate로 표시하거나 제거한다.
- Gold 비즈니스 지표는 duplicate를 제외해 주문량과 전환율이 부풀지 않아야 한다.
- `dq_duplicate_event_id_total`과 `gold_duplicate_event_summary`가 증가해야 한다.

## Late Event 생성

Spark 초기 watermark 설계값은 `event_time` 기준 30분이다. Late scenario는 이 기준의 이내/초과를 모두 만든다.

| 유형 | 설계 결정 | 기대 downstream 동작 | 결정 이유 |
| --- | --- | --- | --- |
| Watermark 이내 late | 선택된 이벤트를 원래 순서보다 늦게 발행하되, 해당 이벤트의 `event_time`이 현재 watermark보다 늦도록 delay를 제한한다. | Spark는 정상 처리하고 Gold window 결과에 포함한다. late metric은 관찰용으로만 증가할 수 있다. | 허용 가능한 네트워크 지연과 partition disorder를 검증한다. |
| Watermark 초과 late | 선택된 이벤트를 보류했다가 downstream watermark가 `event_time + 30분`을 지난 뒤 발행되도록 만든다. | `DQ_EVENT_TOO_LATE` 또는 late-event metric이 증가하고 정책에 따라 quarantine 또는 late output으로 분리된다. Gold 확정 window를 임의 수정하지 않아야 한다. | 너무 늦은 이벤트에 대한 손실/격리 정책을 검증한다. |
| Late duplicate | duplicate event를 watermark 이후에 발행한다. | duplicate metric과 late metric의 중복 집계 정책이 문서대로 작동해야 한다. | retry 중복이 늦게 도착하는 현실적인 장애를 검증한다. |

Late 대상 선택:

- seed 기반 deterministic hash로 대상 row를 고른다.
- 기본 비율은 watermark 이내 `0.5%`, watermark 초과 `0.1%`다.
- `purchase` 이벤트는 별도 비율을 둘 수 있다. 구매 late event가 주문량과 이상 탐지에 미치는 영향이 크기 때문이다.

결정 이유:

- late event를 단일 유형으로만 만들면 watermark 경계값 검증이 부족하다.
- 원천 `event_time`은 바꾸지 않고 발행 시점만 늦춰야 event-time 처리와 processing-time 처리의 차이를 검증할 수 있다.

## Failure Scenario 생성

| 시나리오 | 생성 방식 | 기대 downstream 동작 | 결정 이유 |
| --- | --- | --- | --- |
| `malformed_event` | JSON 구조 손상, 필수 필드 제거, 숫자 필드 문자열 주입, 잘못된 timestamp 형식을 seed 기반으로 만든다. | Bronze는 raw payload와 parse failure를 보존하고, Kafka/Spark 계약에 따라 DLQ 또는 reject metric으로 보낸다. | schema validation과 DLQ 계약을 검증한다. |
| `missing_event` | 선택된 source row를 발행하지 않고 replay report의 skipped count에 기록한다. | Downstream count는 source manifest 대비 부족하고, replay audit에서 누락률을 확인할 수 있어야 한다. | producer 누락과 downstream 처리 누락을 구분하기 위해서다. |
| `out_of_order_event` | 제한된 event-time window 안에서 row 발행 순서를 shuffle한다. | watermark 이내 disorder는 정상 처리되고, 초과 disorder는 late/quarantine 정책을 따른다. | Kafka partition disorder와 Spark event-time 처리 한계를 검증한다. |
| `producer_failure` | configurable 지점에서 publish 실패, retry, producer restart, partial batch resend를 audit으로 모사한다. | Kafka retry 정책에 따라 동일 `event_id` 중복이 생길 수 있고, Silver dedup과 producer failure metric이 증가한다. | 실제 producer 장애는 중복과 누락을 동시에 만들 수 있다. |
| `purchase_burst` | 특정 짧은 event-time window와 선택된 `user_id` 또는 synthetic replay user group에 purchase 이벤트를 집중 재발행한다. | Gold anomaly feature, purchase spike alert, category order volume panel이 반응해야 한다. | 주문 급증과 비정상 구매 패턴 감지 설계를 검증한다. |
| `invalid_reference` | 존재하지 않는 `category_id`, 비어 있는 `category_code`, 비정상 `brand` 조합을 주입한다. | 참조 무결성 품질 규칙이 warning 또는 quarantine으로 분류한다. | Kaggle 원천의 category/brand 결측과 불일치를 운영 품질 규칙으로 확인한다. |
| `negative_price` | 일부 row의 `price`를 음수 또는 비정상적으로 큰 값으로 변형한다. | `DQ_PRICE_NEGATIVE` 또는 range rule failure가 증가하고 Gold 매출성 지표에서 제외된다. | 값 범위 검증과 alert를 확인한다. |

공통 정책:

- Fault scenario는 원본 row를 삭제하거나 변형한 사실을 `fault_scenario`, `fault_injection_seed`, `fault_injection_rule` audit metadata에 남긴다.
- 같은 replay run에서 여러 scenario를 조합할 수 있지만, PR 전 검증은 단일 scenario별 결과를 먼저 확인한다.
- Malformed payload는 표준 schema를 일부러 깨는 예외이므로 schema validation 실패가 성공 조건이다.
- Fault injection으로 만든 이벤트도 Kafka key 기본값은 `user_id`를 사용한다. `user_id` 자체가 손상된 malformed case만 fallback key 정책을 따른다.

## 재생 메타데이터

각 replay run은 다음 정보를 기록해야 한다.

- `replay_run_id`
- source manifest version
- source file name
- row range
- event-time range
- replay speed profile
- effective events per second
- fault scenario name
- fault injection ratio
- fault injection seed
- target topic
- 시작/종료 wall-clock timestamp
- emitted row count
- skipped row count
- duplicate injected count
- late injected count
- malformed injected count
- producer failure count
- backpressure pause count
- delay cap applied count

## 운영 및 모니터링 연계

- Replay producer는 `pipeline_events_in_total`, `producer_publish_failures_total`, `replay_emitted_rows_total` 같은 metric을 방출해야 한다.
- `event_time`과 `ingested_at` 차이로 replay lag를 계산한다.
- `ingested_at`과 Kafka/Spark 수집 시각 차이로 processing latency를 계산한다.
- Fault scenario 이름은 log와 metric label에 포함한다.
- Replay 종료 시 emitted, skipped, failed, injected count를 report로 남긴다.
- `1000x`와 `max` replay는 Kafka consumer lag, Spark input rows per second, DLQ 증가율, checkpoint latency baseline 수집에 사용한다.

## Phase별 재생 설계

| Phase | 적용 내용 | 결정 이유 |
| --- | --- | --- |
| Phase 1 | replay contract, metadata, fault scenario 문서화 | 구현 전 downstream과 테스트 계약을 고정한다. |
| Phase 2 | CSV replay producer 설계와 source manifest 확정 | Kaggle CSV를 반복 가능한 streaming input으로 만든다. |
| Phase 3 | Kafka publish contract와 producer failure 처리 연결 | broker 장애와 retry/DLQ 정책을 검증할 수 있다. |
| Phase 4 | Spark watermark, dedup, aggregation 검증용 fault scenario 실행 | 처리 로직이 정상/비정상 입력을 모두 견디는지 확인한다. |
| Phase 5 | replay speed를 이용해 lag, throughput, alert threshold baseline 수집 | 운영 지표는 실제 부하 패턴이 있어야 조정 가능하다. |

## 검증 체크리스트

- 샘플 payload가 `docs/data-pipeline/schema-guide.md`의 표준 이벤트 스키마와 일치한다.
- `event_time`은 UTC로 보존되고 KST 파생 필드 의미가 schema 문서와 일치한다.
- 동일 timestamp row는 `source_row_number` 순서로 안정 발행된다.
- seed가 같으면 duplicate, late, malformed 대상 row가 동일하다.
- replay speed별 wall-clock 소요 시간이 기대 범위에 있다.
- `max` profile은 sleep 없이 발행하지만 source ordering은 유지한다.
- backpressure 발생 시 임의 drop 없이 pause 또는 retry로 처리된다.
- duplicate scenario가 Spark dedup과 `dq_duplicate_event_id_total` 증가로 이어진다.
- late scenario가 watermark 이내/초과 기대 처리로 나뉜다.
- malformed scenario가 schema reject와 DLQ 계약으로 이어진다.
- producer failure scenario가 retry, duplicate, failure metric으로 추적된다.
- purchase burst scenario가 Gold anomaly feature와 monitoring alert 검증에 사용된다.
