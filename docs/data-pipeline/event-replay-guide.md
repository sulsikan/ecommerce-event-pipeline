# 이벤트 재생 가이드

## 목적

Kaggle CSV 행을 실시간 사용자 행동 이벤트처럼 재생하면서 원천 event-time 의미를 보존한다.

## 재생 입력

- Kaggle CSV 파일 경로
- `1`, `10`, `100`, `1000` 같은 재생 속도 배수
- 선택 가능한 시작/종료 event-time 범위
- 선택 가능한 deterministic fault injection seed
- 대상 Kafka raw topic

## 재생 의미

- 원천 `event_time`은 UTC로 파싱한다.
- 표준 이벤트 스키마와 일치하는 payload를 발행한다.
- `event_type` 값 `view`, `cart`, `purchase`를 보존한다.
- 원천 event-time 간격을 `replay_speed`로 나눈 값으로 이벤트 발행 간격을 정한다.
- 이벤트 발행 시점에 `ingested_at`을 설정한다.
- 발행 전 결정적 `event_id`를 생성한다.
- timestamp가 같은 이벤트는 `source_file`, `source_row_number` 기준으로 안정적인 순서를 유지한다.

## 재생 메타데이터

각 replay run은 다음 정보를 기록해야 한다.

- `replay_run_id`
- source file name
- row range
- event-time range
- replay speed
- fault scenario name
- fault injection seed
- target topic
- wall-clock 기준 시작/종료 timestamp

## 장애 주입 시나리오

| 시나리오 | 설명 | 기대 downstream 동작 |
| --- | --- | --- |
| duplicate_event | 동일한 `event_id`를 가진 일부 이벤트를 재발행 | Spark dedup 제거, 품질 metric 증가 |
| delayed_event | 일부 이벤트를 watermark threshold 이후 발행 | late-event 규칙이 warning 또는 quarantine 기록 |
| missing_event | 재생 중 일부 row drop | replay report에서 missing-rate 시나리오 확인 |
| malformed_event | 필수 필드 또는 타입 손상 | Kafka validation reject 또는 Spark DLQ 전송 |
| out_of_order_event | 제한된 window 안에서 이벤트 순서 shuffle | Spark watermark가 허용 범위 내 disorder 처리 |
| purchase_burst | 한 `user_id`에 짧은 시간 다수 purchase 발행 | Gold anomaly feature와 alert 발생 |

## 검증

- 샘플 발행 payload가 `docs/data-pipeline/schema-guide.md`와 일치한다.
- 짧은 fixture로 replay speed 동작을 검증한다.
- seed가 주어지면 fault injection이 재현 가능하다.
- Producer 실패 로그에 replay run metadata가 포함된다.

