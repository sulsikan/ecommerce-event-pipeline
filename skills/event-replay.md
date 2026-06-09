# 이벤트 재생

## 목적

Kaggle CSV의 과거 이벤트를 실시간 이벤트처럼 재생하면서 원천 event-time 의미를 보존한다.

## 사용 시점

재생 속도, 이벤트 순서, 합성 ingestion timestamp, producer 동작, 장애 주입 시나리오를 설계할 때 사용한다.

## 필수 입력

- Kaggle CSV 경로와 샘플 row 수
- 원하는 재생 속도 배수
- Kafka Topic 계약
- event-time 범위와 타임존 정책
- 장애 시나리오 요구사항

## 절차

1. CSV 행을 streaming 또는 chunk 단위로 읽는다.
2. `event_time`을 UTC 원천 이벤트 시간으로 파싱한다.
3. `event_time` 기준으로 재생하되 같은 timestamp에서는 안정적인 순서를 유지한다.
4. 인접 이벤트 timestamp 차이를 `replay_speed`로 나누어 재생 지연 시간을 계산한다.
5. publish 시점의 `ingested_at`을 포함해 표준 이벤트 payload를 발행한다.
6. `view`, `cart`, `purchase` 이벤트를 축약하지 않고 보존한다.
7. 중복, 지연, 누락, 잘못된 형식, 순서 뒤섞임 이벤트를 설정 가능한 방식으로 주입한다.
8. 재현 가능한 replay run metadata를 기록한다.

## 산출물

- `docs/data-pipeline/event-replay-guide.md`
- 재생 테스트 시나리오 표
- `docs/data-pipeline/kafka-guide.md`의 Producer 계약 갱신

## 검증 체크리스트

- 재생은 파일 읽기 시간이 아니라 `event_time` 의미를 기준으로 한다.
- 재생 속도를 설정할 수 있고 문서화되어 있다.
- 중복과 지연 이벤트 시나리오가 재현 가능하다.
- 재생 payload가 표준 스키마와 일치한다.
- replay를 재시작하거나 offset 기준으로 재개할 수 있다.

## 안티패턴

- 모든 행 사이에 고정 sleep을 넣는다.
- 구매 분석만 필요하다는 이유로 `view` 또는 `cart` 이벤트를 버린다.
- seed 없는 무작위 장애 시나리오를 만든다.
- Producer 성공을 end-to-end 파이프라인 성공으로 간주한다.

## 예시 프롬프트

`Kaggle CSV를 100배속 실시간 이벤트처럼 Kafka에 재생하는 설계를 작성하고 지연, 중복, 누락 테스트 시나리오도 포함해줘.`

