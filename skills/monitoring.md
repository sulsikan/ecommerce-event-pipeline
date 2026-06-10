# 모니터링

## 목적

스트리밍 데이터 플랫폼 운영을 위한 로그, 메트릭, 알림, Grafana 대시보드를 설계한다.

## 사용 시점

파이프라인 관측성, Kafka Consumer Lag 알림, DLQ 모니터링, 처리 실패 감지, 처리량 대시보드, 데이터 지연 알림을 정의할 때 사용한다.

## 필수 입력

- Kafka Topic과 Consumer Group 정의
- Spark Streaming query 이름
- 데이터 품질 규칙 이름과 severity
- 분석 목표와 대시보드 사용자
- 운영 서비스 수준 목표

## 절차

1. Replay, Producer, Kafka Consumer, Spark Job, 품질 검증, 저장 쓰기 단계의 구조화 로그를 정의한다.
2. throughput, latency, lag, failure count, retry count, DLQ count 플랫폼 메트릭을 정의한다.
3. event count, duplicate rate, null rate, conversion rate, purchase spike 데이터 메트릭을 정의한다.
4. 알림 threshold와 routing을 정의한다.
5. Replay, Kafka, Spark, 데이터 품질, 비즈니스 지표 Grafana 패널을 정의한다.
6. Critical alert별 runbook 링크를 정의한다.
7. 모든 데이터 품질 실패가 가시적인 메트릭을 가지는지 검토한다.

## 산출물

- `docs/data-pipeline/monitoring-guide.md`
- 알림 룰 표
- Grafana 대시보드 패널 목록

## 검증 체크리스트

- 모든 중요 파이프라인 단계가 로그와 메트릭을 방출한다.
- Kafka Consumer Lag와 DLQ 증가를 alert로 감지할 수 있다.
- Spark query failure와 checkpoint recovery event를 alert로 감지할 수 있다.
- 데이터 지연이 `event_time`과 `ingested_at`으로 측정된다.
- 대시보드 패널이 프로젝트 분석 목표와 연결된다.

## 안티패턴

- 인프라 메트릭만 모니터링하고 데이터 정확성을 무시한다.
- time window 없는 raw count에 alert를 건다.
- dashboard를 alert의 대체재로 사용한다.
- critical alert의 runbook owner를 생략한다.

## 예시 프롬프트

`Kafka Consumer Lag, DLQ 수, Spark 처리 실패, 데이터 지연, 주문 급증을 감지하는 Grafana 대시보드와 알림 룰을 설계해줘.`

