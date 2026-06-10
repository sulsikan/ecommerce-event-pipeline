# 이커머스 이벤트 파이프라인

Kaggle `E-commerce behavior data from multi category store` 데이터를 실시간 이벤트처럼 재생하여 데이터 수집, 전처리, 스트리밍 처리, 저장, 품질 검증, 모니터링까지 이어지는 실시간 데이터 파이프라인을 구축하는 프로젝트입니다.

현재 단계는 Phase 4 Spark Structured Streaming 처리 구현입니다. 로컬에서는 `Replay Generator -> Kafka -> Spark Bronze/Silver/Gold` 경로로 100건 smoke test를 수행합니다.
