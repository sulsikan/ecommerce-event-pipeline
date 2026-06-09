CREATE SCHEMA IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.replay_runs (
    replay_run_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    replay_speed TEXT NOT NULL,
    max_events INTEGER,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    emitted_count BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ecommerce.raw_events (
    raw_event_seq BIGSERIAL PRIMARY KEY,
    replay_sequence BIGINT NOT NULL CHECK (replay_sequence > 0),
    event_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_time_kst TIMESTAMPTZ NOT NULL,
    event_date_kst DATE NOT NULL,
    event_hour_kst SMALLINT NOT NULL CHECK (event_hour_kst BETWEEN 0 AND 23),
    event_type TEXT NOT NULL CHECK (event_type IN ('view', 'cart', 'purchase')),
    product_id BIGINT NOT NULL,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT,
    price NUMERIC(18, 2),
    user_id BIGINT NOT NULL,
    user_session TEXT,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    replay_run_id TEXT NOT NULL REFERENCES ecommerce.replay_runs(replay_run_id),
    schema_version TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT raw_events_event_id_not_empty CHECK (length(event_id) > 0),
    CONSTRAINT raw_events_schema_version_not_empty CHECK (length(schema_version) > 0),
    CONSTRAINT raw_events_source_row_number_positive CHECK (source_row_number > 0),
    CONSTRAINT raw_events_price_non_negative CHECK (price IS NULL OR price >= 0),
    CONSTRAINT raw_events_replay_event_unique UNIQUE (replay_run_id, event_id),
    CONSTRAINT raw_events_replay_source_row_unique UNIQUE (
        replay_run_id,
        source_file,
        source_row_number
    ),
    CONSTRAINT raw_events_replay_sequence_unique UNIQUE (replay_run_id, replay_sequence)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_replay_run_id
    ON ecommerce.raw_events (replay_run_id);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_time
    ON ecommerce.raw_events (event_time);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_date_kst
    ON ecommerce.raw_events (event_date_kst);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_date_hour_kst
    ON ecommerce.raw_events (event_date_kst, event_hour_kst);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_type
    ON ecommerce.raw_events (event_type);

CREATE INDEX IF NOT EXISTS idx_raw_events_user_id
    ON ecommerce.raw_events (user_id);
