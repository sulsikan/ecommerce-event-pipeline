# Schema Design

## Purpose

Design consistent schemas for source CSV ingestion, canonical events, streaming layers, aggregate outputs, and schema evolution.

## When to Use

Use this skill when defining columns, data types, nullable rules, partition keys, primary key candidates, compatibility rules, or schema validation logic.

## Required Inputs

- Kaggle e-commerce CSV column list.
- Analysis requirements for order volume, conversion, and anomaly detection.
- Storage layer targets: Bronze, Silver, and Gold.
- Kafka message contract requirements.
- Existing schema guide.

## Step-by-Step Procedure

1. Identify source columns: `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`.
2. Define canonical fields, adding `event_id`, `ingested_at`, `event_date_kst`, `event_hour_kst`, `source_file`, and `source_row_number`.
3. Define type, nullable, description, and validation rule for each field.
4. Choose primary key candidates. Prefer `event_id`; fall back to a deterministic hash over source event attributes.
5. Choose partition keys. Use `event_date_kst` for storage and `user_id` for user-behavior Kafka partitioning unless a topic has a category-specific purpose.
6. Define compatibility rules for additive, breaking, and deprecated schema changes.
7. Update schema documentation and validation scripts.

## Output Artifacts

- `docs/data-pipeline/schema-guide.md`
- Schema sections in `docs/data-pipeline/architecture.md`
- Updated `scripts/validate-schema.py`, when rules change.

## Validation Checklist

- Every field has type, nullable rule, description, and owner.
- `event_id` generation is deterministic.
- Timestamp timezone handling is explicit.
- KST partition fields are documented.
- Schema changes include compatibility notes.

## Anti-Patterns

- Inferring primary keys from row order alone.
- Allowing nullable fields without documenting downstream behavior.
- Using local system timezone implicitly.
- Adding schema fields without Kafka, Spark, quality, and monitoring updates.

## Example Prompt

`Kaggle e-commerce CSV를 기준으로 canonical event schema와 Bronze/Silver/Gold schema를 설계해줘.`

