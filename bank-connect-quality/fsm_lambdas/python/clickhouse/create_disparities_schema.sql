CREATE TABLE disparity(
    org_id LowCardinality(String),
    org_name LowCardinality(String),
    link_id String CODEC(ZSTD(1)),
    entity_id UUID,
    account_id UUID,
    statement_id Nullable(UUID),
    usfo_statement_id Nullable(UUID),
    visible_to_client Nullable(Bool),
    account_number String CODEC(ZSTD(1)),
    bank_name LowCardinality(String),
    fraud_type LowCardinality(String),
    transaction_hash String CODEC(ZSTD(1)),
    prev_date Nullable(DateTime),
    curr_date Nullable(DateTime),
    inconsistent_type Nullable(Enum('Account' = 1, 'Statement')),
    created_at DateTime64(9, 'UTC') DEFAULT now(),
    updated_at DateTime64(9, 'UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (org_id, entity_id, bank_name);