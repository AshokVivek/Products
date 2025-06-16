CREATE TABLE bank_connect.mapped_comp_transactions(
    unique_id UUID DEFAULT generateUUIDv4(),
    p_entity_id String,
    b_entity_id String,
    p_account_id String,
    b_account_id String,
    b_bank_name LowCardinality(String),
    p_transaction_note String CODEC(ZSTD(1)),
    p_transaction_type String CODEC(ZSTD(1)),
    p_amount Float32,
    p_balance Float32,
    p_date String,
    p_hash String CODEC(ZSTD(1)),
    b_transaction_note String CODEC(ZSTD(1)),
    b_transaction_type String CODEC(ZSTD(1)),
    b_amount Float32,
    b_balance Float32,
    b_date String,
    b_hash String CODEC(ZSTD(1)),
    perfios_txn_category String CODEC(ZSTD(1)),
    category String CODEC(ZSTD(1)),
    transaction_channel String CODEC(ZSTD(1)),
    description String CODEC(ZSTD(1)),
    merchant_category String CODEC(ZSTD(1)),
    p_counter Int32,
    created_at DateTime
)
ENGINE = MergeTree PARTITION BY toYYYYMM(created_at)
ORDER BY (p_entity_id, p_counter);