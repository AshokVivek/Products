CREATE TABLE cctransactionsQueue(
    org_id LowCardinality(String),
    org_name LowCardinality(String),
    link_id String CODEC(ZSTD(1)),
    entity_id UUID,
    statement_id UUID,
    bank_name LowCardinality(String),
    credit_card_number String CODEC(ZSTD(1)),
    template_id LowCardinality(String),
    page_number Int16,
    sequence_number Int32,
    transaction_type LowCardinality(String),
    transaction_note String CODEC(ZSTD(1)),
    amount Float32,
    date DateTime,
    merchant String CODEC(ZSTD(1)),
    merchant_category LowCardinality(String),
    hash String CODEC(ZSTD(1)),
    created_at DateTime,
    updated_at DateTime
) ENGINE = S3Queue(
    'https://{AWS_BUCKET_URL}/cc_transactions/*/*',
    -- edit the aws bucket url here, each * represents YYYYMMDDHH
    JSONEachRow
) SETTINGS mode = 'unordered',
s3queue_enable_logging_to_s3queue_log = 1,
after_processing = 'delete';

CREATE TABLE cc_transactions(
    org_id LowCardinality(String),
    org_name LowCardinality(String),
    link_id String CODEC(ZSTD(1)),
    entity_id UUID,
    statement_id UUID,
    bank_name LowCardinality(String),
    credit_card_number String CODEC(ZSTD(1)),
    template_id LowCardinality(String),
    page_number Int16,
    sequence_number Int32,
    transaction_type LowCardinality(String),
    transaction_note String CODEC(ZSTD(1)),
    amount Float32,
    date DateTime,
    merchant String CODEC(ZSTD(1)),
    merchant_category LowCardinality(String),
    hash String CODEC(ZSTD(1)),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree PARTITION BY toYYYYMM(created_at)
ORDER BY (statement_id, page_number, sequence_number);

CREATE MATERIALIZED VIEW cctransactionsConsumer TO cc_transactions AS
SELECT *
FROM cctransactionsQueue;