// Take entry from the daily MIS being sent for the AA

CREATE TABLE TcapDailyAAConsentsQueue(
    webtop_id String CODEC(ZSTD(1)),
    aa_vendor String CODEC(ZSTD(1)),
    customer_id String CODEC(ZSTD(1)),
    from_date Nullable(Date),
    to_date Nullable(Date),
    consent_id String CODEC(ZSTD(1)),
    consent_type String CODEC(ZSTD(1)),
    consent_expiry Nullable(DateTime64),
    consent_status String CODEC(ZSTD(1)),
    failure_code String CODEC(ZSTD(1)),
    failure_reason String CODEC(ZSTD(1)),
    created_at DateTime,
    updated_at DateTime
)ENGINE = S3Queue(
    'https://{AWS_BUCKET_URL}/tcap_daily_aa_consents/*/*',
    -- edit the aws bucket url here, each * represents YYYYMMDDHH
    JSONEachRow
) SETTINGS mode = 'unordered',
s3queue_enable_logging_to_s3queue_log = 1,
after_processing = 'delete';

CREATE TABLE TcapDailyAAConsents(
    webtop_id String CODEC(ZSTD(1)),
    aa_vendor String CODEC(ZSTD(1)),
    customer_id String CODEC(ZSTD(1)),
    from_date Nullable(Date),
    to_date Nullable(Date),
    consent_id String CODEC(ZSTD(1)),
    consent_type String CODEC(ZSTD(1)),
    consent_expiry Nullable(DateTime64),
    consent_status String CODEC(ZSTD(1)),
    failure_code String CODEC(ZSTD(1)),
    failure_reason String CODEC(ZSTD(1)),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree PARTITION BY toYYYYMM(created_at)
ORDER BY (webtop_id, consent_id, customer_id);

CREATE MATERIALIZED VIEW TcapDailyAAConsentsConsumer TO TcapDailyAAConsents AS
SELECT *
FROM TcapDailyAAConsentsQueue;