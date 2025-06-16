CREATE TABLE TcapCallDetailsQueue(
    session_id UUID,
    account_id UUID,
    CallTxnId Nullable(Int32),
    TclStmtId String CODEC(ZSTD(1)),
    PerfiosTransactionId String CODEC(ZSTD(1)),
    PerfiosTxnIdDate  Nullable(DateTime),
    Status LowCardinality(String),
    ErrorCode LowCardinality(String),
    Reason LowCardinality(String),
    WebtopNo String CODEC(ZSTD(1)),
    DmsUploadStatus String CODEC(ZSTD(1)),
    DmsUploadDate Nullable(DateTime),
    DmsObjId String CODEC(ZSTD(1)),
    PerfiosXML String CODEC(ZSTD(1)),
    PerfiosXMLStatus LowCardinality(String),
    PerfiosXMLDate Nullable(DateTime),
    OppId String CODEC(ZSTD(1)),
    PerfiosXLSStatus LowCardinality(String),
    PerfiosXLSDate Nullable(DateTime),
    DeleteStatus LowCardinality(String),
    DeleteDate  Nullable(DateTime),
    Destination String CODEC(ZSTD(1)),
    VendorResponse String CODEC(ZSTD(1)),
    created_at DateTime,
    updated_at DateTime
)ENGINE = S3Queue(
    'https://bank-connect-clickhouse-uat.s3.ap-south-1.amazonaws.com/tcap_call_details/*/*',
    -- edit the aws bucket url here, each * represents YYYYMMDDHH
    JSONEachRow
) SETTINGS mode = 'unordered',
s3queue_enable_logging_to_s3queue_log = 1,
after_processing = 'delete';

CREATE TABLE TcapCallDetails(
    session_id UUID,
    account_id UUID,
    CallTxnId Nullable(Int32),
    TclStmtId String CODEC(ZSTD(1)),
    PerfiosTransactionId String CODEC(ZSTD(1)),
    PerfiosTxnIdDate  Nullable(DateTime),
    Status LowCardinality(String),
    ErrorCode LowCardinality(String),
    Reason LowCardinality(String),
    WebtopNo String CODEC(ZSTD(1)),
    DmsUploadStatus String CODEC(ZSTD(1)),
    DmsUploadDate Nullable(DateTime),
    DmsObjId String CODEC(ZSTD(1)),
    PerfiosXML String CODEC(ZSTD(1)),
    PerfiosXMLStatus LowCardinality(String),
    PerfiosXMLDate Nullable(DateTime),
    OppId String CODEC(ZSTD(1)),
    PerfiosXLSStatus LowCardinality(String),
    PerfiosXLSDate Nullable(DateTime),
    DeleteStatus LowCardinality(String),
    DeleteDate  Nullable(DateTime),
    Destination String CODEC(ZSTD(1)),
    VendorResponse String CODEC(ZSTD(1)),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree PARTITION BY toYYYYMM(created_at)
ORDER BY (session_id, account_id);

CREATE MATERIALIZED VIEW TcapCallDetailsConsumer TO TcapCallDetails AS
SELECT *
FROM TcapCallDetailsQueue;