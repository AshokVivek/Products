CREATE TABLE extractionIssue(
    unique_id UUID DEFAULT generateUUIDv4(),
    org_id LowCardinality(String),
    statement_id UUID,
    page_number Int16,
    sequence_number Int32,
    entity_id UUID,
    link_id String CODEC(ZSTD(1)),
    org_name LowCardinality(String),
    bank_name LowCardinality(String),
    account_id UUID,
    account_number LowCardinality(String),
    template_id String CODEC(ZSTD(1)),
    transaction_type LowCardinality(String),
    transaction_note String CODEC(ZSTD(1)),
    chq_num LowCardinality(String),
    amount Float32,
    balance Float32,
    date DateTime,
    optimizations String CODEC(ZSTD(1)),
    transaction_channel LowCardinality(String),
    transaction_channel_regex String CODEC(ZSTD(1)),
    hash LowCardinality(String),
    unclean_merchant LowCardinality(String),
    unclean_merchant_regex String CODEC(ZSTD(1)),
    merchant_category LowCardinality(String),
    description String CODEC(ZSTD(1)),
    is_lender Bool,
    merchant LowCardinality(String),
    description_regex String CODEC(ZSTD(1)),
    category LowCardinality(String),
    perfios_txn_category LowCardinality(String),
    created_at DateTime,
    updated_at DateTime,
    matched_transaction_currency String CODEC(ZSTD(1)),
    creditor_name String CODEC(ZSTD(1)),
    creditor_ifsc String CODEC(ZSTD(1)),
    creditor_upi_handle String CODEC(ZSTD(1)),
    creditor_bank String CODEC(ZSTD(1)),
    creditor_account_number String CODEC(ZSTD(1)),
    receiver_name String CODEC(ZSTD(1)),
    receiver_ifsc String CODEC(ZSTD(1)),
    receiver_upi_handle String CODEC(ZSTD(1)),
    receiver_bank String CODEC(ZSTD(1)),
    reciever_account_number String CODEC(ZSTD(1)),
    merchant_name String CODEC(ZSTD(1)),
    merchant_ifsc String CODEC(ZSTD(1)),
    merchant_upi_handle String CODEC(ZSTD(1)),
    merchant_bank String CODEC(ZSTD(1)),
    matched_merchant_category String CODEC(ZSTD(1)),
    cheque_number String CODEC(ZSTD(1)),
    transaction_reference_1 String CODEC(ZSTD(1)),
    transaction_reference_2 String CODEC(ZSTD(1)),
    primary_channel String CODEC(ZSTD(1)),
    secondary_channel String CODEC(ZSTD(1)),
    tertiary_channel String CODEC(ZSTD(1)),
    transaction_timestamp String CODEC(ZSTD(1)),
    transaction_amount String CODEC(ZSTD(1)),
    raw_location String CODEC(ZSTD(1)),
    currency String CODEC(ZSTD(1)),
    categorization_time_stamp String CODEC(ZSTD(1)),
    regex_id String CODEC(ZSTD(1)),
    attempt_type Nullable(Enum('pdf' = 1, 'aa', 'online', 'csv', 'xml')),
    from_date Nullable(DateTime),
    to_date Nullable(DateTime),
    txn_from_date Nullable(DateTime),
    txn_to_date Nullable(DateTime),
    statement_status Nullable(Int8),
    is_extracted_by_perfios Nullable(Bool),
    is_extracted_by_nanonets Nullable(Bool),
    matched_transaction_category String CODEC(ZSTD(1)),
    extraction_issue_type LowCardinality(String),
    is_extraction_problem_confirmed Nullable(Bool),
    is_issue_solved Nullable(Bool),
    technique_used_to_solve String CODEC(ZSTD(1)),
    issue_verified_by LowCardinality(String)
) ENGINE = ReplacingMergeTree PARTITION BY (toYYYYMM(created_at), extraction_issue_type)
ORDER BY (statement_id, page_number, sequence_number);

ALTER TABLE bank_connect.extractionIssue ADD COLUMN salary_confidence_percentage Nullable(Int8);
ALTER TABLE bank_connect.extractionIssue ADD COLUMN salary_calculation_method Nullable(Enum('keyword' = 1, 'employer_name', 'recurring'));


-- Zero Length Transaction note
CREATE MATERIALIZED VIEW zero_length_transaction_note_seperator TO extractionIssue AS
SELECT
    *,
    'zero_length_transaction_note' as extraction_issue_type
FROM bank_connect.transactions
WHERE length(transaction_note) = 0 AND is_extracted_by_perfios = False AND attempt_type != 'aa'

-- Length of Transaction note > 350
CREATE MATERIALIZED VIEW transaction_note_length_gt_350_seperator TO extractionIssue AS
SELECT
    *,
    'transaction_note_length_gt_350' as extraction_issue_type
FROM bank_connect.transactions
WHERE length(transaction_note) > 350 AND is_extracted_by_perfios = False AND attempt_type != 'aa'

-- Length of cheque number > Length of Transaction note
CREATE MATERIALIZED VIEW length_chq_num_gt_length_transaction_note_seperator TO extractionIssue AS
SELECT
    *,
    'length_chq_num_gt_length_transaction_note' as extraction_issue_type
FROM bank_connect.transactions
WHERE length(chq_num) > length(transaction_note) AND is_extracted_by_perfios = False AND attempt_type != 'aa'

-- Transaction channel as "other"
CREATE MATERIALIZED VIEW transaction_channel_other_seperator TO extractionIssue AS
SELECT
    *,
    'transaction_channel_other' as extraction_issue_type
FROM bank_connect.transactions
WHERE  transaction_channel = 'Other' AND is_extracted_by_perfios = False AND attempt_type != 'aa'