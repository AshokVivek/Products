CREATE TABLE transactions_quality(
    id SERIAL PRIMARY KEY,
    cluster_id VARCHAR(150),
    bank_name VARCHAR(150),
    transaction_type VARCHAR(150),
    cluster_link VARCHAR(150),
    cluster_sample_transaction_note TEXT,
    cluster_regex TEXT,
    cluster_capturing_group_details JSONB,
    cluster_allotted_to VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    requested_at VARCHAR(100),
    approved_at VARCHAR(100) DEFAULT NULL
);

alter table transactions_quality alter COLUMN approved_at set default null;
alter table transactions_quality alter COLUMN requested_at set default null;