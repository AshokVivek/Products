CREATE TABLE bank_connect.transactions_new on cluster '{cluster}'
(
    `entity_id` String,
    `statement_id` String,
    `link_id` String,
    `account_id` String,
    `transaction_type` String,
    `transaction_note` Nullable(String),
    `amount` Float32,
    `balance` Float32,
    `date` String,
    `transaction_channel` Nullable(String),
    `unclean_merchant` Nullable(String),
    `merchant_category` Nullable(String),
    `description` Nullable(String),
    `is_lender` Bool,
    `merchant` Nullable(String),
    `hash` String,
    `page_number` Nullable(String),
    `sequence_number` Nullable(String),
    `bank_name` Nullable(String),
    `created_at` DateTime64(3),
    `perfios_txn_category` Nullable(String),
    `chq_num` Nullable(String),
    `optimizations` Nullable(String),
    `category` Nullable(String),
    `category_regex` Nullable(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/bank_connect_transactions_new', '{replica}')
ORDER BY (toYYYYMM(fromUnixTimestamp64Milli(toInt64(created_at))), entity_id, statement_id, hash)
SETTINGS index_granularity = 8192