CREATE TABLE TcapCustomersQueue(
    session_id UUID,
    account_id UUID,
    Custid Nullable(Int32),
    Tclstmtid String CODEC(ZSTD(1)),
    Applicationno String CODEC(ZSTD(1)),
    Applicantid String CODEC(ZSTD(1)),
    Applicantname String CODEC(ZSTD(1)),
    Applicanttype Nullable(Int32),
    Emailid String CODEC(ZSTD(1)),
    Altemailid String CODEC(ZSTD(1)),
    Loanamount  Float32,
    Loanduration Nullable(Int32),
    Loantype String CODEC(ZSTD(1)),
    Applicantnature String CODEC(ZSTD(1)),
    Form26Asdob String CODEC(ZSTD(1)),
    Webtopno String CODEC(ZSTD(1)),
    Channel LowCardinality(String),
    Oppid String CODEC(ZSTD(1)),
    Apilogtxtid String CODEC(ZSTD(1)),
    Status LowCardinality(String),
    Createddate  Nullable(DateTime),
    Mailsentflg LowCardinality(String),
    Mailsentdate Nullable(DateTime),
    Perfiosredirectdate Nullable(DateTime),
    Perfiosresponse String CODEC(ZSTD(1)),
    Perfioscode LowCardinality(String),
    Perfiosmessage String CODEC(ZSTD(1)),
    Destination String CODEC(ZSTD(1)),
    Yearmonthfrom LowCardinality(String),
    Yearmonthto LowCardinality(String),
    Companycategory LowCardinality(String),
    Sistercompanyname LowCardinality(String),
    Sourcesystemurl String CODEC(ZSTD(1)),
    Pan String CODEC(ZSTD(1)),
    Gstnumber  String CODEC(ZSTD(1)),
    Gstusername String CODEC(ZSTD(1)),
    Perfiostransactionid String CODEC(ZSTD(1)),
    Bankname LowCardinality(String),
    Institutionid Nullable(Int32),
    Passwordflg LowCardinality(String),
    Employmenttype LowCardinality(String),
    Entityname  String CODEC(ZSTD(1)),
    Employername String CODEC(ZSTD(1)),
    Dateofincorporation String CODEC(ZSTD(1)),
    Companyname LowCardinality(String),
    Mailsendflag LowCardinality(String),
    Perfiosloantype LowCardinality(String),
    Appproduct LowCardinality(String),
    Redirecturl String CODEC(ZSTD(1)),
    Apisource String CODEC(ZSTD(1)),
    Statementid String CODEC(ZSTD(1)),
    Borrowertype LowCardinality(String),
    Facility LowCardinality(String),
    Sanctionlimitfixedamount LowCardinality(String),
    created_at DateTime,
    updated_at DateTime
)ENGINE = S3Queue(
    'https://bank-connect-clickhouse-uat.s3.ap-south-1.amazonaws.com/tcap_customers/*/*',
    -- edit the aws bucket url here, each * represents YYYYMMDDHH
    JSONEachRow
) SETTINGS mode = 'unordered',
s3queue_enable_logging_to_s3queue_log = 1,
after_processing = 'delete';

CREATE TABLE TcapCustomers(
    session_id UUID,
    account_id UUID,
    Custid Nullable(Int32),
    Tclstmtid String CODEC(ZSTD(1)),
    Applicationno String CODEC(ZSTD(1)),
    Applicantid String CODEC(ZSTD(1)),
    Applicantname String CODEC(ZSTD(1)),
    Applicanttype Nullable(Int32),
    Emailid String CODEC(ZSTD(1)),
    Altemailid String CODEC(ZSTD(1)),
    Loanamount  Float32,
    Loanduration Nullable(Int32),
    Loantype String CODEC(ZSTD(1)),
    Applicantnature String CODEC(ZSTD(1)),
    Form26Asdob String CODEC(ZSTD(1)),
    Webtopno String CODEC(ZSTD(1)),
    Channel LowCardinality(String),
    Oppid String CODEC(ZSTD(1)),
    Apilogtxtid String CODEC(ZSTD(1)),
    Status LowCardinality(String),
    Createddate  Nullable(DateTime),
    Mailsentflg LowCardinality(String),
    Mailsentdate Nullable(DateTime),
    Perfiosredirectdate Nullable(DateTime),
    Perfiosresponse String CODEC(ZSTD(1)),
    Perfioscode LowCardinality(String),
    Perfiosmessage String CODEC(ZSTD(1)),
    Destination String CODEC(ZSTD(1)),
    Yearmonthfrom LowCardinality(String),
    Yearmonthto LowCardinality(String),
    Companycategory LowCardinality(String),
    Sistercompanyname LowCardinality(String),
    Sourcesystemurl String CODEC(ZSTD(1)),
    Pan String CODEC(ZSTD(1)),
    Gstnumber  String CODEC(ZSTD(1)),
    Gstusername String CODEC(ZSTD(1)),
    Perfiostransactionid String CODEC(ZSTD(1)),
    Bankname LowCardinality(String),
    Institutionid Nullable(Int32),
    Passwordflg LowCardinality(String),
    Employmenttype LowCardinality(String),
    Entityname  String CODEC(ZSTD(1)),
    Employername String CODEC(ZSTD(1)),
    Dateofincorporation String CODEC(ZSTD(1)),
    Companyname LowCardinality(String),
    Mailsendflag LowCardinality(String),
    Perfiosloantype LowCardinality(String),
    Appproduct LowCardinality(String),
    Redirecturl String CODEC(ZSTD(1)),
    Apisource String CODEC(ZSTD(1)),
    Statementid String CODEC(ZSTD(1)),
    Borrowertype LowCardinality(String),
    Facility LowCardinality(String),
    Sanctionlimitfixedamount LowCardinality(String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree PARTITION BY toYYYYMM(created_at)
ORDER BY (session_id, account_id);

CREATE MATERIALIZED VIEW TcapCustomersConsumer TO TcapCustomers AS
SELECT *
FROM TcapCustomersQueue;