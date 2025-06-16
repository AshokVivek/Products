CREATE TABLE null_identity (
    id SERIAL PRIMARY KEY,
    statement_id VARCHAR(255),
    bank_name VARCHAR(255) NOT NULL,
    pdf_password VARCHAR(255),
    name_null_ignore_case BOOLEAN DEFAULT FALSE,
    name_null_ignore_regex_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Kolkata', CURRENT_TIMESTAMP)
);

alter table null_identity
add column name_null_updated_at timestamp default current_timestamp;

alter table null_identity
add column account_null_ignore_case BOOLEAN DEFAULT FALSE,
add column account_null_ignore_regex_id VARCHAR(255),
add column account_null_updated_at timestamp default current_timestamp,
add column date_null_ignore_case BOOLEAN DEFAULT FALSE,
add column date_null_ignore_regex_id VARCHAR(255),
add column date_null_updated_at timestamp default current_timestamp,
add column ac_category_null_ignore_case BOOLEAN DEFAULT FALSE,
add column ac_category_null_ignore_regex_id VARCHAR(255),
add column ac_category_null_updated_at timestamp default current_timestamp,
add column ifsc_null_ignore_case BOOLEAN DEFAULT FALSE,
add column ifsc_null_ignore_regex_id VARCHAR(255),
add column ifsc_null_updated_at timestamp default current_timestamp,
add column micr_null_ignore_case BOOLEAN DEFAULT FALSE,
add column micr_null_ignore_regex_id VARCHAR(255),
add column micr_null_updated_at timestamp default current_timestamp,
add column address_null_ignore_case BOOLEAN DEFAULT FALSE,
add column address_null_ignore_regex_id VARCHAR(255),
add column address_null_updated_at timestamp default current_timestamp;