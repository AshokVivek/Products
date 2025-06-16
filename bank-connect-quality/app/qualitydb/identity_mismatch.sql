create table identity_mismatch_statement (
    id SERIAL PRIMARY KEY,
    statement_id VARCHAR(255) NOT NULL,
    bank_name VARCHAR(255) NOT NULL,
    pdf_password VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    name_mismatch_case boolean default FALSE,
    name_mismatch_ignore_case boolean default FALSE,
    name_mismatch_maker_parked_data JSONB,
    name_mismatch_checker_status boolean default FALSE,
    name_mismatch_maker_status boolean default FALSE,
    name_matched_pattern varchar(255)
);

alter table identity_mismatch_statement
ADD COLUMN ignore_by_user varchar(255);

alter table identity_mismatch_statement
add column name varchar(255);

alter table identity_mismatch_statement
add column account_number varchar,
add column account_number_mismatch_case boolean default FALSE,
add column account_number_mismatch_ignore_case boolean default FALSE,
add column account_number_mismatch_maker_parked_data JSONB,
add column account_number_mismatch_checker_status boolean default FALSE,
add column account_number_mismatch_maker_status boolean default FALSE;

CREATE INDEX idx_identity_mismatch ON identity_mismatch_statement (name_mismatch_case, name_mismatch_ignore_case, name_mismatch_maker_status, name_mismatch_checker_status);