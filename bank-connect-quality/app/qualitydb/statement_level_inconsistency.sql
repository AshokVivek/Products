CREATE TABLE statement_level_inconsistency (
    id SERIAL PRIMARY KEY,
    statement_id VARCHAR(50) NOT NULL,
    inconsistent_hash VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Kolkata', CURRENT_TIMESTAMP),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Kolkata', CURRENT_TIMESTAMP),
    status VARCHAR(30),
    inconsistent_remarks VARCHAR(500),
    reason VARCHAR(100),
    type VARCHAR(100)
);

alter table statement_level_inconsistency
add column pdf_hash varchar,
add column bank_name varchar;

alter table statement_level_inconsistency
add column entity_id VARCHAR(50),
add column account_id VARCHAR(50),
add column organization_name VARCHAR(100),
add column organization_id INTEGER;