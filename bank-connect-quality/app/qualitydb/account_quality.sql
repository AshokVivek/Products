CREATE TABLE account_quality (
    id SERIAL PRIMARY KEY,
    entity_id VARCHAR(255),
    account_id VARCHAR(255),
    statement_list JSONB,
    bank_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Kolkata', CURRENT_TIMESTAMP),
    
    is_inconsistent BOOLEAN DEFAULT FALSE,
    inconsistent_maker_status BOOLEAN DEFAULT FALSE,
    inconsistent_maker_data JSONB,
    inconsistent_context_data JSONB,
    inconsistent_checker_status BOOLEAN DEFAULT FALSE,
    inconsistent_remarks VARCHAR(255)
)

ALTER TABLE account_quality ADD COLUMN inconsistent_ignore_case BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_account_entity_account ON account_quality (entity_id, account_id);