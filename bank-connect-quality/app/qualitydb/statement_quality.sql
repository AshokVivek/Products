CREATE TABLE statement_quality (
    id SERIAL PRIMARY KEY,
    statement_id VARCHAR(255),
    bank_name VARCHAR(255) NOT NULL,
    pdf_password VARCHAR(255),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    name_null BOOLEAN DEFAULT FALSE,
    name_null_maker_status BOOLEAN DEFAULT FALSE,
    name_null_maker_parked_data JSONB,
    name_null_checker_status BOOLEAN DEFAULT FALSE,
    name_null_ignore_case BOOLEAN DEFAULT FALSE,

    account_null BOOLEAN DEFAULT FALSE,
    account_null_maker_status BOOLEAN DEFAULT FALSE,
    account_null_maker_parked_data JSONB,
    account_null_checker_status BOOLEAN DEFAULT FALSE,
    account_null_ignore_case BOOLEAN DEFAULT FALSE,
    
    date_null BOOLEAN DEFAULT FALSE,
    date_null_maker_status BOOLEAN DEFAULT FALSE,
    date_null_maker_parked_data JSONB,
    date_null_checker_status BOOLEAN DEFAULT FALSE,
    date_null_ignore_case BOOLEAN DEFAULT FALSE
);

-- Create an index on name_null field
CREATE INDEX idx_name_null ON statement_quality (name_null);

-- Create an index on name_null, name_null_maker_status, name_null_checker_status fields
CREATE INDEX idx_name_null_status_checker ON statement_quality (name_null, name_null_maker_status, name_null_checker_status);

-- Create an index on date_null field
CREATE INDEX idx_date_null ON statement_quality (date_null);

-- Create an index on date_null, date_null_maker_status, date_null_checker_status fields
CREATE INDEX idx_date_null_status_checker ON statement_quality (date_null, date_null_maker_status, date_null_checker_status);

-- Create an index on account_null field
CREATE INDEX idx_account_null ON statement_quality (account_null);

-- Create an index on account_null, account_null_maker_status, account_null_checker_status fields
CREATE INDEX idx_account_null_status_checker ON statement_quality (account_null, account_null_maker_status, account_null_checker_status);


ALTER TABLE statement_quality
ADD COLUMN logo_null BOOLEAN DEFAULT FALSE,
ADD COLUMN logo_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN logo_null_maker_parked_data JSONB,
ADD COLUMN logo_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN logo_null_ignore_case BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_logo_null ON statement_quality (logo_null);
CREATE INDEX idx_logo_null_status_checker ON statement_quality (logo_null, logo_null_maker_status, logo_null_checker_status);

-- Migration for pdf ignore case
ALTER TABLE statement_quality
ADD COLUMN pdf_ignore_reason VARCHAR(255);

ALTER TABLE statement_quality
ADD COLUMN ac_category_null BOOLEAN DEFAULT FALSE,
ADD COLUMN ac_category_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN ac_category_null_maker_parked_data JSONB,
ADD COLUMN ac_category_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN ac_category_null_ignore_case BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_acc_category_null ON statement_quality (ac_category_null);
CREATE INDEX idx_acc_category_null_status_checker ON statement_quality (ac_category_null, ac_category_null_maker_status, ac_category_null_checker_status);


ALTER TABLE statement_quality
ADD COLUMN ifsc_null BOOLEAN DEFAULT FALSE,
ADD COLUMN ifsc_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN ifsc_null_maker_parked_data JSONB,
ADD COLUMN ifsc_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN ifsc_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN micr_null BOOLEAN DEFAULT FALSE,
ADD COLUMN micr_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN micr_null_maker_parked_data JSONB,
ADD COLUMN micr_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN micr_null_ignore_case BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_ifsc_null ON statement_quality (ifsc_null);
CREATE INDEX idx_ifsc_null_status_checker ON statement_quality (ifsc_null, ifsc_null_maker_status, ifsc_null_checker_status);

CREATE INDEX idx_micr_null ON statement_quality (micr_null);
CREATE INDEX idx_micr_null_status_checker ON statement_quality (micr_null, micr_null_maker_status, micr_null_checker_status);

ALTER TABLE statement_quality
ADD COLUMN is_od_account_detected BOOLEAN DEFAULT FALSE,
ADD COLUMN is_credit_limit_detected BOOLEAN DEFAULT FALSE,
ADD COLUMN is_od_limit_detected BOOLEAN DEFAULT FALSE,

ADD COLUMN is_od_account_bbox_simulated BOOLEAN DEFAULT FALSE,
ADD COLUMN limit_bbox_simulated BOOLEAN DEFAULT FALSE,
ADD COLUMN od_limit_bbox_simulated BOOLEAN DEFAULT FALSE,

ADD COLUMN od_or_limit_null BOOLEAN DEFAULT FALSE,
ADD COLUMN od_or_limit_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN od_or_limit_null_maker_parked_data JSONB,
ADD COLUMN od_or_limit_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN od_or_limit_null_ignore_case BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_od_or_limit_null ON statement_quality (od_or_limit_null);
CREATE INDEX idx_od_or_limit_null_status_checker ON statement_quality (od_or_limit_null, od_or_limit_null_maker_status, od_or_limit_null_checker_status);

ALTER TABLE statement_quality
ADD COLUMN ac_category_ingest_keyword VARCHAR(255),
ADD COLUMN ifsc_ingest_keyword VARCHAR(255),
ADD COLUMN micr_ingest_keyword VARCHAR(255);
CREATE INDEX idx_statement_id_quality ON statement_quality (statement_id);

ALTER TABLE statement_quality
ADD COLUMN address_null BOOLEAN DEFAULT FALSE,
ADD COLUMN address_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN address_null_maker_parked_data JSONB,
ADD COLUMN address_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN address_null_ignore_case BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_address_null_quality ON statement_quality (address_null);
CREATE INDEX idx_address_null_status_checker_quality ON statement_quality (address_null, address_null_maker_status, address_null_checker_status);

CREATE INDEX idx_parked_data_logo_quality ON statement_quality (bank_name, logo_null);

CREATE INDEX idx_acc_category_get_null_checker ON statement_quality (ac_category_null, ac_category_null_maker_status, ac_category_null_checker_status, ac_category_null_ignore_case);
CREATE INDEX idx_name_get_null_checker ON statement_quality (name_null, name_null_maker_status, name_null_checker_status, name_null_ignore_case);

ALTER TABLE statement_quality
ADD COLUMN inconsistency_due_to_extraction BOOLEAN DEFAULT FALSE,
ADD COLUMN inconsistent_statement_data JSONB DEFAULT NULL;

CREATE INDEX idx_inconsistency_due_to_extraction ON statement_quality(inconsistency_due_to_extraction) WHERE inconsistency_due_to_extraction = TRUE;

ALTER TABLE statement_quality
ADD COLUMN client_id INTEGER,
ADD COLUMN organization_id INTEGER,
ADD COLUMN org_name VARCHAR(255);