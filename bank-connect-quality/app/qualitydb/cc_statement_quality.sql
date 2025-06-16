CREATE TABLE cc_statement_quality (
    id SERIAL PRIMARY KEY,
    statement_id VARCHAR(255),
    bank_name VARCHAR(255) NOT NULL,
    pdf_password VARCHAR(255),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    cc_null BOOLEAN DEFAULT FALSE,
    cc_null_maker_status BOOLEAN DEFAULT FALSE,
    cc_null_maker_parked_data JSONB,
    cc_null_checker_status BOOLEAN DEFAULT FALSE,
    cc_null_ignore_case BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_cc_null ON cc_statement_quality (cc_null);
CREATE INDEX idx_cc_null_status_checker ON cc_statement_quality (cc_null, cc_null_maker_status, cc_null_checker_status);

ALTER TABLE cc_statement_quality
ADD COLUMN pdf_ignore_reason VARCHAR(255);

ALTER TABLE cc_statement_quality
ADD COLUMN cc_payment_due_date_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_payment_due_date_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_payment_due_date_null_maker_parked_data JSONB,
ADD COLUMN cc_payment_due_date_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_payment_due_date_null_ignore_case BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_total_dues_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_total_dues_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_total_dues_null_maker_parked_data JSONB,
ADD COLUMN cc_total_dues_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_total_dues_null_ignore_case BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_cc_payment_due_date_null ON cc_statement_quality (cc_payment_due_date_null);
CREATE INDEX idx_cc_payment_due_date_null_status_checker ON cc_statement_quality (cc_payment_due_date_null, cc_payment_due_date_null_maker_status, cc_payment_due_date_null_checker_status);

CREATE INDEX idx_cc_total_dues_null ON cc_statement_quality (cc_total_dues_null);
CREATE INDEX idx_cc_total_dues_null_status_checker ON cc_statement_quality (cc_total_dues_null, cc_total_dues_null_maker_status, cc_total_dues_null_checker_status);

ALTER TABLE cc_statement_quality
ADD COLUMN cc_statement_date_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_statement_date_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_statement_date_null_maker_parked_data JSONB,
ADD COLUMN cc_statement_date_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_statement_date_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_min_amt_due_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_min_amt_due_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_min_amt_due_null_maker_parked_data JSONB,
ADD COLUMN cc_min_amt_due_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_min_amt_due_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_purchase_debits_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_purchase_debits_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_purchase_debits_null_maker_parked_data JSONB,
ADD COLUMN cc_purchase_debits_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_purchase_debits_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_name_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_name_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_name_null_maker_parked_data JSONB,
ADD COLUMN cc_name_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_name_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_credit_limit_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_credit_limit_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_credit_limit_null_maker_parked_data JSONB,
ADD COLUMN cc_credit_limit_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_credit_limit_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_opening_balance_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_opening_balance_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_opening_balance_null_maker_parked_data JSONB,
ADD COLUMN cc_opening_balance_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_opening_balance_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_avl_credit_limit_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_avl_credit_limit_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_avl_credit_limit_null_maker_parked_data JSONB,
ADD COLUMN cc_avl_credit_limit_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_avl_credit_limit_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_avl_cash_limit_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_avl_cash_limit_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_avl_cash_limit_null_maker_parked_data JSONB,
ADD COLUMN cc_avl_cash_limit_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_avl_cash_limit_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_address_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_address_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_address_null_maker_parked_data JSONB,
ADD COLUMN cc_address_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_address_null_ignore_case BOOLEAN DEFAULT FALSE,

ADD COLUMN cc_payment_credits_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_payment_credits_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_payment_credits_null_maker_parked_data JSONB,
ADD COLUMN cc_payment_credits_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_payment_credits_null_ignore_case BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_cc_statement_date_null ON cc_statement_quality (cc_statement_date_null);
CREATE INDEX idx_cc_statement_date_null_status_checker ON cc_statement_quality (cc_statement_date_null, cc_statement_date_null_maker_status, cc_statement_date_null_checker_status);

CREATE INDEX idx_cc_min_amt_due_null ON cc_statement_quality (cc_min_amt_due_null);
CREATE INDEX idx_cc_min_amt_due_null_status_checker ON cc_statement_quality (cc_min_amt_due_null, cc_min_amt_due_null_maker_status, cc_min_amt_due_null_checker_status);

CREATE INDEX idx_cc_purchase_debits_null ON cc_statement_quality (cc_purchase_debits_null);
CREATE INDEX idx_cc_purchase_debits_null_status_checker ON cc_statement_quality (cc_purchase_debits_null, cc_purchase_debits_null_maker_status, cc_purchase_debits_null_checker_status);

CREATE INDEX idx_cc_name_null ON cc_statement_quality (cc_name_null);
CREATE INDEX idx_cc_name_null_status_checker ON cc_statement_quality (cc_name_null, cc_name_null_maker_status, cc_name_null_checker_status);

CREATE INDEX idx_cc_credit_limit_null ON cc_statement_quality (cc_credit_limit_null);
CREATE INDEX idx_cc_credit_limit_null_status_checker ON cc_statement_quality (cc_credit_limit_null, cc_credit_limit_null_maker_status, cc_credit_limit_null_checker_status);

CREATE INDEX idx_cc_opening_balance_null ON cc_statement_quality (cc_opening_balance_null);
CREATE INDEX idx_cc_opening_balance_null_status_checker ON cc_statement_quality (cc_opening_balance_null, cc_opening_balance_null_maker_status, cc_opening_balance_null_checker_status);

CREATE INDEX idx_cc_avl_credit_limit_null ON cc_statement_quality (cc_avl_credit_limit_null);
CREATE INDEX idx_cc_avl_credit_limit_null_status_checker ON cc_statement_quality (cc_avl_credit_limit_null, cc_avl_credit_limit_null_maker_status, cc_avl_credit_limit_null_checker_status);

CREATE INDEX idx_cc_avl_cash_limit_null ON cc_statement_quality (cc_avl_cash_limit_null);
CREATE INDEX idx_cc_avl_cash_limit_null_status_checker ON cc_statement_quality (cc_avl_cash_limit_null, cc_avl_cash_limit_null_maker_status, cc_avl_cash_limit_null_checker_status);

CREATE INDEX idx_cc_address_null ON cc_statement_quality (cc_address_null);
CREATE INDEX idx_cc_address_null_status_checker ON cc_statement_quality (cc_address_null, cc_address_null_maker_status, cc_address_null_checker_status);

CREATE INDEX idx_cc_payment_credits_null ON cc_statement_quality (cc_payment_credits_null);
CREATE INDEX idx_cc_payment_credits_null_status_checker ON cc_statement_quality (cc_payment_credits_null, cc_payment_credits_null_maker_status, cc_payment_credits_null_checker_status);

ALTER TABLE cc_statement_quality ADD COLUMN password_type VARCHAR(255);
CREATE INDEX idx_cc_statement_id ON cc_statement_quality (statement_id);
ALTER TABLE cc_statement_quality
ADD COLUMN cc_type_null BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_type_null_maker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_type_null_maker_parked_data JSONB,
ADD COLUMN cc_type_null_checker_status BOOLEAN DEFAULT FALSE,
ADD COLUMN cc_type_null_ignore_case BOOLEAN DEFAULT FALSE;



DROP INDEX IF EXISTS 
 idx_cc_payment_due_date_null_status_checker,
 idx_cc_total_dues_null_status_checker, 
 idx_cc_statement_date_null_status_checker, 
 idx_cc_min_amt_due_null_status_checker,
 idx_cc_purchase_debits_null_status_checker, 
 idx_cc_name_null_status_checker, 
 idx_cc_credit_limit_null_status_checker, 
 idx_cc_opening_balance_null_status_checker,
 idx_cc_avl_credit_limit_null_status_checker, 
 idx_cc_avl_cash_limit_null_status_checker, 
 idx_cc_address_null_status_checker,
 idx_cc_payment_credits_null_status_checker;


CREATE INDEX idx_cc_payment_due_date_null_status_checker ON cc_statement_quality (cc_payment_due_date_null, cc_payment_due_date_null_maker_status, cc_payment_due_date_null_checker_status, cc_null);

CREATE INDEX idx_cc_total_dues_null_status_checker ON cc_statement_quality (cc_total_dues_null, cc_total_dues_null_maker_status, cc_total_dues_null_checker_status, cc_null);

CREATE INDEX idx_cc_statement_date_null_status_checker ON cc_statement_quality (cc_statement_date_null, cc_statement_date_null_maker_status, cc_statement_date_null_checker_status, cc_null);

CREATE INDEX idx_cc_min_amt_due_null_status_checker ON cc_statement_quality (cc_min_amt_due_null, cc_min_amt_due_null_maker_status, cc_min_amt_due_null_checker_status, cc_null);

CREATE INDEX idx_cc_purchase_debits_null_status_checker ON cc_statement_quality (cc_purchase_debits_null, cc_purchase_debits_null_maker_status, cc_purchase_debits_null_checker_status, cc_null);

CREATE INDEX idx_cc_name_null_status_checker ON cc_statement_quality (cc_name_null, cc_name_null_maker_status, cc_name_null_checker_status, cc_null);

CREATE INDEX idx_cc_credit_limit_null_status_checker ON cc_statement_quality (cc_credit_limit_null, cc_credit_limit_null_maker_status, cc_credit_limit_null_checker_status, cc_null);

CREATE INDEX idx_cc_opening_balance_null_status_checker ON cc_statement_quality (cc_opening_balance_null, cc_opening_balance_null_maker_status, cc_opening_balance_null_checker_status, cc_null);

CREATE INDEX idx_cc_avl_credit_limit_null_status_checker ON cc_statement_quality (cc_avl_credit_limit_null, cc_avl_credit_limit_null_maker_status, cc_avl_credit_limit_null_checker_status, cc_null);

CREATE INDEX idx_cc_avl_cash_limit_null_status_checker ON cc_statement_quality (cc_avl_cash_limit_null, cc_avl_cash_limit_null_maker_status, cc_avl_cash_limit_null_checker_status, cc_null);

CREATE INDEX idx_cc_address_null_status_checker ON cc_statement_quality (cc_address_null, cc_address_null_maker_status, cc_address_null_checker_status, cc_null);

CREATE INDEX idx_cc_payment_credits_null_status_checker ON cc_statement_quality (cc_payment_credits_null, cc_payment_credits_null_maker_status, cc_payment_credits_null_checker_status, cc_null);
