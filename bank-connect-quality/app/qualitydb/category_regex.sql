CREATE TYPE transaction_type_enum AS ENUM ('debit', 'credit');

CREATE TABLE category_regex (
    id SERIAL PRIMARY KEY,
    bank_name VARCHAR(255) NOT NULL,
    transaction_type transaction_type_enum,
    regex TEXT NOT NULL,
    capturing_group_details JSONB NOT NULL,
    requested_by VARCHAR(255),
    approved_by VARCHAR(255),
    approved_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE category_regex ADD column cluster_id VARCHAR(150);

ALTER TABLE category_regex ADD old_category_regex JSONB DEFAULT NULL;