CREATE TABLE mocktemplates (
    template_uuid VARCHAR,
    template_type VARCHAR,
    template_json VARCHAR,
    bank_name VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Kolkata', CURRENT_TIMESTAMP),
    request_by VARCHAR,
    active_status INTEGER,
    statement_id VARCHAR,
    priority INTEGER,
    priority_to INTEGER
);