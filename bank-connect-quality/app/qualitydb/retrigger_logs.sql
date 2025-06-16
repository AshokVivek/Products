CREATE TABLE retrigger_logs (
    id SERIAL PRIMARY KEY,
    retriggered_by VARCHAR NOT NULL,
    entity_id UUID NOT NULL,
    statement_id UUID NOT NULL,
    template_uuid VARCHAR(50),
    retrigger_type VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);