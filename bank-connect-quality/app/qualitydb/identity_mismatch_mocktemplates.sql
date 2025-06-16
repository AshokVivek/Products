create table identity_mismatch_mocktemplates(
    id SERIAL PRIMARY KEY,
    template_uuid varchar(255) NOT NULL,
    bank_name varchar(255) NOT NULL,
    active_status integer,
    statement_id varchar(255),
    request_by varchar(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    template_json JSONB
);

alter table identity_mismatch_mocktemplates
ADD COLUMN template_type varchar;