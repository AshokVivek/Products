CREATE TABLE fsmlib_data_update_requests (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(50) NOT NULL,
    requested_data JSONB NOT NULL,
    request_type VARCHAR(100) NOT NULL,
    requested_by VARCHAR(255) NOT NULL,
    statement_id VARCHAR(50) NOT NULL,
    operation_type VARCHAR(6) CHECK (operation_type IN ('update', 'delete')) NOT NULL,
    approval_status VARCHAR(8) CHECK (approval_status IN ('APPROVED', 'REJECTED', 'PENDING')) NOT NULL,
    approved_by VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Kolkata', CURRENT_TIMESTAMP),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Kolkata', CURRENT_TIMESTAMP)
);