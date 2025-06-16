CREATE TABLE users (
    username VARCHAR NOT NULL,
    password VARCHAR NOT NULL,
    type varchar default 'requester',
    PRIMARY KEY (username)
);

ALTER TABLE users ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Kolkata', CURRENT_TIMESTAMP);

CREATE INDEX search_user_key ON users (username);