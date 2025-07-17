-- SQL statements to store moonlink secret related fields.
CREATE TABLE secrets (
    id SERIAL PRIMARY KEY,          -- unique row identifier
    database_id INTEGER,            -- Database id.
    table_id INTEGER,               -- Table id.
    secret_type TEXT,               -- One of (S3, GCS)
    key_id TEXT,
    secret TEXT,
    project TEXT,          -- (optional)  
    endpoint TEXT,         -- (optional)
    region TEXT            -- (optional)
);

-- Index to enable query on (database_id, table_id).
CREATE INDEX idx_secrets_uid_oid ON secrets (database_id, table_id);
