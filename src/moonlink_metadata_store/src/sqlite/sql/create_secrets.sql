-- SQL statements to store moonlink secret related fields.
CREATE TABLE secrets (
    id SERIAL PRIMARY KEY,      -- Unique row identifier
    mooncake_database TEXT,     -- column store database name
    mooncake_table TEXT,        -- column store table name
    secret_type TEXT,           -- One of (S3, GCS)
    key_id TEXT,
    secret TEXT,
    project TEXT,          -- (optional)  
    endpoint TEXT,         -- (optional)
    region TEXT            -- (optional)
);

-- Index to enable query on (mooncake_database, mooncake_table).
CREATE INDEX idx_secrets_uid_oid ON secrets (mooncake_database, mooncake_table);
