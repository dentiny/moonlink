-- SQL statements to store moonlink secret related fields.
CREATE TABLE secrets (
    id SERIAL PRIMARY KEY,          -- unique row identifier
    uid TEXT DEFAULT current_user,  -- user for the secret
    oid INTEGER,                    -- Mooncake table OID.
    secret_type TEXT,               -- One of (S3, GCS)
    key_id TEXT,        
    secret TEXT,        
    project TEXT,          -- (optional)  
    endpoint TEXT,         -- (optional)
    region TEXT            -- (optional)
);

-- Index to enable query on (oid, uid).
CREATE INDEX idx_secrets_uid_oid ON secrets (uid, oid);
