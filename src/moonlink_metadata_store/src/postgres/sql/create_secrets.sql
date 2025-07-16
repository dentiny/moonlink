-- SQL statements to store moonlink secret related fields.
BEGIN;

CREATE TABLE mooncake.secrets (
    id SERIAL PRIMARY KEY,          -- unique row identifier
    uid TEXT DEFAULT current_user,  -- user for the secret
    oid oid,                        -- Mooncake table OID.
    secret_type TEXT,               -- One of (S3, GCS)
    key_id TEXT,        
    secret TEXT,        
    project TEXT,          -- (optional)  
    endpoint TEXT,         -- (optional)
    region TEXT            -- (optional)
);

-- Index to enable query on (oid, uid).
CREATE INDEX idx_secrets_uid_oid ON mooncake.secrets (uid, oid);

ALTER TABLE mooncake.secrets ENABLE ROW LEVEL SECURITY;

-- Only user who inserts the row could access it.
CREATE POLICY secrets_self_access_policy
  ON mooncake.secrets
  FOR ALL
  USING (uid = current_user)
  WITH CHECK (uid = current_user);

GRANT SELECT, INSERT, UPDATE, DELETE ON mooncake.secrets TO PUBLIC;

COMMIT;
