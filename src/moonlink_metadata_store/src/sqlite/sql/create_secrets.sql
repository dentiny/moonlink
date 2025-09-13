-- SQL statements to store moonlink secret related fields.
CREATE TABLE secrets (
    id SERIAL PRIMARY KEY,      -- Unique row identifier
    "database" TEXT,            -- column store database name
    "table" TEXT,               -- column store table name
    usage_type TEXT CHECK (usage_type IN ('cloud', 'iceberg_storage', 'wal_storage')),
    provider TEXT CHECK (storage_provider IN ('aws', 's3', 'gcs')),
    key_id TEXT,
    secret TEXT,
    project TEXT,          -- (optional)  
    endpoint TEXT,         -- (optional)
    region TEXT            -- (optional)
);

-- Ensure at most one secret per usage type per table
CREATE UNIQUE INDEX IF NOT EXISTS idx_secrets_db_table_usage_type ON secrets ("database", "table", usage_type);
