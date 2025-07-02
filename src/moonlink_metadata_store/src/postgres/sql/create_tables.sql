-- SQL statement(s) to store moonlink managed column store tables.
CREATE TABLE mooncake.tables (
    oid oid PRIMARY KEY,          -- column store table OID
    table_name text NOT NULL,     -- source table name
    uri text,                     -- source URI
    config json,                  -- mooncake and persistence configurations
    cardinality bigint            -- estimated row count or similar
);
