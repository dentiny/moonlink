-- SQL statement(s) to store moonlink managed column store tables.
CREATE TABLE tables (
    oid INTEGER PRIMARY KEY,      -- column store table OID
    table_name text NOT NULL,     -- source table name
    uri text,                     -- source URI
    config TEXT,                  -- mooncake and persistence configurations
    cardinality INTEGER           -- estimated row count or similar
);
