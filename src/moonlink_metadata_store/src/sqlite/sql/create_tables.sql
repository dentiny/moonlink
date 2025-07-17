-- SQL statement(s) to store moonlink managed column store tables.
CREATE TABLE tables (
    database_id INTEGER,        -- column store database OID
    table_id INTEGER,           -- column store table OID
    table_name text NOT NULL,   -- source table name
    uri text,                   -- source URI
    config TEXT,                -- mooncake and persistence configurations
    cardinality INTEGER,        -- estimated row count or similar
    PRIMARY KEY (database_id, table_id)
);
