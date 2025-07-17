-- SQL statement(s) to store moonlink managed column store tables.
CREATE TABLE tables (
    database_id oid,              -- column store database OID
    table_id oid,                 -- column store table OID
    table_name text NOT NULL,     -- source table name
    uri text,                     -- source URI
    config json,                  -- mooncake and persistence configurations
    cardinality bigint,           -- estimated row count or similar
    PRIMARY KEY (database_id, table_id)
);
