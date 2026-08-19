-- Copyright (c) 2025 ADBC Drivers Contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--         http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Create primary test keyspace
CREATE KEYSPACE IF NOT EXISTS adbc_test
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Create secondary test keyspace
CREATE KEYSPACE IF NOT EXISTS adbc_test2
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Switch to test keyspace
USE adbc_test;

-- Create test tables with various data types
CREATE TABLE IF NOT EXISTS type_test (
    id UUID PRIMARY KEY,
    text_col TEXT,
    varchar_col VARCHAR,
    ascii_col ASCII,
    int_col INT,
    bigint_col BIGINT,
    smallint_col SMALLINT,
    tinyint_col TINYINT,
    float_col FLOAT,
    double_col DOUBLE,
    boolean_col BOOLEAN,
    timestamp_col TIMESTAMP,
    date_col DATE,
    time_col TIME,
    uuid_col UUID,
    timeuuid_col TIMEUUID,
    blob_col BLOB,
    decimal_col DECIMAL,
    varint_col VARINT,
    inet_col INET
) WITH comment = 'Table for testing all Cassandra data types';

-- Counter table must be separate
CREATE TABLE IF NOT EXISTS counter_test (
    id UUID PRIMARY KEY,
    counter_col COUNTER
) WITH comment = 'Table for testing counter type';

-- Create table for collection types
CREATE TABLE IF NOT EXISTS collection_test (
    id UUID PRIMARY KEY,
    list_col LIST<TEXT>,
    set_col SET<INT>,
    map_col MAP<TEXT, INT>
) WITH comment = 'Table for testing collection types';

-- Create table for testing primary key and clustering
CREATE TABLE IF NOT EXISTS clustering_test (
    partition_key TEXT,
    clustering_key1 INT,
    clustering_key2 TEXT,
    value TEXT,
    PRIMARY KEY (partition_key, clustering_key1, clustering_key2)
) WITH CLUSTERING ORDER BY (clustering_key1 ASC, clustering_key2 DESC);

-- Materialized views are disabled by default in Cassandra 5.0
-- Skipping materialized view creation

-- Insert sample data
INSERT INTO type_test (
    id, text_col, varchar_col, ascii_col,
    int_col, bigint_col, smallint_col, tinyint_col,
    float_col, double_col, boolean_col,
    timestamp_col, date_col, time_col,
    uuid_col, timeuuid_col, blob_col,
    decimal_col, varint_col, inet_col
) VALUES (
    uuid(),
    'Sample Text',
    'Sample Varchar',
    'sample',
    42,
    9223372036854775807,
    32767,
    127,
    3.14,
    2.718281828459045,
    true,
    toTimestamp(now()),
    '2025-03-29',
    '12:30:45.123456789',
    uuid(),
    now(),
    0x48656c6c6f,
    123.45,
    123456789,
    '127.0.0.1'
);

INSERT INTO collection_test (id, list_col, set_col, map_col)
VALUES (
    uuid(),
    ['item1', 'item2', 'item3'],
    {1, 2, 3, 4, 5},
    {'key1': 100, 'key2': 200}
);

INSERT INTO clustering_test (partition_key, clustering_key1, clustering_key2, value)
VALUES ('partition1', 1, 'a', 'value1');

INSERT INTO clustering_test (partition_key, clustering_key1, clustering_key2, value)
VALUES ('partition1', 2, 'b', 'value2');

INSERT INTO clustering_test (partition_key, clustering_key1, clustering_key2, value)
VALUES ('partition2', 1, 'c', 'value3');

-- Create a simple table for basic queries
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    username TEXT,
    email TEXT,
    created_at TIMESTAMP,
    age INT,
    is_active BOOLEAN
);

-- Insert sample users
INSERT INTO users (user_id, username, email, created_at, age, is_active)
VALUES (uuid(), 'alice', 'alice@example.com', toTimestamp(now()), 30, true);

INSERT INTO users (user_id, username, email, created_at, age, is_active)
VALUES (uuid(), 'bob', 'bob@example.com', toTimestamp(now()), 25, true);

INSERT INTO users (user_id, username, email, created_at, age, is_active)
VALUES (uuid(), 'charlie', 'charlie@example.com', toTimestamp(now()), 35, false);

-- Create secondary index
CREATE INDEX IF NOT EXISTS users_username_idx ON users (username);
CREATE INDEX IF NOT EXISTS users_is_active_idx ON users (is_active);

-- Switch to secondary keyspace
USE adbc_test2;

-- Create a simple table in the secondary keyspace
CREATE TABLE IF NOT EXISTS metadata_test (
    id UUID PRIMARY KEY,
    name TEXT,
    description TEXT
);

INSERT INTO metadata_test (id, name, description)
VALUES (uuid(), 'Test1', 'Description for test 1');

INSERT INTO metadata_test (id, name, description)
VALUES (uuid(), 'Test2', 'Description for test 2');
