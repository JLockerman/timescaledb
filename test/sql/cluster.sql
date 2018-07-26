CREATE TABLE cluster_test(time timestamptz, temp float, location int);

SELECT create_hypertable('cluster_test', 'time', chunk_time_interval => interval '1 day');

-- Show default indexes
SELECT * FROM test.show_indexes('cluster_test');

-- Create two chunks
INSERT INTO cluster_test VALUES ('2017-01-20T09:00:01', 23.4, 1),
       ('2017-01-21T09:00:01', 21.3, 2);

-- Run cluster
CLUSTER VERBOSE cluster_test USING cluster_test_time_idx;

-- Create a third chunk
INSERT INTO cluster_test VALUES ('2017-01-22T09:00:01', 19.5, 3);

-- Show clustered indexes
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true;

-- Recluster just our table
CLUSTER VERBOSE cluster_test;

-- Show clustered indexes, including new chunk
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true;

-- Recluster all tables (although will only be our test table)
CLUSTER VERBOSE;

-- Change the clustered index
CREATE INDEX ON cluster_test (time, location);

CLUSTER VERBOSE cluster_test USING cluster_test_time_location_idx;

-- Show updated clustered indexes
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true;

-- CLUSTER a chunk directly
CLUSTER VERBOSE _timescaledb_internal._hyper_1_2_chunk;

-- CLUSTER a chunk directly with an explicit index
CLUSTER VERBOSE _timescaledb_internal._hyper_1_2_chunk using cluster_test_time_idx;

-- CLUSTER a chunk directly with an chunk index
CLUSTER VERBOSE _timescaledb_internal._hyper_1_2_chunk using _hyper_1_2_chunk_cluster_test_time_idx;

-- we should start read_optimized
SELECT setting FROM pg_settings WHERE name = 'timescaledb.cluster_method';

-- Set guc to run basic cluster
UPDATE pg_settings SET setting = 'native' WHERE name = 'timescaledb.cluster_method';

-- and old cluster should be run
CLUSTER VERBOSE cluster_test USING cluster_test_time_location_idx;
CLUSTER VERBOSE _timescaledb_internal._hyper_1_2_chunk;

-- Set guc back
UPDATE pg_settings SET setting = 'read_optimized' WHERE name = 'timescaledb.cluster_method';

-- and new cluster should be run
CLUSTER VERBOSE cluster_test USING cluster_test_time_location_idx;
CLUSTER VERBOSE _timescaledb_internal._hyper_1_2_chunk;
