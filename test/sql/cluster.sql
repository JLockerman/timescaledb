CREATE TABLE cluster_test(time timestamptz, temp float, location int);

SELECT create_hypertable('cluster_test', 'time', chunk_time_interval => interval '1 day');

-- Show default indexes
SELECT * FROM test.show_indexes('cluster_test');

-- Create two chunks
INSERT INTO cluster_test VALUES ('2017-01-20T09:00:01', 23.4, 1),
       ('2017-01-21T09:00:01', 21.3, 2);

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- Run cluster
CLUSTER VERBOSE cluster_test USING cluster_test_time_idx;

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- Create a third chunk
INSERT INTO cluster_test VALUES ('2017-01-22T09:00:01', 19.5, 3);

-- Show clustered indexes
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true;

-- Recluster just our table
CLUSTER VERBOSE cluster_test;

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- Show clustered indexes, including new chunk
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true;

-- Recluster all tables (although will only be our test table)
CLUSTER VERBOSE;

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- Change the clustered index
CREATE INDEX ON cluster_test (time, location);

CLUSTER VERBOSE cluster_test USING cluster_test_time_location_idx;

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

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

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- we should start read_optimized
SELECT setting FROM pg_settings WHERE name = 'timescaledb.cluster_method';

-- Set guc to run basic cluster
UPDATE pg_settings SET setting = 'native' WHERE name = 'timescaledb.cluster_method';

-- and old cluster should be run
CLUSTER VERBOSE cluster_test USING cluster_test_time_location_idx;
CLUSTER VERBOSE _timescaledb_internal._hyper_1_2_chunk;

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- Set guc back
UPDATE pg_settings SET setting = 'read_optimized' WHERE name = 'timescaledb.cluster_method';

-- and new cluster should be run
CLUSTER VERBOSE cluster_test USING cluster_test_time_location_idx;
CLUSTER VERBOSE _timescaledb_internal._hyper_1_2_chunk;

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
    SELECT * FROM cluster_test WHERE time='2017-01-21T09:00:01';
COMMIT;


-- test with TOAST (based on size_utils.sql)
CREATE TABLE toast_test(time TIMESTAMP, value TEXT);
-- Set storage type to EXTERNAL to prevent PostgreSQL from compressing my
-- easily compressable string and instead store it with TOAST
ALTER TABLE toast_test ALTER COLUMN value SET STORAGE EXTERNAL;
SELECT * FROM create_hypertable('toast_test', 'time');

INSERT INTO toast_test VALUES('2004-10-19 10:23:54', $$
this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k.
$$);

SELECT * FROM toast_test;

CLUSTER VERBOSE toast_test USING toast_test_time_idx;
SELECT * FROM toast_test;

-- force CLUSTER to vaccum the toast_table
ALTER TABLE toast_test DROP COLUMN value;
SELECT * FROM toast_test;
CLUSTER VERBOSE toast_test USING cluster_test_time_idx;
SELECT * FROM toast_test;

-- after each test we ensure bitmap scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=true;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2004-10-19 10:23:54';
    SELECT * FROM toast_test WHERE time='2004-10-19 10:23:54';
COMMIT;

-- after each test we ensure index scans work
BEGIN;
    SET LOCAL enable_seqscan=false;
    SET LOCAL enable_indexscan=true;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2004-10-19 10:23:54';
    SELECT * FROM toast_test WHERE time='2004-10-19 10:23:54';
COMMIT;

-- after each test we ensure sequntial scans work
BEGIN;
    SET LOCAL enable_seqscan=true;
    SET LOCAL enable_indexscan=false;
    SET LOCAL enable_bitmapscan=false;
    EXPLAIN (costs off) SELECT * FROM cluster_test WHERE time='2004-10-19 10:23:54';
    SELECT * FROM toast_test WHERE time='2004-10-19 10:23:54';
COMMIT;

-- if we someone elses index error
CLUSTER VERBOSE toast_test USING cluster_test_time_idx;

--check the setting of cluster indexes on hypertables and chunks
ALTER TABLE cluster_test CLUSTER ON cluster_test_time_idx;

SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true
ORDER BY 1,2;

CLUSTER VERBOSE cluster_test;

ALTER TABLE cluster_test SET WITHOUT CLUSTER;

SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true
ORDER BY 1,2;

\set ON_ERROR_STOP 0
CLUSTER VERBOSE cluster_test;
\set ON_ERROR_STOP 1

ALTER TABLE _timescaledb_internal._hyper_1_1_chunk CLUSTER ON _hyper_1_1_chunk_cluster_test_time_idx;

SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true
ORDER BY 1,2;

CLUSTER VERBOSE _timescaledb_internal._hyper_1_1_chunk;

ALTER TABLE _timescaledb_internal._hyper_1_1_chunk SET WITHOUT CLUSTER;

SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true
ORDER BY 1,2;

\set ON_ERROR_STOP 0
CLUSTER VERBOSE _timescaledb_internal._hyper_1_1_chunk;
\set ON_ERROR_STOP 1
