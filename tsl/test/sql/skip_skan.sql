-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE test_table(time INT, dev INT, val INT);

INSERT INTO test_table SELECT t, d, random() FROM generate_series(1, 1000) t, generate_series(1, 10) d;
INSERT INTO test_table VALUES (NULL, 0, -1), (0, NULL, -1);

CREATE INDEX ON test_table(dev);
CREATE INDEX ON test_table(dev NULLS FIRST);
CREATE INDEX ON test_table(dev, time);

CREATE TABLE skip_skan_results(idx BIGSERIAL PRIMARY KEY, time INT, dev INT, val INT, test TEXT);
CREATE TABLE base_results(idx BIGSERIAL PRIMARY KEY, time INT, dev INT, val INT, test TEXT);

\set PREFIX 'INSERT INTO skip_skan_results(time, dev, val, test)'
\ir include/skip_skan_test_query.sql

SET timescaledb.enable_skipskan TO false;

\set PREFIX 'INSERT INTO base_results(time, dev, val, test)'
\ir include/skip_skan_test_query.sql

SELECT *
FROM skip_skan_results FULL JOIN base_results ON skip_skan_results.idx = base_results.idx
WHERE skip_skan_results.time != base_results.time
   OR (skip_skan_results.time IS NULL != (base_results.time IS NULL))
   OR skip_skan_results.dev != base_results.dev
   OR (skip_skan_results.dev IS NULL != (base_results.dev IS NULL))
   OR skip_skan_results.val != base_results.val
   OR (skip_skan_results.val IS NULL != (base_results.val IS NULL));
