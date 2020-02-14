-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE test_table(time INT, dev INT, val INT);

INSERT INTO test_table SELECT t, d, random() FROM generate_series(1, 1000) t, generate_series(1, 10) d;
INSERT INTO test_table VALUES (NULL, 0, -1), (0, NULL, -1);

CREATE INDEX ON test_table(dev);
CREATE INDEX ON test_table(dev NULLS FIRST);
CREATE INDEX ON test_table(dev, time);

\set PREFIX 'EXPLAIN (COSTS OFF)'
\ir include/skip_skan_test_query.sql

-- Doesn't SkipSkan with RuntimeKeys
EXPLAIN (COSTS off) SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM test_table WHERE dev >= a.v) b;
