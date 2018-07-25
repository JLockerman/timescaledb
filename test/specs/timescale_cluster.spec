setup
{
 CREATE TABLE ts_cluster_test(time timestamptz, temp float, location int);
 SELECT create_hypertable('ts_cluster_test', 'time', chunk_time_interval => interval '1 day');
 INSERT INTO ts_cluster_test VALUES ('2017-01-20T09:00:01', 23.4, 1),
       ('2017-01-21T09:00:01', 21.3, 2),
       ('2017-01-22T09:00:01', 19.5, 3);
}

teardown { DROP TABLE ts_cluster_test; }

session "s1"
setup		{ BEGIN; }
step "s1a"	{ SELECT * FROM ts_cluster_test; }
step "s1b"	{ INSERT INTO ts_cluster_test VALUES ('2017-01-22T09:00:01', 19.5, 3); }
step "s1c"	{ COMMIT; }

session "s2"
step "s2"	{ CLUSTER ts_cluster_test USING ts_cluster_test_time_idx; }

permutation "s1a" "s2" "s1b" "s1c"
