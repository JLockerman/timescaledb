
-- we add an addition optional argument to locf
DROP FUNCTION IF EXISTS locf(ANYELEMENT,ANYELEMENT);

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.optional_index_info (
    hypertable_index_name NAME PRIMARY KEY,
    is_scheduled BOOLEAN DEFAULT false
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_index', '');

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_scheduled_index (
    job_id          		INTEGER     PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		INTEGER     UNIQUE NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	hypertable_index_name	NAME		NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_scheduled_index', '');

DROP VIEW IF EXISTS timescaledb_information.policy_stats;
CREATE OR REPLACE VIEW timescaledb_information.policy_stats as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.job_id, j.job_type, js.last_run_success, js.last_finish, js.last_start, js.next_start,
    js.total_runs, js.total_failures
  FROM (SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_reorder
        UNION SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_drop_chunks
        UNION SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_scheduled_index) p
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id
    INNER JOIN _timescaledb_internal.bgw_job_stat js on p.job_id = js.job_id
  ORDER BY ht.schema_name, ht.table_name;
