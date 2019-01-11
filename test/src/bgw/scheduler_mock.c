/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>
#include <utils/snapmgr.h>
#include <utils/memutils.h>
#include <utils/builtins.h>
#include <access/xact.h>
#include <pgstat.h>
#include <signal.h>

#include "extension.h"
#include "log.h"
#include "bgw/scheduler.h"
#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "timer_mock.h"
#include "test_jobs.h"
#include "params.h"


TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish);
TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_test_run);
TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_test_wait_for_scheduler_finish);
TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_test_main);
TS_FUNCTION_INFO_V1(ts_bgw_job_execute_test);
TS_FUNCTION_INFO_V1(ts_test_bgw_job_insert_relation);
TS_FUNCTION_INFO_V1(ts_test_bgw_job_delete_by_id);

static char *
serialize_test_parameters(int32 ttl)
{
	JsonbValue *result;
	JsonbValue	ttl_value;
	JsonbParseState *parseState = NULL;
	Jsonb	   *jb;
	StringInfo	jtext = makeStringInfo();

	ttl_value.type = jbvNumeric;
	ttl_value.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(ttl)));

	result = pushJsonbValue(&parseState, WJB_BEGIN_ARRAY, NULL);

	result = pushJsonbValue(&parseState, WJB_ELEM, &ttl_value);

	result = pushJsonbValue(&parseState, WJB_END_ARRAY, NULL);

	jb = JsonbValueToJsonb(result);
	(void) JsonbToCString(jtext, &jb->root, VARSIZE(jb));
	Assert(jtext->len < BGW_EXTRALEN);

	return jtext->data;
}

static void
deserialize_test_parameters(char *params, int32 *ttl)
{
	Jsonb	   *jb = (Jsonb *) DatumGetPointer(DirectFunctionCall1(jsonb_in, CStringGetDatum(params)));
	JsonbValue *ttl_v = getIthJsonbValueFromContainer(&jb->root, 0);
	Numeric		ttl_numeric;

	Assert(ttl_v->type == jbvNumeric);

	ttl_numeric = ttl_v->val.numeric;
	*ttl = DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(ttl_numeric)));
}

extern Datum
ts_bgw_db_scheduler_test_main(PG_FUNCTION_ARGS)
{
	Oid			db_oid = DatumGetObjectId(MyBgworkerEntry->bgw_main_arg);
	int32		ttl;

	BackgroundWorkerBlockSignals();
	/* Setup any signal handlers here */
	BackgroundWorkerUnblockSignals();
	ts_bgw_scheduler_setup_callbacks();

	deserialize_test_parameters(MyBgworkerEntry->bgw_extra, &ttl);

	elog(WARNING, "running a test in the background: db=%d ttl=%d", db_oid, ttl);

	BackgroundWorkerInitializeConnectionByOidCompat(db_oid, InvalidOid);

	StartTransactionCommand();
	ts_params_get();
	ts_initialize_timer_latch();
	CommitTransactionCommand();

	ts_bgw_log_set_application_name("DB Scheduler");
	ts_register_emit_log_hook();

	ts_timer_set(&ts_mock_timer);

	ts_bgw_job_set_job_entrypoint_function_name("ts_bgw_job_execute_test");

	pgstat_report_appname("DB Scheduler Test");

	ts_bgw_scheduler_process(ttl, ts_timer_mock_register_bgw_handle);

	PG_RETURN_VOID();
}

extern Datum
ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(PG_FUNCTION_ARGS)
{
	char	   *params = serialize_test_parameters(PG_GETARG_INT32(0));
	BackgroundWorkerHandle *worker_handle;
	pid_t		pid;

	worker_handle = ts_bgw_start_worker("ts_bgw_db_scheduler_test_main", "ts_bgw_db_scheduler_test_main", params);

	Assert(BGWH_STARTED == WaitForBackgroundWorkerStartup(worker_handle, &pid));
	Assert(BGWH_STOPPED == WaitForBackgroundWorkerShutdown(worker_handle));

	PG_RETURN_VOID();
}

static BackgroundWorkerHandle *current_handle = NULL;

extern Datum
ts_bgw_db_scheduler_test_run(PG_FUNCTION_ARGS)
{
	char	   *params = serialize_test_parameters(PG_GETARG_INT32(0));
	pid_t		pid;
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	current_handle = ts_bgw_start_worker("ts_bgw_db_scheduler_test_main", "ts_bgw_db_scheduler_test_main", params);
	MemoryContextSwitchTo(old_ctx);


	Assert(BGWH_STARTED == WaitForBackgroundWorkerStartup(current_handle, &pid));

	PG_RETURN_VOID();
}

extern Datum
ts_bgw_db_scheduler_test_wait_for_scheduler_finish(PG_FUNCTION_ARGS)
{
	Assert(BGWH_STOPPED == WaitForBackgroundWorkerShutdown(current_handle));
	PG_RETURN_VOID();
}

static bool
test_job_dispatcher(BgwJob *job)
{
	int job_id;
	job_type job_fn;
	ts_register_emit_log_hook();
	ts_bgw_log_set_application_name(NameStr(job->fd.application_name));

	StartTransactionCommand();
	ts_params_get();
	CommitTransactionCommand();

	job_id = ts_test_job_get_by_name(&job->fd.job_type);
	if (job_id < 0)
		elog(ERROR, "unrecognized test job type: %s", NameStr(job->fd.job_type));

	job_fn = ts_test_job_get(job_id);

	if (job_fn == test_job_4_delayed_start)
	{
		/* Set next_start to 200ms */
		Interval   *new_interval = DatumGetIntervalP(DirectFunctionCall7(make_interval, Int32GetDatum(0), Int32GetDatum(0), Int32GetDatum(0), Int32GetDatum(0), Int32GetDatum(0), Int32GetDatum(0), Float8GetDatum(0.2)));

		return ts_bgw_job_run_and_set_next_start(job, job_fn, 3, new_interval);
	}
	else
		return job_fn();

	return false;
}

Datum
ts_bgw_job_execute_test(PG_FUNCTION_ARGS)
{
	ts_timer_set(&ts_mock_timer);
	ts_bgw_job_set_unknown_job_type_hook(test_job_dispatcher);

	return ts_bgw_job_entrypoint(fcinfo);
}

Datum
ts_test_bgw_job_insert_relation(PG_FUNCTION_ARGS)
{
	ts_bgw_job_insert_relation(PG_GETARG_NAME(0), PG_GETARG_NAME(1), PG_GETARG_INTERVAL_P(2), PG_GETARG_INTERVAL_P(3), PG_GETARG_INT32(4), PG_GETARG_INTERVAL_P(5));

	PG_RETURN_NULL();
}

Datum
ts_test_bgw_job_delete_by_id(PG_FUNCTION_ARGS)
{
	ts_bgw_job_delete_by_id(PG_GETARG_INT32(0));
	PG_RETURN_NULL();
}
