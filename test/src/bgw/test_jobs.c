#include <postgres.h>

#include <utils/builtins.h>
#include <access/xact.h>

#include "extension.h"
#include "timer_mock.h"

#include "test_jobs.h"

static bool test_job_1(void);
static bool test_job_2_error(void);
static bool test_job_3_long(void);

typedef enum TestJobType
{
	TEST_JOB_TYPE_JOB_1 = 0,
	TEST_JOB_TYPE_JOB_2_ERROR,
	TEST_JOB_TYPE_JOB_3_LONG,
	TEST_JOB_TYPE_JOB_4,
    _MAX_STATIC_JOBS,
} TestJobType;

static int next_job_id = _MAX_STATIC_JOBS;
static const char *test_job_type_names[TS_MAX_TEST_JOB_TYPE] = {
	[TEST_JOB_TYPE_JOB_1] = "bgw_test_job_1",
	[TEST_JOB_TYPE_JOB_2_ERROR] = "bgw_test_job_2_error",
	[TEST_JOB_TYPE_JOB_3_LONG] = "bgw_test_job_3_long",
	[TEST_JOB_TYPE_JOB_4] = "bgw_test_job_4",
};

job_type ts_test_jobs[TS_MAX_TEST_JOB_TYPE] = {
	[TEST_JOB_TYPE_JOB_1] = test_job_1,
	[TEST_JOB_TYPE_JOB_2_ERROR] = test_job_2_error,
	[TEST_JOB_TYPE_JOB_3_LONG] = test_job_3_long,
	[TEST_JOB_TYPE_JOB_4] = test_job_4_delayed_start,
};

TSDLLEXPORT int
ts_test_job_add(const char *job_name, job_type job_fn)
{
    int job_id;
    if (next_job_id >= TS_MAX_TEST_JOB_TYPE)
        elog(ERROR, "Cannot not add more jobs out of job ids");

    job_id = next_job_id;
    next_job_id += 1;

    test_job_type_names[next_job_id] = job_name;
    ts_test_jobs[next_job_id] = job_fn;

    return next_job_id;
}

TSDLLEXPORT job_type
ts_test_job_get(int job_id)
{
    job_type job = NULL;
    Assert(next_job_id <= TS_MAX_TEST_JOB_TYPE);
    if (job_id < 0 || job_id >= next_job_id)
        elog(ERROR, "Invalid test job id %d", job_id);

    job = ts_test_jobs[job_id];
    if (job == NULL)
        elog(ERROR, "Unrecognized test job %d", job_id);

    return job;
}

TSDLLEXPORT const char *
ts_test_job_get_name(int job_id)
{
    const char *job_name = NULL;
    Assert(next_job_id <= TS_MAX_TEST_JOB_TYPE);
    if (job_id < 0 || job_id >= next_job_id)
        elog(ERROR, "Invalid test job id %d", job_id);

    job_name = test_job_type_names[job_id];
    if (job_name == NULL)
        elog(ERROR, "Unrecognized test job %d", job_id);

    return job_name;
}

TSDLLEXPORT int
ts_test_job_get_by_name(Name job_type_name)
{
	int			i;
    Assert(next_job_id <= TS_MAX_TEST_JOB_TYPE);

	for (i = 0; i < TS_MAX_TEST_JOB_TYPE; i++)
	{
        if (test_job_type_names[i] == NULL)
            return -1;

		if (namestrcmp(job_type_name, test_job_type_names[i]) == 0)
			return i;
	}
	return -1;
}


///////////////////////////////////////

static bool
test_job_1()
{
	StartTransactionCommand();
	elog(WARNING, "Execute job 1");

	CommitTransactionCommand();
	return true;
}

static bool
test_job_2_error()
{
	StartTransactionCommand();
	elog(WARNING, "Before error job 2");

	elog(ERROR, "Error job 2");

	elog(WARNING, "After error job 2");

	CommitTransactionCommand();
	return true;
}

static pqsigfunc prev_signal_func = NULL;

static void
log_terminate_signal(SIGNAL_ARGS)
{
	elog(WARNING, "Job got term signal");

	if (prev_signal_func != NULL)
		prev_signal_func(postgres_signal_arg);
}

static bool
test_job_3_long()
{
	BackgroundWorkerBlockSignals();

	/*
	 * Only set prev_signal_func once to prevent it from being set to
	 * log_terminate_signal.
	 */
	if (prev_signal_func == NULL)
		prev_signal_func = pqsignal(SIGTERM, log_terminate_signal);
	/* Setup any signal handlers here */
	BackgroundWorkerUnblockSignals();

	elog(WARNING, "Before sleep job 3");

	DirectFunctionCall1(pg_sleep, Float8GetDatum(0.5L));

	elog(WARNING, "After sleep job 3");
	return true;
}

/* Exactly like job 1, except a wrapper will change its next_start. */
bool
test_job_4_delayed_start(void)
{
	StartTransactionCommand();
	elog(WARNING, "Execute job 4");
	CommitTransactionCommand();
	return true;
}
