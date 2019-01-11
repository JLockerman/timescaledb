
#include <postgres.h>

#include <export.h>

#define TS_MAX_TEST_JOB_TYPE 128

typedef bool (*job_type)(void);

extern TSDLLEXPORT job_type ts_test_jobs[TS_MAX_TEST_JOB_TYPE];

extern TSDLLEXPORT int ts_test_job_add(const char *job_name, job_type job_fn);
extern TSDLLEXPORT job_type ts_test_job_get(int job_id);
extern TSDLLEXPORT const char *ts_test_job_get_name(int job_id);
extern TSDLLEXPORT int ts_test_job_get_by_name(Name job_name);
extern bool test_job_4_delayed_start(void);
