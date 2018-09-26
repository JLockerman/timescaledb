#include <postgres.h>

#include <miscadmin.h>
#include <fmgr.h>

#include "bgw_interface.h"
#include "../compat.h"
#include "bgw_counter.h"
#include "bgw_message_queue.h"
#include "../extension_constants.h"


/* This is where versioned-extension facing functions live. They shouldn't live anywhere else. */

const int32 ts_bgw_loader_api_version = 1;

TS_FUNCTION_INFO_V1(bgw_worker_reserve);
TS_FUNCTION_INFO_V1(bgw_worker_release);
TS_FUNCTION_INFO_V1(bgw_num_unreserved);
TS_FUNCTION_INFO_V1(bgw_db_workers_start);

TS_FUNCTION_INFO_V1(bgw_db_workers_stop);

TS_FUNCTION_INFO_V1(bgw_db_workers_restart);

void
bgw_interface_register_api_version()
{
	void	  **versionptr = find_rendezvous_variable(RENDEZVOUS_BGW_LOADER_API_VERSION);

	/* Cast away the const to store in the rendezvous variable */
	*versionptr = (void *) &ts_bgw_loader_api_version;
}

TS_FUNCTION(bgw_worker_reserve)
{
	PG_RETURN_BOOL(bgw_total_workers_increment());
}

TS_FUNCTION(bgw_worker_release)
{
	bgw_total_workers_decrement();
	PG_RETURN_VOID();
}

TS_FUNCTION(bgw_num_unreserved)
{
	int			unreserved_workers;

	unreserved_workers = guc_max_background_workers - bgw_total_workers_get();
	PG_RETURN_INT32(unreserved_workers);
}

TS_FUNCTION(bgw_db_workers_start)
{
	PG_RETURN_BOOL(bgw_message_send_and_wait(START, MyDatabaseId));
}

TS_FUNCTION(bgw_db_workers_stop)
{
	PG_RETURN_BOOL(bgw_message_send_and_wait(STOP, MyDatabaseId));
}


TS_FUNCTION(bgw_db_workers_restart)
{
	PG_RETURN_BOOL(bgw_message_send_and_wait(RESTART, MyDatabaseId));
}
