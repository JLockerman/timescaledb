
#ifndef TIMESCALEDB_BGW_LAUNCHER_H
#define TIMESCALEDB_BGW_LAUNCHER_H

#include <postgres.h>
#include <fmgr.h>

extern void bgw_cluster_launcher_register(void);

/*called by postmaster at launcher bgw startup*/
extern TS_FUNCTION_HEADER(bgw_cluster_launcher_main);
extern TS_FUNCTION_HEADER(bgw_db_scheduler_entrypoint);



#endif							/* TIMESCALEDB_BGW_LAUNCHER_H */
