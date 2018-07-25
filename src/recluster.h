#ifndef TIMESCALEDB_RECLUSTER_H
#define TIMESCALEDB_RECLUSTER_H

/*-------------------------------------------------------------------------
 *
 * recluster.h //TODO what shoould this file be called
 *	  Intercept cluster command to only block concurrent writes instead of
 *    blocking both reads and writes as postgres does
 *
 *-------------------------------------------------------------------------
 */


#include <postgres.h>
#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"

extern void timescale_recluster_rel(Oid tableOid, Oid indexOid, bool recheck,
						bool verbose);

#endif							/* TIMESCALEDB_RECLUSTER_H */
