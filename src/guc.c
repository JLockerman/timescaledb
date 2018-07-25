#include <postgres.h>
#include <utils/guc.h>
#include <miscadmin.h>

#include "guc.h"
#include "hypertable_cache.h"

bool		guc_disable_optimizations = false;
bool		guc_optimize_non_hypertables = false;
bool		guc_restoring = false;
bool		guc_constraint_aware_append = true;
int			guc_max_open_chunks_per_insert = 10;
int			guc_max_cached_chunks_per_hypertable = 10;
int			guc_timescale_cluster = 1;

 static const struct config_enum_entry guc_timescale_cluster_options[] = {
     {"read_optimized", GUC_TIMESCALE_CLUSTER_READ_OPT, false},
     {"native", GUC_TIMESCALE_CLUSTER_NATIVE, false},
     {NULL, 0, false}
 };

static void
assign_max_cached_chunks_per_hypertable_hook(int newval, void *extra)
{
	/* invalidate the hypertable cache to reset */
	hypertable_cache_invalidate_callback();
}

void
_guc_init(void)
{
	/* Main database to connect to. */
	DefineCustomBoolVariable("timescaledb.disable_optimizations", "Disable all timescale query optimizations",
							 NULL,
							 &guc_disable_optimizations,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("timescaledb.optimize_non_hypertables", "Apply timescale query optimization to plain tables",
							 "Apply timescale query optimization to plain tables in addition to hypertables",
							 &guc_optimize_non_hypertables,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.restoring", "Install timescale in restoring mode",
							 "Used for running pg_restore",
							 &guc_restoring,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.constraint_aware_append", "Enable constraint-aware append scans",
							 "Enable constraint exclusion at execution time",
							 &guc_constraint_aware_append,
							 true,
							 PGC_USERSET
							 ,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("timescaledb.max_open_chunks_per_insert",
							"Maximum open chunks per insert",
							"Maximum number of open chunk tables per insert",
							&guc_max_open_chunks_per_insert,
							work_mem * 1024L / 512L,	/* Assume each chunk
														 * takes up 512 bytes
														 * (work_mem is in
														 * kbytes) */
							0,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("timescaledb.max_cached_chunks_per_hypertable",
							"Maximum cached chunks",
							"Maximum number of chunks stored in the cache",
							&guc_max_cached_chunks_per_hypertable,
							100,
							0,
							65536,
							PGC_USERSET,
							0,
							NULL,
							assign_max_cached_chunks_per_hypertable_hook,
							NULL);

DefineCustomEnumVariable("timescaledb.cluster_method", "Enable cluster with reduced locking.",
							 "Enable cluster which only acquires an AccessExclusive lock during the final swap.",
							 &guc_timescale_cluster,
							 GUC_TIMESCALE_CLUSTER_READ_OPT, 
							 guc_timescale_cluster_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

void
_guc_fini(void)
{
}
