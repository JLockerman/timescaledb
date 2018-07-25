#ifndef TIMESCALEDB_GUC_H
#define TIMESCALEDB_GUC_H
#include <postgres.h>

extern bool guc_disable_optimizations;
extern bool guc_optimize_non_hypertables;
extern bool guc_constraint_aware_append;
extern bool guc_restoring;
extern int	guc_max_open_chunks_per_insert;
extern int	guc_max_cached_chunks_per_hypertable;
extern int guc_timescale_cluster;
#define GUC_TIMESCALE_CLUSTER_READ_OPT	2
#define GUC_TIMESCALE_CLUSTER_NATIVE	1

void		_guc_init(void);
void		_guc_fini(void);

#endif							/* TIMESCALEDB_GUC_H */
