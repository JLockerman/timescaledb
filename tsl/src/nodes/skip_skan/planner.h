/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H
#define TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H

#include <postgres.h>
#include <optimizer/planner.h>

typedef struct SkipSkanPath
{
    CustomPath cpath;
    IndexPath *index_path;
    int num_distinct_cols;
    Oid *comparison_operators;
} SkipSkanPath;

void ts_add_skip_skan_paths(PlannerInfo *root, RelOptInfo *output_rel);

#endif /* TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H */
