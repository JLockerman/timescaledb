/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H
#define TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H

#include <postgres.h>
#include <access/relscan.h>
#include <access/skey.h>
#include <nodes/execnodes.h>
#include <optimizer/planner.h>

typedef struct SkipSkanState
{
	CustomScanState cscan_state;
	ExprState *recheck_state;
	IndexScanDesc scan_desc;
	MemoryContext ctx;
	ScanKey scan_keys;
	int num_scan_keys;
	int num_distinct_cols;
	int max_distinct_col;
	int *distinc_col_attnums;
	bool *distinct_by_val;
	int *distinct_typ_len;
	Datum *prev_vals;
	bool *prev_is_null;
	Buffer *index_only_buffer;
	bool found_first;
	bool needs_rescan;
	bool index_only_scan;

	Relation index_rel;
	void *idx;
	void *idx_scan;
} SkipSkanState;


typedef struct SkipSkanPath
{
	CustomPath cpath;
	IndexPath *index_path;
	int num_distinct_cols;
	/* list of index clauses (RestricInfo *) which we'll use to skip past elements we've already seen */
	List *comparison_clauses;
	List *comparison_columns;
	int *comparison_table_attnums;
	bool *distinct_by_val;
	int *distinct_typ_len;
} SkipSkanPath;

void ts_add_skip_skan_paths(PlannerInfo *root, RelOptInfo *output_rel);

#endif /* TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H */
