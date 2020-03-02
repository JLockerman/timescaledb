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

typedef enum SkipColumnState
{
	SkipColumnFoundNothing = 0x0,
	SkipColumnFoundVal = 0x1,
	SkipColumnFoundNull = 0x2,
	SkipColumnFoundValAndNull = SkipColumnFoundNull | SkipColumnFoundVal,
} SkipColumnState;

typedef struct SkipSkanState
{
	CustomScanState cscan_state;
	IndexScanDesc *scan_desc;
	MemoryContext ctx;

	/* Interior Index(Only)Scan the SkipSkan runs over */
	ScanState *idx;

	int *num_scan_keys;
	ScanKey *scan_keys;

	int distinct_col_attnum;
	bool distinct_by_val;
	int distinct_typ_len;
	Datum prev_distinct_val;
	bool prev_is_null;

	SkipColumnState distinct_col_state;

	Buffer *index_only_buffer;
	bool *reached_end;

	bool found_first_tuple;
	bool distinct_col_updated;
	bool index_only_scan;

	Relation index_rel;
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
	bool *distinct_by_val;
	int *distinct_typ_len;
} SkipSkanPath;

extern void ts_add_skip_skan_paths(PlannerInfo *root, RelOptInfo *output_rel);
extern Node *ts_skip_skan_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H */