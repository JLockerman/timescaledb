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

typedef enum SkipSkanStage
{
	SkipSkanSearchingForFirst = 0x0,
	SkipSkanFoundNull = 0x1,
	SkipSkanFoundVal = 0x2,
	SkipSkanSearchingForAdditional = 0x4,

	SkipSkanSearchingForNull = SkipSkanSearchingForAdditional | SkipSkanFoundVal,
	SkipSkanSearchingForVal = SkipSkanSearchingForAdditional | SkipSkanFoundNull,

	SkipSkanFoundNullAndVal = SkipSkanFoundVal | SkipSkanFoundNull,
} SkipSkanStage;

typedef struct SkipSkanState
{
	CustomScanState cscan_state;
	IndexScanDesc *scan_desc;
	MemoryContext ctx;

	/* Interior Index(Only)Scan the SkipSkan runs over */
	ScanState *idx;

	/* Pointers into the Index(Only)Scan */
	int *num_scan_keys;
	ScanKey *scan_keys;
	Buffer *index_only_buffer;
	bool *reached_end;

	Datum prev_distinct_val;
	bool prev_is_null;

	/* Info about the type we are performing DISTINCT on */
	bool distinct_by_val;
	int distinct_col_attnum;
	int distinct_typ_len;

	SkipSkanStage stage;

	ScanKeyData skip_qual;
	int skip_qual_offset;
	bool skip_qual_removed;

	bool index_only_scan;

	Relation index_rel;
	void *idx_scan;
} SkipSkanState;

typedef struct SkipSkanPath
{
	CustomPath cpath;
	IndexPath *index_path;

	/* Index clause which we'll use to skip past elements we've already seen */
	RestrictInfo *skip_clause;
	/* The column offset, on the index, of the column we are calling DISTINCT on */
	int distinct_column;
	int distinct_typ_len;
	bool distinct_by_val;
} SkipSkanPath;

extern void ts_add_skip_skan_paths(PlannerInfo *root, RelOptInfo *output_rel);
extern Node *ts_skip_skan_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H */
