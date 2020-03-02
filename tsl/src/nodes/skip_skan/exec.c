/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <access/visibilitymap.h>
#include <catalog/pg_type.h>
#include <executor/nodeIndexscan.h>
#include <executor/nodeIndexOnlyscan.h>
#include <miscadmin.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/paramassign.h>
#include <optimizer/planmain.h>
#include <optimizer/planner.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <storage/predicate.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <parser/parse_func.h>

#include "guc.h"
#include "license.h"
#include "nodes/skip_skan/skip_skan.h"

static void
skip_skan_begin(CustomScanState *node, EState *estate, int eflags)
{
	SkipSkanState *state = (SkipSkanState *) node;
	if (IsA(state->idx_scan, IndexScan))
	{
		IndexScanState *idx = castNode(IndexScanState, ExecInitNode(state->idx_scan, estate, eflags));
		state->index_only_scan = false;

		node->custom_ps = list_make1(&idx->ss.ps);

		state->idx = &idx->ss;
		state->scan_keys = &idx->iss_ScanKeys;
		state->num_scan_keys = &idx->iss_NumScanKeys;
		state->index_rel = idx->iss_RelationDesc;
		state->scan_desc = &idx->iss_ScanDesc;
		state->index_only_buffer = NULL;
		state->reached_end = &idx->iss_ReachedEnd;

		/* we do not support orderByKeys out of conservatism; we do not know what,
		 * if any, work would be required to support them. The planner should
		 * never plan a SkipSkan which would cause this ERROR.
		 */
		if (idx->iss_NumOrderByKeys > 0)
			elog(ERROR, "cannot SkipSkan with OrderByKeys");

		/* right now we want our skip keys to be at the front of the ScanKey,
		 * while regular IndexScans want the same for RuntimeKeys, while this
		 * should be fixable once we teach SkipScan to remove ScanKeys from the
		 * middle of the ScanKey, for now we just disable it.  The planner should
		 * never plan a SkipSkan which would cause this ERROR.
		 */
		if (idx->iss_NumRuntimeKeys > 0)
			elog(ERROR, "cannot SkipSkan with RuntimeKeys");
	}
	else if(IsA(state->idx_scan, IndexOnlyScan))
	{
		IndexOnlyScanState *idx = castNode(IndexOnlyScanState, ExecInitNode(state->idx_scan, estate, eflags));
		state->index_only_scan = true;

		node->custom_ps = list_make1(&idx->ss.ps);

		state->idx = &idx->ss;
		state->scan_keys = &idx->ioss_ScanKeys;
		state->num_scan_keys = &idx->ioss_NumScanKeys;
		state->index_rel = idx->ioss_RelationDesc;
		state->scan_desc = &idx->ioss_ScanDesc;
		state->index_only_buffer = &idx->ioss_VMBuffer;
		/* IndexOnlyScan does not have a reached_end field */
		state->reached_end = NULL;

		/* we do not support orderByKeys out of conservatism; we do not know what,
		 * if any, work would be required to support them.  The planner should
		 * never plan a SkipSkan which would cause this ERROR.
		 */
		if (idx->ioss_NumOrderByKeys > 0)
			elog(ERROR, "cannot SkipSkan with OrderByKeys");

		/* right now we want our skip keys to be at the front of the ScanKey,
		 * while regular IndexScans want the same for RuntimeKeys, while this
		 * should be fixable once we teach SkipScan to remove ScanKeys from the
		 * middle of the ScanKey, for now we just disable it. The planner should
		 * never plan a SkipSkan which would cause this ERROR.
		 */
		if (idx->ioss_NumRuntimeKeys > 0)
			elog(ERROR, "cannot SkipSkan with RuntimeKeys");
	}
	else
		elog(ERROR, "unknown subscan type in SkipSkan");

	state->prev_distinct_val = 0;
	state->prev_is_null = true;
	state->distinct_col_state = SkipColumnFoundNothing;
	state->found_first_tuple = false;
	state->distinct_col_updated = false;
}

static void update_skip_key(SkipSkanState *state, TupleTableSlot *slot);
static TupleTableSlot *skip_skan_search_for_null(SkipSkanState *state);
static TupleTableSlot *skip_skan_search_for_nonnull(SkipSkanState *state);

static void skip_skan_state_remove_skip_qual(SkipSkanState *state);
static inline void skip_skan_state_readd_skip_qual_if_needed(SkipSkanState *state);

static inline IndexScanDesc
skip_skan_state_get_scandesc(SkipSkanState *state)
{
	return *state->scan_desc;
}

static inline ScanKey
skip_skan_state_get_scankeys(SkipSkanState *state)
{
	return *state->scan_keys;
}

static inline ScanKey
skip_skan_state_get_scankey(SkipSkanState *state, int idx)
{
	Assert(idx < *state->num_scan_keys);
	return &skip_skan_state_get_scankeys(state)[idx];
}

/* end the previous ScanDesc, if it exists, and start a new one. We call this
 * when we change the number of scan keys: on the first run, to set up the scan
 * and on the first one after that to set up our skip qual.
 */
static void
skip_skan_state_beginscan(SkipSkanState *state)
{
	IndexScanDesc new_scan_desc;
	CustomScanState *node = &state->cscan_state;
	EState *estate = node->ss.ps.state;
	IndexScanDesc old_scan_desc = skip_skan_state_get_scandesc(state);
	if(old_scan_desc != NULL)
		index_endscan(old_scan_desc);

	new_scan_desc = index_beginscan(node->ss.ss_currentRelation,
		state->index_rel,
		estate->es_snapshot,
		*state->num_scan_keys,
		0 /*norderbys*/);

	if (state->index_only_scan)
	{
		new_scan_desc->xs_want_itup = true;
		*state->index_only_buffer = InvalidBuffer;
	}

	*state->scan_desc = new_scan_desc;
}

static TupleTableSlot *
skip_skan_exec(CustomScanState *node)
{
	SkipSkanState *state = (SkipSkanState *)node;
	TupleTableSlot *result;

	if (skip_skan_state_get_scandesc(state) == NULL)
	{
		/* first time through we ignore the inital scan keys, which are used to
		 * skip previously seen values, we'll change back the number of scan keys
		 * the first time through update_skip_key
		 */
		skip_skan_state_remove_skip_qual(state);
		skip_skan_state_beginscan(state);

		/* ignore the first scan key, which is the qual we add to skip repeat
		 * values, the other quals still need to be applied. We'll set this back
		 * once we have the inital values, and our qual can be applied.
		 */
		index_rescan(skip_skan_state_get_scandesc(state),
			skip_skan_state_get_scankeys(state),
			*state->num_scan_keys,
			NULL /*orderbys*/,
			0 /*norderbys*/);
	}
	else if (state->distinct_col_updated)
	{
		/* in subsequent call we rescan based on the previously found element
		 * which will have been set below in update_skip_key
		 */
		index_rescan(skip_skan_state_get_scandesc(state),
			skip_skan_state_get_scankeys(state),
			*state->num_scan_keys,
			NULL /*orderbys*/,
			0 /*norderbys*/);
	}

	/* get the next node from the underlying Index(Only)Scan */
	result = state->idx->ps.ExecProcNode(&state->idx->ps);
	bool index_scan_finished = TupIsNull(result);
	if (!index_scan_finished)
	{
		/* rescan can invalidate tuples, so if we're below a MergeAppend, we need
		 * to materialize the slot to ensure it won't be freed. (Technically, we
		 * do not need to do this if we're directly below the Unique node)
		 */
		ExecMaterializeSlot(result);
		update_skip_key(state, result);
	}
	else if (state->distinct_col_state == SkipColumnFoundValAndNull || state->distinct_col_state == SkipColumnFoundNothing)
		return result;
	else
	{
		if (!state->found_first_tuple)
			return result; /* nothing to find in the index, the non-skip-quals exclude everything */

		/* We've run out of tuples from the underlying scan, but we may not be done.
		 * NULL values don't participate in the normal ordering of values
		 * (eg. in SQL column < NULL will never be true, and column < value
		 * implies column IS NOT NULL), so they have to be handled specially.
		 * Further, NULL values can be returned either before or after the other
		 * values in the column depending on whether the index was declaed
		 * NULLS FIRST or NULLS LAST. Therefore just because we've reached the
		 * end of the IndexScan doesn't mean we're done; if we've only seen NULL
		 * values that means we may be in a NULLS FIRST index, and we need to
		 * check if a non-null value exists. Alternatively, if we haven't seen a
		 * NULL, we may be in a NULLS LAST column, so we need to check if a NULL
		 * value exists.
		 */
		if(!(state->distinct_col_state & SkipColumnFoundNull))
			return skip_skan_search_for_null(state);
		else if (!(state->distinct_col_state & SkipColumnFoundVal))
			return skip_skan_search_for_nonnull(state);
	}

	return result;
}

static TupleTableSlot *
skip_skan_search_for_null(SkipSkanState *state)
{
	/* We haven't seen a NULL, redo the scan with the skip-qual set to
	 * only allow NULL values, to see if there is a valid NULL to return.
	 * We'll remove the SK_SEARCHNULL in update_skip_key.
	 */
	skip_skan_state_get_scankey(state, 0)->sk_flags = SK_SEARCHNULL | SK_ISNULL;
	state->distinct_col_state |= SkipColumnFoundNull;
	if (state->reached_end != NULL)
		*state->reached_end = false;
	return skip_skan_exec(&state->cscan_state);
}

static TupleTableSlot *
skip_skan_search_for_nonnull(SkipSkanState *state)
{
	/* We only seen NULL values, redo the scan with the skip-qual set to
	 * exclude NULL values, to see if there is are valid non-NULL values
	 * to return. We'll remove the SK_SEARCHNOTNULL in update_skip_key.
	 */
	skip_skan_state_get_scankey(state, 0)->sk_flags = SK_SEARCHNOTNULL | SK_ISNULL;
	state->distinct_col_state |= SkipColumnFoundVal;
	if (state->reached_end != NULL)
		*state->reached_end = false;
	return skip_skan_exec(&state->cscan_state);
}

static void
update_skip_key(SkipSkanState *state, TupleTableSlot *slot)
{

	/* if this is the first tuple we found, re-add our skip-qual to the list of quals */
	skip_skan_state_readd_skip_qual_if_needed(state);

	int col = state->distinct_col_attnum;

	if (!state->prev_is_null && !state->distinct_by_val)
	{
		pfree(DatumGetPointer(state->prev_distinct_val));
	}

	MemoryContext old_ctx = MemoryContextSwitchTo(state->ctx);
	state->prev_distinct_val = slot_getattr(slot, col, &state->prev_is_null);
	if (!state->prev_is_null)
		state->prev_distinct_val = datumCopy(state->prev_distinct_val,
			state->distinct_by_val,
			state->distinct_typ_len);
	ScanKey key = skip_skan_state_get_scankey(state, 0);
	key->sk_argument = state->prev_distinct_val;
	if (state->prev_is_null)
	{
		/* Once we've seen a NULL we don't need another, so we remove the
		 * SEARCHNULL to enable us to finish early, if that's what's driving
		 * us.
		 */
		key->sk_flags &= ~SK_SEARCHNULL;
		key->sk_flags |= SK_ISNULL;
		state->distinct_col_state |= SkipColumnFoundNull;
	}
	else
	{
		/* Once we've found a value, we only want to find values after that
		 * one, so remove SEARCHNOTNULL in case we were using that to find
		 * the first non-NULL value.
		 */
		key->sk_flags &= ~(SK_ISNULL | SK_SEARCHNOTNULL);
		state->distinct_col_state |= SkipColumnFoundVal;
	}
	MemoryContextSwitchTo(old_ctx);

	if (!state->found_first_tuple)
	{
		/* if this is the first tuple we found */
		skip_skan_state_beginscan(state);
		state->found_first_tuple = true;
	}

	state->distinct_col_updated = true;
}

static void
skip_skan_state_remove_skip_qual(SkipSkanState *state)
{
	Assert(*state->num_scan_keys >= 1);
	*state->num_scan_keys -= 1;
	*state->scan_keys = *state->scan_keys + 1;
}

static inline void
skip_skan_state_readd_skip_qual_if_needed(SkipSkanState *state)
{
	if (!state->found_first_tuple)
	{
		*state->scan_keys = *state->scan_keys - 1;
		*state->num_scan_keys += 1;
	}
}


static void
skip_skan_end(CustomScanState *node)
{
	SkipSkanState *state = (SkipSkanState *) node;
	if (state->index_only_scan)
		ExecEndIndexOnlyScan(castNode(IndexOnlyScanState, state->idx));
	else
		ExecEndIndexScan(castNode(IndexScanState, state->idx));
}


static void
skip_skan_rescan(CustomScanState *node)
{
	SkipSkanState *state = (SkipSkanState *) node;
	IndexScanDesc old_scan_desc = skip_skan_state_get_scandesc(state);
	if(old_scan_desc != NULL)
	{
		index_endscan(old_scan_desc);
		/* If we never found any values (which can happen if we have a qual on a
		 * param that excludes all of the rows), we'll never have
		 * called update_skip_key so the scan keys will still be setup to skip
		 * the skip qual. Fix that here.
		 */
		skip_skan_state_readd_skip_qual_if_needed(state);
	}
	*state->scan_desc = NULL;

	if (state->index_only_scan)
		ExecReScanIndexOnlyScan(castNode(IndexOnlyScanState, state->idx));
	else
		ExecReScanIndexScan(castNode(IndexScanState, state->idx));

	state->prev_distinct_val = 0;
	state->prev_is_null = true;
	state->distinct_col_state = SkipColumnFoundNothing;
	state->found_first_tuple = false;
	state->distinct_col_updated = false;
}

static CustomExecMethods skip_skan_state_methods = {
	.CustomName = "SkipSkanState",
	.BeginCustomScan = skip_skan_begin,
	.EndCustomScan = skip_skan_end,
	.ExecCustomScan = skip_skan_exec,
	.ReScanCustomScan = skip_skan_rescan,
};

Node *
ts_skip_skan_state_create(CustomScan *cscan)
{
	SkipSkanState *state = (SkipSkanState *) newNode(sizeof(SkipSkanState), T_CustomScanState);

	state->idx_scan = linitial(cscan->custom_plans);

	state->distinct_col_attnum = lsecond_int(cscan->custom_private);

	state->distinct_by_val = lthird_int(cscan->custom_private);

	state->distinct_typ_len = lfourth_int(cscan->custom_private);

	state->cscan_state.methods = &skip_skan_state_methods;
	return (Node *)state;
}