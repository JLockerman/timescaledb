/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
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
#include <optimizer/planmain.h>
#include <optimizer/planner.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <parser/parse_func.h>

#include "guc.h"
#include "license.h"
#include "nodes/skip_skan/planner.h"

static void
skip_skan_begin(CustomScanState *node, EState *estate, int eflags)
{
	SkipSkanState *state = (SkipSkanState *) node;
	if (IsA(state->idx_scan, IndexScan))
	{
		IndexScanState *idx = ExecInitIndexScan(state->idx_scan, estate, eflags);
		state->idx = idx;
		state->scan_keys = idx->iss_ScanKeys;
		state->num_scan_keys = idx->iss_NumScanKeys;
		state->index_rel = idx->iss_RelationDesc;
		state->recheck_state = idx->indexqualorig;
		state->index_only_scan = false;
		if (idx->iss_NumOrderByKeys > 0)
			elog(ERROR, "cannot SkipSkan with OrderByKeys");

		/* we may be able to handle this at some point */
		if (idx->iss_NumRuntimeKeys > 0)
			elog(ERROR, "cannot SkipSkan with RuntimeKeys");
	}
	else if(IsA(state->idx_scan, IndexOnlyScan))
	{
		//FIXME impl pending
		elog(ERROR, "impl pending");
		IndexOnlyScanState *idx = ExecInitIndexOnlyScan(state->idx_scan, estate, eflags);
		state->idx = idx;
		state->scan_keys = idx->ioss_ScanKeys;
		state->num_scan_keys = idx->ioss_NumScanKeys;
		state->index_rel = idx->ioss_RelationDesc;
		state->recheck_state = idx->indexqual;
		state->index_only_scan = true;
		if (idx->ioss_NumOrderByKeys > 0)
			elog(ERROR, "cannot SkipSkan with OrderByKeys");

		/* we may be able to handle this at some point */
		if (idx->ioss_NumRuntimeKeys > 0)
			elog(ERROR, "cannot SkipSkan with RuntimeKeys");
	}
	else
		elog(ERROR, "unknown subscan type in SkipSkan");


	state->prev_vals = palloc0(sizeof(*state->prev_vals) * state->num_distinct_cols);
	state->prev_is_null = palloc(sizeof(*state->prev_is_null) * state->num_distinct_cols);
	memset(state->prev_is_null, true, sizeof(*state->prev_is_null) * state->num_distinct_cols);
	state->found_first = false;
	state->needs_rescan = false;
}

static TupleTableSlot *
skip_skan_exec(CustomScanState *node)
{
	SkipSkanState *state = (SkipSkanState *)node;
	EState	   *estate = node->ss.ps.state;

	if (state->scan_desc == NULL)
	{
		/* first time through we ignore the inital scan keys which are used to
		 * skip previously seen values
		 */
		int nkeys = state->num_scan_keys - state->num_distinct_cols;
		state->scan_desc = index_beginscan(node->ss.ss_currentRelation,
			state->index_rel,
			estate->es_snapshot,
			nkeys,
			0 /*norderbys*/);

		index_rescan(state->scan_desc,
			/* ignore the first scan keys, which are used for skipping */
			state->scan_keys + state->num_distinct_cols,
			nkeys,
			NULL /*orderbys*/,
			0 /*norderbys*/);
	}
	else if (state->needs_rescan)
	{
		/* in subsequent times we rescan based on the previously found element */
		index_rescan(state->scan_desc,
			state->scan_keys,
			state->num_scan_keys,
			NULL /*orderbys*/,
			0 /*norderbys*/);;
	}

	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	HeapTuple tuple;

	//FIXME get scan dir from interior plan
	while ((tuple = index_getnext(state->scan_desc, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		ExecStoreTuple(tuple, /* tuple to store */
			slot, /* slot to store in */
			state->scan_desc->xs_cbuf,   /* buffer containing tuple */
			false); /* don't pfree */

		 /*
          * If the index was lossy, we have to recheck the index quals using
          * the fetched tuple.
          */
		if (state->scan_desc->xs_recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(state->recheck_state, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				continue;
			}
		}

		slot_getsomeattrs(slot, state->max_distinct_col);
		for(int i = 0; i < state->num_distinct_cols; i++)
		{
			int col = state->distinc_col_attnums[i];
			if (!state->prev_is_null[i] && !state->distinct_by_val[i])
			{
				pfree(DatumGetPointer(state->prev_vals[i]));
			}

			MemoryContext old_ctx = MemoryContextSwitchTo(state->ctx);
			state->prev_vals[i] = datumCopy(slot_getattr(slot, col, &state->prev_is_null[i]),
				state->distinct_by_val[i],
				state->distinct_typ_len[i]);
			state->scan_keys[i].sk_argument = state->prev_vals[i];
			if (state->prev_is_null[i])
				state->scan_keys[i].sk_flags |= SK_ISNULL;
			else
				state->scan_keys[i].sk_flags &= ~SK_ISNULL;
			MemoryContextSwitchTo(old_ctx);
		}

		//FIXME handle NULLs
		if (!state->found_first)
		{
			index_endscan(state->scan_desc);
			state->scan_desc = index_beginscan(node->ss.ss_currentRelation,
				state->index_rel,
				estate->es_snapshot,
				state->num_scan_keys,
				0 /*norderbys*/);
			state->found_first = true;
		}

		//FIXME state->needs_rescan = !all_null?
		state->needs_rescan = true;

		return slot;
	}

	return NULL;
	// elog(ERROR, "unimplemented");
}

static void
skip_skan_end(CustomScanState *node)
{
	SkipSkanState *state = (SkipSkanState *) node;
	if (state->index_only_scan)
		ExecEndIndexOnlyScan(state->idx);
	else
		ExecEndIndexScan(state->idx);
	if (state->scan_desc != NULL)
		index_endscan(state->scan_desc);
	// elog(ERROR, "unimplemented");
}

static void
skip_skan_rescan(CustomScanState *node)
{
	elog(ERROR, "unimplemented");
	SkipSkanState *state = (SkipSkanState *) node;
	ExecReScanIndexScan(state->idx);

}

static CustomExecMethods skip_skan_state_methods = {
	.CustomName = "SkipSkanState",
	.BeginCustomScan = skip_skan_begin,
	.EndCustomScan = skip_skan_end,
	.ExecCustomScan = skip_skan_exec,
	.ReScanCustomScan = skip_skan_rescan,
};

static Node *
skip_skan_state_create(CustomScan *cscan)
{
	int col = 0;
	int max_distinct_col = 0;
	SkipSkanState *state = (SkipSkanState *) newNode(sizeof(SkipSkanState), T_CustomScanState);
	state->num_distinct_cols = (int)linitial(cscan->custom_private);

	state->idx_scan = lsecond(cscan->custom_private);

	state->distinc_col_attnums = lthird(cscan->custom_private);

	for(col = 0; col < state->num_distinct_cols; col++)
	{
		if (state->distinc_col_attnums[col] > max_distinct_col)
			max_distinct_col = state->distinc_col_attnums[col];
	}

	state->max_distinct_col = max_distinct_col;

	state->distinct_by_val = lfourth(cscan->custom_private);

	state->distinct_typ_len = lfirst(lnext(lnext(lnext(lnext(list_head(cscan->custom_private))))));

	state->cscan_state.methods = &skip_skan_state_methods;
	return (Node *)state;
}

static CustomScanMethods skip_skan_plan_methods = {
	.CustomName = "SkipSkan",
	.CreateCustomScanState = skip_skan_state_create,
};

static Plan *
skip_skan_plan_create(PlannerInfo *root, RelOptInfo *relopt, CustomPath *best_path,
						   List *tlist, List *clauses, List *custom_plans)
{
	SkipSkanPath *path = (SkipSkanPath *) best_path;
	CustomScan *skip_plan = makeNode(CustomScan);
	int num_skip_clauses = list_length(path->comparison_clauses);
	IndexPath *index_path = path->index_path;

	index_path->indexclauses = list_concat(path->comparison_clauses, index_path->indexclauses);
	index_path->indexquals = list_concat(path->comparison_clauses, index_path->indexquals);
	index_path->indexqualcols = list_concat(path->comparison_columns, index_path->indexqualcols);

	Plan *plan = create_plan(root, &index_path->path);
	if (IsA(plan, IndexScan))
	{
		IndexScan *idx_plan = castNode(IndexScan, plan);
		skip_plan->scan = idx_plan->scan;
	}
	else if (IsA(plan, IndexOnlyScan))
	{
		IndexOnlyScan *idx_plan = castNode(IndexOnlyScan, plan);
		skip_plan->scan = idx_plan->scan;
	}
	else
		elog(ERROR, "bad plan");

	skip_plan->scan.plan.qual = NIL;
	skip_plan->scan.plan.type = T_CustomScan;
	skip_plan->scan.plan.parallel_safe = false;
	skip_plan->scan.plan.parallel_aware = false;
	skip_plan->methods = &skip_skan_plan_methods;
	skip_plan->custom_private = lappend(skip_plan->custom_private, num_skip_clauses);
	skip_plan->custom_private = lappend(skip_plan->custom_private, plan);
	skip_plan->custom_private = lappend(skip_plan->custom_private, path->comparison_table_attnums);
	skip_plan->custom_private = lappend(skip_plan->custom_private, path->distinct_by_val);
	skip_plan->custom_private = lappend(skip_plan->custom_private, path->distinct_typ_len);
	return &skip_plan->scan.plan;
}

static CustomPathMethods skip_skan_path_methods = {
	.CustomName = "SkipSkanPath",
	.PlanCustomPath = skip_skan_plan_create,
};

void
ts_add_skip_skan_paths(PlannerInfo *root, RelOptInfo *output_rel)
{
	if (!ts_guc_enable_skip_skan)
		return;
	ListCell *lc;
	List *pathlist = output_rel->pathlist;
	foreach (lc, pathlist)
	{
		UpperUniquePath *unique_path;
		IndexPath *index_path;
		SkipSkanPath *skip_skan_path = NULL;
		Path *path = lfirst(lc);
		int col;

		if (!IsA(path, UpperUniquePath))
			continue;

		unique_path = castNode(UpperUniquePath, path);
		//TODO IndexOnlyPath
		if (!IsA(unique_path->subpath, IndexPath))
			continue;

		index_path = castNode(IndexPath, unique_path->subpath);

		if(index_path->indexinfo->sortopfamily == NULL)
			continue; /* non-orderable index, skip these for now */

		if(index_path->indexinfo->unique)
			continue; /* unique indexes are better off using the regular scan */

		if (index_path->indexorderbys != NIL)
			continue;

		int num_distinct_cols = unique_path->numkeys;
		skip_skan_path = palloc0(sizeof(*skip_skan_path));
		skip_skan_path->cpath.path = unique_path->path;
		skip_skan_path->cpath.path.type = T_CustomPath;
		skip_skan_path->cpath.path.pathtype = T_CustomScan;
		skip_skan_path->cpath.methods = &skip_skan_path_methods;
		skip_skan_path->index_path = index_path;
		skip_skan_path->num_distinct_cols = num_distinct_cols;
		skip_skan_path->comparison_clauses = NIL;
		skip_skan_path->comparison_table_attnums = palloc(sizeof(*skip_skan_path->comparison_table_attnums) * num_distinct_cols);
		skip_skan_path->distinct_by_val = palloc(sizeof(*skip_skan_path->distinct_by_val) * num_distinct_cols);
		skip_skan_path->distinct_typ_len = palloc(sizeof(*skip_skan_path->distinct_typ_len) * num_distinct_cols);
		Assert(num_distinct_cols <= index_path->indexinfo->nkeycolumns);

		IndexOptInfo *idx_info = index_path->indexinfo;
		Index rel_index = idx_info->rel->relid;
		Oid rel_oid = root->simple_rte_array[rel_index]->relid;

		/* find the ordering operator we'll use to skip around each key column */
		for(col = 0; col < num_distinct_cols; col++)
		{
			IndexOptInfo *idx_info = index_path->indexinfo;
			/* this is a bit of a hack: the Unique node will deduplicate based
			 * off the sort based off of the first numkeys of the path's pathkeys,
			 * working under the assumption that its subpath will return things
			 * in that order. Instead of looking through the pathkeys to
			 * determine the columns being deduplicated on, we assume that the
			 * index's column order will match that
			 */
			int table_col = idx_info->indexkeys[col];
			if(table_col == 0)
				goto next_index; /* cannot use this index */

			HeapTuple column_tuple = SearchSysCache2(ATTNUM,
				ObjectIdGetDatum(rel_oid),
				Int16GetDatum(table_col));
			if (!HeapTupleIsValid(column_tuple))
				goto next_index; /* cannot use this index */

			Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(column_tuple);

			Oid column_type = att_tup->atttypid;
			int32 column_typmod = att_tup->atttypmod;
			Oid column_collation = att_tup->attcollation;

			skip_skan_path->distinct_by_val[col] = att_tup->attbyval;
			skip_skan_path->distinct_typ_len[col] = att_tup->attlen;
			ReleaseSysCache(column_tuple);
			if(!OidIsValid(column_type))
				goto next_index; /* cannot use this index */

			Oid btree_opfamily = idx_info->sortopfamily[col];
			int16 strategy = idx_info->reverse_sort[col] ? BTLessStrategyNumber: BTGreaterStrategyNumber;
			Oid comparator = get_opfamily_member(btree_opfamily, column_type, column_type, strategy);
			if (!OidIsValid(comparator))
				goto next_index; /* cannot use this index */

			//TODO should this be a Const or a Var?
			Const *prev_val = makeNullConst(column_type, column_typmod, column_collation);
			Var *current_val = makeVar(rel_index /*varno*/,
				table_col /*varattno*/,
				column_type /*vartype*/,
				column_typmod /*vartypmod*/,
				column_collation /*varcollid*/,
				0 /*varlevelsup*/);

			Expr *comparsion_expr = make_opclause(comparator,
				BOOLOID /*opresulttype*/,
				false /*opretset*/,
				&current_val->xpr /*leftop*/,
				&prev_val->xpr /*rightop*/,
				InvalidOid /*opcollid*/,
				idx_info->indexcollations[col] /*inputcollid*/);
			set_opfuncid(castNode(OpExpr, comparsion_expr));
			RestrictInfo *clause = make_simple_restrictinfo(comparsion_expr);
			skip_skan_path->comparison_clauses = lappend(skip_skan_path->comparison_clauses, clause);
			skip_skan_path->comparison_columns = lappend_int(skip_skan_path->comparison_columns, col);
			skip_skan_path->comparison_table_attnums[col] = table_col;
		}

		//FIXME figure out costing Selectivity should be approximately n_distinct/total_tuples
		// total_cost = (index_cpu_cost + table_cpu_cost) + (index_IO_cost + table_IO_cost)
		skip_skan_path->cpath.path.total_cost = log2(unique_path->path.total_cost);
		// noop_unique_path->path.total_cost /= index_path->indexselectivity;
		// elog(WARNING, "cost %f", noop_unique_path->path.total_cost);
		// noop_unique_path->path.total_cost *= n_distinct;
		add_path(output_rel, &skip_skan_path->cpath.path);
		return;
next_index:;
	}
}
