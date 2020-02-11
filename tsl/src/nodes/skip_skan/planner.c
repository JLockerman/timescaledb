/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planner.h>
#include <optimizer/tlist.h>
#include <utils/lsyscache.h>
#include <parser/parse_func.h>

#include "license.h"
#include "nodes/skip_skan/planner.h"


static Node *
skip_skan_state_create(CustomScan *cscan)
{
	// GapFillState *state = (GapFillState *) newNode(sizeof(GapFillState), T_CustomScanState);

	// state->csstate.methods = &gapfill_state_methods;
	// state->subplan = linitial(cscan->custom_plans);

	// return (Node *) state;
	elog(ERROR, "unimplemented");
	return NULL;
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
	extern Plan *create_plan(PlannerInfo *root, Path *best_path);

	Plan *plan = create_plan(root, &path->index_path->path);
	if (IsA(plan, IndexScan))// || IsA(plan, IndexOnlyScan))
	{
		elog(WARNING, "index");
		IndexScan *idx_plan = castNode(IndexScan, plan);
		CustomScan *skip_plan = makeNode(CustomScan);
		skip_plan->custom_private = list_make1(idx_plan);
		skip_plan->scan = idx_plan->scan;
		// skip_plan->scan = NIL;
		// idx_plan->indexqual = idx_plan->scan.plan.qual;
		skip_plan->scan.plan.qual = NIL;
		skip_plan->scan.plan.type = T_CustomScan;
		skip_plan->scan.plan.parallel_safe = false;
		skip_plan->scan.plan.parallel_aware = false;
		skip_plan->methods = &skip_skan_plan_methods;
		// skip_plan->custom_plans = custom_plans;
		return skip_plan;
	}
	elog(ERROR, "bad plan");
}

static CustomPathMethods skip_skan_path_methods = {
	.CustomName = "SkipSkanPath",
	.PlanCustomPath = skip_skan_plan_create,
};

void
ts_add_skip_skan_paths(PlannerInfo *root, RelOptInfo *output_rel)
{
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


		skip_skan_path = palloc0(sizeof(*skip_skan_path));
		skip_skan_path->cpath.path = unique_path->path;
		skip_skan_path->cpath.path.type = T_CustomPath;
		skip_skan_path->cpath.path.pathtype = T_CustomScan;
		skip_skan_path->cpath.methods = &skip_skan_path_methods;
		skip_skan_path->index_path = index_path;
		skip_skan_path->num_distinct_cols = unique_path->numkeys;
		skip_skan_path->comparison_operators = palloc(sizeof(*skip_skan_path->comparison_operators) * skip_skan_path->num_distinct_cols);
		Assert(skip_skan_path->num_distinct_cols <= index_path->indexinfo->nkeycolumns);

		IndexOptInfo *idx_info = index_path->indexinfo;
		Index rel_index = idx_info->rel->relid;
		Oid rel_oid = root->simple_rte_array[rel_index]->relid;

		/* find the ordering operator we'll use to skip around each key column */
		for(col = 0; col < skip_skan_path->num_distinct_cols; col++)
		{
			IndexOptInfo *idx_info = index_path->indexinfo;
			int table_col = idx_info->indexkeys[col];
			if(table_col == 0)
				goto next_index; /* cannot use this index */

			Oid column_type = get_atttype(rel_oid, table_col);
			if(!OidIsValid(column_type))
				goto next_index; /* cannot use this index */

			Oid btree_opfamily = idx_info->sortopfamily[col];
			int16 strategy = idx_info->reverse_sort[col] ? BTLessStrategyNumber: BTGreaterStrategyNumber;
			Oid comparator = get_opfamily_member(btree_opfamily, column_type, column_type, strategy);
			if (!OidIsValid(comparator))
				goto next_index; /* cannot use this index */

			skip_skan_path->comparison_operators[col] = comparator;
		}

		//FIXME figure out costing Selectivity should be approximately n_distinct/total_tuples
		// total_cost = (index_cpu_cost + table_cpu_cost) + (index_IO_cost + table_IO_cost)
		skip_skan_path->cpath.path.total_cost = log2(unique_path->path.total_cost);
		// noop_unique_path->path.total_cost /= index_path->indexselectivity;
		// elog(WARNING, "cost %f", noop_unique_path->path.total_cost);
		// noop_unique_path->path.total_cost *= n_distinct;
		// add_path(output_rel, &skip_skan_path->cpath.path);
		return;
next_index:;
	}

}
