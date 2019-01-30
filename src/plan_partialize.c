
#include <postgres.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/planner.h>
#include <utils/lsyscache.h>

#include "export.h"

#include "plan_partialize.h"

TS_FUNCTION_INFO_V1(ts_partialize);

/*
 * the partialize function mainly serves as a marker that the aggregate called
 * within should return a partial instead of a result. Most of the actual work
 * occurs in the planner, with the actual function just used to ensure the
 * return type is correct.
 */
TSDLLEXPORT Datum
ts_partialize(PG_FUNCTION_ARGS)
{
    Datum arg;
    Oid arg_type;
    Oid send_fn;
    bool type_is_varlena;
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    arg = PG_GETARG_DATUM(0);
    arg_type = get_fn_expr_argtype(fcinfo->flinfo, 0);

    if (arg_type == BYTEAOID)
        PG_RETURN_DATUM(arg);

    getTypeBinaryOutputInfo(arg_type, &send_fn, &type_is_varlena);

    PG_RETURN_BYTEA_P(OidSendFunctionCall(send_fn, arg));

}

///////////////////////////////////////

static bool
partialize_function_call_walker(Node *node, bool *found_partialize)
{

    if (node == NULL)
        return false;

    if (IsA(node, FuncExpr) && strncmp(get_func_name(castNode(FuncExpr, node)->funcid), "partialize", NAMEDATALEN) == 0)
    {
        *found_partialize = true;
    }
    else if (*found_partialize && IsA(node, Aggref))
    {
        Aggref *agg_ref = castNode(Aggref, node);
        agg_ref->aggsplit = AGGSPLIT_INITIAL_SERIAL;
        if (agg_ref->aggtranstype == INTERNALOID && DO_AGGSPLIT_SERIALIZE(AGGSPLIT_INITIAL_SERIAL))
            agg_ref->aggtype = BYTEAOID;
        else
            agg_ref->aggtype = agg_ref->aggtranstype;
    }

	return expression_tree_walker((Node *) node, partialize_function_call_walker, found_partialize);
}

void
plan_add_partialize(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	Query	   *parse = root->parse;
    bool found_partialize = false;
    ListCell *lc;;

    if (CMD_SELECT != parse->commandType)
		return;

    expression_tree_walker((Node *) parse->targetList, partialize_function_call_walker, &found_partialize);

    if (found_partialize)
    {
        foreach(lc, input_rel->pathlist)
		{
            Path *path = lfirst(lc);
            if (IsA(path, AggPath))
                ((AggPath *)path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
		}

        foreach(lc, output_rel->pathlist)
		{
            Path *path = lfirst(lc);
            if (IsA(path, AggPath))
                ((AggPath *)path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
		}
    }
}
