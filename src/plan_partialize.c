
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


    {
        bytea *binary = OidSendFunctionCall(send_fn, arg);
        StringInfo serialized = makeStringInfo();
        Oid recv_fn, typIOParam, out_fn;
        getTypeOutputInfo(arg_type, &out_fn, &type_is_varlena);
        getTypeBinaryInputInfo(arg_type, &recv_fn, &typIOParam);

        appendBinaryStringInfo(serialized, VARDATA_ANY(binary), VARSIZE_ANY_EXHDR(binary));

        elog(WARNING, "output: %s", OidOutputFunctionCall(out_fn, arg));

        Datum received = OidReceiveFunctionCall(recv_fn, serialized, typIOParam, 0);
        elog(WARNING, "ioParam %d, arg %d", typIOParam, arg_type);
        elog(WARNING, "received: %s", OidOutputFunctionCall(out_fn, received));
    }

    PG_RETURN_BYTEA_P(OidSendFunctionCall(send_fn, arg));

}

///////////////////////////////////////

typedef struct PartializeWalkerState
{
    bool found_partialize;
    bool looking_for_agg;
} PartializeWalkerState;

static bool
partialize_function_call_walker(Node *node, PartializeWalkerState *state)
{
    if (node == NULL)
        return false;

    if (state->looking_for_agg)
    {
        Aggref *agg_ref;
        if (!IsA(node, Aggref))
            elog(ERROR, "The input to partialize must be an aggregate");

        elog(WARNING, "aggref");
        agg_ref = castNode(Aggref, node);
        agg_ref->aggsplit = AGGSPLIT_INITIAL_SERIAL;
        if (agg_ref->aggtranstype == INTERNALOID && DO_AGGSPLIT_SERIALIZE(AGGSPLIT_INITIAL_SERIAL))
            agg_ref->aggtype = BYTEAOID;
        else
            agg_ref->aggtype = agg_ref->aggtranstype;

        state->looking_for_agg = false;
    }
    else if (IsA(node, FuncExpr) && strncmp(get_func_name(castNode(FuncExpr, node)->funcid), "partialize", NAMEDATALEN) == 0)
    {
        elog(WARNING, "partialize");
        state->found_partialize = true;
        state->looking_for_agg = true;
    }

	return expression_tree_walker((Node *) node, partialize_function_call_walker, state);
}

static bool
ensure_only_partials(Node *node, void *state)
{
    if (node == NULL)
        return false;

    if (IsA(node, Aggref) && castNode(Aggref, node)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
            elog(ERROR, "Cannot mix partialized and non-partialized aggregates in the same statement");

	return expression_tree_walker((Node *) node, ensure_only_partials, state);
}

void
plan_add_partialize(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	Query	   *parse = root->parse;
    PartializeWalkerState state = {
        .found_partialize = false,
        .looking_for_agg = false,
    };
    ListCell *lc;

    if (CMD_SELECT != parse->commandType)
		return;

    expression_tree_walker((Node *) parse->targetList, partialize_function_call_walker, &state);

    if (state.found_partialize)
    {
        expression_tree_walker((Node *) parse->targetList, ensure_only_partials,NULL);

        foreach(lc, input_rel->pathlist)
		{
            Path *path = lfirst(lc);
            if (IsA(path, AggPath)) {
                elog(WARNING, "in path");
                ((AggPath *)path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
            }
		}

        foreach(lc, output_rel->pathlist)
		{
            Path *path = lfirst(lc);
            if (IsA(path, AggPath)) {
                elog(WARNING, "out path");
                ((AggPath *)path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
            }
		}

        elog(WARNING, "");
    }
}
