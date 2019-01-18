/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <c.h>

#include <commands/defrem.h>
#include <nodes/parsenodes.h>
#include <utils/builtins.h>

#include "with_clause_parser.h"

void
ts_with_clause_filter(const List *def_elems, const char *namespace, List **within_namespace, List **not_within_namespace)
{
    ListCell *cell;
    foreach(cell, def_elems)
    {
        DefElem    *def = (DefElem *) lfirst(cell);
        if (def->defnamespace != NULL && pg_strcasecmp(def->defnamespace, "hypertable") == 0)
        {
            if (within_namespace != NULL)
                *within_namespace = lappend(*within_namespace, def);
        }
        else if (not_within_namespace != NULL)
        {
            *not_within_namespace = lappend(*not_within_namespace, def);
        }
    }
}


void
ts_with_clauses_apply(const List *def_elems, const char *namespace, ts_with_clause_on_arg on_arg, const WithClauseArg *args, Size nargs, void *state)
{
	ListCell *cell;
	foreach(cell, def_elems)
	{
		DefElem    *def = (DefElem *) lfirst(cell);
		const char *value;
		Size 		i;
		bool		argument_recognized = false;
		bool		argument_valid = false;
		Assert(def->defnamespace != NULL && pg_strcasecmp(def->defnamespace, namespace) == 0);

		for(i = 0; i < nargs; i++)
		{
			if (pg_strcasecmp(def->defname, args[i].arg_name) == 0)
			{
				Datum val;
				argument_recognized = true;

				if (def->arg != NULL)
					value = defGetString(def);
				else
					value = "true";

				if (args[i].deserializer != NULL)
					val = args[i].deserializer(def, value, state);
				else
					val = CStringGetDatum(value);

				argument_valid = (*on_arg)(i, def->defname, val, state);
				break;
			}
		}
		if (!argument_recognized)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("unrecognized parameter \"%s.%s\"",
						def->defnamespace, def->defname)));

		if (!argument_valid)
			ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_PARAMETER),
					errmsg("invalid or duplicate parameter \"%s.%s\"",
						def->defnamespace, def->defname)));
	}
}

Datum
ts_with_clause_deserialize_unimplemented(const DefElem *element, const char *value, void *state)
{
	elog(ERROR, "argument \"%s.%s\" not implemented", element->defnamespace, element->defname);
}

Datum
ts_with_clause_deserialize_bool(const DefElem *element, const char *value, void *state)
{
	if (pg_strcasecmp(value, "true") == 0 || pg_strcasecmp(value, "on") == 0)
		return BoolGetDatum(true);
	else if (pg_strcasecmp(value, "false") == 0 || pg_strcasecmp(value, "off") == 0)
		return BoolGetDatum(false);
	else
		TS_WITH_DESERIALIZE_ERROR(element, value, "BOOLEAN");
}

Datum
ts_with_clause_deserialize_int32(const DefElem *element, const char *value, void *state)
{
	PG_TRY();
	{
		return Int32GetDatum(pg_atoi(value, sizeof(int32), '\0'));
	}
	PG_CATCH();
	{
		TS_WITH_DESERIALIZE_ERROR(element, value, "INTEGER");
	}
	PG_END_TRY();
}

static Datum
timeinterval_from_cstr(Oid table, const DefElem *def, const char *str)
{
	elog(ERROR, "unimplemented %s", str);
}

Datum
ts_with_clause_deserialize_name(const DefElem *element, const char *value, void *state)
{
	Name name = palloc(sizeof(*name));
	namestrcpy(name, value);
	return NameGetDatum(name);
}
