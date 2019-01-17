/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <c.h>

#include <nodes/parsenodes.h>

#define TS_WITH_DESERIALIZE_ERROR(def, value, type) \
	ereport(ERROR, \
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
			errmsg("invalid value for %s.%s '%s'", \
				def->defnamespace, def->defname, value), \
			errhint("%s.%s must be a %s", def->defnamespace, def->defname, type)))

#define TS_WITH_DUPLICATE_PARAMETER_ERROR(def) \
    ereport(ERROR, \
            (errcode(ERRCODE_AMBIGUOUS_PARAMETER), \
                errmsg("duplicate parameter \"%s.%s\"", \
                    def->defnamespace, def->defname))); \


typedef struct WithClauseArg
{
	const char *arg_name;
    Datum (*deserializer)(const DefElem *element, const char *value, void *state);
} WithClauseArg;

typedef bool (*ts_with_clause_on_arg)(Size index, const char *name, Datum value, void *state);

extern void ts_with_clause_filter(const List *def_elems, const char *namespace, List **within_namespace, List **not_within_namespace);

extern void ts_with_clauses_apply(const List *def_elems, const char *namespace, ts_with_clause_on_arg on_arg, const WithClauseArg *args, Size nargs, void *state);


/* deserializers for specific argument types */

extern Datum ts_with_clause_deserialize_unimplemented(const DefElem *element, const char *value, void *state);
extern Datum ts_with_clause_deserialize_bool(const DefElem *element, const char *value, void *state);
extern Datum ts_with_clause_deserialize_int32(const DefElem *element, const char *value, void *state);
extern Datum ts_with_clause_deserialize_name(const DefElem *element, const char *value, void *state);
