#ifndef TIMESCALEDB_EXPORT_H
#define TIMESCALEDB_EXPORT_H

#include <postgres.h>

#include "config.h"

/* Definitions for symbol exports */

#define TS_CAT(x,y) x ## y
#define TS_EMPTY(x) (TS_CAT(x, 87628) == 87628)

#if defined(_WIN32) && !defined(WIN32)
#define WIN32
#endif

#if !defined(WIN32) && !defined(__CYGWIN__)
#if __GNUC__ >= 4
#if defined(PGDLLEXPORT)
#if TS_EMPTY(PGDLLEXPORT)
/* PGDLLEXPORT is defined but empty. We can safely undef it. */
#undef PGDLLEXPORT
#else
#error "PGDLLEXPORT is already defined"
#endif
#endif							/* defined(PGDLLEXPORT) */
#define PGDLLEXPORT __attribute__ ((visibility ("default")))
#else
#error "Unsupported GNUC version"
#endif							/* __GNUC__ */
#endif

#define TS_PREFIX(fn) \
	TS_CAT(ts_, fn)

/*
 * Functions exposed to postgres always have the signature
 *   `PGDLLEXPORT Datum function(PG_FUNCTION_ARGS)`
 * and function name should always be of the form `ts_realname`
 * We define the following macros to assist with this
 */

/*
 * Used as to declare and define publically-accessible functions, e.g.
 *
 *    extern TS_FUNCTION(realname);
 *
 * or
 *
 *    TS_FUNCTION(realname)
 *    {
 *    	// body
 *    }
 *
 */
#define TS_FUNCTION(fn) \
	PGDLLEXPORT Datum TS_PREFIX(fn)(PG_FUNCTION_ARGS)

/*
 * postgres requires some additional informations beyond what C provides.
 * Every function we expose to postgres needs exactly one
 * `TS_FUNCTION_INFO_V1(realname)` somewhere in a c file
 * (NOT in a header file)
 */
#define TS_FUNCTION_INFO_V1(fn) \
	TS_FUNCTION(fn); \
	PG_FUNCTION_INFO_V1(TS_PREFIX(fn))

#endif							/* TIMESCALEDB_EXPORT_H */
