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
 * We define the following macros `TS_FUNCTION` and `TS_FUNCTION_HEADER` to
 * assist with this:
 *
 * C files:
 *
 *    extern TS_FUNCTION(realname)
 *    {
 *    	// body
 *    }
 *
 * Header files:
 *
 *    extern TS_FUNCTION_HEADER(realname);
 *
 */
#define TS_FUNCTION_HEADER(fn) \
	PGDLLEXPORT Datum TS_PREFIX(fn)(PG_FUNCTION_ARGS)

/*
 * NOTE: this macro needs both calls to TS_FUNCTION, the first one sets up a
 * prototype which is used in PG_FUNCTION_INFO_V1
 */
#define TS_FUNCTION(fn) \
	TS_FUNCTION_HEADER(fn); \
	PG_FUNCTION_INFO_V1(TS_PREFIX(fn)); \
	TS_FUNCTION_HEADER(fn)

#endif							/* TIMESCALEDB_EXPORT_H */
