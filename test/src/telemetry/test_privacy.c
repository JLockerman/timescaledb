#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>

#include "compat.h"
#include "telemetry/telemetry.h"
#include "telemetry/uuid.h"


TS_FUNCTION(test_privacy)
{
	/* This test should only run when timescaledb.telemetry_level=off */
	telemetry_main("", "", "");
	PG_RETURN_NULL();
}
