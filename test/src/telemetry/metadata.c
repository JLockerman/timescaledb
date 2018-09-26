#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>

#include "export.h"
#include "telemetry/metadata.h"

TS_FUNCTION_INFO_V1(test_uuid);
TS_FUNCTION_INFO_V1(test_exported_uuid);
TS_FUNCTION_INFO_V1(test_install_timestamp);

TS_FUNCTION(test_uuid)
{
	PG_RETURN_DATUM(metadata_get_uuid());
}

TS_FUNCTION(test_exported_uuid)

{
	PG_RETURN_DATUM(metadata_get_exported_uuid());
}

TS_FUNCTION(test_install_timestamp)
{
	PG_RETURN_DATUM(metadata_get_install_timestamp());
}
