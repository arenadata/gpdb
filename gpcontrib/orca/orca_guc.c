#include "postgres.h"

#include "optimizer/orca_guc.h"
#include "utils/guc.h"


bool		optimizer_log;
int			optimizer_log_failure;
bool		optimizer_trace_fallback;
int			optimizer_minidump;


static const struct config_enum_entry optimizer_log_failure_options[] = {
	{"all", OPTIMIZER_ALL_FAIL},
	{"unexpected", OPTIMIZER_UNEXPECTED_FAIL},
	{"expected", OPTIMIZER_EXPECTED_FAIL},
	{NULL, 0}
};

static const struct config_enum_entry optimizer_minidump_options[] = {
	{"onerror", OPTIMIZER_MINIDUMP_FAIL},
	{"always", OPTIMIZER_MINIDUMP_ALWAYS},
	{NULL, 0}
};

void
orca_guc_define()
{
	DefineCustomBoolVariable("optimizer_log",
							 "Log optimizer messages.",
							 NULL,
							 &optimizer_log,
							 true,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("optimizer_log_failure",
							 "Sets which optimizer failures are logged.",
							 "Valid values are unexpected, expected, all",
							 &optimizer_log_failure,
							 OPTIMIZER_UNEXPECTED_FAIL,
							 optimizer_log_failure_options,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_trace_fallback",
							 "Print a message at INFO level, whenever GPORCA falls back.",
							 NULL,
							 &optimizer_trace_fallback,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("optimizer_minidump",
							 "Generate optimizer minidump.",
							 "Valid values are onerror, always",
							 &optimizer_minidump,
							 OPTIMIZER_MINIDUMP_FAIL,
							 optimizer_minidump_options,
							 PGC_USERSET,
							 GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);
}
