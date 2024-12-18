#include "postgres.h"

#include "optimizer/orca_guc.h"
#include "utils/guc.h"


bool		optimizer_log;
int			optimizer_log_failure;


static const struct config_enum_entry optimizer_log_failure_options[] = {
	{"all", OPTIMIZER_ALL_FAIL},
	{"unexpected", OPTIMIZER_UNEXPECTED_FAIL},
	{"expected", OPTIMIZER_EXPECTED_FAIL},
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
}
