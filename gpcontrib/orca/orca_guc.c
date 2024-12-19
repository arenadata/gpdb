#include "postgres.h"

#include <limits.h>

#include "optimizer/orca_guc.h"
#include "utils/guc.h"


bool		optimizer_log;
int			optimizer_log_failure;
bool		optimizer_trace_fallback;
int			optimizer_minidump;
int			optimizer_cost_model;
bool		optimizer_metadata_caching;
int			optimizer_mdcache_size;
bool		optimizer_use_gpdb_allocators;

/* Optimizer debugging GUCs */
bool		optimizer_print_query;
bool		optimizer_print_plan;
bool		optimizer_print_xform;
bool		optimizer_print_memo_after_exploration;
bool		optimizer_print_memo_after_implementation;
bool		optimizer_print_memo_after_optimization;
bool		optimizer_print_job_scheduler;
bool		optimizer_print_expression_properties;
bool		optimizer_print_group_properties;
bool		optimizer_print_optimization_context;
bool		optimizer_print_optimization_stats;
bool		optimizer_print_xform_results;
bool		optimizer_print_missing_stats;

/* array of xforms disable flags */
bool		optimizer_xforms[OPTIMIZER_XFORMS_COUNT] = {[0 ... OPTIMIZER_XFORMS_COUNT - 1] = false};

char	   *optimizer_search_strategy_path = NULL;


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

static const struct config_enum_entry optimizer_cost_model_options[] = {
	{"legacy", OPTIMIZER_GPDB_LEGACY},
	{"calibrated", OPTIMIZER_GPDB_CALIBRATED},
	{"experimental", OPTIMIZER_GPDB_EXPERIMENTAL},
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

	DefineCustomEnumVariable("optimizer_cost_model",
							 "Set optimizer cost model.",
							 "Valid values are legacy, calibrated, experimental",
							 &optimizer_cost_model,
							 OPTIMIZER_GPDB_CALIBRATED,
							 optimizer_cost_model_options,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_metadata_caching",
							 "This guc enables the optimizer to cache and reuse metadata.",
							 NULL,
							 &optimizer_metadata_caching,
							 true,
							 PGC_USERSET,
							 GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("optimizer_mdcache_size",
							"Sets the size of MDCache.",
							NULL,
							&optimizer_mdcache_size,
							16384,
							0,
							INT_MAX,
							PGC_USERSET,
							GUC_UNIT_KB | GUC_GPDB_NO_SYNC,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("optimizer_use_gpdb_allocators",
							 "Enable ORCA to use GPDB Memory Contexts",
							 NULL,
							 &optimizer_use_gpdb_allocators,
							 true,
							 PGC_POSTMASTER,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_query",
							 "Prints the optimizer's input query expression tree.",
							 NULL,
							 &optimizer_print_query,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_plan",
							 "Prints the plan expression tree produced by the optimizer.",
							 NULL,
							 &optimizer_print_plan,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_xform",
							 "Prints optimizer transformation information.",
							 NULL,
							 &optimizer_print_xform,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_missing_stats",
							 "Print columns with missing statistics.",
							 NULL,
							 &optimizer_print_missing_stats,
							 true,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_xform_results",
							 "Print the input and output of optimizer transformations.",
							 NULL,
							 &optimizer_print_xform_results,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_memo_after_exploration",
							 "Print optimizer memo structure after the exploration phase.",
							 NULL,
							 &optimizer_print_memo_after_exploration,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_memo_after_implementation",
							 "Print optimizer memo structure after the implementation phase.",
							 NULL,
							 &optimizer_print_memo_after_implementation,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_memo_after_optimization",
							 "Print optimizer memo structure after optimization.",
							 NULL,
							 &optimizer_print_memo_after_optimization,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_job_scheduler",
							 "Print the jobs in the scheduler on each job completion.",
							 NULL,
							 &optimizer_print_job_scheduler,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_expression_properties",
							 "Print expression properties.",
							 NULL,
							 &optimizer_print_expression_properties,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_group_properties",
							 "Print group properties.",
							 NULL,
							 &optimizer_print_group_properties,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_optimization_context",
							 "Print the optimization context.",
							 NULL,
							 &optimizer_print_optimization_context,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_print_optimization_stats",
							 "Print optimization stats.",
							 NULL,
							 &optimizer_print_optimization_stats,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("optimizer_search_strategy_path",
							   "Sets the search strategy used by gp optimizer.",
							   NULL,
							   &optimizer_search_strategy_path,
							   "default",
							   PGC_USERSET,
							   GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							   NULL,
							   NULL,
							   NULL);

}
