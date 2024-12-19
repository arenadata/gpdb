#include "postgres.h"

#include <float.h>
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

/* GUCs to tell Optimizer to enable a physical operator */
bool		optimizer_enable_nljoin;
bool		optimizer_enable_indexjoin;
bool		optimizer_enable_motions_coordinatoronly_queries;
bool		optimizer_enable_motions;
bool		optimizer_enable_motion_broadcast;
bool		optimizer_enable_motion_gather;
bool		optimizer_enable_motion_redistribute;
bool		optimizer_enable_sort;
bool		optimizer_enable_materialize;
bool		optimizer_enable_partition_propagation;
bool		optimizer_enable_partition_selection;
bool		optimizer_enable_outerjoin_rewrite;
bool		optimizer_enable_multiple_distinct_aggs;

bool		optimizer_enable_direct_dispatch;

bool		optimizer_enable_coordinator_only_queries;
bool		optimizer_enable_hashjoin;
bool		optimizer_enable_dynamictablescan;
bool		optimizer_enable_dynamicindexscan;
bool		optimizer_enable_dynamicindexonlyscan;
bool		optimizer_enable_dynamicbitmapscan;
bool		optimizer_enable_indexscan;
bool		optimizer_enable_indexonlyscan;
bool		optimizer_enable_tablescan;
bool		optimizer_enable_hashagg;
bool		optimizer_enable_groupagg;

bool		optimizer_enable_derive_stats_all_groups;

bool		optimizer_force_multistage_agg;

bool		optimizer_force_agg_skew_avoidance;
bool		optimizer_penalize_skew;

bool		optimizer_multilevel_partitioning;

bool		optimizer_enable_space_pruning;

bool		optimizer_enable_hashjoin_redistribute_broadcast_children;
bool		optimizer_enable_broadcast_nestloop_outer_child;
bool		optimizer_discard_redistribute_hashjoin;
bool		optimizer_enable_streaming_material;
bool		optimizer_enable_gather_on_segment_for_dml;
bool		optimizer_enable_assert_maxonerow;
bool		optimizer_enable_constant_expression_evaluation;
bool		optimizer_enable_bitmapscan;
bool		optimizer_enable_outerjoin_to_unionall_rewrite;
bool		optimizer_enable_ctas;
bool		optimizer_enable_dml;
bool		optimizer_enable_dml_constraints;

bool		optimizer_expand_fulljoin;
bool		optimizer_enable_mergejoin;
bool		optimizer_enable_redistribute_nestloop_loj_inner_child;
bool		optimizer_force_comprehensive_join_implementation;
bool		optimizer_enable_replicated_table;
bool		optimizer_enable_foreign_table;
bool		optimizer_enable_right_outer_join;

bool		optimizer_enable_eageragg;

bool		optimizer_enable_orderedagg;

/* Optimizer plan enumeration related GUCs */
bool		optimizer_enumerate_plans;
bool		optimizer_sample_plans;
int			optimizer_plan_id;
int			optimizer_samples_number;

/* GUCs for Just In Time (JIT) compilation */
double		optimizer_jit_above_cost;
double		optimizer_jit_inline_above_cost;
double		optimizer_jit_optimize_above_cost;

/* Cardinality estimation related GUCs used by the Optimizer */
bool		optimizer_extract_dxl_stats;
bool		optimizer_extract_dxl_stats_all_nodes;
double		optimizer_damping_factor_filter;
double		optimizer_damping_factor_join;
double		optimizer_damping_factor_groupby;
bool		optimizer_dpe_stats;

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

	DefineCustomBoolVariable("optimizer_enable_nljoin",
							 "Enable nested loops join plans in the optimizer.",
							 NULL,
							 &optimizer_enable_nljoin,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_indexjoin",
							 "Enable index nested loops join plans in the optimizer.",
							 NULL,
							 &optimizer_enable_indexjoin,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_motions_masteronly_queries",
							 "Enable plans with Motion operators in the optimizer for queries with no distributed tables.",
							 NULL,
							 &optimizer_enable_motions_coordinatoronly_queries,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_motions",
							 "Enable plans with Motion operators in the optimizer.",
							 NULL,
							 &optimizer_enable_motions,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_motion_broadcast",
							 "Enable plans with Motion Broadcast operators in the optimizer.",
							 NULL,
							 &optimizer_enable_motion_broadcast,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_motion_gather",
							 "Enable plans with Motion Gather operators in the optimizer.",
							 NULL,
							 &optimizer_enable_motion_gather,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_motion_redistribute",
							 "Enable plans with Motion Redistribute operators in the optimizer.",
							 NULL,
							 &optimizer_enable_motion_redistribute,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_sort",
							 "Enable plans with Sort operators in the optimizer.",
							 NULL,
							 &optimizer_enable_sort,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_materialize",
							 "Enable plans with Materialize operators in the optimizer.",
							 NULL,
							 &optimizer_enable_materialize,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);
 
	DefineCustomBoolVariable("optimizer_enable_partition_propagation",
							 "Enable plans with Partition Propagation operators in the optimizer.",
							 NULL,
							 &optimizer_enable_partition_propagation,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_partition_selection",
							 "Enable plans with Partition Selection operators in the optimizer.",
							 NULL,
							 &optimizer_enable_partition_selection,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_outerjoin_rewrite",
							 "Enable outer join to inner join rewrite in the optimizer.",
							 NULL,
							 &optimizer_enable_outerjoin_rewrite,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_direct_dispatch",
							 "Enable direct dispatch in the optimizer.",
							 NULL,
							 &optimizer_enable_direct_dispatch,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_space_pruning",
							 "Enable space pruning in the optimizer.",
							 NULL,
							 &optimizer_enable_space_pruning,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_master_only_queries",
							 "Process coordinator only queries via the optimizer.",
							 NULL,
							 &optimizer_enable_coordinator_only_queries,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_hashjoin",
							 "Enables the optimizer's use of hash join plans.",
							 NULL,
							 &optimizer_enable_hashjoin,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_dynamictablescan",
							 "Enables the optimizer's use of plans with dynamic table scan.",
							 NULL,
							 &optimizer_enable_dynamictablescan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_dynamicindexscan",
							 "Enables the optimizer's use of plans with dynamic index scan.",
							 NULL,
							 &optimizer_enable_dynamicindexscan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_dynamicindexonlyscan",
							 "Enables the optimizer's use of plans with dynamic index only scan.",
							 NULL,
							 &optimizer_enable_dynamicindexonlyscan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_dynamicbitmapscan",
							 "Enables the optimizer's use of plans with dynamic bitmap scan.",
							 NULL,
							 &optimizer_enable_dynamicbitmapscan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_indexscan",
							 "Enables the optimizer's use of plans with index scan.",
							 NULL,
							 &optimizer_enable_indexscan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_indexonlyscan",
							 "Enables the optimizer's use of plans with index only scan.",
							 NULL,
							 &optimizer_enable_indexonlyscan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_tablescan",
							 "Enables the optimizer's use of plans with table scan.",
							 NULL,
							 &optimizer_enable_tablescan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_hashagg",
							 "Enables GPORCA to use hash aggregates.",
							 NULL,
							 &optimizer_enable_hashagg,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_groupagg",
							 "Enables GPORCA to use group aggregates.",
							 NULL,
							 &optimizer_enable_groupagg,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_force_agg_skew_avoidance",
							 "Always pick a plan for aggregate distinct that minimizes skew.",
							 NULL,
							 &optimizer_force_agg_skew_avoidance,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_penalize_skew",
							 "Penalize operators with skewed hash redistribute below it.",
							 NULL,
							 &optimizer_penalize_skew,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_multilevel_partitioning",
							 "Enable optimization of queries on multilevel partitioned tables.",
							 NULL,
							 &optimizer_multilevel_partitioning,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_derive_stats_all_groups",
							 "Enable stats derivation for all groups after exploration.",
							 NULL,
							 &optimizer_enable_derive_stats_all_groups,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_force_multistage_agg",
							 "Force optimizer to always pick multistage aggregates when such a plan alternative is generated.",
							 NULL,
							 &optimizer_force_multistage_agg,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_multiple_distinct_aggs",
							 "Enable plans with multiple distinct aggregates in the optimizer.",
							 NULL,
							 &optimizer_enable_multiple_distinct_aggs,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_hashjoin_redistribute_broadcast_children",
							 "Enable hash join plans with, Redistribute outer child and Broadcast inner child, in the optimizer.",
							 NULL,
							 &optimizer_enable_hashjoin_redistribute_broadcast_children,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_broadcast_nestloop_outer_child",
							 "Enable nested loops join plans with replicated outer child in the optimizer.",
							 NULL,
							 &optimizer_enable_broadcast_nestloop_outer_child,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_discard_redistribute_hashjoin",
							 "Discard hash join with redistribute motion in the optimizer.",
							 NULL,
							 &optimizer_discard_redistribute_hashjoin,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_expand_fulljoin",
							 "Enables the optimizer's support of expanding full outer joins using union all.",
							 NULL,
							 &optimizer_expand_fulljoin,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_mergejoin",
							 "Enables the optimizer's support of merge joins.",
							 NULL,
							 &optimizer_enable_mergejoin,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_streaming_material",
							 "Enable plans with a streaming material node in the optimizer.",
							 NULL,
							 &optimizer_enable_streaming_material,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_gather_on_segment_for_dml",
							 "Enable DML optimization by enforcing a non-coordinator gather in the optimizer.",
							 NULL,
							 &optimizer_enable_gather_on_segment_for_dml,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_assert_maxonerow",
							 "Enable Assert MaxOneRow plans to check number of rows at runtime.",
							 NULL,
							 &optimizer_enable_assert_maxonerow,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_constant_expression_evaluation",
							 "Enable constant expression evaluation in the optimizer",
							 NULL,
							 &optimizer_enable_constant_expression_evaluation,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_bitmapscan",
							 "Enable bitmap plans in the optimizer",
							 NULL,
							 &optimizer_enable_bitmapscan,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_outerjoin_to_unionall_rewrite",
							 "Enable rewriting Left Outer Join to UnionAll",
							 NULL,
							 &optimizer_enable_outerjoin_to_unionall_rewrite,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_ctas",
							 "Enable CTAS plans in the optimizer",
							 NULL,
							 &optimizer_enable_ctas,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_dml",
							 "Enable DML plans in GPORCA.",
							 NULL,
							 &optimizer_enable_dml,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_dml_constraints",
							 "Support DML with CHECK constraints and NOT NULL constraints.",
							 NULL,
							 &optimizer_enable_dml_constraints,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_orderedagg",
							 "Enable ordered aggregate plans.",
							 NULL,
							 &optimizer_enable_orderedagg,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_eageragg",
							 "Enable Eager Agg transform for pushing aggregate below an innerjoin.",
							 NULL,
							 &optimizer_enable_eageragg,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_redistribute_nestloop_loj_inner_child",
							 "Enable nested loops left join plans with redistributed inner child in the optimizer.",
							 NULL,
							 &optimizer_enable_redistribute_nestloop_loj_inner_child,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_force_comprehensive_join_implementation",
							 "Explore a nested loop join even if a hash join is possible",
							 NULL,
							 &optimizer_force_comprehensive_join_implementation,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_replicated_table",
							 "Enable replicated tables.",
							 NULL,
							 &optimizer_enable_replicated_table,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_foreign_table",
							 "Enable foreign tables in Orca.",
							 NULL,
							 &optimizer_enable_foreign_table,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enable_right_outer_join",
							 "Enable Orca to generate plans containing right outer joins.",
							 "Right outer join can be re-written from left outer join. "
							 "However, there are scenarios due to cardinality and cost "
							 "misestimation, right outer join plan may be sub-optimal and "
							 "can either be slower than the left outer join plan alternative "
							 "or hit out-of-memory (OOM). The root cause can be identified "
							 "by viewing the explain analyze plan and observing that the "
							 "right outer join plan node is consuming all resources "
							 "(CPU/memory) or the explain analyze itself hits OOM. By "
							 "setting this GUC value to \"false\" users can force GPORCA to "
							 "generate an equivalent left outer join plan. We recommend that "
							 "the GUC be set at the query level as there can be several use "
							 "cases where right outer join is the best plan alternative to "
							 "choose.",
							 &optimizer_enable_right_outer_join,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_enumerate_plans",
							 "Enable plan enumeration",
							 NULL,
							 &optimizer_enumerate_plans,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_sample_plans",
							 "Enable plan sampling",
							 NULL,
							 &optimizer_sample_plans,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("optimizer_plan_id",
							"Choose a plan alternative",
							NULL,
							&optimizer_plan_id,
							0,
							0,
							INT_MAX,
							PGC_USERSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("optimizer_samples_number",
							"Set the number of plan samples",
							NULL,
							&optimizer_samples_number,
							1000,
							1,
							INT_MAX,
							PGC_USERSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							NULL,
							NULL,
							NULL);

	DefineCustomRealVariable("optimizer_jit_above_cost",
							 "Perform JIT compilation if query is more expensive.",
							 "-1 disables JIT compilation.",
							 &optimizer_jit_above_cost,
							 7500,
							 -1,
							 DBL_MAX,
							 PGC_USERSET,
							 GUC_EXPLAIN | GUC_GPDB_NEED_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("optimizer_jit_optimize_above_cost",
							 "Optimize JITed functions if query is more expensive.",
							 "-1 disables JIT optimization.",
							 &optimizer_jit_optimize_above_cost,
							 37500,
							 -1,
							 DBL_MAX,
							 PGC_USERSET,
							 GUC_EXPLAIN | GUC_GPDB_NEED_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("optimizer_jit_inline_above_cost",
							 "Perform JIT inlining if query is more expensive.",
							 "-1 disables inlining.",
							 &optimizer_jit_inline_above_cost,
							 37500,
							 -1,
							 DBL_MAX,
							 PGC_USERSET,
							 GUC_EXPLAIN | GUC_GPDB_NEED_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_extract_dxl_stats",
							 "Extract plan stats in dxl.",
							 NULL,
							 &optimizer_extract_dxl_stats,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_extract_dxl_stats_all_nodes",
							 "Extract plan stats for all physical dxl nodes.",
							 NULL,
							 &optimizer_extract_dxl_stats_all_nodes,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("optimizer_damping_factor_filter",
							 "select predicate damping factor in optimizer, 1.0 means no damping",
							 NULL,
							 &optimizer_damping_factor_filter,
							 0.75,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("optimizer_damping_factor_join",
							 "join predicate damping factor in optimizer, 1.0 means no damping, 0.0 means square root method",
							 NULL,
							 &optimizer_damping_factor_join,
							 0.0,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("optimizer_damping_factor_groupby",
							 "groupby operator damping factor in optimizer, 1.0 means no damping",
							 NULL,
							 &optimizer_damping_factor_groupby,
							 0.75,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("optimizer_dpe_stats",
							 "Enable statistics derivation for partitioned tables with dynamic partition elimination.",
							 NULL,
							 &optimizer_dpe_stats,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NO_SYNC,
							 NULL,
							 NULL,
							 NULL);
}
