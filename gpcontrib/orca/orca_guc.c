#include "postgres.h"

#include <float.h>
#include <limits.h>

#include "optimizer/orca_guc.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"

#define ORCA_GUC_PROCESS_NO_SYNC_FLAG(flags) \
	((flags & GUC_GPDB_NEED_SYNC) ? flags : (flags | GUC_GPDB_NO_SYNC))


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

/* Costing related GUCs used by the Optimizer */
int			optimizer_segments;
int			optimizer_penalize_broadcast_threshold;
double		optimizer_cost_threshold;
double		optimizer_nestloop_factor;
double		optimizer_sort_factor;

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

struct config_bool configure_names_bool_orca[] =
{
	{
		{"optimizer_log", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Log optimizer messages."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_log,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_trace_fallback", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print a message at INFO level, whenever GPORCA falls back."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_trace_fallback,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_query", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints the optimizer's input query expression tree."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_query,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_plan", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints the plan expression tree produced by the optimizer."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_plan,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_xform", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints optimizer transformation information."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_xform,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_metadata_caching", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("This guc enables the optimizer to cache and reuse metadata."),
			NULL
		},
		&optimizer_metadata_caching,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_missing_stats", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print columns with missing statistics."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_missing_stats,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_xform_results", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print the input and output of optimizer transformations."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_xform_results,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_memo_after_exploration", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print optimizer memo structure after the exploration phase."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_memo_after_exploration,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_memo_after_implementation", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print optimizer memo structure after the implementation phase."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_memo_after_implementation,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_memo_after_optimization", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print optimizer memo structure after optimization."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_memo_after_optimization,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_job_scheduler", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print the jobs in the scheduler on each job completion."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_job_scheduler,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_expression_properties", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print expression properties."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_expression_properties,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_group_properties", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print group properties."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_group_properties,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_optimization_context", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print the optimization context."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_optimization_context,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_print_optimization_stats", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Print optimization stats."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_print_optimization_stats,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_extract_dxl_stats", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Extract plan stats in dxl."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_extract_dxl_stats,
		false,
		NULL, NULL, NULL
	},
	{
		{"optimizer_extract_dxl_stats_all_nodes", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Extract plan stats for all physical dxl nodes."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_extract_dxl_stats_all_nodes,
		false,
		NULL, NULL, NULL
	},
	{
		{"optimizer_dpe_stats", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable statistics derivation for partitioned tables with dynamic partition elimination."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_dpe_stats,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_nljoin", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable nested loops join plans in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_nljoin,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_indexjoin", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable index nested loops join plans in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_indexjoin,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_motions_masteronly_queries", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Motion operators in the optimizer for queries with no distributed tables."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_motions_coordinatoronly_queries,
		false,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_motions", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Motion operators in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_motions,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_motion_broadcast", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Motion Broadcast operators in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_motion_broadcast,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_motion_gather", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Motion Gather operators in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_motion_gather,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_motion_redistribute", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Motion Redistribute operators in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_motion_redistribute,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_sort", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Sort operators in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_sort,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_materialize", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Materialize operators in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_materialize,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_partition_propagation", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Partition Propagation operators in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_partition_propagation,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_partition_selection", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Partition Selection operators in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_partition_selection,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_outerjoin_rewrite", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable outer join to inner join rewrite in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_outerjoin_rewrite,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_direct_dispatch", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable direct dispatch in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_direct_dispatch,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_space_pruning", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable space pruning in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_space_pruning,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_master_only_queries", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Process coordinator only queries via the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_coordinator_only_queries,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_hashjoin", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the optimizer's use of hash join plans."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_hashjoin,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_dynamictablescan", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the optimizer's use of plans with dynamic table scan."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_dynamictablescan,
		true,
		NULL, NULL, NULL
	},

	{
			{"optimizer_enable_dynamicindexscan", PGC_USERSET, QUERY_TUNING_METHOD,
					gettext_noop("Enables the optimizer's use of plans with dynamic index scan."),
					NULL,
					GUC_NOT_IN_SAMPLE
			},
			&optimizer_enable_dynamicindexscan,
			true,
			NULL, NULL, NULL
	},

	{
		{"optimizer_enable_dynamicindexonlyscan", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the optimizer's use of plans with dynamic index only scan."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_dynamicindexonlyscan,
		true,
		NULL, NULL, NULL
	},

	{
			{"optimizer_enable_dynamicbitmapscan", PGC_USERSET, QUERY_TUNING_METHOD,
					gettext_noop("Enables the optimizer's use of plans with dynamic bitmap scan."),
					NULL,
					GUC_NOT_IN_SAMPLE
			},
			&optimizer_enable_dynamicbitmapscan,
			true,
			NULL, NULL, NULL
	},

	{
		{"optimizer_enable_indexscan", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the optimizer's use of plans with index scan."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_indexscan,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_indexonlyscan", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the optimizer's use of plans with index only scan."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_indexonlyscan,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_tablescan", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the optimizer's use of plans with table scan."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_tablescan,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_hashagg", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables GPORCA to use hash aggregates."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_hashagg,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_groupagg", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables GPORCA to use group aggregates."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_groupagg,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_force_agg_skew_avoidance", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Always pick a plan for aggregate distinct that minimizes skew."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_force_agg_skew_avoidance,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_penalize_skew", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Penalize operators with skewed hash redistribute below it."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_penalize_skew,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_multilevel_partitioning", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable optimization of queries on multilevel partitioned tables."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_multilevel_partitioning,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_derive_stats_all_groups", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable stats derivation for all groups after exploration."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_derive_stats_all_groups,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_force_multistage_agg", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Force optimizer to always pick multistage aggregates when such a plan alternative is generated."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_force_multistage_agg,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_multiple_distinct_aggs", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with multiple distinct aggregates in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_multiple_distinct_aggs,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_hashjoin_redistribute_broadcast_children", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable hash join plans with, Redistribute outer child and Broadcast inner child, in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_hashjoin_redistribute_broadcast_children,
		false,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_broadcast_nestloop_outer_child", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable nested loops join plans with replicated outer child in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_broadcast_nestloop_outer_child,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_discard_redistribute_hashjoin", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Discard hash join with redistribute motion in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_discard_redistribute_hashjoin,
		false,
		NULL, NULL, NULL
	},
	{
		{"optimizer_expand_fulljoin", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the optimizer's support of expanding full outer joins using union all."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_expand_fulljoin,
		false,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_mergejoin", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the optimizer's support of merge joins."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_mergejoin,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_streaming_material", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with a streaming material node in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_streaming_material,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_gather_on_segment_for_dml", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable DML optimization by enforcing a non-coordinator gather in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_gather_on_segment_for_dml,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_assert_maxonerow", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable Assert MaxOneRow plans to check number of rows at runtime."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_assert_maxonerow,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enumerate_plans", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Enable plan enumeration"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_enumerate_plans,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_sample_plans", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable plan sampling"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_sample_plans,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_constant_expression_evaluation", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable constant expression evaluation in the optimizer"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_constant_expression_evaluation,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_bitmapscan", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable bitmap plans in the optimizer"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_bitmapscan,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_outerjoin_to_unionall_rewrite", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable rewriting Left Outer Join to UnionAll"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_outerjoin_to_unionall_rewrite,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_ctas", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable CTAS plans in the optimizer"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_ctas,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_dml", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable DML plans in GPORCA."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_dml,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_dml_constraints", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Support DML with CHECK constraints and NOT NULL constraints."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_dml_constraints,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_use_gpdb_allocators", PGC_POSTMASTER, RESOURCES_MEM,
			gettext_noop("Enable ORCA to use GPDB Memory Contexts"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_use_gpdb_allocators,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_orderedagg", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable ordered aggregate plans."),
			NULL
		},
		&optimizer_enable_orderedagg,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_eageragg", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable Eager Agg transform for pushing aggregate below an innerjoin."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_eageragg,
		false,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_redistribute_nestloop_loj_inner_child", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable nested loops left join plans with redistributed inner child in the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_redistribute_nestloop_loj_inner_child,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_force_comprehensive_join_implementation", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Explore a nested loop join even if a hash join is possible"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_force_comprehensive_join_implementation,
		false,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_replicated_table", PGC_USERSET, DEVELOPER_OPTIONS,
		 gettext_noop("Enable replicated tables."),
		 NULL,
		 GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_replicated_table,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_foreign_table", PGC_USERSET, DEVELOPER_OPTIONS,
		 gettext_noop("Enable foreign tables in Orca."),
		 NULL,
		 GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_foreign_table,
		true,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_right_outer_join", PGC_USERSET, QUERY_TUNING_METHOD,
		 gettext_noop("Enable Orca to generate plans containing right outer joins."),
		 gettext_noop("Right outer join can be re-written from left outer join. "
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
					  "choose."),
		 GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_right_outer_join,
		true,
		NULL, NULL, NULL
	},

	{
		{"optimizer_enable_coordinator_only_queries", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Process coordinator only queries via the optimizer."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_coordinator_only_queries,
		false,
		NULL, NULL, NULL
	},
	{
		{"optimizer_enable_motions_coordinatoronly_queries", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable plans with Motion operators in the optimizer for queries with no distributed tables."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_enable_motions_coordinatoronly_queries,
		false,
		NULL, NULL, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, false, NULL, NULL
	}
};

struct config_int configure_names_int_orca[] =
{
	{
		{"optimizer_plan_id", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Choose a plan alternative"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_plan_id,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"optimizer_samples_number", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Set the number of plan samples"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_samples_number,
		1000, 1, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"optimizer_segments", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Number of segments to be considered by the optimizer during costing, or 0 to take the actual number of segments."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_segments,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"optimizer_penalize_broadcast_threshold", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Maximum number of rows of a relation that can be broadcasted without penalty. A value of 0 disables."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_penalize_broadcast_threshold,
		100000, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"optimizer_mdcache_size", PGC_USERSET, RESOURCES_MEM,
			gettext_noop("Sets the size of MDCache."),
			NULL,
			GUC_UNIT_KB
		},
		&optimizer_mdcache_size,
		16384, 0, INT_MAX,
		NULL, NULL, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL
	}
};

struct config_real configure_names_real_orca[] =
{
	{
		{"optimizer_damping_factor_filter", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("select predicate damping factor in optimizer, 1.0 means no damping"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_damping_factor_filter,
		0.75, 0.0, 1.0,
		NULL, NULL, NULL
	},

	{
		{"optimizer_damping_factor_join", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("join predicate damping factor in optimizer, 1.0 means no damping, 0.0 means square root method"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_damping_factor_join,
		0.0, 0.0, 1.0,
		NULL, NULL, NULL
	},
	{
		{"optimizer_damping_factor_groupby", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("groupby operator damping factor in optimizer, 1.0 means no damping"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_damping_factor_groupby,
		0.75, 0.0, 1.0,
		NULL, NULL, NULL
	},

	{
		{"optimizer_cost_threshold", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Set the threshold for plan sampling relative to the cost of best plan, 0.0 means unbounded"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_cost_threshold,
		0.0, 0.0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"optimizer_nestloop_factor", PGC_USERSET, QUERY_TUNING_OTHER,
			gettext_noop("Set the nestloop join cost factor in the optimizer"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_nestloop_factor,
		1024.0, 1.0, DBL_MAX,
		NULL, NULL, NULL
	},

	{
		{"optimizer_sort_factor",PGC_USERSET, QUERY_TUNING_OTHER,
			gettext_noop("Set the sort cost factor in the optimizer, 1.0 means same as default, > 1.0 means more costly than default, < 1.0 means means less costly than default"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_sort_factor,
		1.0, 0.0, DBL_MAX,
		NULL, NULL, NULL
	},
	{
		{"optimizer_jit_above_cost",PGC_USERSET, QUERY_TUNING_COST,
			gettext_noop("Perform JIT compilation if query is more expensive."),
			gettext_noop("-1 disables JIT compilation."),
			GUC_EXPLAIN | GUC_GPDB_NEED_SYNC
		},
		&optimizer_jit_above_cost,
		7500, -1, DBL_MAX,
		NULL, NULL, NULL
	},
	{
		{"optimizer_jit_optimize_above_cost",PGC_USERSET, QUERY_TUNING_COST,
			gettext_noop("Optimize JITed functions if query is more expensive."),
			gettext_noop("-1 disables JIT optimization."),
			GUC_EXPLAIN | GUC_GPDB_NEED_SYNC
		},
		&optimizer_jit_optimize_above_cost,
		37500, -1, DBL_MAX,
		NULL, NULL, NULL
	},
	{
		{"optimizer_jit_inline_above_cost",PGC_USERSET, QUERY_TUNING_COST,
			gettext_noop("Perform JIT inlining if query is more expensive."),
			gettext_noop("-1 disables inlining."),
			GUC_EXPLAIN | GUC_GPDB_NEED_SYNC
		},
		&optimizer_jit_inline_above_cost,
		37500, -1, DBL_MAX,
		NULL, NULL, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, 0.0, 0.0, 0.0, NULL, NULL
	}
};

struct config_string configure_names_string_orca[] =
{
	{
		{"optimizer_search_strategy_path", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Sets the search strategy used by gp optimizer."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_search_strategy_path,
		"default",
		NULL, NULL, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, NULL, NULL, NULL
	}
};

struct config_enum configure_names_enum_orca[] =
{
	{
		{"optimizer_log_failure", PGC_USERSET, LOGGING_WHEN,
			gettext_noop("Sets which optimizer failures are logged."),
			gettext_noop("Valid values are unexpected, expected, all"),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&optimizer_log_failure,
		OPTIMIZER_UNEXPECTED_FAIL, optimizer_log_failure_options,
		NULL, NULL, NULL
	},

	{
		{"optimizer_minidump", PGC_USERSET, LOGGING_WHEN,
			gettext_noop("Generate optimizer minidump."),
			gettext_noop("Valid values are onerror, always"),
		},
		&optimizer_minidump,
		OPTIMIZER_MINIDUMP_FAIL, optimizer_minidump_options,
		NULL, NULL, NULL
	},

	{
		{"optimizer_cost_model", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Set optimizer cost model."),
			gettext_noop("Valid values are legacy, calibrated, experimental"),
			GUC_NOT_IN_SAMPLE
		},
		&optimizer_cost_model,
		OPTIMIZER_GPDB_CALIBRATED, optimizer_cost_model_options,
		NULL, NULL, NULL
	},


	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, 0, NULL, NULL, NULL
	}
};

void
orca_guc_define()
{
	int i;

	for (i = 0; configure_names_bool_orca[i].gen.name; i++)
	{
		struct config_bool *conf = &configure_names_bool_orca[i];

		DefineCustomBoolVariable(conf->gen.name,
							 conf->gen.short_desc,
							 conf->gen.long_desc,
							 conf->variable,
							 conf->boot_val,
							 conf->gen.context,
							 ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags),
							 conf->check_hook,
							 conf->assign_hook,
							 conf->show_hook);
	}

	for (i = 0; configure_names_int_orca[i].gen.name; i++)
	{
		struct config_int *conf = &configure_names_int_orca[i];

		DefineCustomIntVariable(conf->gen.name,
								conf->gen.short_desc,
								conf->gen.long_desc,
								conf->variable,
								conf->boot_val,
								conf->min,
								conf->max,
								conf->gen.context,
								ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags),
								conf->check_hook,
								conf->assign_hook,
								conf->show_hook);
	}

	for (i = 0; configure_names_real_orca[i].gen.name; i++)
	{
		struct config_real *conf = &configure_names_real_orca[i];

		DefineCustomRealVariable(conf->gen.name,
								 conf->gen.short_desc,
								 conf->gen.long_desc,
								 conf->variable,
								 conf->boot_val,
								 conf->min,
								 conf->max,
								 conf->gen.context,
								 ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags),
								 conf->check_hook,
								 conf->assign_hook,
								 conf->show_hook);
	}

	for (i = 0; configure_names_string_orca[i].gen.name; i++)
	{
		struct config_string *conf = &configure_names_string_orca[i];

		DefineCustomStringVariable(conf->gen.name,
								 conf->gen.short_desc,
								 conf->gen.long_desc,
								 conf->variable,
								 conf->boot_val,
								 conf->gen.context,
								 ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags),
								 conf->check_hook,
								 conf->assign_hook,
								 conf->show_hook);
	}

	for (i = 0; configure_names_enum_orca[i].gen.name; i++)
	{
		struct config_enum *conf = &configure_names_enum_orca[i];

		DefineCustomEnumVariable(conf->gen.name,
								 conf->gen.short_desc,
								 conf->gen.long_desc,
								 conf->variable,
								 conf->boot_val,
								 conf->options,
								 conf->gen.context,
								 ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags),
								 conf->check_hook,
								 conf->assign_hook,
								 conf->show_hook);
	}
}
