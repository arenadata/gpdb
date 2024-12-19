#ifndef ORCA_GUC_H
#define ORCA_GUC_H

/* ORCA related definitions */
#define OPTIMIZER_XFORMS_COUNT 400 /* number of transformation rules */

/* types of optimizer failures */
#define OPTIMIZER_ALL_FAIL 			0  /* all failures */
#define OPTIMIZER_UNEXPECTED_FAIL 	1  /* unexpected failures */
#define OPTIMIZER_EXPECTED_FAIL 	2 /* expected failures */

/* optimizer minidump mode */
#define OPTIMIZER_MINIDUMP_FAIL  	0  /* create optimizer minidump on failure */
#define OPTIMIZER_MINIDUMP_ALWAYS 	1  /* always create optimizer minidump */

/* optimizer cost model */
#define OPTIMIZER_GPDB_LEGACY           0       /* GPDB's legacy cost model */
#define OPTIMIZER_GPDB_CALIBRATED       1       /* GPDB's calibrated cost model */
#define OPTIMIZER_GPDB_EXPERIMENTAL     2       /* GPDB's experimental cost model */

extern bool	optimizer_log;
extern int  optimizer_log_failure;
extern bool	optimizer_trace_fallback;
extern int	optimizer_minidump;
extern int  optimizer_cost_model;
extern bool optimizer_metadata_caching;
extern int	optimizer_mdcache_size;
extern bool optimizer_use_gpdb_allocators;

/* Optimizer debugging GUCs */
extern bool optimizer_print_query;
extern bool optimizer_print_plan;
extern bool optimizer_print_xform;
extern bool	optimizer_print_memo_after_exploration;
extern bool	optimizer_print_memo_after_implementation;
extern bool	optimizer_print_memo_after_optimization;
extern bool	optimizer_print_job_scheduler;
extern bool	optimizer_print_expression_properties;
extern bool	optimizer_print_group_properties;
extern bool	optimizer_print_optimization_context;
extern bool optimizer_print_optimization_stats;
extern bool optimizer_print_xform_results;
extern bool optimizer_print_missing_stats;

/* array of xforms disable flags */
extern bool optimizer_xforms[OPTIMIZER_XFORMS_COUNT];

extern char *optimizer_search_strategy_path;

/* GUCs to tell Optimizer to enable a physical operator */
extern bool optimizer_enable_nljoin;
extern bool optimizer_enable_indexjoin;
extern bool optimizer_enable_motions_coordinatoronly_queries;
extern bool optimizer_enable_motions;
extern bool optimizer_enable_motion_broadcast;
extern bool optimizer_enable_motion_gather;
extern bool optimizer_enable_motion_redistribute;
extern bool optimizer_enable_sort;
extern bool optimizer_enable_materialize;
extern bool optimizer_enable_partition_propagation;
extern bool optimizer_enable_partition_selection;
extern bool optimizer_enable_outerjoin_rewrite;
extern bool optimizer_enable_multiple_distinct_aggs;

extern bool optimizer_enable_direct_dispatch;
extern bool optimizer_enable_coordinator_only_queries;
extern bool optimizer_enable_hashjoin;
extern bool optimizer_enable_dynamictablescan;
extern bool optimizer_enable_dynamicindexscan;
extern bool optimizer_enable_dynamicindexonlyscan;
extern bool optimizer_enable_dynamicbitmapscan;
extern bool optimizer_enable_indexscan;
extern bool optimizer_enable_indexonlyscan;
extern bool optimizer_enable_tablescan;

extern bool optimizer_enable_hashagg;
extern bool optimizer_enable_groupagg;

extern bool optimizer_enable_derive_stats_all_groups;

extern bool optimizer_force_multistage_agg;

extern bool optimizer_force_agg_skew_avoidance;
extern bool optimizer_penalize_skew;

extern bool optimizer_multilevel_partitioning;

extern bool optimizer_enable_space_pruning;

extern bool optimizer_enable_hashjoin_redistribute_broadcast_children;
extern bool optimizer_enable_broadcast_nestloop_outer_child;
extern bool optimizer_discard_redistribute_hashjoin;
extern bool optimizer_enable_streaming_material;
extern bool optimizer_enable_gather_on_segment_for_dml;
extern bool optimizer_enable_assert_maxonerow;
extern bool optimizer_enable_constant_expression_evaluation;
extern bool optimizer_enable_bitmapscan;
extern bool optimizer_enable_outerjoin_to_unionall_rewrite;
extern bool optimizer_enable_ctas;
extern bool optimizer_enable_dml;
extern bool	optimizer_enable_dml_constraints;
extern bool optimizer_enable_eageragg;
extern bool optimizer_enable_orderedagg;
extern bool optimizer_expand_fulljoin;
extern bool optimizer_enable_mergejoin;
extern bool optimizer_enable_redistribute_nestloop_loj_inner_child;
extern bool optimizer_force_comprehensive_join_implementation;
extern bool optimizer_enable_replicated_table;
extern bool optimizer_enable_foreign_table;
extern bool optimizer_enable_right_outer_join;

/* Optimizer plan enumeration related GUCs */
extern bool optimizer_enumerate_plans;
extern bool optimizer_sample_plans;
extern int	optimizer_plan_id;
extern int	optimizer_samples_number;

/* Optimizer Just In Time (JIT) compilation related GUCs*/
extern  double  optimizer_jit_above_cost;
extern  double  optimizer_jit_inline_above_cost;
extern  double  optimizer_jit_optimize_above_cost;

/* Cardinality estimation related GUCs used by the Optimizer */
extern bool optimizer_extract_dxl_stats;
extern bool optimizer_extract_dxl_stats_all_nodes;
extern double optimizer_damping_factor_filter;
extern double optimizer_damping_factor_join;
extern double optimizer_damping_factor_groupby;
extern bool optimizer_dpe_stats;

/* Costing or tuning related GUCs used by the Optimizer */
extern int optimizer_segments;
extern int optimizer_penalize_broadcast_threshold;
extern double optimizer_cost_threshold;
extern double optimizer_nestloop_factor;
extern double optimizer_sort_factor;

/* Optimizer hints */
extern int optimizer_array_expansion_threshold;
extern int optimizer_join_order_threshold;
extern int optimizer_join_order;
extern int optimizer_join_arity_for_associativity_commutativity;
extern int optimizer_cte_inlining_bound;
extern int optimizer_push_group_by_below_setop_threshold;
extern int optimizer_xform_bind_threshold;
extern int optimizer_skew_factor;

void orca_guc_define(void);

#endif /* ORCA_GUC_H */
