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

void orca_guc_define(void);

#endif /* ORCA_GUC_H */
