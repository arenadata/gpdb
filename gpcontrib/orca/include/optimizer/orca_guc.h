#ifndef ORCA_GUC_H
#define ORCA_GUC_H

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

void orca_guc_define(void);

#endif /* ORCA_GUC_H */
