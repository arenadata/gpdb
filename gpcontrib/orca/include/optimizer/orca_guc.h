#ifndef ORCA_GUC_H
#define ORCA_GUC_H

extern bool	optimizer_log;
extern int  optimizer_log_failure;
extern bool	optimizer_trace_fallback;
extern int	optimizer_minidump;
extern int  optimizer_cost_model;
extern bool optimizer_metadata_caching;
extern int	optimizer_mdcache_size;

void orca_guc_define(void);

#endif /* ORCA_GUC_H */
