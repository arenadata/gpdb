#ifndef ORCA_GUC_H
#define ORCA_GUC_H

extern bool	optimizer_log;
extern int  optimizer_log_failure;
extern bool	optimizer_trace_fallback;

void orca_guc_define(void);

#endif /* ORCA_GUC_H */
