#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/orca.h"
#include "optimizer/planner.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/vmem_tracker.h"

PG_MODULE_MAGIC;

extern void InitGPOPT();
extern void TerminateGPOPT();

extern void compute_jit_flags(PlannedStmt *pstmt);

void _PG_init(void);
void _PG_fini(void);

static planner_hook_type prev_planner = NULL;

static void
gp_orca_shutdown(int code, Datum arg)
{
	(void) code;
	(void) arg;

	TerminateGPOPT();

	if (NULL != OptimizerMemoryContext)
		MemoryContextDelete(OptimizerMemoryContext);
}

static void
gp_orca_init()
{
	/* Initialize GPOPT */
	OptimizerMemoryContext = AllocSetContextCreate(
		TopMemoryContext, "GPORCA Top-level Memory Context",
		ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	InitGPOPT();

	before_shmem_exit(gp_orca_shutdown, 0);
}

static PlannedStmt *
gp_orca_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result = NULL;

	/*
	 * Use ORCA only if it is enabled and we are in a coordinator QD process.
	 *
	 * ORCA excels in complex queries, most of which will access distributed
	 * tables. We can't run such queries from the segments slices anyway
	 * because they require dispatching a query within another - which is not
	 * allowed in GPDB (see querytree_safe_for_qe()). Note that this
	 * restriction also applies to non-QD coordinator slices.  Furthermore,
	 * ORCA doesn't currently support pl/<lang> statements (relevant when they
	 * are planned on the segments). For these reasons, restrict to using ORCA
	 * on the coordinator QD processes only.
	 *
	 * PARALLEL RETRIEVE CURSOR is not supported by ORCA yet.
	 */
	if (optimizer && GP_ROLE_DISPATCH == Gp_role && IS_QUERY_DISPATCHER() &&
		(cursorOptions & CURSOR_OPT_SKIP_FOREIGN_PARTITIONS) == 0 &&
		(cursorOptions & CURSOR_OPT_PARALLEL_RETRIEVE) == 0)
	{
		instr_time starttime;
		instr_time endtime;

		if (NULL == OptimizerMemoryContext)
			gp_orca_init();

		if (gp_log_optimization_time)
			INSTR_TIME_SET_CURRENT(starttime);

		result = optimize_query(parse, cursorOptions, boundParams);

		/* decide jit state */
		if (result)
		{
			/*
			 * Setting Jit flags for Optimizer
			 */
			compute_jit_flags(result);
		}

		if (gp_log_optimization_time)
		{
			INSTR_TIME_SET_CURRENT(endtime);
			INSTR_TIME_SUBTRACT(endtime, starttime);
			elog(LOG, "Optimizer Time: %.3f ms",
				 INSTR_TIME_GET_MILLISEC(endtime));
		}

		if (result)
			return result;
	}

	if (prev_planner)
		result = (*prev_planner)(parse, cursorOptions, boundParams);
	else
		result = standard_planner(parse, cursorOptions, boundParams);

	return result;
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(
			ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg(
				 "This module can only be loaded via shared_preload_libraries")));

	if (!(IS_QUERY_DISPATCHER() && (GP_ROLE_DISPATCH == Gp_role)))
		return;

	/* When compile with ORCA it will commit 6MB more */
	Size orca_mem = 6L << BITS_IN_MB;

	/*
	 * When optimizer_use_gpdb_allocators is on, at least 2MB of above will be
	 * tracked by vmem tracker later, so do not recount them.
	 */
	if (optimizer_use_gpdb_allocators)
		orca_mem -= (2L << BITS_IN_MB);

	GPMemoryProtect_RequestAddinStartupMemory(orca_mem);

	prev_planner = planner_hook;
	planner_hook = gp_orca_planner;

	SetConfigOption("optimizer", "on", PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);
}

void
_PG_fini(void)
{
	planner_hook = prev_planner;
}
