#include "postgres.h"

#include "cdb/cdbvars.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/orca.h"
#include "optimizer/planner.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/vmem_tracker.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

static planner_hook_type prev_planner = NULL;
static ExplainOneQuery_hook_type prev_explain = NULL;

/*
 * Decide JIT settings for the given plan and record them in PlannedStmt.jitFlags.
 *
 * Since the costing model of ORCA and Postgres planner are different
 * (Postgres planner cost usually higher), setting the JIT flags based on the
 * common JIT costing GUCs could lead to false triggering of JIT.
 *
 * To prevent this situation, separate costing GUCs are created
 * for Orca and used here for setting the JIT flags.
 */
static void
orca_compute_jit_flags(PlannedStmt *pstmt)
{
	compute_jit_flags(pstmt, optimizer_jit_above_cost,
					  optimizer_jit_inline_above_cost,
					  optimizer_jit_optimize_above_cost);
}

static void
orca_shutdown(int code, Datum arg)
{
	(void) code;
	(void) arg;

	TerminateGPOPT();

	if (NULL != OptimizerMemoryContext)
		MemoryContextDelete(OptimizerMemoryContext);
}

static void
orca_init()
{
	/* Initialize GPOPT */
	OptimizerMemoryContext = AllocSetContextCreate(
		TopMemoryContext, "GPORCA Top-level Memory Context",
		ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	InitGPOPT();

	before_shmem_exit(orca_shutdown, 0);
}

static PlannedStmt *
orca_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
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
			orca_init();

		if (gp_log_optimization_time)
			INSTR_TIME_SET_CURRENT(starttime);

		result = optimize_query(parse, cursorOptions, boundParams);

		/* decide jit state */
		if (result)
		{
			/*
			 * Setting Jit flags for Optimizer
			 */
			orca_compute_jit_flags(result);
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

/*
 * orca_explain_dxl -
 *	  print out the execution plan for one Query in DXL format
 *	  this function implicitly uses optimizer
 */
static void
orca_explain_dxl(Query *query, ExplainState *es, ParamListInfo params)
{
	MemoryContext oldcxt = CurrentMemoryContext;
	bool save_enumerate;
	char *dxl = NULL;
	PlannerInfo *root;
	PlannerGlobal *glob;
	Query *pqueryCopy;

	save_enumerate = optimizer_enumerate_plans;

	/* Do the EXPLAIN. */

	/* enable plan enumeration before calling optimizer */
	optimizer_enumerate_plans = true;

	/*
	 * Initialize a dummy PlannerGlobal struct. ORCA doesn't use it, but the
	 * pre- and post-processing steps do.
	 */
	glob = makeNode(PlannerGlobal);
	glob->subplans = NIL;
	glob->subroots = NIL;
	glob->rewindPlanIDs = NULL;
	glob->transientPlan = false;
	glob->oneoffPlan = false;
	glob->share.shared_inputs = NULL;
	glob->share.shared_input_count = 0;
	glob->share.motStack = NIL;
	glob->share.qdShares = NULL;
	/* these will be filled in below, in the pre- and post-processing steps */
	glob->finalrtable = NIL;
	glob->relationOids = NIL;
	glob->invalItems = NIL;

	root = makeNode(PlannerInfo);
	root->parse = query;
	root->glob = glob;
	root->query_level = 1;
	root->planner_cxt = CurrentMemoryContext;
	root->wt_param_id = -1;

	/* create a local copy to hand to the optimizer */
	pqueryCopy = (Query *) copyObject(query);

	/*
	 * Pre-process the Query tree before calling optimizer.
	 *
	 * Constant folding will add dependencies to functions or relations in
	 * glob->invalItems, for any functions that are inlined or eliminated
	 * away. (We will find dependencies to other objects later, after planning).
	 */
	pqueryCopy = fold_constants(root, pqueryCopy, params,
								GPOPT_MAX_FOLDED_CONSTANT_SIZE);

	/*
	 * If any Query in the tree mixes window functions and aggregates, we need to
	 * transform it such that the grouped query appears as a subquery
	 */
	pqueryCopy = (Query *) transformGroupedWindows((Node *) pqueryCopy, NULL);


	/* optimize query using optimizer and get generated plan in DXL format */
	dxl = SerializeDXLPlan(pqueryCopy);

	/* restore old value of enumerate plans GUC */
	optimizer_enumerate_plans = save_enumerate;

	if (NULL == dxl)
		elog(NOTICE, "Optimizer failed to produce plan");
	else
	{
		appendStringInfoString(es->str, dxl);
		appendStringInfoChar(es->str, '\n'); /* separator line */
		pfree(dxl);
	}

	/* Free the memory we used. */
	MemoryContextSwitchTo(oldcxt);
}

static void
orca_explain(Query *query, int cursorOptions, IntoClause *into,
				ExplainState *es, const char *queryString, ParamListInfo params,
				QueryEnvironment *queryEnv)
{
	if (es->dxl)
	{
		if (NULL == OptimizerMemoryContext)
			orca_init();

		orca_explain_dxl(query, es, params);
	}
	else if (prev_explain)
		(*prev_explain)(query, cursorOptions, into, es, queryString, params,
						queryEnv);
	else
		standard_ExplainOneQuery(query, cursorOptions, into, es, queryString,
								 params, queryEnv);
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
	planner_hook = orca_planner;

	prev_explain = ExplainOneQuery_hook;
	ExplainOneQuery_hook = orca_explain;

	SetConfigOption("optimizer", "on", PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);
}

void
_PG_fini(void)
{
	planner_hook = prev_planner;
	ExplainOneQuery_hook = prev_explain;
}
