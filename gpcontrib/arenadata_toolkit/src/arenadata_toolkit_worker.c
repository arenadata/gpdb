#include "postgres.h"

#include "access/xact.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_extension.h"
#include "catalog/indexing.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "libpq-fe.h"
#include "postmaster/bgworker.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#include "arenadata_toolkit_worker.h"
#include "arenadata_toolkit_guc.h"
#include "bloom_set.h"
#include "tf_shmem.h"

#define TOOLKIT_BINARY_NAME "arenadata_toolkit"

typedef struct
{
	Oid			dbid;
	Name		dbname;
	bool		get_full_snapshot_on_recovery;
}	tracked_db_t;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

void arenadata_toolkit_main(Datum);

/* parse array of GUCs, find desired and analyze it */
static bool
is_db_tracked(ArrayType *array)
{
	bool		is_tracked = false;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;

	deconstruct_array(array, TEXTOID, -1, false, 'i',
					  &elems, &nulls, &nelems);
	for (int i = 0; i < nelems; i++)
	{
		char	   *s;
		char	   *name;
		char	   *value;

		if (nulls[i])
			continue;

		s = TextDatumGetCString(elems[i]);
		ParseLongOption(s, &name, &value);

		if (!value)
		{
			free(name);
			continue;
		}

		if (strcmp(name, "arenadata_toolkit.tracking_is_db_tracked") == 0 &&
			strcmp(value, "t") == 0)
		{
			is_tracked = true;
			break;
		}

		free(name);
		if (value)
			free(value);
		pfree(s);
	}

	return is_tracked;
}

static bool
full_snapshot_on_recovery(ArrayType *array)
{
	bool		take_snapshot = false;
	bool		found = false;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;

	deconstruct_array(array, TEXTOID, -1, false, 'i',
					  &elems, &nulls, &nelems);

	for (int i = 0; i < nelems; i++)
	{
		char	   *s;
		char	   *name;
		char	   *value;

		if (nulls[i])
			continue;

		s = TextDatumGetCString(elems[i]);
		ParseLongOption(s, &name, &value);

		if (!value)
		{
			free(name);
			continue;
		}

		if (strcmp(name, "arenadata_toolkit.tracking_snapshot_on_recovery") == 0)
		{
			found = true;
			if (strcmp(value, "t") == 0)
				take_snapshot = true;
			break;
		}

		free(name);
		if (value)
			free(value);
		pfree(s);
	}

	if (!found)
		take_snapshot = get_full_snapshot_on_recovery;

	return take_snapshot;
}

/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake
 * it up.
 */
static void
tracking_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 * Set a flag to tell the main loop to reread the config file, and set
 * our latch to wake it up.
 */
static void
tracking_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static List*
get_tracked_dbs()
{
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tup;
	List		*tracked_dbs = NIL;
	tracked_db_t *trackedDb;

	rel = heap_open(DbRoleSettingRelationId, AccessShareLock);
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		bool		isnull;
		Datum		str_datum;
		Datum		oid_datum;
		ArrayType  *a;

		str_datum = heap_getattr(tup, Anum_pg_db_role_setting_setconfig,
								 RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;

		oid_datum = heap_getattr(tup, Anum_pg_db_role_setting_setrole,
								 RelationGetDescr(rel), &isnull);
		if (DatumGetObjectId(oid_datum) != InvalidOid)
			continue;

		oid_datum = heap_getattr(tup, Anum_pg_db_role_setting_setdatabase,
								 RelationGetDescr(rel), &isnull);
		if (DatumGetObjectId(oid_datum) == InvalidOid)
			continue;

		a = DatumGetArrayTypeP(str_datum);

		if (is_db_tracked(a))
		{
			trackedDb = (tracked_db_t *) palloc0(sizeof(tracked_db_t));

			trackedDb->dbid = DatumGetObjectId(oid_datum);
			trackedDb->get_full_snapshot_on_recovery = full_snapshot_on_recovery(a);
			tracked_dbs = lappend(tracked_dbs, trackedDb);
		}
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return tracked_dbs;
}

static void
track_dbs(List *tracked_dbs)
{
	ListCell   *cell;
	tracked_db_t *trackedDb;

	foreach(cell, tracked_dbs)
	{
		trackedDb = (tracked_db_t *) lfirst(cell);

		bloom_set_bind(&tf_shared_state->bloom_set, trackedDb->dbid);
		bloom_set_trigger_bits(&tf_shared_state->bloom_set, trackedDb->dbid,
							   trackedDb->get_full_snapshot_on_recovery);
	}
}

static void
worker_tracking_status_check()
{
	List	   *tracked_dbs = NIL;

	StartTransactionCommand();
	tracked_dbs = get_tracked_dbs();

	if (pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_is_initialized) && list_length(tracked_dbs) > 0)
	{
		track_dbs(tracked_dbs);

		pg_atomic_test_set_flag(&tf_shared_state->tracking_is_initialized);
	}

	/*
	 * Here is quite a dump check, which imitates consistency validation.
	 * Written as an example of segment erroneous tracking status.
	 */
	if (list_length(tracked_dbs) != bloom_set_count(&tf_shared_state->bloom_set))
	{
		if (pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_error))
			pg_atomic_test_set_flag(&tf_shared_state->tracking_error);
	}

	if (tracked_dbs)
		list_free_deep(tracked_dbs);

	CommitTransactionCommand();
}

/* scan pg_db_role_setting, find all databases, bind blooms if necessary */
void
arenadata_toolkit_main(Datum main_arg)
{
	instr_time current_time_timeout;
	instr_time start_time_timeout;
	long current_timeout = -1;

	elog(LOG, "[arenadata toolkit] Starting background worker");

	/*
	 * The worker shouldn't exist when the master boots in utility mode.
	 * Otherwise BackgroundWorkerInitializeConnection will explode with FATAL.
	 */
	if(IS_QUERY_DISPATCHER() && Gp_role != GP_ROLE_DISPATCH)
	{
		proc_exit(0);
	}

	/*
	 * Kludge for scanning pg_db_role_setting on segments.
	 */
	if (!IS_QUERY_DISPATCHER() && Gp_role == GP_ROLE_DISPATCH)
	{
		Gp_role = GP_ROLE_UTILITY;
		Gp_session_role = GP_ROLE_UTILITY;
	}

	pqsignal(SIGHUP, tracking_sighup);
	pqsignal(SIGTERM, tracking_sigterm);

	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(DB_FOR_COMMON_ACCESS, NULL);

	while (!got_sigterm)
	{
		int			rc;
		long timeout = tracking_worker_naptime_sec * 1000;

		if (current_timeout <= 0)
			INSTR_TIME_SET_CURRENT(start_time_timeout);

		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   timeout);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(&MyProc->procLatch);
			CHECK_FOR_INTERRUPTS();
		}

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(LOG, (errmsg("[arenadata toolkit] bgworker is being terminated by postmaster death.")));
			proc_exit(1);
		}

		if (got_sighup)
		{
			elog(DEBUG1, "[arenadata_tookit] got sighup");
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		INSTR_TIME_SET_CURRENT(current_time_timeout);
		INSTR_TIME_SUBTRACT(current_time_timeout, start_time_timeout);
		current_timeout = timeout - (long) INSTR_TIME_GET_MILLISEC(current_time_timeout);
		if (current_timeout <= 0) worker_tracking_status_check();
	}

	if (got_sigterm)
		ereport(LOG, (errmsg("[arenadata toolkit] stop worker process")));

	proc_exit(0);
}

void
arenadata_toolkit_worker_register()
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(BackgroundWorker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, TOOLKIT_BINARY_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "arenadata_toolkit_main");
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "arenadata_toolkit");

	RegisterBackgroundWorker(&worker);
}
