#include "postgres.h"

#include "access/xact.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_extension.h"
#include "catalog/indexing.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "libpq-fe.h"
#include "postmaster/bgworker.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#include "arenadata_toolkit_worker.h"
#include "arenadata_toolkit_guc.h"
#include "tf_shmem.h"

#define EXTENSIONNAME "arenadata_toolkit"

typedef struct
{
	Oid			dbid;
	bool		get_full_snapshot_on_recovery;
}	tracked_db_t;

static BackgroundWorker worker;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

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

static List *
get_uninitialized_segments()
{
	int			i;
	CdbPgResults cdb_pgresults = {NULL, 0};
	List	   *list = NIL;

	CdbDispatchCommand("select * from arenadata_toolkit.tracking_is_segment_initialized()", 0, &cdb_pgresults);

	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			elog(ERROR, "is_initialized: resultStatus not tuples_Ok: %s %s",
				 PQresStatus(PQresultStatus(pgresult)), PQresultErrorMessage(pgresult));
		}
		else
		{
			int32		segindex = 0;
			bool		is_initialized = false;

			segindex = atoi(PQgetvalue(pgresult, 0, 0));
			is_initialized = strcmp(PQgetvalue(pgresult, 0, 1), "t") == 0;

			elog(LOG, "get_uninitialized_segments, segindex: %d, is_initialized: %d", segindex, is_initialized);

			if (!is_initialized)
				list = lappend_int(list, segindex);
		}
	}

	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return list;
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

/*
 * Signal handler for SIGUSR1
 * Set a flag to tell the launcher to handle extension ddl message
 */
static void
tracking_sigusr1(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigusr1 = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static bool
extension_created()
{
	bool		exists = false;
	Relation	rel;
	SysScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];

	rel = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(EXTENSIONNAME));

	scandesc = systable_beginscan(rel, ExtensionNameIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	exists = HeapTupleIsValid(tuple);

	systable_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	return exists;
}

static void
dispatch_register_to_master(List *dbids)
{
	ListCell   *cell;
	tracked_db_t *trackedDb;

	foreach(cell, dbids)
	{
		trackedDb = (tracked_db_t *) lfirst(cell);

		bloom_set_bind(&tf_shared_state->bloom_set, trackedDb->dbid);
		bloom_set_trigger_bits(&tf_shared_state->bloom_set, trackedDb->dbid,
							   trackedDb->get_full_snapshot_on_recovery);
	}

	LWLockAcquire(tf_shared_state->bloom_set.lock, LW_EXCLUSIVE);
	tf_shared_state->is_initialized = true;
	LWLockRelease(tf_shared_state->bloom_set.lock);
}

static void
dispatch_register_to_segments(List *dbids, List *uninitialized_segments)
{
	ListCell   *cell;
	tracked_db_t *trackedDb;
	CdbPgResults cdb_pgresults = {NULL, 0};

	if (uninitialized_segments == NIL)
		return;

	foreach(cell, dbids)
	{
		trackedDb = (tracked_db_t *) lfirst(cell);

		char	   *cmd = psprintf("select arenadata_toolkit.tracking_register_db(%u)", trackedDb->dbid);

		CdbDispatchCommandToSegments(cmd,
									 0,
									 uninitialized_segments,
									 &cdb_pgresults);

		if (trackedDb->get_full_snapshot_on_recovery)
		{
			cmd = psprintf("select arenadata_toolkit.tracking_trigger_initial_snapshot(%u)", trackedDb->dbid);

			CdbDispatchCommandToSegments(cmd,
										 0,
										 uninitialized_segments,
										 &cdb_pgresults);
		}
	}
}

static void
dispatch_register(bool dispatch_to_master, List *uninitialized_segments)
{
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tup;
	List	   *dbids = NIL;
	tracked_db_t *trackedDb;

	rel = heap_open(DbRoleSettingRelationId, RowExclusiveLock);
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
			dbids = lappend(dbids, trackedDb);
		}
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	if (dbids != NIL)
	{
		ListCell   *cell;

		if (dispatch_to_master)
			dispatch_register_to_master(dbids);

		dispatch_register_to_segments(dbids, uninitialized_segments);

		foreach(cell, dbids)
		{
			pfree(lfirst(cell));
		}

		list_free(dbids);
	}

	LWLockAcquire(tf_shared_state->bloom_set.lock, LW_EXCLUSIVE);
	tf_shared_state->bgworker_ready = true;
	LWLockRelease(tf_shared_state->bloom_set.lock);
}

/* scan pg_db_role_setting, find all databases, bind blooms if necessary */
static void
arenadata_toolkit_worker(Datum main_arg)
{
	elog(LOG, "[arenadata toolkit] Starting background worker");

	bool		master_initialized = false;

	pqsignal(SIGHUP, tracking_sighup);
	pqsignal(SIGTERM, tracking_sigterm);
	pqsignal(SIGUSR1, tracking_sigusr1);

	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(DB_FOR_COMMON_ACCESS, NULL);

	while (!got_sigterm)
	{
		int			rc;
		List	   *uninitialized_segments = NIL;

		CHECK_FOR_INTERRUPTS();

		StartTransactionCommand();

		if (extension_created())
		{
			elog(LOG, "[arenadata toolkit] Getting uninitialized segments");
			uninitialized_segments = get_uninitialized_segments(uninitialized_segments);

			if (!master_initialized || list_length(uninitialized_segments) > 0)
			{
				elog(LOG, "Dispatching register to segments");
				dispatch_register(!master_initialized, uninitialized_segments);
				list_free(uninitialized_segments);
				uninitialized_segments = NIL;
				master_initialized = true;
			}
		}
		CommitTransactionCommand();

		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   tracking_worker_naptime_sec * 1000);
		ResetLatch(&MyProc->procLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(LOG, (errmsg("[arenadata toolkit] bgworker is being terminated by postmaster death.")));
			proc_exit(1);
		}

		if (got_sighup)
		{
			got_sighup = false;
		}
	}

	if (got_sigterm)
		ereport(LOG, (errmsg("[arenadata toolkit] stop worker process")));

	proc_exit(0);
}

void
arenadata_toolkit_worker_register(void)
{
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	worker.bgw_main = arenadata_toolkit_worker;
	worker.bgw_notify_pid = 0;
	worker.bgw_start_rule = NULL;
	sprintf(worker.bgw_name, "arenadata_toolkit");

	RegisterBackgroundWorker(&worker);
}
