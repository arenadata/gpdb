/*-------------------------------------------------------------------------
 *
 * ipci.c
 *	  POSTGRES inter-process communication initialization code.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/ipci.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/distributedlog.h"
#include "catalog/storage_pending.h"
#include "cdb/cdblocaldistribxact.h"
#include "cdb/cdbvars.h"
#include "commands/async.h"
#include "executor/nodeShareInputScan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/fts.h"
#include "replication/logicallauncher.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/origin.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "utils/backend_cancel.h"
#include "utils/resource_manager.h"
#include "utils/faultinjector.h"
#include "utils/sharedsnapshot.h"
#include "utils/gpexpand.h"
#include "utils/snapmgr.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbtm.h"
#include "postmaster/backoff.h"
#include "cdb/memquota.h"
#include "executor/instrument.h"
#include "executor/spi.h"
#include "utils/workfile_mgr.h"
#include "utils/session_state.h"
#include "cdb/cdbendpoint.h"
#include "replication/gp_replication.h"
#include "cdb/ic_proxy_bgworker.h"

/* GUCs */
int			shared_memory_type = DEFAULT_SHARED_MEMORY_TYPE;

shmem_startup_hook_type shmem_startup_hook = NULL;

static Size total_addin_request = 0;
static bool addin_request_allowed = true;


/*
 * RequestAddinShmemSpace
 *		Request that extra shmem space be allocated for use by
 *		a loadable module.
 *
 * This is only useful if called from the _PG_init hook of a library that
 * is loaded into the postmaster via shared_preload_libraries.  Once
 * shared memory has been allocated, calls will be ignored.  (We could
 * raise an error, but it seems better to make it a no-op, so that
 * libraries containing such calls can be reloaded if needed.)
 */
void
RequestAddinShmemSpace(Size size)
{
	if (IsUnderPostmaster || !addin_request_allowed)
		return;					/* too late */
	total_addin_request = add_size(total_addin_request, size);
}

LFL_PDL_ShmemArrayStruct * LFL_PDL_ShmemArray = NULL;

static dsa_area *
LFL_PDL_AttachDsa()
{
	MemoryContext oldcxt;

	static dsa_area *pendingDeleteDsa = NULL;	/* ptr to DSA area attached by
											 * current process */

	if (pendingDeleteDsa)
		return pendingDeleteDsa;

	/*
	 * Keep the DSA area ptr in TopMemoryContext to avoid excessive
	 * attach/detach at every add/remove
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	pendingDeleteDsa = dsa_attach_in_place(LFL_PDL_ShmemArray->dsa_mem, NULL);
	MemoryContextSwitchTo(oldcxt);

	/* pin mappings, so they can survive res owner life end */
	dsa_pin_mapping(pendingDeleteDsa);
	/* Set up a process-exit hook to clean up */
	on_shmem_exit(dsa_on_shmem_exit_release_in_place, (Datum) LFL_PDL_ShmemArray->dsa_mem);

	elog(DEBUG3, "Pending delete DSA attached");

	return pendingDeleteDsa;
}

static Size
LFL_PDL_ShmemSize(void)
{
	Size		size;

	/* dsa initialized over flexible static dsa_mem */
	size = offsetof(LFL_PDL_ShmemArrayStruct, dsa_mem);

	/* dsa initialized over flexible static dsa_mem */
	size = add_size(size, dsa_minimum_size());

	return size;
}

/*
 * Initialize pending delete shmem struct.
 */
static void
LFL_PDL_ShmemInit(void)
{
	Size		size;
	bool		found;
	int 		i;

	size = LFL_PDL_ShmemSize();

	LFL_PDL_ShmemArray = (LFL_PDL_ShmemArrayStruct *)
			ShmemInitStruct("LFL Pending Deletes Array",
							size,
							&found);

	if (!found)
	{
		dsa_area *dsa = dsa_create_in_place(
				LFL_PDL_ShmemArray->dsa_mem,
				dsa_minimum_size(),
				LWTRANCHE_PENDING_DELETE_DSA_LFL,
				NULL
		);

		LFL_PDL_ShmemArray->lock_free_list_array =
				(lock_free_list *) ShmemAlloc(MaxBackends * sizeof(lock_free_list));

		MemSet(LFL_PDL_ShmemArray->lock_free_list_array, 0,
			   MaxBackends * sizeof(lock_free_list));

		for (i = 0; i < MaxBackends; i++) {
			lock_free_list_init(&LFL_PDL_ShmemArray->lock_free_list_array[i],
								&LFL_PDL_AttachDsa);
		}

		/*
		 * segments will be released by dsm_postmaster_shutdown(), but keep it
		 * clean anyway
		 */
		on_shmem_exit(dsa_on_shmem_exit_release_in_place, (Datum) LFL_PDL_ShmemArray->dsa_mem);

		/*
		 * we don't need dsa ptr here, all future dsa calls will be in
		 * backends
		 */
		dsa_detach(dsa);

		elog(DEBUG3, "LFL Pending delete shared memory initialized.");
	}
}

void PdlLflAttachToBackend()
{
	elog(LOG, "[RELOG] PdlLflAttachToBackend backend ID %d", MyBackendId);
	lock_free_list_attach_to_writer(&LFL_PDL_ShmemArray->lock_free_list_array[MyBackendId]);
}


/*
 * ===============================================
 * Draft implementation of list with locks.
 * Functions below are used only for comparison testing of performance.
 * ===============================================
 */

/* A struct to track pending deletes. Placed in static shared memory area. */
typedef struct OLD_PDL_ShmemStruct
{
	dsa_pointer pdl_head;		/* ptr to list head of OLD_PDL_ListNode */
	size_t		pdl_count;		/* count of OLD_PDL_ListNode nodes */
	char		dsa_mem[FLEXIBLE_ARRAY_MEMBER]; /* a minimal memory area which
												 * can be used for dsa
												 * initialization */
}			OLD_PDL_ShmemStruct;

/*
 * Shared pending delete list node.
 * Doubly linked list provides O(1) remove.
 */
typedef struct OLD_PDL_ListNode
{
	void *value;
	dsa_pointer next;
	dsa_pointer prev;
}			OLD_PDL_ListNode;

static OLD_PDL_ShmemStruct * OLD_PDL_Shmem = NULL;	/* shared pending delete
																 * state  */

dsa_pointer
OLD_PDL_LinkNode(void * value);

void
OLD_PDL_UnlinkNode(dsa_pointer cur);

/*
 * Calculate size for pending delete shmem.
 * The flexible array member should fit DSA.
 */
static Size
OLD_PDL_ShmemSize(void)
{
	Size		size;

	size = offsetof(OLD_PDL_ShmemStruct, dsa_mem);
	/* dsa initialized over flexible static dsa_mem */
	size = add_size(size, dsa_minimum_size());

	return size;
}

static void
OLD_PDL_ShmemInit(void)
{
	Size		size = OLD_PDL_ShmemSize();
	bool		found;

	OLD_PDL_Shmem = (OLD_PDL_ShmemStruct *)
		ShmemInitStruct("OLD_PDL_Shmem",
						size,
						&found);

	if (!found)
	{
		dsa_area   *dsa = dsa_create_in_place(OLD_PDL_Shmem->dsa_mem,
											  dsa_minimum_size(),
											  LWTRANCHE_PENDING_DELETE_DSA_OLD_PDL,
											  NULL);

		/*
		 * we can't allocate memory segments inside postmaster, so list will
		 * be initialized at runtime
		 */
		elog(LOG, "OLD PDL shared memory initialized.");

		/*
		 * segments will be released by dsm_postmaster_shutdown(), but keep it
		 * clean anyway
		 */
		on_shmem_exit(dsa_on_shmem_exit_release_in_place, (Datum) OLD_PDL_Shmem->dsa_mem);

		/*
		 * we don't need dsa ptr here, all future dsa calls will be in
		 * backends
		 */
		dsa_detach(dsa);
	}
}

/*
 * Attach dsa once per process.
 */
static dsa_area *
OLD_PDL_AttachDsa(void)
{
	MemoryContext oldcxt;

	static dsa_area *pendingDeleteDsa = NULL;	/* ptr to DSA area attached by
											 * current process */

	if (pendingDeleteDsa)
		return pendingDeleteDsa;

	/*
	 * Keep the DSA area ptr in TopMemoryContext to avoid excessive
	 * attach/detach at every add/remove
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	pendingDeleteDsa = dsa_attach_in_place(OLD_PDL_Shmem->dsa_mem, NULL);
	MemoryContextSwitchTo(oldcxt);

	/* pin mappings, so they can survive res owner life end */
	dsa_pin_mapping(pendingDeleteDsa);
	/* disconnect from dsa on shmem exit */
	on_shmem_exit(dsa_on_shmem_exit_release_in_place, (Datum) OLD_PDL_Shmem->dsa_mem);

	elog(DEBUG3, "Pending delete DSA attached");

	return pendingDeleteDsa;
}

/*
 * Prepend shared list with new pending delete node.
 */
dsa_pointer
OLD_PDL_LinkNode(void * value)
{
	dsa_area *dsa = OLD_PDL_AttachDsa();
	dsa_pointer cur = dsa_allocate(dsa, sizeof(OLD_PDL_ListNode));

	dsa_pointer head;
	OLD_PDL_ListNode *cur_node;

	cur_node = (OLD_PDL_ListNode *) dsa_get_address(dsa, cur);

	cur_node->value = value;

	LWLockAcquire(PendingDeleteLock, LW_EXCLUSIVE);

	head = OLD_PDL_Shmem->pdl_head;
	cur_node->next = head;
	cur_node->prev = InvalidDsaPointer;
	if (DsaPointerIsValid(head))
	{
		OLD_PDL_ListNode *head_node = (OLD_PDL_ListNode *) dsa_get_address(dsa, head);

		head_node->prev = cur;
	}
	OLD_PDL_Shmem->pdl_head = cur;
	OLD_PDL_Shmem->pdl_count++;

	LWLockRelease(PendingDeleteLock);

	elog(DEBUG2, "Pending delete rel added to shmem.");

	return cur;
}

/*
 * Remove pending delete node from shared list
 * cur - ptr to node which is already linked to list
 */
void
OLD_PDL_UnlinkNode(dsa_pointer cur)
{
	dsa_area *dsa = OLD_PDL_AttachDsa();
	dsa_pointer head;
	OLD_PDL_ListNode *cur_node;

	cur_node = dsa_get_address(dsa, cur);

	LWLockAcquire(PendingDeleteLock, LW_EXCLUSIVE);

	head = OLD_PDL_Shmem->pdl_head;

	if (DsaPointerIsValid(cur_node->next))
	{
		OLD_PDL_ListNode *next_node = dsa_get_address(dsa, cur_node->next);

		next_node->prev = cur_node->prev;
	}

	if (DsaPointerIsValid(cur_node->prev))
	{
		OLD_PDL_ListNode *prev_node = dsa_get_address(dsa, cur_node->prev);

		prev_node->next = cur_node->next;
	}

	if (cur == head)
		OLD_PDL_Shmem->pdl_head = cur_node->next;

	OLD_PDL_Shmem->pdl_count--;

	LWLockRelease(PendingDeleteLock);

	dsa_free(dsa, cur);

	elog(DEBUG2, "Pending delete rel removed from shmem.");
}


/*
 * ===============================================
 * End.
 * ===============================================
 */


/*
 * CreateSharedMemoryAndSemaphores
 *		Creates and initializes shared memory and semaphores.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 */
void
CreateSharedMemoryAndSemaphores(int port)
{
	PGShmemHeader *shim = NULL;

	if (!IsUnderPostmaster)
	{
		PGShmemHeader *seghdr;
		Size		size;
		int			numSemas;

		/* Compute number of semaphores we'll need */
		numSemas = ProcGlobalSemas();
		numSemas += SpinlockSemas();

        elog(DEBUG3,"reserving %d semaphores",numSemas);
		/*
		 * Size of the Postgres shared-memory block is estimated via
		 * moderately-accurate estimates for the big hogs, plus 100K for the
		 * stuff that's too small to bother with estimating.
		 *
		 * We take some care during this phase to ensure that the total size
		 * request doesn't overflow size_t.  If this gets through, we don't
		 * need to be so careful during the actual allocation phase.
		 */
		size = 150000;
		size = add_size(size, PGSemaphoreShmemSize(numSemas));
		size = add_size(size, SpinlockSemaSize());
		size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
												 sizeof(ShmemIndexEnt)));
		size = add_size(size, BufferShmemSize());
		size = add_size(size, LockShmemSize());
		size = add_size(size, PredicateLockShmemSize());

		if (IsResQueueEnabled() && Gp_role == GP_ROLE_DISPATCH)
		{
			size = add_size(size, ResSchedulerShmemSize());
			size = add_size(size, ResPortalIncrementShmemSize());
		}
		else if (IsResGroupEnabled())
			size = add_size(size, ResGroupShmemSize());
		size = add_size(size, SharedSnapshotShmemSize());
		if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
			size = add_size(size, FtsShmemSize());

		/* size of pending delete nodes struct */
		size = add_size(size, OLD_PDL_ShmemSize());

		/* size of pending delete nodes struct */
		size = add_size(size, LFL_PDL_ShmemSize());

		/* keep it before call to LWLockShmemSize() */
		size = add_size(size, PdlShmemSize());

		size = add_size(size, ProcGlobalShmemSize());
		size = add_size(size, XLOGShmemSize());
		size = add_size(size, DistributedLog_ShmemSize());
		size = add_size(size, CLOGShmemSize());
		size = add_size(size, CommitTsShmemSize());
		size = add_size(size, SUBTRANSShmemSize());
		size = add_size(size, TwoPhaseShmemSize());
		size = add_size(size, BackgroundWorkerShmemSize());
		size = add_size(size, MultiXactShmemSize());
		size = add_size(size, LWLockShmemSize());
		size = add_size(size, ProcArrayShmemSize());
		size = add_size(size, BackendStatusShmemSize());
		size = add_size(size, SInvalShmemSize());
		size = add_size(size, PMSignalShmemSize());
		size = add_size(size, ProcSignalShmemSize());
		size = add_size(size, CheckpointerShmemSize());
		size = add_size(size, AutoVacuumShmemSize());
		size = add_size(size, ReplicationSlotsShmemSize());
		size = add_size(size, ReplicationOriginShmemSize());
		size = add_size(size, WalSndShmemSize());
		size = add_size(size, WalRcvShmemSize());
		size = add_size(size, PgArchShmemSize());
		size = add_size(size, ApplyLauncherShmemSize());
		size = add_size(size, FTSReplicationStatusShmemSize());
		size = add_size(size, SnapMgrShmemSize());
		size = add_size(size, BTreeShmemSize());
		size = add_size(size, SyncScanShmemSize());
		size = add_size(size, AsyncShmemSize());
#ifdef EXEC_BACKEND
		size = add_size(size, ShmemBackendArraySize());
#endif

		size = add_size(size, tmShmemSize());
		size = add_size(size, CheckpointerShmemSize());
		size = add_size(size, CancelBackendMsgShmemSize());
		size = add_size(size, WorkFileShmemSize());
		size = add_size(size, ShareInputShmemSize());

#ifdef FAULT_INJECTOR
		size = add_size(size, FaultInjector_ShmemSize());
#endif			

#ifdef ENABLE_IC_PROXY
		size = add_size(size, ICProxyShmemSize());
#endif

		/* This elog happens before we know the name of the log file we are supposed to use */
		elog(DEBUG1, "Size not including the buffer pool %lu",
			 (unsigned long) size);

		/* freeze the addin request size and include it */
		addin_request_allowed = false;
		size = add_size(size, total_addin_request);

		/* might as well round it off to a multiple of a typical page size */
		size = add_size(size, BLCKSZ - (size % BLCKSZ));

		/* Consider the size of the SessionState array */
		size = add_size(size, SessionState_ShmemSize());

		/* size of Instrumentation slots */
		size = add_size(size, InstrShmemSize());

		/* size of expand version */
		size = add_size(size, GpExpandVersionShmemSize());

		/* size of token and endpoint shared memory */
		size = add_size(size, EndpointShmemSize());

		/* size of parallel cursor count */
		size = add_size(size, ParallelCursorCountSize());

		elog(DEBUG3, "invoking IpcMemoryCreate(size=%zu)", size);

		/*
		 * Create the shmem segment
		 */
		seghdr = PGSharedMemoryCreate(size, port, &shim);

		InitShmemAccess(seghdr);

		/*
		 * Create semaphores
		 */
		PGReserveSemaphores(numSemas, port);

		/*
		 * If spinlocks are disabled, initialize emulation layer (which
		 * depends on semaphores, so the order is important here).
		 */
#ifndef HAVE_SPINLOCKS
		SpinlockSemaInit();
#endif
	}
	else
	{
		/*
		 * We are reattaching to an existing shared memory segment. This
		 * should only be reached in the EXEC_BACKEND case.
		 */
#ifndef EXEC_BACKEND
		elog(PANIC, "should be attached to shared memory already");
#endif
	}

	/*
	 * Set up shared memory allocation mechanism
	 */
	if (!IsUnderPostmaster)
		InitShmemAllocation();

	/*
	 * Now initialize LWLocks, which do shared memory allocation and are
	 * needed for InitShmemIndex.
	 */
	CreateLWLocks();

	/*
	 * Set up shmem.c index hashtable
	 */
	InitShmemIndex();

	/*
	 * Set up xlog, clog, and buffers
	 */
	XLOGShmemInit();
	CLOGShmemInit();
	DistributedLog_ShmemInit();
	CommitTsShmemInit();
	SUBTRANSShmemInit();
	MultiXactShmemInit();
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
		FtsShmemInit();
	tmShmemInit();
	InitBufferPool();

	/*
	 * Set up lock manager
	 */
	InitLocks();

	/*
	 * Set up predicate lock manager
	 */
	InitPredicateLocks();

	/*
	 * Set up resource manager 
	 */
	ResManagerShmemInit();

	/*
	 * Set up process table
	 */
	if (!IsUnderPostmaster)
		InitProcGlobal();

	/* Initialize SessionState shared memory array */
	SessionState_ShmemInit();
	/* Initialize vmem protection */
	GPMemoryProtect_ShmemInit();

	CreateSharedProcArray();
	CreateSharedBackendStatus();
	
	/*
	 * Set up Shared snapshot slots
	 *
	 * TODO: only need to do this if we aren't the QD. for now we are just 
	 *		 doing it all the time and wasting shemem on the QD.  This is 
	 *		 because this happens at postmaster startup time when we don't
	 *		 know who we are.  
	 */
	CreateSharedSnapshotArray();
	TwoPhaseShmemInit();
	BackgroundWorkerShmemInit();

	/*
	 * Set up shared-inval messaging
	 */
	CreateSharedInvalidationState();

	/*
	 * Set up interprocess signaling mechanisms
	 */
	PMSignalShmemInit();
	ProcSignalShmemInit();
	CheckpointerShmemInit();
	AutoVacuumShmemInit();
	ReplicationSlotsShmemInit();
	ReplicationOriginShmemInit();
	WalSndShmemInit();
	WalRcvShmemInit();
	PgArchShmemInit();
	ApplyLauncherShmemInit();
	FTSReplicationStatusShmemInit();

#ifdef FAULT_INJECTOR
	FaultInjector_ShmemInit();
#endif

#ifdef ENABLE_IC_PROXY
	ICProxyShmemInit();
#endif

	/*
	 * Set up other modules that need some shared memory space
	 */
	SnapMgrInit();
	BTreeShmemInit();
	SyncScanShmemInit();
	AsyncShmemInit();
	BackendCancelShmemInit();
	WorkFileShmemInit();
	ShareInputShmemInit();

	/*
	 * Set up Instrumentation free list
	 */
	if (!IsUnderPostmaster)
		InstrShmemInit();

	GpExpandVersionShmemInit();

#ifdef EXEC_BACKEND

	/*
	 * Alloc the win32 shared backend array
	 */
	if (!IsUnderPostmaster)
		ShmemBackendArrayAllocation();
#endif

	if (gp_enable_resqueue_priority)
		BackoffStateInit();

	/* Initialize dynamic shared memory facilities. */
	if (!IsUnderPostmaster)
		dsm_postmaster_startup(shim);

	/* Initialize shared memory for parallel retrieve cursor */
	if (!IsUnderPostmaster)
		EndpointShmemInit();
	
	if (Gp_role == GP_ROLE_DISPATCH)
		ParallelCursorCountInit();

	OLD_PDL_ShmemInit();
	LFL_PDL_ShmemInit();
	PdlShmemInit();

	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();
}