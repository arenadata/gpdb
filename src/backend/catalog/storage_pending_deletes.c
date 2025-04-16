/*-------------------------------------------------------------------------
 *
 * storage_pending_deletes.c
 *	  code to support collecting of pending deletes from backends
 *
 * Copyright (c) 2025 Greengage Community
 *
 *	  src/backend/catalog/storage_pending_deletes.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/storage_pending_deletes.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/guc.h"

typedef struct PendingDeleteListNode
{
	PendingRelXactDelete xrelnode;
	dsa_pointer next;
	dsa_pointer prev;
}	PendingDeleteListNode;

typedef struct PendingDeletesList
{
	LWLock	   *lock;			/* protects the list */
	dsa_pointer head;			/* ptr to PendingDeleteListNode list head */
}	PendingDeletesList;

typedef struct BackendsPendingDeletesArray
{
	PendingDeletesList *list;
	char		dsa_mem[FLEXIBLE_ARRAY_MEMBER];
}	BackendsPendingDeletesArray;

static BackendsPendingDeletesArray *BackendsPendingDeletes = NULL;
static dsa_area *pendingDeletesDsa = NULL;		/* ptr to DSA area attached by
												 * current process */

static inline bool
is_tracking_enabled()
{
	return !IsBootstrapProcessingMode() &&
		gp_track_pending_delete &&
		dynamic_shared_memory_type != DSM_IMPL_NONE;
}

/*
 * Calculate shmem size for pending deletes.
 * BackendsPendingDeletesArray.dsa_mem should fit DSA.
 */
Size
PdlShmemSize(void)
{
	if (!gp_track_pending_delete)
		return 0;

	Size		size = add_size(offsetof(BackendsPendingDeletesArray, dsa_mem),
								dsa_minimum_size());

	return add_size(size, mul_size(sizeof(PendingDeletesList), MaxBackends));
}

/* Initialize shared memory pending delete lists for all backends */
void
PdlShmemInit(void)
{
	if (!is_tracking_enabled())
		return;

	bool		found;

	BackendsPendingDeletes = (BackendsPendingDeletesArray *)
		ShmemInitStruct("Pending deletes array",
						add_size(offsetof(BackendsPendingDeletesArray, dsa_mem),
								 dsa_minimum_size()),
						&found);
	if (found)
		return;

	const Size	pdl_size = mul_size(sizeof(PendingDeletesList), MaxBackends);

	BackendsPendingDeletes->list = (PendingDeletesList *) ShmemAlloc(pdl_size);
	for (int i = 0; i < MaxBackends; i++)
	{
		BackendsPendingDeletes->list[i] = (PendingDeletesList)
		{
			.head = InvalidDsaPointer,
			.lock = LWLockAssign()
		};
	}

	dsa_area   *dsa = dsa_create_in_place(
						 BackendsPendingDeletes->dsa_mem, dsa_minimum_size(),
						 LWLockNewTrancheId(), "storage_pending_deletes", NULL);

	on_shmem_exit(dsa_on_shmem_exit_release_in_place,
				  (Datum) BackendsPendingDeletes->dsa_mem);
	dsa_detach(dsa);
}

/*
 * Cleanup pending deletes list.
 * When the function is called, the list should be empty
 */
static void
pdl_beshutdown_hook(int code, Datum arg)
{
	dsa_release_in_place(BackendsPendingDeletes->dsa_mem);

	if (MyBackendId == InvalidBackendId)
		return;

	PendingDeletesList *list = &BackendsPendingDeletes->list[MyBackendId];

	if (!DsaPointerIsValid(list->head))
		return;

	/* Assert on debug build and warning on release */
	Assert(false);
	ereport(WARNING,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Pending deletes list is not empty. "
					"MyBackend: %d, MyProcPid: %d", MyBackendId, MyProcPid)));
	list->head = InvalidDsaPointer;
}

/* Attach DSA once per process. */
static void
PdlAttachDsa(void)
{
	if (pendingDeletesDsa)
		return;

	/*
	 * Keep the DSA area ptr in TopMemoryContext to avoid excessive
	 * attach/detach at every add/remove
	 */
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	pendingDeletesDsa = dsa_attach_in_place(BackendsPendingDeletes->dsa_mem,
											NULL);
	MemoryContextSwitchTo(oldcxt);

	/* pin mappings, so they can survive res owner life end */
	dsa_pin_mapping(pendingDeletesDsa);

	on_shmem_exit(pdl_beshutdown_hook, 0);
}

/*
 * Add pending delete node to the list of current backend.
 * Return DSA ptr of a created node. This ptr can be passed to PdlShmemRemove.
 */
dsa_pointer
PdlShmemAdd(const RelFileNodePendingDelete * relnode, TransactionId xid)
{
	if (!is_tracking_enabled() || xid == InvalidTransactionId ||
		MyBackendId == InvalidBackendId)
		return InvalidDsaPointer;

	PdlAttachDsa();

	PendingDeleteListNode *node;
	const dsa_pointer node_dsa = dsa_allocate(pendingDeletesDsa,
											  sizeof(*node));

	node = dsa_get_address(pendingDeletesDsa, node_dsa);
	*node = (PendingDeleteListNode)
	{
		.xrelnode =
		{
			.relnode = *relnode,
			.xid = xid
		},
		.prev = InvalidDsaPointer
	};

	PendingDeletesList *list = &BackendsPendingDeletes->list[MyBackendId];

	LWLockAcquire(list->lock, LW_EXCLUSIVE);
	node->next = list->head;
	if (DsaPointerIsValid(node->next))
	{
		PendingDeleteListNode *next_node = (PendingDeleteListNode *)
			dsa_get_address(pendingDeletesDsa, node->next);

		next_node->prev = node_dsa;
	}
	list->head = node_dsa;
	LWLockRelease(list->lock);

	return node_dsa;
}

/*
 * Remove pending delete node from the list of current backend.
 * node_ptr is a ptr to already added node (see PdlShmemAdd)
 */
void
PdlShmemRemove(dsa_pointer node_ptr)
{
	if (!is_tracking_enabled() || MyBackendId == InvalidBackendId)
		return;

	Assert(DsaPointerIsValid(node_ptr));

	PendingDeletesList *list = &BackendsPendingDeletes->list[MyBackendId];
	const PendingDeleteListNode *node =
		dsa_get_address(pendingDeletesDsa, node_ptr);

	LWLockAcquire(list->lock, LW_EXCLUSIVE);
	if (DsaPointerIsValid(node->next))
	{
		PendingDeleteListNode *next_node = dsa_get_address(pendingDeletesDsa,
														   node->next);

		next_node->prev = node->prev;
	}

	if (DsaPointerIsValid(node->prev))
	{
		PendingDeleteListNode *prev_node = dsa_get_address(pendingDeletesDsa,
														   node->prev);

		prev_node->next = node->next;
	}
	else
		list->head = node->next;

	LWLockRelease(list->lock);

	dsa_free(pendingDeletesDsa, node_ptr);
}

/*
 * Collect info about pending deletes from all backends and return
 * the accumulated result. Return NULL if there are no nodes in the lists.
 * Note: the returned result is palloc'ed. Caller is responsible for
 * freeing it.
 */
PendingRelXactDeleteArray *
PdlXLogShmemDump(void)
{
	PdlAttachDsa();

	PendingRelXactDeleteArray *ret = NULL;

	Size		size = offsetof(PendingRelXactDeleteArray, array);

	for (int i = 0; i < MaxBackends; i++)
	{
		PendingDeletesList *list = &BackendsPendingDeletes->list[i];

		LWLockAcquire(list->lock, LW_SHARED);

		for (dsa_pointer pdl_node_dsa = list->head;
			 DsaPointerIsValid(pdl_node_dsa);)
		{
			const PendingDeleteListNode *pdl_node =
				dsa_get_address(pendingDeletesDsa, pdl_node_dsa);

			size += sizeof(*ret->array);
			if (ret != NULL)
				ret = repalloc(ret, size);
			else
			{
				ret = palloc(size);
				ret->count = 0;
			}

			ret->array[ret->count++] = pdl_node->xrelnode;
			pdl_node_dsa = pdl_node->next;
		}

		LWLockRelease(list->lock);
	}

	return ret;
}
