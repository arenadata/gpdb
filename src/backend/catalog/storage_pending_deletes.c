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
} PendingDeleteListNode;

typedef struct PendingDeletesList 
{
	LWLock *lock; /* protects the fields below */
	dsa_pointer head; /* ptr to list head of PendingDeleteListNode */
	Size count; /* count of PendingDeleteListNode nodes */
} PendingDeletesList;

typedef struct BackendsPendingDeletesArray
{
	PendingDeletesList 	*list;
	char				dsa_mem[FLEXIBLE_ARRAY_MEMBER];
} BackendsPendingDeletesArray;

static BackendsPendingDeletesArray *BackendsPendingDeletes = NULL;
static dsa_area *pendingDeletesDsa = NULL;	/* ptr to DSA area attached by
											 * current process */
static inline bool is_tracking_enabled()
{
	return !IsBootstrapProcessingMode() &&
		   gp_track_pending_delete &&
		   dynamic_shared_memory_type != DSM_IMPL_NONE;
}

static void PdlAttachDsa(void);

PendingRelXactDeleteArray *
PdlXLogShmemDump(Size *size)
{
	PdlAttachDsa();

	PendingRelXactDeleteArray *ret = NULL;

	*size = offsetof(PendingRelXactDeleteArray, array);
	for (int i = 0; i < MaxBackends; i++)
	{
		PendingDeletesList *list = &BackendsPendingDeletes->list[i];

		LWLockAcquire(list->lock, LW_SHARED);

		if (list->count > 0 && DsaPointerIsValid(list->head))
		{
			*size += sizeof(*ret->array) * list->count;
			if (ret != NULL)
				ret = repalloc(ret, *size);
			else
			{
				ret = palloc(*size);
				ret->count = 0;
			}
			

			for (dsa_pointer pdl_node_dsa = list->head; DsaPointerIsValid(pdl_node_dsa);)
			{
				PendingDeleteListNode *pdl_node = dsa_get_address(pendingDeletesDsa, pdl_node_dsa);

				ret->array[ret->count++] = pdl_node->xrelnode;
				pdl_node_dsa = pdl_node->next;
			}
		}

		LWLockRelease(list->lock);
	}

	if (ret == NULL)
	{
		*size = 0;
		return NULL;
	}

	return ret;
}

/*
 * Calculate size for pending delete shmem.
 * The flexible array member should fit DSA.
 */
Size
PdlShmemSize(void)
{
	if (!gp_track_pending_delete)
		return 0;

	Size size = add_size(offsetof(BackendsPendingDeletesArray, dsa_mem),
						 dsa_minimum_size());
	return add_size(size, mul_size(sizeof(PendingDeletesList), MaxBackends));
}

void
PdlShmemInit(void)
{
	if (!is_tracking_enabled())
		return;

	bool found;
	BackendsPendingDeletes = (BackendsPendingDeletesArray *)
		ShmemInitStruct("Pending deletes array", 
			add_size(offsetof(BackendsPendingDeletesArray, dsa_mem),
					 dsa_minimum_size()),
			&found);
	if (found)
		return;

	Size pdl_size = mul_size(sizeof(PendingDeletesList), MaxBackends);
	BackendsPendingDeletes->list = (PendingDeletesList *) ShmemAlloc(pdl_size);
	for (int i = 0; i < MaxBackends; i++)
	{
		BackendsPendingDeletes->list[i] = (PendingDeletesList) {
			.head = InvalidDsaPointer,
			.count = 0,
			.lock = LWLockAssign()
		};
	}

	dsa_area *dsa = dsa_create_in_place(
		BackendsPendingDeletes->dsa_mem, dsa_minimum_size(),
		LWLockNewTrancheId(), "storage_pending_deletes", NULL);
	on_shmem_exit(dsa_on_shmem_exit_release_in_place,
		(Datum) BackendsPendingDeletes->dsa_mem);
	dsa_detach(dsa);
}


static void
pdl_beshutdown_hook(int code, Datum arg)
{
	if (MyBackendId == InvalidBackendId)
		return;

	PendingDeletesList *list = &BackendsPendingDeletes->list[MyBackendId];
	if (list->head == InvalidDsaPointer && list->count == 0)
		return;

	/* Assert on debug build and warning on release */
	Assert(false);
	ereport(WARNING,
		(errcode(ERRCODE_INTERNAL_ERROR),
		 errmsg("Pending deletes list is not empty. "
				"MyBackend: %d, MyProcPid: %d", MyBackendId, MyProcPid)));

	list->head = InvalidDsaPointer;
	list->count = 0;
}
/*
 * Attach dsa once per process.
 */
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

	/* disconnect from dsa on shmem exit */
	on_shmem_exit(dsa_on_shmem_exit_release_in_place,
		(Datum) BackendsPendingDeletes->dsa_mem);
	on_shmem_exit(pdl_beshutdown_hook, 0);
}

/*
 * Add pending delete node to shmem.
 * Return dsa ptr of newly created node. This ptr can be used for fast remove.
 */
dsa_pointer
PdlShmemAdd(RelFileNodePendingDelete *relnode, TransactionId xid)
{
	if (!is_tracking_enabled() || xid == InvalidTransactionId ||
		MyBackendId == InvalidBackendId)
		return InvalidDsaPointer;

	PdlAttachDsa();

	PendingDeleteListNode *node;
	dsa_pointer node_dsa = dsa_allocate(pendingDeletesDsa,
										sizeof(*node));
	node = dsa_get_address(pendingDeletesDsa, node_dsa);
	*node = (PendingDeleteListNode)	{
		.xrelnode = {
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
	list->count++;
	LWLockRelease(list->lock);
	
	return node_dsa;
}

/*
 * Fast remove pending delete node from shmem.
 * node_ptr is a ptr to already added node.
 */
void
PdlShmemRemove(dsa_pointer node_ptr)
{
	if (!is_tracking_enabled() || MyBackendId == InvalidBackendId)
		return;

	Assert(DsaPointerIsValid(node_ptr));

	PendingDeletesList *list = &BackendsPendingDeletes->list[MyBackendId];
	PendingDeleteListNode *node = dsa_get_address(pendingDeletesDsa, node_ptr);

	LWLockAcquire(list->lock, LW_EXCLUSIVE);
	if (DsaPointerIsValid(node->next))
	{
		PendingDeleteListNode *next_node = dsa_get_address(pendingDeletesDsa, node->next);
		next_node->prev = node->prev;
	}

	if (DsaPointerIsValid(node->prev))
	{
		PendingDeleteListNode *prev_node = dsa_get_address(pendingDeletesDsa, node->prev);
		prev_node->next = node->next;
	}
	else
		list->head = node->next;

	list->count--;
	LWLockRelease(list->lock);


	dsa_free(pendingDeletesDsa, node_ptr);
}
