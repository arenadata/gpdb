#include "postgres.h"

#include "miscadmin.h"

#include "access/clog.h"
#include "access/transam.h"
#include "catalog/storage_pending.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

/*
 * Shared pending delete list node.
 * Doubly linked list provides O(1) remove.
 */
typedef struct PendingDeleteListNode
{
	PendingRelXactDelete xrelnode;
	dsa_pointer next;
	dsa_pointer prev;
} PendingDeleteListNode;

typedef struct PendingDeletesShmemListStruct
{
	LWLock	   		*pdl_lock;		/* protects the fields below */
	dsa_pointer 	pdl_head;		/* ptr to list head of PendingDeleteListNode */
	size_t			pdl_count;		/* count of PendingDeleteListNode nodes */
} PendingDeletesShmemListStruct;

/* A struct to track pending deletes. Placed in static shared memory area. */
typedef struct PendingDeletesArrayShmemStruct
{
	PendingDeletesShmemListStruct 	*pdl_list;
	char				dsa_mem[FLEXIBLE_ARRAY_MEMBER]; /* a minimal memory area which
												 * can be used for dsa
												 * initialization */
} PendingDeletesArrayShmemStruct;

static dsa_area *pendingDeleteDsa = NULL;	/* ptr to DSA area attached by
											 * current process */

static PendingDeletesArrayShmemStruct *PendingDeletesArrayShmem = NULL;	/* shared pending delete
																 * state  */

/*
 * Calculate size for pending delete shmem.
 * The flexible array member should fit DSA.
 */
Size
PdlShmemSize(void)
{
	Size		size;

	size = offsetof(PendingDeletesArrayShmemStruct, dsa_mem);
	/* dsa initialized over flexible static dsa_mem */
	size = add_size(size, dsa_minimum_size());

	RequestNamedLWLockTranche("PdlLocks", MaxBackends);

	return size;
}

static void
PendingDeletesArrayShmemStructInit(PendingDeletesShmemListStruct *pdl, LWLock *lock)
{
	Assert(pdl);
	Assert(lock);

	pdl->pdl_lock = lock;
	pdl->pdl_head = InvalidDsaPointer;
	pdl->pdl_count = 0;
}

/*
 * Initialize pending delete shmem struct.
 */
void
PdlShmemInit(void)
{
	Size		size;
	bool		found;


	size = PdlShmemSize();

	PendingDeletesArrayShmem = (PendingDeletesArrayShmemStruct *)
		ShmemInitStruct("Per-backend lwlock Pending Deletes Array",
						size,
						&found);

	if (!found)
	{
		dsa_area   *dsa = dsa_create_in_place(
											  PendingDeletesArrayShmem->dsa_mem,
											dsa_minimum_size(),
											LWTRANCHE_PENDING_DELETE_DSA_PDL,
											NULL
		);

		PendingDeletesArrayShmem->pdl_list =
				(PendingDeletesShmemListStruct *) ShmemAlloc(MaxBackends * sizeof(PendingDeletesShmemListStruct));

		MemSet(PendingDeletesArrayShmem->pdl_list, 0,
			   MaxBackends * sizeof(PendingDeletesShmemListStruct));

		LWLockPadded *lock_base = GetNamedLWLockTranche("PdlLocks");

		for (int i = 0; i < MaxBackends; i++) {
			PendingDeletesArrayShmemStructInit(&PendingDeletesArrayShmem->pdl_list[i], &lock_base[i].lock);
		}

		/*
		 * we can't allocate memory segments inside postmaster, so list will
		 * be initialized at runtime
		 */
		elog(LOG, "Pending delete shared memory initialized");

		/*
		 * segments will be released by dsm_postmaster_shutdown(), but keep it
		 * clean anyway
		 */
		on_shmem_exit(dsa_on_shmem_exit_release_in_place, (Datum) PendingDeletesArrayShmem->dsa_mem);

		/*
		 * we don't need dsa ptr here, all future dsa calls will be in
		 * backends
		 */
		dsa_detach(dsa);
	}
}

/*
 * Prepend shared list with new pending delete node.
 * cur - ptr to already allocated node
 */
static void
PdlShmemLinkNode(dsa_pointer cur)
{
	dsa_pointer head;
	PendingDeleteListNode *cur_node;
	PendingDeletesShmemListStruct *pdl_list;

	pdl_list = &PendingDeletesArrayShmem->pdl_list[MyBackendId];

	cur_node = (PendingDeleteListNode *) dsa_get_address(pendingDeleteDsa, cur);

	LWLockAcquire(pdl_list->pdl_lock, LW_EXCLUSIVE);

	head = pdl_list->pdl_head;
	cur_node->next = head;
	cur_node->prev = InvalidDsaPointer;
	if (DsaPointerIsValid(head))
	{
		PendingDeleteListNode *head_node = (PendingDeleteListNode *) dsa_get_address(pendingDeleteDsa, head);

		head_node->prev = cur;
	}
	pdl_list->pdl_head = cur;
	pdl_list->pdl_count++;

	LWLockRelease(pdl_list->pdl_lock);

	elog(DEBUG1, "Pending delete rel added to shmem.");
}

/*
 * Remove pending delete node from shared list
 * dsa - a ptr tu currently attached dsa area
 * cur - ptr to node which is already linked to list
 */
static void
PdlShmemUnlinkNode(dsa_pointer cur)
{
	dsa_pointer head;
	PendingDeleteListNode *cur_node;
	PendingDeletesShmemListStruct *pdl_list;

	pdl_list = &PendingDeletesArrayShmem->pdl_list[MyBackendId];

	cur_node = dsa_get_address(pendingDeleteDsa, cur);

	LWLockAcquire(pdl_list->pdl_lock, LW_EXCLUSIVE);

	head = pdl_list->pdl_head;

	if (DsaPointerIsValid(cur_node->next))
	{
		PendingDeleteListNode *next_node = dsa_get_address(pendingDeleteDsa, cur_node->next);

		next_node->prev = cur_node->prev;
	}

	if (DsaPointerIsValid(cur_node->prev))
	{
		PendingDeleteListNode *prev_node = dsa_get_address(pendingDeleteDsa, cur_node->prev);

		prev_node->next = cur_node->next;
	}

	if (cur == head)
		pdl_list->pdl_head = cur_node->next;

	pdl_list->pdl_count--;

	LWLockRelease(pdl_list->pdl_lock);

	elog(DEBUG1, "Pending delete rel removed from shmem.");
}

/*
 * Attach dsa once per process.
 */
static void
PdlAttachDsa(void)
{
	MemoryContext oldcxt;

	if (pendingDeleteDsa)
		return;

	/*
	 * Keep the DSA area ptr in TopMemoryContext to avoid excessive
	 * attach/detach at every add/remove
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	pendingDeleteDsa = dsa_attach_in_place(PendingDeletesArrayShmem->dsa_mem, NULL);
	MemoryContextSwitchTo(oldcxt);

	/* pin mappings, so they can survive res owner life end */
	dsa_pin_mapping(pendingDeleteDsa);
	/* disconnect from dsa on shmem exit */
	on_shmem_exit(dsa_on_shmem_exit_release_in_place, (Datum) PendingDeletesArrayShmem->dsa_mem);

	elog(DEBUG1, "Pending delete DSA attached");
}

/*
 * Add pending delete node to shmem.
 * Return dsa ptr of newly created node. This ptr can be used for fast remove.
 */
dsa_pointer
PdlShmemAdd(RelFileNodePendingDelete * relnode, TransactionId xid)
{
	dsa_pointer pdl_node_dsa;
	PendingDeleteListNode *pdl_node;

	elog(DEBUG1, "Trying to add pending delete rel %d to shmem (xid: %d).", relnode->node.relNode, xid);

	PdlAttachDsa();

	pdl_node_dsa = dsa_allocate(pendingDeleteDsa, sizeof(*pdl_node));
	pdl_node = dsa_get_address(pendingDeleteDsa, pdl_node_dsa);

	memcpy(&pdl_node->xrelnode.relnode, relnode, sizeof(*relnode));
	pdl_node->xrelnode.xid = xid;

	PdlShmemLinkNode(pdl_node_dsa);

	return pdl_node_dsa;
}

/*
 * Fast remove pending delete node from shmem.
 * node_ptr is a ptr to already added node.
 */
void
PdlShmemRemove(dsa_pointer node_ptr)
{
	elog(DEBUG1, "Trying to remove pending delete rel from shmem.");

	PdlShmemUnlinkNode(node_ptr);

	dsa_free(pendingDeleteDsa, node_ptr);
}


void
PdlDump()
{
	dsa_pointer pdl_node_dsa;
	PendingDeletesShmemListStruct *pdl_list;
	int 	i;
	Size 	pdl_list_count;

	PdlAttachDsa();

	for (i = 0; i < MaxBackends; i++)
	{
		pdl_list = &PendingDeletesArrayShmem->pdl_list[i];

		LWLockAcquire(pdl_list->pdl_lock, LW_SHARED);

		pdl_list_count = pdl_list->pdl_count;

		if (pdl_list_count > 0)
		{
			elog(DEBUG1, "Found pending list with %ld elements", pdl_list_count);

			pdl_node_dsa = pdl_list->pdl_head;

			if (!DsaPointerIsValid(pdl_node_dsa))
			{
				LWLockRelease(pdl_list->pdl_lock);
				continue;
			}

			while (DsaPointerIsValid(pdl_node_dsa))
			{
				PendingDeleteListNode *pdl_node = dsa_get_address(pendingDeleteDsa, pdl_node_dsa);

				elog(LOG, "[RELOG][READER][BACKEND-PDL] <%u>", pdl_node->xrelnode.xid);

				pdl_node_dsa = pdl_node->next;
			}

		}

		LWLockRelease(pdl_list->pdl_lock);
	}
}
