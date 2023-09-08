#include "postgres.h"

#include "miscadmin.h"

#include "catalog/storage_pending.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/shmem.h"
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
}			PendingDeleteListNode;

/* A struct to track pending deletes. Placed in static shared memory area. */
typedef struct PendingDeleteShmemStruct
{
	dsa_pointer pdl_head;		/* ptr to list head of PendingDeleteListNode */
	size_t		pdl_count;		/* count of PendingDeleteListNode nodes */
	char		dsa_mem[FLEXIBLE_ARRAY_MEMBER]; /* a minimal memory area which
												 * can be used for dsa
												 * initialization */
}			PendingDeleteShmemStruct;

/*
 * HTAB entry for pending deletes for the given xid.
 */
typedef struct PendingDeleteHtabNode
{
	TransactionId xid;
	List	   *relnode_list;	/* list of RelFileNodePendingDelete */
}			PendingDeleteHtabNode;

static dsa_area *pendingDeleteDsa = NULL;	/* ptr to DSA area attached by
											 * current process */
static HTAB *pendingDeleteRedo = NULL;	/* HTAB for storing pending deletes
										 * during redo */

static PendingDeleteShmemStruct * PendingDeleteShmem = NULL;	/* shared pending delete
																 * state  */
/*
 * Calculate size for pending delete shmem.
 * The flexible array member should fit DSA.
 */
Size
PendingDeleteShmemSize(void)
{
	Size		size;

	size = offsetof(PendingDeleteShmemStruct, dsa_mem);
	/* dsa initialized over flexible static dsa_mem */
	size = add_size(size, dsa_minimum_size());

	return size;
}

/*
 * Initialize pending delete shmem struct.
 */
void
PendingDeleteShmemInit(void)
{
	Size		size = PendingDeleteShmemSize();
	bool		found;

	PendingDeleteShmem = (PendingDeleteShmemStruct *)
		ShmemInitStruct("Pending Delete",
						size,
						&found);

	if (!found)
	{
		dsa_area   *dsa = dsa_create_in_place(
											  PendingDeleteShmem->dsa_mem,
											  dsa_minimum_size(),
											  LWTRANCHE_PENDING_DELETE_DSA,
											  NULL
		);

		/*
		 * we can't allocate memory segments inside postmaster, so list will
		 * be initialized at runtime
		 */
		PendingDeleteShmem->pdl_head = InvalidDsaPointer;
		PendingDeleteShmem->pdl_count = 0;
		elog(DEBUG2, "Pending delete shared memory initialized.");

		/*
		 * segments will be released by dsm_postmaster_shutdown(), but keep it
		 * clean anyway
		 */
		on_shmem_exit(dsa_on_shmem_exit_release_in_place, (Datum) PendingDeleteShmem->dsa_mem);

		/*
		 * we don't need dsa ptr here, all future dsa calls will be in
		 * backends
		 */
		dsa_detach(dsa);
	}
}

/*
 * Prepend shared list with new pending delete node.
 * dsa - a ptr to currently attached dsa area
 * cur - ptr to already allocated node
 */
static void
PendingDeleteShmemLinkNode(dsa_area *dsa, dsa_pointer cur)
{
	dsa_pointer head;
	PendingDeleteListNode *cur_node;

	cur_node = (PendingDeleteListNode *) dsa_get_address(dsa, cur);

	LWLockAcquire(PendingDeleteLock, LW_EXCLUSIVE);

	head = PendingDeleteShmem->pdl_head;
	cur_node->next = head;
	cur_node->prev = InvalidDsaPointer;
	if (DsaPointerIsValid(head))
	{
		PendingDeleteListNode *head_node = (PendingDeleteListNode *) dsa_get_address(dsa, head);

		head_node->prev = cur;
	}
	PendingDeleteShmem->pdl_head = cur;
	PendingDeleteShmem->pdl_count++;

	LWLockRelease(PendingDeleteLock);

	elog(DEBUG2, "Pending delete rel added to shmem.");
}

/*
 * Remove pending delete node from shared list
 * dsa - a ptr tu currently attached dsa area
 * cur - ptr to node which is already linked to list
 */
static void
PendingDeleteShmemUnlinkNode(dsa_area *dsa, dsa_pointer cur)
{
	dsa_pointer head;
	PendingDeleteListNode *cur_node;

	cur_node = dsa_get_address(dsa, cur);

	LWLockAcquire(PendingDeleteLock, LW_EXCLUSIVE);

	head = PendingDeleteShmem->pdl_head;

	if (DsaPointerIsValid(cur_node->next))
	{
		PendingDeleteListNode *next_node = dsa_get_address(dsa, cur_node->next);

		next_node->prev = cur_node->prev;
	}

	if (DsaPointerIsValid(cur_node->prev))
	{
		PendingDeleteListNode *prev_node = dsa_get_address(dsa, cur_node->prev);

		prev_node->next = cur_node->next;
	}

	if (cur == head)
		PendingDeleteShmem->pdl_head = cur_node->next;

	PendingDeleteShmem->pdl_count--;

	LWLockRelease(PendingDeleteLock);

	elog(DEBUG2, "Pending delete rel removed from shmem.");
}

/*
 * Attach dsa once per process.
 */
static void
PendingDeleteAttachDsa(void)
{
	MemoryContext oldcxt;

	if (pendingDeleteDsa)
		return;

	/*
	 * Keep the DSA area ptr in TopMemoryContext to avoid excessive
	 * attach/detach at every add/remove
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	pendingDeleteDsa = dsa_attach_in_place(PendingDeleteShmem->dsa_mem, NULL);
	MemoryContextSwitchTo(oldcxt);

	/* pin mappings, so they can survive res owner life end */
	dsa_pin_mapping(pendingDeleteDsa);
	/* disconnect from dsa on shmem exit */
	on_shmem_exit(dsa_on_shmem_exit_release_in_place, (Datum) PendingDeleteShmem->dsa_mem);

	elog(DEBUG3, "Pending delete DSA attached");
}

/*
 * Add pending delete node to shmem.
 * Return dsa ptr of newly created node. This ptr can be used for fast remove.
 */
dsa_pointer
PendingDeleteShmemAdd(RelFileNodePendingDelete * relnode, TransactionId xid)
{
	dsa_pointer pdl_node_dsa;
	PendingDeleteListNode *pdl_node;

	elog(DEBUG2, "Trying to add pending delete rel %d to shmem (xid: %d).", relnode->node.relNode, xid);

	if (xid == InvalidTransactionId || !IsUnderPostmaster)
		return InvalidDsaPointer;

	PendingDeleteAttachDsa();

	pdl_node_dsa = dsa_allocate(pendingDeleteDsa, sizeof(*pdl_node));
	pdl_node = dsa_get_address(pendingDeleteDsa, pdl_node_dsa);

	memcpy(&pdl_node->xrelnode.relnode, relnode, sizeof(*relnode));
	pdl_node->xrelnode.xid = xid;

	PendingDeleteShmemLinkNode(pendingDeleteDsa, pdl_node_dsa);

	return pdl_node_dsa;
}

/*
 * Fast remove pending delete node from shmem.
 * node_ptr is a ptr to already added node.
 */
void
PendingDeleteShmemRemove(dsa_pointer node_ptr)
{
	elog(DEBUG2, "Trying to remove pending delete rel from shmem.");

	if (!DsaPointerIsValid(node_ptr) || !pendingDeleteDsa)
		return;

	PendingDeleteShmemUnlinkNode(pendingDeleteDsa, node_ptr);

	dsa_free(pendingDeleteDsa, node_ptr);
}

/*
 * Dump all pending delete nodes to char array.
 * Return NULL if there no nodes.
 */
static PendingRelXactDeleteArray *
PendingDeleteXLogShmemDump(Size *size)
{
	dsa_pointer pdl_node_dsa;
	PendingRelXactDeleteArray *xrelnode_array;

	elog(DEBUG2, "Serializing pending deletes to array.");

	PendingDeleteAttachDsa();

	/*
	 * For now, this function can't be called concurrently, so we can use
	 * LW_EXCLUSIVE, but keep the code strict.
	 */
	LWLockAcquire(PendingDeleteLock, LW_SHARED);

	pdl_node_dsa = PendingDeleteShmem->pdl_head;

	if (!DsaPointerIsValid(pdl_node_dsa))
	{
		LWLockRelease(PendingDeleteLock);
		return NULL;
	}

	*size = sizeof(size_t) + sizeof(PendingRelXactDelete) * PendingDeleteShmem->pdl_count;
	xrelnode_array = palloc(*size);
	xrelnode_array->count = 0;

	while (DsaPointerIsValid(pdl_node_dsa))
	{
		PendingDeleteListNode *pdl_node = dsa_get_address(pendingDeleteDsa, pdl_node_dsa);

		memcpy(&xrelnode_array->array[xrelnode_array->count], &pdl_node->xrelnode, sizeof(pdl_node->xrelnode));
		xrelnode_array->count++;

		pdl_node_dsa = pdl_node->next;
	}

	Assert(xrelnode_array->count == PendingDeleteShmem->pdl_count);

	LWLockRelease(PendingDeleteLock);

	elog(DEBUG2, "Pending deletes serialized. Count: %lu.", xrelnode_array->count);

	return xrelnode_array;
}

/*
 * Insert XLOG_PENDING_DELETE record to XLog.
 */
XLogRecPtr
PendingDeleteXLogInsert(void)
{
	XLogRecPtr	recptr;
	Size		size;
	PendingRelXactDeleteArray *xrelnode_array = PendingDeleteXLogShmemDump(&size);

	if (!xrelnode_array)
		return InvalidXLogRecPtr;

	XLogBeginInsert();
	XLogRegisterData((char *) xrelnode_array, size);
	recptr = XLogInsert(RM_XLOG_ID, XLOG_PENDING_DELETE);
	XLogFlush(recptr);

	elog(DEBUG3, "Pending delete XLog record inserted.");

	pfree(xrelnode_array);

	return recptr;
}

/*
 * Add pending delete node during processing of redo records.
 */
void
PendingDeleteRedoAdd(PendingRelXactDelete * pd)
{
	PendingDeleteHtabNode *h_node;
	bool		found;
	RelFileNodePendingDelete *relnode;

	elog(DEBUG2, "Trying to add pending delete rel %d during redo (xid: %d).", pd->relnode.node.relNode, pd->xid);

	if (pd->xid == InvalidTransactionId)
		return;

	if (!pendingDeleteRedo)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));

		ctl.keysize = sizeof(TransactionId);
		ctl.entrysize = sizeof(PendingDeleteHtabNode);

		pendingDeleteRedo = hash_create("Pending Delete Data", 0, &ctl, HASH_ELEM);

		elog(DEBUG3, "New hash table initialized for pending delete redo.");
	}

	h_node = (PendingDeleteHtabNode *) hash_search(pendingDeleteRedo, &pd->xid, HASH_ENTER, &found);
	if (!found)
	{
		h_node->xid = pd->xid;
		h_node->relnode_list = NIL;

		elog(DEBUG3, "New list initialized for pending delete redo (xid: %d).", pd->xid);
	}

	relnode = palloc(sizeof(*relnode));
	memcpy(relnode, &pd->relnode, sizeof(*relnode));
	h_node->relnode_list = lappend(h_node->relnode_list, relnode);

	elog(DEBUG2, "Pending delete rel %d added during redo (xid: %d).", pd->relnode.node.relNode, pd->xid);
}

/*
 * Replay XLOG_PENDING_DELETE XLog record.
 * Remember all pending delete nodes for possible drop.
 */
void
PendingDeleteRedoRecord(XLogReaderState *record)
{
	PendingRelXactDeleteArray *xrelnode_array = (PendingRelXactDeleteArray *) XLogRecGetData(record);

	Assert(xrelnode_array->count > 0);
	Assert(XLogRecGetDataLen(record) ==
		   (sizeof(xrelnode_array->count) + sizeof(PendingRelXactDelete) * xrelnode_array->count));

	elog(DEBUG2, "Processing pending delete redo record");

	for (size_t i = 0; i < xrelnode_array->count; i++)
		PendingDeleteRedoAdd(&xrelnode_array->array[i]);
}

/*
 * Remove pending delete nodes from redo list.
 * Remove all nodes at once for the given xact.
 */
void
PendingDeleteRedoRemove(TransactionId xid)
{
	PendingDeleteHtabNode *h_node;

	elog(DEBUG2, "Trying to remove pending delete rels during redo (xid: %d).", xid);

	if (xid == InvalidTransactionId || !pendingDeleteRedo)
		return;

	h_node = (PendingDeleteHtabNode *) hash_search(pendingDeleteRedo, &xid, HASH_REMOVE, NULL);

	if (!h_node)
		return;

	/*
	 * Free the whole list. Rels for the given xid are not pending anymore.
	 */
	list_free_deep(h_node->relnode_list);

	elog(DEBUG2, "Pending delete rels removed during redo (xid: %d).", xid);
}

/*
 * Drop all orphaned pending delete nodes.
 */
void
PendingDeleteRedoDropFiles(void)
{
	HASH_SEQ_STATUS seq_status;
	PendingDeleteHtabNode *h_node;

	elog(DEBUG2, "Trying to drop pending delete rels.");

	if (!pendingDeleteRedo || hash_get_num_entries(pendingDeleteRedo) == 0)
		return;

	/* iterate over whole htab */
	hash_seq_init(&seq_status, pendingDeleteRedo);
	while ((h_node = (PendingDeleteHtabNode *) hash_seq_search(&seq_status)) != NULL)
	{
		ListCell   *cell;
		int			i;
		int			count;
		RelFileNodePendingDelete *relnode_array;

		/*
		 * In concurrent environment CREATE and PENDING_DELETE log records
		 * with same rel node may come one after another. This cause
		 * duplicates in pending delete list. To avoid smgr errors, we should
		 * filter it before DropRelationFiles() call.
		 */
		foreach(cell, h_node->relnode_list)
		{
			RelFileNodePendingDelete *o_relnode = (RelFileNodePendingDelete *) lfirst(cell);
			ListCell   *i_cell = lnext(cell);
			ListCell   *i_cell_prev = cell;

			while (i_cell)
			{
				ListCell   *i_cell_next = lnext(i_cell);
				RelFileNodePendingDelete *i_relnode = (RelFileNodePendingDelete *) lfirst(i_cell);

				if (RelFileNodeEquals(o_relnode->node, i_relnode->node))
				{
					elog(DEBUG2, "Duplicate pending delete node found: (rel: %d; xid: %d)",
						 o_relnode->node.relNode, h_node->xid);
					h_node->relnode_list = list_delete_cell(h_node->relnode_list, i_cell, i_cell_prev);
					pfree(i_relnode);
				}
				else
					i_cell_prev = i_cell;

				i_cell = i_cell_next;
			}
		}

		count = list_length(h_node->relnode_list);
		relnode_array = palloc(sizeof(*relnode_array) * count);

		/* copy rels from list to array, we use one batch per one xid to drop */
		foreach_with_count(cell, h_node->relnode_list, i)
		{
			RelFileNodePendingDelete *relnode = (RelFileNodePendingDelete *) lfirst(cell);

			memcpy(&relnode_array[i], relnode, sizeof(*relnode_array));
		}

		DropRelationFiles(relnode_array, count, true);

		elog(DEBUG2, "Pending delete rels were dropped (count: %d; xid: %d).", count, h_node->xid);

		pfree(relnode_array);
		/* we don't need rels list anymore, drop is a final stage */
		list_free_deep(h_node->relnode_list);
	}

	elog(DEBUG2, "Dropping of pending delete rels completed.");

	/* free all the memory, we don't heed htab after recovery */
	hash_destroy(pendingDeleteRedo);
	pendingDeleteRedo = NULL;
}