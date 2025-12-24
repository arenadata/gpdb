#include "storage/lock_free_list.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "catalog/storage_pending.h"

#define LFL_MARK_CELL(cell)			(cell->next = (dsa_pointer)((uintptr_t)cell->next | 0x1))
#define LFL_IS_CELL_MARKED(cell)	((uintptr_t)cell->next & 0x1)

static dsa_pointer
lock_free_list_cell_get_next(lock_free_list_cell *cell)
{
	dsa_pointer mask = (dsa_pointer) -2;
	return cell->next & mask;
}

void
lock_free_list_init(lock_free_list *ls, dsa_area_allocator dsa_alloc_area)
{
	Assert(ls);

	ls->head = InvalidDsaPointer;
	ls->dsa_alloc_area = dsa_alloc_area;
	ls->count = 0;
	ls->lf_procpid = InvalidPid;
}

static void
lock_free_list_shutdown_hook(int code, Datum arg)
{
	elog(LOG, "[RELOG] lock_free_list_shutdown_hook backend ID %d", MyBackendId);
	lock_free_list *ls = (lock_free_list *)arg;

	ls->lf_procpid = InvalidPid;
}

void
lock_free_list_attach_to_writer(lock_free_list *ls)
{
	Assert(ls);
	Assert(ls->lf_procpid == InvalidPid);
	ls->lf_procpid = MyProcPid;

	// TODO: what if the list is not empty from the previous backend???

	/* Set up a process-exit hook to clean up */
	on_shmem_exit(lock_free_list_shutdown_hook, (Datum) ls);
}

/*
 * Allowed caller: writer.
 */
lock_free_list_cell *
lock_free_list_push(lock_free_list *ls, dsa_pointer value)
{
	Assert(ls);
	Assert(ls->dsa_alloc_area);
	Assert(ls->lf_procpid == MyProcPid);

	dsa_area *area = ls->dsa_alloc_area();

	dsa_pointer new_cell_dsa = dsa_allocate(area, sizeof(lock_free_list_cell));
	Assert(DsaPointerIsValid(new_cell_dsa));

	lock_free_list_cell *new_cell = (lock_free_list_cell*)dsa_get_address(area, new_cell_dsa);

	new_cell->value = value;
	new_cell->next = ls->head;
	ls->head = new_cell_dsa;
	ls->count++;

	return new_cell;
}

/*
 * Allowed caller: writer.
 */
void
lock_free_list_delete(lock_free_list *ls, lock_free_list_cell *cell)
{
	Assert(ls);
	Assert(ls->lf_procpid == MyProcPid);

	if (cell != NULL)
	{
		/*
		 * We need to use CAS as the next node could be freed by the reader
		 * process, if the next node was previously also marked as deleted,
		 * while we are updating it here. We do not want to set back the freed
		 * pointer here.
		 */
		bool update_completed = false;
		dsa_pointer old_next = cell->next;
		while (!update_completed)
		{
			dsa_pointer new_next = old_next | 0x1;
			dsa_pointer_atomic *target = (dsa_pointer_atomic *) &(cell->next);
			update_completed = dsa_pointer_atomic_compare_exchange(target,
					&old_next,
					new_next);
		}

		if (ls->count > 0)
		{
			ls->count--;
		}
	}
}

lock_free_list_cell *
LFL_PdlShmemAdd(lock_free_list *ls, RelFileNodePendingDelete * relnode, TransactionId xid)
{
	dsa_pointer xrelnode_dsa;
	PendingRelXactDelete *xrelnode;

	elog(DEBUG1, "LFL: Trying to add pending delete rel %d to shmem (xid: %d).", relnode->node.relNode, xid);

	dsa_area *area = ls->dsa_alloc_area();

	xrelnode_dsa = dsa_allocate(area, sizeof(*xrelnode));
	xrelnode = dsa_get_address(area, xrelnode_dsa);

	memcpy(&xrelnode->relnode, relnode, sizeof(*relnode));
	xrelnode->xid = xid;

	return lock_free_list_push(ls, xrelnode_dsa);
}

/*
 * Allowed caller: reader.
 * Will return the first not 'deleted' cell in a list, or NULL if no such cell.
 * Will free all 'deleted' cells between (HEAD) cell and the returned cell.
 */
lock_free_list_cell *
lock_free_list_first(lock_free_list *ls)
{
	Assert(ls);
	Assert(ls->dsa_alloc_area);

	/*
	 * Read the head from the list only once. Consider it as a snapshot of the
	 * list at this particular moment. 'ls->head' may be changed by the element
	 * push even before we leave this function, but we will ignore it and work
	 * only with the list snaphot.
	 */
	dsa_pointer head_snapshot_dsa = ls->head;
	if (!DsaPointerIsValid(head_snapshot_dsa))
		return NULL;

	dsa_area *area = ls->dsa_alloc_area();

	lock_free_list_cell *head_snapshot = (lock_free_list_cell*)dsa_get_address(area, head_snapshot_dsa);

	dsa_pointer c_dsa = head_snapshot_dsa;
	lock_free_list_cell *c = head_snapshot;

	while (DsaPointerIsValid(c_dsa) && LFL_IS_CELL_MARKED(c))
	{
		/*
		 * If we are here, HEAD is marked, so its 'next' should be redirected
		 * to first not marked node (if it exists). All nodes between can be freed.
		 * And do not forget to keep the HEAD marked.
		 * We do not free HEAD if it is marked, as we need it for the push.
		 */
		dsa_pointer tmp_dsa = c_dsa;
		c_dsa = lock_free_list_cell_get_next(c);
		c = (lock_free_list_cell*)dsa_get_address(area, c_dsa);

		if (tmp_dsa != head_snapshot_dsa)
		{
			lock_free_list_cell *t = (lock_free_list_cell *)dsa_get_address(area, tmp_dsa);

			if (DsaPointerIsValid(t->value))
				dsa_free(area, t->value);

			dsa_free(area, tmp_dsa);
		}

		head_snapshot->next = c_dsa;
		LFL_MARK_CELL(head_snapshot);
	}

	return c;
}

/*
 * Allowed caller: reader.
 * Will return the next not 'deleted' cell after current_cell in a list, or NULL if no such cell.
 * Will free all 'deleted' cells between current_cell and the returned cell.
 */
lock_free_list_cell *
lock_free_list_next(lock_free_list *ls, lock_free_list_cell *current_cell)
{
	Assert(ls);
	Assert(ls->dsa_alloc_area);
	Assert(current_cell);

	lock_free_list_cell *c = current_cell;
	dsa_pointer c_dsa = InvalidDsaPointer;
	dsa_area *area = ls->dsa_alloc_area();

	do
	{
		dsa_pointer tmp_dsa = c_dsa;
		c_dsa = lock_free_list_cell_get_next(c);
		c = (lock_free_list_cell*)dsa_get_address(area, c_dsa);

		if (DsaPointerIsValid(tmp_dsa))
		{
			lock_free_list_cell *t = (lock_free_list_cell *)dsa_get_address(area, tmp_dsa);

			if (DsaPointerIsValid(t->value))
				dsa_free(area, t->value);

			dsa_free(area, tmp_dsa);
		}

		/*
		 * current_cell could be marked by the writer process while we were
		 * iterating here. We need to consider this possibility when updating
		 * the 'next' pointer of current_cell.
		 */
		bool update_completed = false;

		dsa_pointer old_next = current_cell->next;

		while (!update_completed)
		{
			dsa_pointer new_next = c_dsa;
			if (old_next & 0x1)
				new_next = new_next | 0x1;

			dsa_pointer_atomic *target = (dsa_pointer_atomic *) &(current_cell->next);
			update_completed = dsa_pointer_atomic_compare_exchange(target,
					&old_next,
					new_next);
		}
	} while (DsaPointerIsValid(c_dsa) && LFL_IS_CELL_MARKED(c));

	return c;
}

dsa_pointer
lock_free_list_get_value(lock_free_list_cell * cell)
{
	Assert(cell);
	return cell->value;
}

dsa_area *
lock_free_list_get_associated_area(lock_free_list *ls)
{
	Assert(ls);
	Assert(ls->dsa_alloc_area);

	return ls->dsa_alloc_area();
}
