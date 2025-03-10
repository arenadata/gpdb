#include "storage/lock_free_list.h"
#include "storage/ipc.h"

struct lock_free_list
{
	dsa_pointer head;
};

struct lock_free_list_cell
{
	void *value;
	dsa_pointer next;
};

#define LFL_MARK_CELL(cell)			(cell->next = (dsa_pointer)((uintptr_t)cell->next | 0x1))
#define LFL_IS_CELL_MARKED(cell)	((uintptr_t)cell->next & 0x1)

static dsa_pointer
lock_free_list_cell_get_next(lock_free_list_cell *cell)
{
	dsa_pointer mask = (dsa_pointer) -2;
	return cell->next & mask;
}

dsa_pointer
lock_free_list_create()
{
	dsa_area *area = PendingDeleteAttachDsa();
	dsa_pointer ls_dsa = dsa_allocate(area, sizeof(lock_free_list));
	Assert(DsaPointerIsValid(ls_dsa));
	lock_free_list *ls = (lock_free_list *)dsa_get_address(area, ls_dsa);

	ls->head = InvalidDsaPointer;
	return ls_dsa;
}

lock_free_list *
lock_free_list_get_local_list(dsa_pointer ls_dsa)
{
	Assert(DsaPointerIsValid(ls_dsa));

	dsa_area *area = PendingDeleteAttachDsa();
	return (lock_free_list *)dsa_get_address((dsa_area *) area, ls_dsa);
}

/* Maybe we do not need it at all. Nobody calls it for now. */
void
lock_free_list_destroy(dsa_pointer ls_dsa)
{
	dsa_area *area = PendingDeleteAttachDsa();

	lock_free_list *ls = lock_free_list_get_local_list(ls_dsa);

	if (ls)
	{
		dsa_pointer c_dsa = ls->head;
		while (DsaPointerIsValid(c_dsa))
		{
			dsa_pointer tmp_dsa = c_dsa;
			lock_free_list_cell* c = (lock_free_list_cell*)dsa_get_address(area, c_dsa);
			c_dsa = lock_free_list_cell_get_next(c);
			dsa_free(area, tmp_dsa);
		}
	}
	dsa_free(area, ls_dsa);
}

/*
 * Allowed caller: writer.
 */
lock_free_list_cell *
lock_free_list_push(lock_free_list *ls, void *value)
{
	Assert(ls);

	dsa_area *area = PendingDeleteAttachDsa();

	dsa_pointer new_cell_dsa = dsa_allocate(area, sizeof(lock_free_list_cell));
	Assert(DsaPointerIsValid(new_cell_dsa));

	lock_free_list_cell *new_cell = (lock_free_list_cell*)dsa_get_address(area, new_cell_dsa);

	new_cell->value = value;
	new_cell->next = ls->head;
	ls->head = new_cell_dsa;

	return new_cell;
}

/*
 * Allowed caller: writer.
 */
void
lock_free_list_delete(lock_free_list_cell *cell)
{
	if (cell != NULL)
		LFL_MARK_CELL(cell);
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

	/*
	 * Read the head from the list only once. Consider it as a snapshot of the
	 * list at this particular moment. 'ls->head' may be changed by the element
	 * push even before we leave this function, but we will ignore it and work
	 * only with the list snaphot.
	 */
	dsa_pointer head_snapshot_dsa = ls->head;
	if (!DsaPointerIsValid(head_snapshot_dsa))
		return NULL;

	dsa_area *area = PendingDeleteAttachDsa();

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
			dsa_free(area, tmp_dsa);

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
	Assert(current_cell);

	lock_free_list_cell *c = current_cell;
	dsa_pointer c_dsa = InvalidDsaPointer;
	dsa_area *area = PendingDeleteAttachDsa();

	do
	{
		dsa_pointer tmp_dsa = c_dsa;
		c_dsa = lock_free_list_cell_get_next(c);
		c = (lock_free_list_cell*)dsa_get_address(area, c_dsa);

		if (DsaPointerIsValid(tmp_dsa))
			dsa_free(area, tmp_dsa);

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

void *
lock_free_list_get_value(lock_free_list_cell * cell)
{
	Assert(cell);
	return cell->value;
}

