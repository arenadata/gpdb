#ifndef LOCK_FREE_LIST_H
#define LOCK_FREE_LIST_H

#ifndef FRONTEND

#include "postgres.h"
#include "utils/dsa.h"
#include "storage/relfilenode.h"

typedef dsa_area* (*dsa_area_allocator)(void);

typedef struct lock_free_list
{
	dsa_pointer 	head;
	dsa_area_allocator 	dsa_alloc_area;
	int 			count;
	int				lf_procpid;
} lock_free_list;

typedef struct lock_free_list_cell
{
	dsa_pointer value;
	dsa_pointer next;
} lock_free_list_cell;

void
lock_free_list_init(lock_free_list *ls, dsa_area_allocator dsa_alloc);

void
lock_free_list_attach_to_writer(lock_free_list *ls);

/*
 * Allowed caller: writer.
 */
lock_free_list_cell *
lock_free_list_push(lock_free_list *ls, dsa_pointer value);

/*
 * Allowed caller: writer.
 */
void
lock_free_list_delete(lock_free_list *ls, lock_free_list_cell *cell);

lock_free_list_cell *
LFL_PdlShmemAdd(lock_free_list *ls, RelFileNodePendingDelete * relnode, TransactionId xid);

/*
 * Allowed caller: reader.
 * Will return the first not 'deleted' cell in a list, or NULL if no such cell.
 * Will free all 'deleted' cells between (HEAD) cell and the returned cell.
 */
lock_free_list_cell *
lock_free_list_first(lock_free_list *ls);

/*
 * Allowed caller: reader.
 * Will return the next not 'deleted' cell after current_cell in a list, or NULL if no such cell.
 * Will free all 'deleted' cells between current_cell and the returned cell.
 */
lock_free_list_cell *
lock_free_list_next(lock_free_list *ls, lock_free_list_cell *current_cell);

dsa_pointer
lock_free_list_get_value(lock_free_list_cell * cell);

dsa_area *
lock_free_list_get_associated_area(lock_free_list *ls);

#endif /* FRONTEND */

#endif /* LOCK_FREE_LIST_H */
