#ifndef LOCK_FREE_LIST_H
#define LOCK_FREE_LIST_H

#ifndef FRONTEND

#include "postgres.h"
#include "utils/dsa.h"

typedef dsa_area* (*dsa_allocator)(void *dsa_mem);

typedef struct lock_free_list
{
	dsa_pointer 	head;
	dsa_allocator 	dsa_alloc;
	int 			count;
	void			*dsa_mem;
	int				lf_procpid;
} lock_free_list;

typedef struct lock_free_list_cell
{
	void 		*value;
	dsa_pointer next;
} lock_free_list_cell;

void
lock_free_list_init(lock_free_list *ls, dsa_allocator dsa_alloc, void *dsa_mem);

/*
 * Allowed caller: writer.
 */
lock_free_list_cell *
lock_free_list_push(lock_free_list *ls, void *value);

/*
 * Allowed caller: writer.
 */
void
lock_free_list_delete(lock_free_list *ls, lock_free_list_cell *cell);

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

void *
lock_free_list_get_value(lock_free_list_cell * cell);

#endif /* FRONTEND */

#endif /* LOCK_FREE_LIST_H */
