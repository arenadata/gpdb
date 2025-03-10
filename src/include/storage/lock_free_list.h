#ifndef LOCK_FREE_LIST_H

#ifndef FRONTEND

#include "postgres.h"

#include "utils/dsa.h" 

struct lock_free_list_cell;
typedef struct lock_free_list_cell lock_free_list_cell;

struct lock_free_list;
typedef struct lock_free_list lock_free_list;

dsa_pointer
lock_free_list_create(void);

lock_free_list *
lock_free_list_get_local_list(uint64 ls_dsa);

/* TODO: change type to dsa */
void
lock_free_list_destroy(dsa_pointer ls_dsa);

/*
 * Allowed caller: writer.
 */
lock_free_list_cell *
lock_free_list_push(lock_free_list *ls, void *value);

/*
 * Allowed caller: writer.
 */
void
lock_free_list_delete(lock_free_list_cell *cell);

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

void
lock_free_list_dump(FILE *fout, lock_free_list *ls);

#endif /* FRONTEND */

#endif /* LOCK_FREE_LIST_H */
