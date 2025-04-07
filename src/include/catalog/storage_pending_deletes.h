/*-------------------------------------------------------------------------
 *
 * storage_pending_deletes.h
 *	  prototypes for functions in backend/catalog/storage_pending_deletes.c
 *
 * Copyright (c) 2025 Greengage Community
 *
 * src/include/catalog/storage_pending_deletes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_PENDING_DELETES_H
#define STORAGE_PENDING_DELETES_H

#include "postgres.h"

#include "storage/relfilenode.h"

/* Pending delete node linked to xact which created it */
typedef struct PendingRelXactDelete
{
	RelFileNodePendingDelete relnode;
	TransactionId xid;
}	PendingRelXactDelete;

typedef struct PendingRelXactDeleteArray
{
	Size		count;
	PendingRelXactDelete array[FLEXIBLE_ARRAY_MEMBER];
}	PendingRelXactDeleteArray;

/*
 * This function collects info about pending deletes from all backends and
 * returns the accumulated result.
 * Note: the returned result is always palloc'ed. Caller is responsible for
 * freeing it.
 */
PendingRelXactDeleteArray *PdlXLogShmemDump(Size *size);

#endif   /* STORAGE_PENDING_DELETES_H */
