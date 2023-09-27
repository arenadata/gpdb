#ifndef STORAGE_PENDING_H
#define STORAGE_PENDING_H

#include "access/xlog.h"

/* Pending delete node linked to xact it created */
typedef struct PendingRelXactDelete
{
	RelFileNodePendingDelete relnode;
	TransactionId xid;
}			PendingRelXactDelete;

typedef struct PendingRelXactDeleteArray
{
	size_t		count;
	PendingRelXactDelete array[FLEXIBLE_ARRAY_MEMBER];
}			PendingRelXactDeleteArray;

extern Size PendingDeleteShmemSize(void);
extern void PendingDeleteShmemInit(void);

/*
 * We can't include "dsa.h" here as it use "atomics.h", which can't be included if FRONTEND defined.
 * To avoid including, void* is used. dsa_ptr is a pointer to dsa_pointer.
 */
extern void PendingDeleteShmemAdd(RelFileNodePendingDelete * relnode, TransactionId xid, void *dsa_ptr);
extern void PendingDeleteShmemRemove(void *dsa_ptr);

extern XLogRecPtr PendingDeleteXLogInsert(void);

extern void PendingDeleteRedoAdd(PendingRelXactDelete * pd);
extern void PendingDeleteRedoRecord(XLogReaderState *record);
extern void PendingDeleteRedoRemove(TransactionId xid);
extern void PendingDeleteRedoDropFiles(void);

#endif							/* STORAGE_PENDING_H */
