#ifndef STORAGE_PENDING_H
#define STORAGE_PENDING_H

#include "access/xlog.h"

//TODO: Find a better WA for "atomics.h may not be included from frontend code"
#ifdef FRONTEND
#undef FRONTEND
#include "utils/dsa.h"
#define FRONTEND
#else
#include "utils/dsa.h"
#endif

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

extern dsa_pointer PendingDeleteShmemAdd(RelFileNodePendingDelete * relnode, TransactionId xid);
extern void PendingDeleteShmemRemove(dsa_pointer node_ptr);

extern XLogRecPtr PendingDeleteXLogInsert(void);

extern void PendingDeleteRedoAdd(PendingRelXactDelete * pd);
extern void PendingDeleteRedoRecord(XLogReaderState *record);
extern void PendingDeleteRedoRemove(TransactionId xid);
extern void PendingDeleteRedoDropFiles(void);

#endif							/* STORAGE_PENDING_H */
