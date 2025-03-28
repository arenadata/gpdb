#ifndef STORAGE_PENDING_DELETES_REDO_H
#define STORAGE_PENDING_DELETES_REDO_H

#include "postgres.h"

#include "access/xlog.h"
#include "nodes/pg_list.h"
#include "storage/relfilenode.h"

/* Pending delete node linked to xact it created */
typedef struct PendingRelXactDelete
{
	RelFileNodePendingDelete relnode;
	TransactionId xid;
}	PendingRelXactDelete;

typedef struct PendingRelXactDeleteArray
{
	size_t		count;
	PendingRelXactDelete array[FLEXIBLE_ARRAY_MEMBER];
}	PendingRelXactDeleteArray;

extern void PdlXLogInsert(void);

extern void PdlRedoAdd(PendingRelXactDelete * pd);

extern void PdlRedoXLogRecord(XLogRecord *record);

extern void PdlRedoRemoveTree(TransactionId xid,
				  TransactionId *sub_xids, int nsubxacts);

extern void PdlRedoDropFiles(void);

#endif   /* STORAGE_PENDING_DELETES_REDO_H */
