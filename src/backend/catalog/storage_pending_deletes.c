#include "catalog/storage_pending_deletes.h"

PendingRelXactDeleteArray *
PdlXLogShmemDump(Size *size)
{
	/*
	 * For now it is only a stub. Should be implemented in scope of
	 * ADBDEV-7303.
	 */
	*size = 0;
	return NULL;
}
