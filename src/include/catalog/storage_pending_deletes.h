#include "catalog/storage_pending_deletes_redo.h"

/*
 * This function collects info about pending deletes from all backends and
 * returns the accumulated result.
 * Note: the returned result is always palloc'ed. Caller is responsible for
 * freeing it.
 */
PendingRelXactDeleteArray *PdlXLogShmemDump(Size *size);
