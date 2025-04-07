/*-------------------------------------------------------------------------
 *
 * storage_pending_deletes.c
 *	  code to support collecting of pending deletes from backends
 *
 * Copyright (c) 2025 Greengage Community
 *
 * IDENTIFICATION
 *	  src/backend/catalog/storage_pending_deletes.c
 *
 *-------------------------------------------------------------------------
 */
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
