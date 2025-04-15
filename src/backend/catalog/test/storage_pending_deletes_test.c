/*-------------------------------------------------------------------------
 *
 * storage_pending_deletes_test.c
 *	  code to test functionality from storage_pending_deletes.c
 *
 * Copyright (c) 2025 Greengage Community
 *
 *	  src/backend/catalog/test/storage_pending_deletes_test.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"

#include "access/clog.h"
#include "access/transam.h"
#include "catalog/storage_pending_deletes.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define TEST_TABLESPACE_OID1	11111
#define TEST_TABLESPACE_OID2	11112

#define TEST_DB_OID1			11121
#define TEST_DB_OID2			11122

#define TEST_REL_OID1			11211
#define TEST_REL_OID2			11212

#define TEST_XID 10



/*
 * Tests
 */

/*
 * Dump without additions
 */
static void
test_empty(void **state)
{
	assert_true(PdlXLogShmemDump() == NULL);
}

/*
  * Add single pending delete node
 */
static void
test_1(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	dsa_pointer p = PdlShmemAdd(&relnode, TEST_XID);
	PendingRelXactDeleteArray *arr = PdlXLogShmemDump();

	assert_true(arr != NULL);
	assert_int_equal(arr->count, 1);
	assert_int_equal(arr->array[0].xid, TEST_XID);
	assert_memory_equal(&arr->array[0].relnode, &relnode, sizeof(relnode));
	
/* clean up */
	pfree(arr);
	PdlShmemRemove(p);
}

/*
  * Add nodes and remove the first one before dump
 */
static void
test_remove_fisrt(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	dsa_pointer p[5];
	p[0] = PdlShmemAdd(&relnode, TEST_XID);
	relnode.node.spcNode = TEST_TABLESPACE_OID2;
	p[1] = PdlShmemAdd(&relnode, TEST_XID + 1);
	relnode.node.dbNode = TEST_DB_OID2;
	p[2] = PdlShmemAdd(&relnode, TEST_XID + 3);
	relnode.node.relNode = TEST_REL_OID2;
	p[3] = PdlShmemAdd(&relnode, TEST_XID);
	relnode.node.relNode = TEST_REL_OID1;
	p[4] = PdlShmemAdd(&relnode, TEST_XID);

	PdlShmemRemove(p[0]);

	PendingRelXactDeleteArray *arr = PdlXLogShmemDump();
	assert_true(arr != NULL);
	assert_int_equal(arr->count, 4);
	
/* clean up */
	for (int i = 1; i < ARRAY_SIZE(p); i++)
		PdlShmemRemove(p[i]);
}

/*
  * Add nodes and remove a node from the middle before dump
 */
static void
test_remove_middle(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	dsa_pointer p[4];
	p[0] = PdlShmemAdd(&relnode, TEST_XID);
	relnode.node.spcNode = TEST_TABLESPACE_OID2;
	p[1] = PdlShmemAdd(&relnode, TEST_XID + 1);
	
	relnode.node.dbNode = TEST_DB_OID2;
	dsa_pointer p_middle = PdlShmemAdd(&relnode, TEST_XID);

	relnode.node.relNode = TEST_REL_OID2;
	p[2] = PdlShmemAdd(&relnode, TEST_XID + 3);
	relnode.node.relNode = TEST_REL_OID1;
	p[3] = PdlShmemAdd(&relnode, TEST_XID);

	PdlShmemRemove(p_middle);

	PendingRelXactDeleteArray *arr = PdlXLogShmemDump();
	assert_true(arr != NULL);
	assert_int_equal(arr->count, 4);
	
/* clean up */
	for (int i = 0; i < ARRAY_SIZE(p); i++)
		PdlShmemRemove(p[i]);
}

/*
  * Add nodes and remove the lastest one before dump
 */
static void
test_remove_lastest(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	dsa_pointer p[4];
	p[0] = PdlShmemAdd(&relnode, TEST_XID);
	relnode.node.spcNode = TEST_TABLESPACE_OID2;
	p[1] = PdlShmemAdd(&relnode, TEST_XID + 1);
	relnode.node.dbNode = TEST_DB_OID2;
	p[2] = PdlShmemAdd(&relnode, TEST_XID + 3);
	relnode.node.relNode = TEST_REL_OID2;
	p[3] = PdlShmemAdd(&relnode, TEST_XID);

	relnode.node.relNode = TEST_REL_OID1;
	dsa_pointer p_lastest = PdlShmemAdd(&relnode, TEST_XID);
	PdlShmemRemove(p_lastest);

	PendingRelXactDeleteArray *arr = PdlXLogShmemDump();
	assert_true(arr != NULL);
	assert_int_equal(arr->count, 4);
	
/* clean up */
	for (int i = 0; i < ARRAY_SIZE(p); i++)
		PdlShmemRemove(p[i]);
}

/*
  * Add node with invalid transaction id
 */
static void
test_invalid_xid(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	assert_false(DsaPointerIsValid(
					PdlShmemAdd(&relnode, InvalidTransactionId)));
	assert_true(PdlXLogShmemDump() == NULL);
}

/*
  * Add node when MyBackendId is invalid
 */
static void
test_invalid_backend(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	BackendId old = MyBackendId;
	MyBackendId = InvalidBackendId;

	assert_false(DsaPointerIsValid(
					PdlShmemAdd(&relnode, InvalidTransactionId)));
	assert_true(PdlXLogShmemDump() == NULL);

/* clean up */
	MyBackendId = old;
}


/*
  * Add node when Mode == BootstrapProcessing
 */
static void
test_invalid_mode(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	ProcessingMode old = Mode;
	Mode = BootstrapProcessing;

	assert_false(DsaPointerIsValid(
					PdlShmemAdd(&relnode, InvalidTransactionId)));
	assert_true(PdlXLogShmemDump() == NULL);

/* clean up */
	Mode = old;
}

/*
  * Add node when tracking is disabled
 */
static void
test_tracking_disabled(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	bool old = gp_track_pending_delete;
	gp_track_pending_delete = false;

	assert_false(DsaPointerIsValid(
					PdlShmemAdd(&relnode, InvalidTransactionId)));
	assert_true(PdlXLogShmemDump() == NULL);

/* clean up */
	gp_track_pending_delete = old;
}

/*
  * Add node when dynamic_shared_memory_type == DSM_IMPL_NONE
 */
static void
test_shmem_type(void **state)
{
	RelFileNodePendingDelete relnode = {
		.node = {
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	int old = dynamic_shared_memory_type;
	dynamic_shared_memory_type = DSM_IMPL_NONE;

	assert_false(DsaPointerIsValid(
					PdlShmemAdd(&relnode, InvalidTransactionId)));
	assert_true(PdlXLogShmemDump() == NULL);

/* clean up */
	dynamic_shared_memory_type = old;
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test_empty),
			unit_test(test_1),
			unit_test(test_remove_fisrt),
			unit_test(test_remove_middle),
			unit_test(test_remove_lastest),
			unit_test(test_invalid_xid),
			unit_test(test_invalid_backend),
			unit_test(test_invalid_mode),
			unit_test(test_tracking_disabled),
			unit_test(test_shmem_type)
	};

	MemoryContextInit();

	gp_track_pending_delete = true;
	dynamic_shared_memory_type = DSM_IMPL_POSIX;
	DataDir=".";
	MaxBackends = 5;

	PGShmemHeader *shim = NULL;
	InitShmemAccess(PGSharedMemoryCreate(10000000, 5432, &shim));
	InitShmemAllocation();
	CreateLWLocks();
	InitShmemIndex();
	dsm_postmaster_startup(shim);

	PdlShmemInit();

	IsUnderPostmaster = true;
	MyBackendId = 1;
	PGPROC proc = { .backendId = MyBackendId };
	MyProc = &proc;
	return run_tests(tests);
}
