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

#include "catalog/storage_pending_deletes.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"

enum
{
	TEST_TABLESPACE_OID1 = 11111,
	TEST_TABLESPACE_OID2 = 11112,

	TEST_DB_OID1 = 11121,
	TEST_DB_OID2 = 11122,

	TEST_REL_OID1 = 11211,
	TEST_REL_OID2 = 11212,

	TEST_XID1 = 10,
	TEST_XID2 = TEST_XID1 + 1,
	TEST_XID3 = TEST_XID1 + 3,
	TEST_XID4 = TEST_XID1 + 8,
};

/* Don't try to read a non-existent postmaster.pid file */
void		__wrap_AddToDataDirLockFile(int target_line, const char *str);
void
__wrap_AddToDataDirLockFile(int target_line, const char *str)
{
}


/* Function to sort array of PendingRelXactDelete using qsort */
static int
cmp_pdl(const void *p1, const void *p2)
{
	return memcmp(p1, p2, sizeof(PendingRelXactDelete));
}

/* Check and clean up when PdlXLogShmemDump returns not NULL */
static void 
check_and_clean(PendingRelXactDeleteArray *arr,
				PendingRelXactDelete *expected, Size expectedCnt,
				dsa_pointer *p, Size pCnt)
{
	assert_true(arr != NULL);

	assert_int_equal(arr->count, expectedCnt);

	/* Order doesn't matter */
	qsort (expected,   expectedCnt, sizeof(*expected), cmp_pdl);
	qsort (arr->array, expectedCnt, sizeof(*expected), cmp_pdl);
	assert_memory_equal(arr->array, expected, expectedCnt*sizeof(*expected));

	/* Clean up */
	pfree(arr);
	for (int i = 0; i < pCnt; i++)
		PdlShmemRemove(p[i]);

	/* Check whether cleanup is ok */
	assert_true(PdlXLogShmemDump() == NULL);
}

/* Call PdlXLogShmemDump(), check its result and clean up */
static void 
dump_and_clean(PendingRelXactDelete *expected, Size expectedCnt,
			   dsa_pointer *p, Size pCnt)
{
	check_and_clean(PdlXLogShmemDump(), expected, expectedCnt, p, pCnt);
}


/* Dump without additions */
static void
test_empty(void **state)
{
	assert_true(PdlXLogShmemDump() == NULL);
}

/* Add single pending delete node */
static void
test_1(void **state)
{
	const RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		},
		.relstorage = RELSTORAGE_HEAP
	};

	dsa_pointer p = PdlShmemAdd(&relnode, TEST_XID1);
	
	PendingRelXactDelete expected = 
	{
		.relnode = relnode,
		.xid = TEST_XID1
	};

	dump_and_clean(&expected, 1, &p, 1);
}

/* Add nodes and remove the first one before dump */
static void
test_remove_fisrt(void **state)
{
	RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	dsa_pointer p_first = PdlShmemAdd(&relnode, TEST_XID1);

	dsa_pointer p[4];
	
	relnode.node.spcNode = TEST_TABLESPACE_OID2;
	p[0] = PdlShmemAdd(&relnode, TEST_XID2);

	relnode.node.dbNode = TEST_DB_OID2;
	p[1] = PdlShmemAdd(&relnode, TEST_XID3);

	relnode.node.relNode = TEST_REL_OID2;
	p[2] = PdlShmemAdd(&relnode, TEST_XID1);

	relnode.node.spcNode = TEST_TABLESPACE_OID1;
	p[3] = PdlShmemAdd(&relnode, TEST_XID1);

	PdlShmemRemove(p_first);

	PendingRelXactDelete expected[] = 
	{
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID1, TEST_REL_OID1}},
			.xid = TEST_XID2
		},
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID2, TEST_REL_OID1}},
			.xid = TEST_XID3
		},
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID2, TEST_REL_OID2}},
			.xid = TEST_XID1
		},
		{
			.relnode = {{TEST_TABLESPACE_OID1, TEST_DB_OID2, TEST_REL_OID2}},
			.xid = TEST_XID1
		},
	};

	dump_and_clean(expected, ARRAY_SIZE(expected), p, ARRAY_SIZE(p));
}

/* Add nodes and remove a node from the middle before dump */
static void
test_remove_middle(void **state)
{
	RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	dsa_pointer p[4];

	p[0] = PdlShmemAdd(&relnode, TEST_XID1);

	relnode.node.spcNode = TEST_TABLESPACE_OID2;
	p[1] = PdlShmemAdd(&relnode, TEST_XID2);

	relnode.node.dbNode = TEST_DB_OID2;
	dsa_pointer p_middle = PdlShmemAdd(&relnode, TEST_XID1);

	relnode.node.relNode = TEST_REL_OID2;
	p[2] = PdlShmemAdd(&relnode, TEST_XID3);

	relnode.node.spcNode = TEST_TABLESPACE_OID1;
	p[3] = PdlShmemAdd(&relnode, TEST_XID1);

	PdlShmemRemove(p_middle);

	PendingRelXactDelete expected[] = 
	{
		{
			.relnode = {{TEST_TABLESPACE_OID1, TEST_DB_OID2, TEST_REL_OID2}},
			.xid = TEST_XID1
		},
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID2, TEST_REL_OID2}},
			.xid = TEST_XID3
		},
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID1, TEST_REL_OID1}},
			.xid = TEST_XID2
		},
		{
			.relnode = {{TEST_TABLESPACE_OID1, TEST_DB_OID1, TEST_REL_OID1}},
			.xid = TEST_XID1
		},
	};

	dump_and_clean(expected, ARRAY_SIZE(expected), p, ARRAY_SIZE(p));
}

/* Add nodes and remove the last one before dump */
static void
test_remove_last(void **state)
{
	RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	dsa_pointer p[4];

	p[0] = PdlShmemAdd(&relnode, TEST_XID1);

	relnode.node.spcNode = TEST_TABLESPACE_OID2;
	p[1] = PdlShmemAdd(&relnode, TEST_XID2);

	relnode.node.dbNode = TEST_DB_OID2;
	p[2] = PdlShmemAdd(&relnode, TEST_XID3);

	relnode.node.relNode = TEST_REL_OID2;
	p[3] = PdlShmemAdd(&relnode, TEST_XID1);

	relnode.node.relNode = TEST_REL_OID1;
	dsa_pointer p_last = PdlShmemAdd(&relnode, TEST_XID1);

	PdlShmemRemove(p_last);

	PendingRelXactDelete expected[] = 
	{
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID2, TEST_REL_OID2}},
			.xid = TEST_XID1
		},
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID2, TEST_REL_OID1}},
			.xid = TEST_XID3
		},
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID1, TEST_REL_OID1}},
			.xid = TEST_XID2
		},
		{
			.relnode = {{TEST_TABLESPACE_OID1, TEST_DB_OID1, TEST_REL_OID1}},
			.xid = TEST_XID1
		},
	};

	dump_and_clean(expected, ARRAY_SIZE(expected), p, ARRAY_SIZE(p));
}

/* Add node with invalid transaction id */
static void
test_invalid_xid(void **state)
{
	const RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	assert_false(DsaPointerIsValid(
							   PdlShmemAdd(&relnode, InvalidTransactionId)));
	assert_true(PdlXLogShmemDump() == NULL);
}

/* Add node when MyBackendId is invalid */
static void
test_invalid_backend(void **state)
{
	const RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	BackendId	old = MyBackendId;

	MyBackendId = InvalidBackendId;

	assert_false(DsaPointerIsValid(PdlShmemAdd(&relnode, TEST_XID1)));
	assert_true(PdlXLogShmemDump() == NULL);

	/* Clean up */
	MyBackendId = old;
}

/* Add node when Mode == BootstrapProcessing */
static void
test_invalid_mode(void **state)
{
	const RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	ProcessingMode old = Mode;

	Mode = BootstrapProcessing;

	assert_false(DsaPointerIsValid(PdlShmemAdd(&relnode, TEST_XID1)));
	assert_true(PdlXLogShmemDump() == NULL);

	/* Clean up */
	Mode = old;
}

/* Add node when tracking is disabled */
static void
test_tracking_disabled(void **state)
{
	const RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	bool		old = gp_track_pending_delete;

	gp_track_pending_delete = false;

	assert_false(DsaPointerIsValid(PdlShmemAdd(&relnode, TEST_XID1)));
	assert_true(PdlXLogShmemDump() == NULL);

	/* Clean up */
	gp_track_pending_delete = old;
}

/* Add node when dynamic_shared_memory_type == DSM_IMPL_NONE */
static void
test_shmem_type(void **state)
{
	const RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	int			old = dynamic_shared_memory_type;

	dynamic_shared_memory_type = DSM_IMPL_NONE;

	assert_false(DsaPointerIsValid(PdlShmemAdd(&relnode, TEST_XID1)));
	assert_true(PdlXLogShmemDump() == NULL);

	/* Clean up */
	dynamic_shared_memory_type = old;
}

/* Add nodes for two backends */
static void
test_2_backends(void **state)
{
	RelFileNodePendingDelete relnode =
	{
		.node =
		{
			.spcNode = TEST_TABLESPACE_OID1,
			.dbNode  = TEST_DB_OID1,
			.relNode = TEST_REL_OID1
		}
	};

	dsa_pointer p[5];

	p[0] = PdlShmemAdd(&relnode, TEST_XID1);

	relnode.node.spcNode = TEST_TABLESPACE_OID2;
	p[1] = PdlShmemAdd(&relnode, TEST_XID2);

	relnode.node.dbNode = TEST_DB_OID2;
	p[2] = PdlShmemAdd(&relnode, TEST_XID1);

	BackendId	old = MyBackendId;

	MyBackendId = 3;

	relnode.node.relNode = TEST_REL_OID2;
	p[3] = PdlShmemAdd(&relnode, TEST_XID3);

	relnode.node.spcNode = TEST_TABLESPACE_OID1;
	p[4] = PdlShmemAdd(&relnode, TEST_XID4);

	PendingRelXactDelete expected[] = 
	{
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID2, TEST_REL_OID1}},
			.xid = TEST_XID1
		},
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID1, TEST_REL_OID1}},
			.xid = TEST_XID2
		},
		{
			.relnode = {{TEST_TABLESPACE_OID1, TEST_DB_OID1, TEST_REL_OID1}},
			.xid = TEST_XID1
		},
		{
			.relnode = {{TEST_TABLESPACE_OID1, TEST_DB_OID2, TEST_REL_OID2}},
			.xid = TEST_XID4
		},
		{
			.relnode = {{TEST_TABLESPACE_OID2, TEST_DB_OID2, TEST_REL_OID2}},
			.xid = TEST_XID3
		},
	};

	PendingRelXactDeleteArray	*arr = PdlXLogShmemDump();

	/* 
	 * Clean up.
	 * Elements which were added for backend 3 should be removed
	 * when MyBackendId is 3. Other elements are removed in check_and_clean
	 * after restoring MyBackendId.
	 */
	PdlShmemRemove(p[3]);
	PdlShmemRemove(p[4]);

	MyBackendId = old;

	check_and_clean(arr, expected, ARRAY_SIZE(expected), p, 3);
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
		unit_test(test_remove_last),
		unit_test(test_invalid_xid),
		unit_test(test_invalid_backend),
		unit_test(test_invalid_mode),
		unit_test(test_tracking_disabled),
		unit_test(test_shmem_type),
		unit_test(test_2_backends)
	};

	MemoryContextInit();

	gp_track_pending_delete = true;
	dynamic_shared_memory_type = DSM_IMPL_POSIX;
	DataDir = ".";
	MaxBackends = 5;

	PGShmemHeader *shim = NULL;

	InitShmemAccess(PGSharedMemoryCreate(300000, 6000, &shim));
	InitShmemAllocation();
	CreateLWLocks();
	InitShmemIndex();
	dsm_postmaster_startup(shim);

	PdlShmemInit();

	IsUnderPostmaster = true;
	MyBackendId = 1;

	PGPROC		proc = {.backendId = MyBackendId};

	MyProc = &proc;
	return run_tests(tests);
}
