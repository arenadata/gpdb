#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
//#include "utils/hsearch.h"
#include "access/hash.h"
//#include "utils/memutils.h"
//#include "access/xlog.h"
//#include "storage/smgr.h"

//bool IsUnderPostmaster = true;
/*
#include "nodes/pg_list.h"
#include "utils/dsa.h"
#include "storage/ipc.h"
#include "catalog/storage.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/md.h"*/

void
hashbucketcleanup(Relation rel, Bucket cur_bucket, Buffer bucket_buf, BlockNumber bucket_blkno, BufferAccessStrategy bstrategy, uint32 maxbucket, uint32 highmask, uint32 lowmask, double * tuples_removed, double * num_index_tuples, bool split_cleanup, IndexBulkDeleteCallback callback, void * callback_state)
{
	check_expected(rel);
	check_expected(cur_bucket);
	check_expected(bucket_buf);
	check_expected(bucket_blkno);
	check_expected(bstrategy);
	check_expected(maxbucket);
	check_expected(highmask);
	check_expected(lowmask);
	check_expected(tuples_removed);
	check_expected(num_index_tuples);
	check_expected(split_cleanup);
	check_expected(callback);
	check_expected(callback_state);
	optional_assignment(tuples_removed);
	optional_assignment(num_index_tuples);
	optional_assignment(callback_state);
	mock();
}

const Oid fmgr_last_builtin_oid; /* highest function OID in table */
const uint16 fmgr_builtin_oid_index[1];

//#include "../storage_pending.c"
#include "catalog/storage_pending.h"

static void
test_dummy(void **state)
{
	bool found = true;
	will_return(ShmemInitStruct, NULL);
	will_assign_value(ShmemInitStruct, foundPtr, found);

	expect_any_count(ShmemInitStruct, name, 1);
	expect_any_count(ShmemInitStruct, size, 1);
	expect_any_count(ShmemInitStruct, foundPtr, 1);

	PendingDeleteShmemInit();
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(
			test_dummy)
	};

	return run_tests(tests);
}
