#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "utils/memutils.h"

#include "../storage_pending.c"
//#include "catalog/storage_pending.h"

static PendingDeleteListNode pdl_node1;
static PendingDeleteListNode pdl_node2;
static PendingDeleteListNode pdl_node3;
static dsa_pointer dsa_ptr1 = 1;
static dsa_pointer dsa_ptr2 = 2;
static dsa_pointer dsa_ptr3 = 3;

static void
setup_globals(void **state)
{
	IsUnderPostmaster = true;
	pendingDeleteDsa = (dsa_area*) 1;

	MemoryContextInit();
	PendingDeleteShmem = palloc0(sizeof(PendingDeleteShmemStruct));
}

static void
teardown_globals(void **state)
{
	//free(PendingDeleteShmem);
}

static PendingDeleteListNode* dsa_ptr_to_node(dsa_pointer dsa_ptr)
{
	if (dsa_ptr == dsa_ptr1)
		return &pdl_node1;
	else if (dsa_ptr == dsa_ptr2)
		return &pdl_node2;
	else if (dsa_ptr == dsa_ptr3)
		return &pdl_node3;

	return NULL;
}

extern void* __wrap_dsa_get_address(dsa_area * area, dsa_pointer dp);
extern void* __real_dsa_get_address(dsa_area * area, dsa_pointer dp);
void *
__wrap_dsa_get_address(dsa_area * area, dsa_pointer dp)
{
	will_return(dsa_get_address, dsa_ptr_to_node(dp));
	expect_any(dsa_get_address, area);
	expect_any(dsa_get_address, dp);
	return __real_dsa_get_address(area, dp);
}

static void add_node(dsa_pointer dsa_ptr, TransactionId xid)
{
	RelFileNodePendingDelete relnode;
	PendingDeleteListNode *pdl_node = dsa_ptr_to_node(dsa_ptr);
	
	will_return(dsa_allocate_extended, dsa_ptr);
	expect_any(dsa_allocate_extended, area);
	expect_any(dsa_allocate_extended, size);
	expect_any(dsa_allocate_extended, flags);

	will_be_called(LWLockAcquire);
	will_be_called(LWLockRelease);
	expect_any(LWLockAcquire, lock);
	expect_any(LWLockAcquire, mode);
	expect_any(LWLockRelease, lock);

	PendingDeleteShmemAdd(&relnode, xid, &dsa_ptr);

	assert_true(pdl_node->xrelnode.xid == xid);
	assert_memory_equal(&pdl_node->xrelnode.relnode, &relnode, sizeof(relnode));
}

static void remove_node(dsa_pointer dsa_ptr)
{
	will_be_called(LWLockAcquire);
	will_be_called(LWLockRelease);
	expect_any(LWLockAcquire, lock);
	expect_any(LWLockAcquire, mode);
	expect_any(LWLockRelease, lock);

	will_be_called(dsa_free);
	expect_any(dsa_free, area);
	expect_any(dsa_free, dp);

	PendingDeleteShmemRemove(&dsa_ptr);
}

static void
test__add_node_to_shmem(void **state)
{
	assert_true(PendingDeleteShmem->pdl_head == InvalidDsaPointer);
	assert_int_equal(PendingDeleteShmem->pdl_count, 0);
	
	add_node(dsa_ptr1, 1);
	assert_true(pdl_node1.next == InvalidDsaPointer);
	assert_true(pdl_node1.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr1);
	assert_int_equal(PendingDeleteShmem->pdl_count, 1);

	add_node(dsa_ptr2, 2);
	assert_true(pdl_node1.next == InvalidDsaPointer);
	assert_true(pdl_node1.prev == dsa_ptr2);
	assert_true(pdl_node2.next == dsa_ptr1);
	assert_true(pdl_node2.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr2);
	assert_int_equal(PendingDeleteShmem->pdl_count, 2);

	add_node(dsa_ptr3, 3);
	assert_true(pdl_node1.next == InvalidDsaPointer);
	assert_true(pdl_node1.prev == dsa_ptr2);
	assert_true(pdl_node2.next == dsa_ptr1);
	assert_true(pdl_node2.prev == dsa_ptr3);
	assert_true(pdl_node3.next == dsa_ptr2);
	assert_true(pdl_node3.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr3);
	assert_int_equal(PendingDeleteShmem->pdl_count, 3);
}

static void
test__remove_node_from_shmem(void **state)
{
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr3);
	assert_int_equal(PendingDeleteShmem->pdl_count, 3);
	
	remove_node(dsa_ptr2);
	assert_true(pdl_node1.next == InvalidDsaPointer);
	assert_true(pdl_node1.prev == dsa_ptr3);
	assert_true(pdl_node3.next == dsa_ptr1);
	assert_true(pdl_node3.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr3);
	assert_int_equal(PendingDeleteShmem->pdl_count, 2);

	remove_node(dsa_ptr1);
	assert_true(pdl_node3.next == InvalidDsaPointer);
	assert_true(pdl_node3.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr3);
	assert_int_equal(PendingDeleteShmem->pdl_count, 1);

	remove_node(dsa_ptr3);
	assert_true(PendingDeleteShmem->pdl_head == InvalidDsaPointer);
	assert_int_equal(PendingDeleteShmem->pdl_count, 0);
}

static void
test__dump_shmem_to_array(void **state)
{
	Size		size;
	PendingRelXactDeleteArray *xrelnode_array;
	
	will_be_called(LWLockAcquire);
	will_be_called(LWLockRelease);
	expect_any(LWLockAcquire, lock);
	expect_any(LWLockAcquire, mode);
	expect_any(LWLockRelease, lock);

	xrelnode_array = PendingDeleteXLogShmemDump(&size);

	assert_int_equal(xrelnode_array->count, 3);

	assert_memory_equal(&xrelnode_array->array[0], &pdl_node3.xrelnode, sizeof(pdl_node3.xrelnode));
	assert_memory_equal(&xrelnode_array->array[1], &pdl_node2.xrelnode, sizeof(pdl_node2.xrelnode));
	assert_memory_equal(&xrelnode_array->array[2], &pdl_node1.xrelnode, sizeof(pdl_node1.xrelnode));
}

static void
test__dump_empty_shmem_to_array(void **state)
{
	Size		size;
	PendingRelXactDeleteArray *xrelnode_array;
	
	will_be_called(LWLockAcquire);
	will_be_called(LWLockRelease);
	expect_any(LWLockAcquire, lock);
	expect_any(LWLockAcquire, mode);
	expect_any(LWLockRelease, lock);

	xrelnode_array = PendingDeleteXLogShmemDump(&size);

	assert_true(xrelnode_array == NULL);
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test_setup(setup_globals, setup_globals),
		unit_test(test__add_node_to_shmem),
		unit_test(test__dump_shmem_to_array),
		unit_test(test__remove_node_from_shmem),
		unit_test(test__dump_empty_shmem_to_array),
		unit_test_teardown(teardown_globals, teardown_globals),
	};

	return run_tests(tests);
}
