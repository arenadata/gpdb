#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"

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
	PendingDeleteShmem = calloc(1, sizeof(PendingDeleteShmemStruct));
}

static void
teardown_globals(void **state)
{
	free(PendingDeleteShmem);
}

static void add_node(PendingDeleteListNode *pdl_node, dsa_pointer dsa_ptr, TransactionId xid, PendingDeleteListNode *prev_pdl_node)
{
	RelFileNodePendingDelete relnode;
	
	will_return(dsa_allocate_extended, dsa_ptr);
	expect_any_count(dsa_allocate_extended, area, 1);
	expect_any_count(dsa_allocate_extended, size, 1);
	expect_any_count(dsa_allocate_extended, flags, 1);

	will_return_count(dsa_get_address, pdl_node, 2);
	expect_any_count(dsa_get_address, area, 2);
	expect_any_count(dsa_get_address, dp, 2);

	if (prev_pdl_node)
	{
		will_return_count(dsa_get_address, prev_pdl_node, 1);
		expect_any_count(dsa_get_address, area, 1);
		expect_any_count(dsa_get_address, dp, 1);
	}

	will_be_called_count(LWLockAcquire, 1);
	will_be_called_count(LWLockRelease, 1);
	expect_any_count(LWLockAcquire, lock, 1);
	expect_any_count(LWLockAcquire, mode, 1);
	expect_any_count(LWLockRelease, lock, 1);

	PendingDeleteShmemAdd(&relnode, xid, &dsa_ptr);

	assert_true(pdl_node->xrelnode.xid == xid);
	assert_memory_equal(&pdl_node->xrelnode.relnode, &relnode, sizeof(relnode));
}

static void remove_node(PendingDeleteListNode *pdl_node, dsa_pointer dsa_ptr, PendingDeleteListNode *next_pdl_node, PendingDeleteListNode *prev_pdl_node)
{
	will_return_count(dsa_get_address, pdl_node, 1);
	expect_any_count(dsa_get_address, area, 1);
	expect_any_count(dsa_get_address, dp, 1);

	if (next_pdl_node)
	{
		will_return_count(dsa_get_address, next_pdl_node, 1);
		expect_any_count(dsa_get_address, area, 1);
		expect_any_count(dsa_get_address, dp, 1);
	}

	if (prev_pdl_node)
	{
		will_return_count(dsa_get_address, prev_pdl_node, 1);
		expect_any_count(dsa_get_address, area, 1);
		expect_any_count(dsa_get_address, dp, 1);
	}

	will_be_called_count(LWLockAcquire, 1);
	will_be_called_count(LWLockRelease, 1);
	expect_any_count(LWLockAcquire, lock, 1);
	expect_any_count(LWLockAcquire, mode, 1);
	expect_any_count(LWLockRelease, lock, 1);

	will_be_called_count(dsa_free, 1);
	expect_any_count(dsa_free, area, 1);
	expect_any_count(dsa_free, dp, 1);

	PendingDeleteShmemRemove(&dsa_ptr);
}

static void
test__add_node_to_shmem(void **state)
{
	add_node(&pdl_node1, dsa_ptr1, 1, NULL);
	assert_true(pdl_node1.next == InvalidDsaPointer);
	assert_true(pdl_node1.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr1);
	assert_int_equal(PendingDeleteShmem->pdl_count, 1);

	add_node(&pdl_node2, dsa_ptr2, 2, &pdl_node1);
	assert_true(pdl_node1.next == InvalidDsaPointer);
	assert_true(pdl_node1.prev == dsa_ptr2);
	assert_true(pdl_node2.next == dsa_ptr1);
	assert_true(pdl_node2.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr2);
	assert_int_equal(PendingDeleteShmem->pdl_count, 2);

	add_node(&pdl_node3, dsa_ptr3, 3, &pdl_node2);
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
	remove_node(&pdl_node2, dsa_ptr2, &pdl_node1, &pdl_node3);
	assert_true(pdl_node1.next == InvalidDsaPointer);
	assert_true(pdl_node1.prev == dsa_ptr3);
	assert_true(pdl_node3.next == dsa_ptr1);
	assert_true(pdl_node3.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr3);
	assert_int_equal(PendingDeleteShmem->pdl_count, 2);

	remove_node(&pdl_node1, dsa_ptr1, NULL, &pdl_node3);
	assert_true(pdl_node3.next == InvalidDsaPointer);
	assert_true(pdl_node3.prev == InvalidDsaPointer);
	assert_true(PendingDeleteShmem->pdl_head == dsa_ptr3);
	assert_int_equal(PendingDeleteShmem->pdl_count, 1);

	remove_node(&pdl_node3, dsa_ptr3, NULL, NULL);
	assert_true(PendingDeleteShmem->pdl_head == InvalidDsaPointer);
	assert_int_equal(PendingDeleteShmem->pdl_count, 0);
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test_setup(setup_globals, setup_globals),
		unit_test(test__add_node_to_shmem),
		unit_test(test__remove_node_from_shmem),
		unit_test_teardown(teardown_globals, teardown_globals),
	};

	return run_tests(tests);
}
