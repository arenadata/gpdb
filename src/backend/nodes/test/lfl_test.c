#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdio.h>
#include "cmockery.h"

#include "postgres.h"

#include "nodes/pg_list.h"
#include "utils/memutils.h"

static void
validate_list_single(lock_free_list *ls, List *cmp_ls)
{
	ListCell *compare_cell = list_head(cmp_ls);
	lock_free_list_cell *cell;
	int len = 0;
	for (cell = lock_free_list_first(ls);
		 cell != NULL;
		 cell = lock_free_list_next(cell))
	{
		assert_true(compare_cell != NULL);
		assert_true(len < length(cmp_ls));
		assert_int_equal((uint32_t) lock_free_list_get_value(cell), lfirst_int(compare_cell));
		compare_cell = lnext(compare_cell);
		len++;
	}
	assert_int_equal(len, length(cmp_ls));
}

static void
validate_list(lock_free_list *ls, List *cmp_ls)
{
	/*
	 * Always validate twice, as the reader does cleanup of deleted cells 
	 * on its first iteration. On the second run we ensure that the cleanup
	 * didn't break anything.
	 */
	validate_list_single(ls, cmp_ls);
	validate_list_single(ls, cmp_ls);
}


static void
test_lfl_1(void **state)
{
	lock_free_list_cell *c;
	FILE * fout = fopen("/tmp/test_flf_1", "w");
	if (!fout)
		return;

	/* check list is empty */
	lock_free_list *ls = lock_free_list_create();
	List *cmp_ls = NIL;
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	/* check one element list */
	c = lock_free_list_push(ls, 0x1);
	cmp_ls = lcons_int(0x1, cmp_ls);
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	/* check list after deletion of the only element */
	lock_free_list_delete(c);
	cmp_ls = list_delete_int(cmp_ls, 0x1);
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	/* check adding of elements after deletion */
	c = lock_free_list_push(ls, 0x2);
	cmp_ls = lcons_int(0x2, cmp_ls);
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	c = lock_free_list_push(ls, 0x3);
	cmp_ls = lcons_int(0x3, cmp_ls);
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_destroy(ls);

	fclose(fout);
}

static void
test_lfl_2(void **state)
{
	lock_free_list_cell *c;
	FILE * fout = fopen("/tmp/test_flf_2", "w");
	if (!fout)
		return;

	lock_free_list *ls = lock_free_list_create();
	List *cmp_ls = NIL;

	/* check deletion of tail element */
	c = lock_free_list_push(ls, 0x1);
	cmp_ls = lcons_int(0x1, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_push(ls, 0x2);
	cmp_ls = lcons_int(0x2, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_delete(c);
	cmp_ls = list_delete_int(cmp_ls, 0x1);

	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_destroy(ls);

	fclose(fout);
}

static void
test_lfl_3(void **state)
{
	lock_free_list_cell *c;
	FILE * fout = fopen("/tmp/test_flf_3", "w");
	if (!fout)
		return;

	lock_free_list *ls = lock_free_list_create();
	List *cmp_ls = NIL;

	/* check deletion of middle element */
	lock_free_list_push(ls, 0x1);
	cmp_ls = lcons_int(0x1, cmp_ls);

	lock_free_list_dump(fout, ls);

	c = lock_free_list_push(ls, 0x2);
	cmp_ls = lcons_int(0x2, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_push(ls, 0x3);
	cmp_ls = lcons_int(0x3, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_delete(c);
	cmp_ls = list_delete_int(cmp_ls, 0x2);

	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_destroy(ls);

	fclose(fout);
}

static void
test_lfl_4(void **state)
{
	FILE * fout = fopen("/tmp/test_flf_4", "w");
	if (!fout)
		return;

	lock_free_list *ls = lock_free_list_create();
	List *cmp_ls = NIL;

	/* check multiple deletions */
	lock_free_list_cell *c[20] = {0};
	for (int i = 0; i < 20; i++)
	{
		c[i] = lock_free_list_push(ls, i);
		cmp_ls = lcons_int(i, cmp_ls);
	}

	lock_free_list_dump(fout, ls);

	validate_list(ls, cmp_ls);

	lock_free_list_delete(c[0]);
	cmp_ls = list_delete_int(cmp_ls, 0);
	lock_free_list_delete(c[1]);
	cmp_ls = list_delete_int(cmp_ls, 1);

	lock_free_list_delete(c[6]);
	cmp_ls = list_delete_int(cmp_ls, 6);
	lock_free_list_delete(c[7]);
	cmp_ls = list_delete_int(cmp_ls, 7);
	lock_free_list_delete(c[8]);
	cmp_ls = list_delete_int(cmp_ls, 8);

	lock_free_list_delete(c[14]);
	cmp_ls = list_delete_int(cmp_ls, 14);

	lock_free_list_delete(c[18]);
	cmp_ls = list_delete_int(cmp_ls, 18);
	lock_free_list_delete(c[19]);
	cmp_ls = list_delete_int(cmp_ls, 19);

	lock_free_list_dump(fout, ls);

	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_destroy(ls);

	fclose(fout);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_lfl_1),
		unit_test(test_lfl_2),
		unit_test(test_lfl_3),
		unit_test(test_lfl_4)
	};

	MemoryContextInit();

	return run_tests(tests);
}
