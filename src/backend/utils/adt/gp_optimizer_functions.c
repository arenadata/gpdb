/*
 * gp_optimizer_functions.c
 *    Defines builtin transformation functions for the optimizer.
 *
 * enable_xform: This function wraps EnableXform.
 *
 * disable_xform: This function wraps DisableXform.
 *
 * gp_opt_version: This function wraps LibraryVersion. 
 *
 * Copyright(c) 2012 - present, EMC/Greenplum
 */

#include "postgres.h"

#include "funcapi.h"
#include "utils/builtins.h"

#include "nodes/pg_list.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "utils/timestamp.h"
#include "libpq-fe.h"
#include "cdb/cdbutil.h"
#include "storage/proc.h"

extern Datum EnableXform(PG_FUNCTION_ARGS);

/*
* Enables transformations in the optimizer.
*/
Datum
enable_xform(PG_FUNCTION_ARGS)
{
#ifdef USE_ORCA
	return EnableXform(fcinfo);
#else
	return CStringGetTextDatum("Server has been compiled without ORCA");
#endif
}

extern Datum DisableXform(PG_FUNCTION_ARGS);

/* 
* Disables transformations in the optimizer.
*/
Datum
disable_xform(PG_FUNCTION_ARGS)
{
#ifdef USE_ORCA
	return DisableXform(fcinfo);
#else
	return CStringGetTextDatum("Server has been compiled without ORCA");
#endif
}

extern Datum LibraryVersion();
	
/*
* Returns the optimizer and gpos library versions.
*/
Datum
gp_opt_version(PG_FUNCTION_ARGS pg_attribute_unused())
{
#ifdef USE_ORCA
	return LibraryVersion();
#else
	return CStringGetTextDatum("Server has been compiled without ORCA");
#endif
}

#if 0
#define assert_true(a) Assert(a)
#define assert_int_equal(a, b) Assert((uint64_t)a == (uint64_t)b)

static void
validate_list_single(lock_free_list *ls, List *cmp_ls)
{
	ListCell *compare_cell = list_head(cmp_ls);
	lock_free_list_cell *cell;
	int len = 0;
	for (cell = lock_free_list_first(ls);
		 cell != NULL;
		 cell = lock_free_list_next(ls, cell))
	{
		assert_true(compare_cell != NULL);
		assert_true(len < list_length(cmp_ls));
		assert_int_equal(lock_free_list_get_value(cell), lfirst_int(compare_cell));
		compare_cell = lnext(compare_cell);
		len++;
	}
	assert_int_equal(len, list_length(cmp_ls));
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
test_lfl_1()
{
	lock_free_list_cell *c;

	char filename[100] = {0};
	sprintf(filename, "/tmp/%s_SEG_%d", __FUNCTION__, GpIdentity.segindex);
	FILE * fout = fopen(filename, "w");

	if (!fout)
		return;

	/* check list is empty */
	uint64 ls_dsa = lock_free_list_create();
	lock_free_list *ls = lock_free_list_get_local_list(ls_dsa);

	List *cmp_ls = NIL;
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	/* check one element list */
	c = lock_free_list_push(ls, (void*)0x1);
	cmp_ls = lcons_int(0x1, cmp_ls);
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	/* check list after deletion of the only element */
	lock_free_list_delete(c);
	cmp_ls = list_delete_int(cmp_ls, 0x1);
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	/* check adding of elements after deletion */
	c = lock_free_list_push(ls, (void*)0x2);
	cmp_ls = lcons_int(0x2, cmp_ls);
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	c = lock_free_list_push(ls, (void*)0x3);
	cmp_ls = lcons_int(0x3, cmp_ls);
	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_destroy(ls_dsa);

	fclose(fout);
}

static void
test_lfl_2()
{
	lock_free_list_cell *c;

	char filename[100] = {0};
	sprintf(filename, "/tmp/%s_SEG_%d", __FUNCTION__, GpIdentity.segindex);
	FILE * fout = fopen(filename, "w");

	if (!fout)
		return;

	uint64 ls_dsa = lock_free_list_create();
	lock_free_list *ls = lock_free_list_get_local_list(ls_dsa);

	List *cmp_ls = NIL;

	/* check deletion of tail element */
	c = lock_free_list_push(ls, (void*)0x1);
	cmp_ls = lcons_int(0x1, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_push(ls, (void*)0x2);
	cmp_ls = lcons_int(0x2, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_delete(c);
	cmp_ls = list_delete_int(cmp_ls, 0x1);

	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_destroy(ls_dsa);

	fclose(fout);
}

static void
test_lfl_3()
{
	lock_free_list_cell *c;

	char filename[100] = {0};
	sprintf(filename, "/tmp/%s_SEG_%d", __FUNCTION__, GpIdentity.segindex);
	FILE * fout = fopen(filename, "w");

	if (!fout)
		return;

	uint64 ls_dsa = lock_free_list_create();
	lock_free_list *ls = lock_free_list_get_local_list(ls_dsa);

	List *cmp_ls = NIL;

	/* check deletion of middle element */
	lock_free_list_push(ls, (void*)0x1);
	cmp_ls = lcons_int(0x1, cmp_ls);

	lock_free_list_dump(fout, ls);

	c = lock_free_list_push(ls, (void*)0x2);
	cmp_ls = lcons_int(0x2, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_push(ls, (void*)0x3);
	cmp_ls = lcons_int(0x3, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_delete(c);
	cmp_ls = list_delete_int(cmp_ls, 0x2);

	validate_list(ls, cmp_ls);

	lock_free_list_dump(fout, ls);

	lock_free_list_destroy(ls_dsa);

	fclose(fout);
}

static void
test_lfl_4()
{
	char filename[100] = {0};
	sprintf(filename, "/tmp/%s_SEG_%d", __FUNCTION__, GpIdentity.segindex);
	FILE * fout = fopen(filename, "w");

	if (!fout)
		return;

	uint64 ls_dsa = lock_free_list_create();
	lock_free_list *ls = lock_free_list_get_local_list(ls_dsa);

	List *cmp_ls = NIL;

	/* check multiple deletions */
	lock_free_list_cell *c[20] = {0};
	for (uint64_t i = 0; i < 20; i++)
	{
		c[i] = lock_free_list_push(ls, (void*)i);
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

	lock_free_list_destroy(ls_dsa);

	fclose(fout);
}

Datum
lfl_test(PG_FUNCTION_ARGS)
{
	char	*testName = TextDatumGetCString(PG_GETARG_DATUM(0));

	if (strcmp(testName, "unit_check") == 0)
	{
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			CdbPgResults cdb_pgresults = {NULL, 0};
			StringInfoData buffer;

			initStringInfo(&buffer);
			appendStringInfo(&buffer,
							 "SELECT lfl_test('%s')",
							 testName);

			CdbDispatchCommand(buffer.data, DF_WITH_SNAPSHOT, &cdb_pgresults);

			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			return CStringGetTextDatum("lfl unit check done");
		}
		else
		{
			test_lfl_1();
			test_lfl_2();
			test_lfl_3();
			test_lfl_4();
			return CStringGetTextDatum("done...");
		}
	}
	return CStringGetTextDatum("No tests performed...");
}
#endif

typedef struct
{
	int start_ts;
	int elem_num;
	int duration_us;
	StringInfo list_type;
	int sess_id;
	int segment; 
} SLflPerfTestSegmentResult;

typedef struct
{
	SLflPerfTestSegmentResult results[1];
} SLflPerfTestCtx;

static Timestamp
DoLflPerfTest(int elements)
{
	dsa_pointer ls_dsa = InvalidDsaPointer;

	if (DsaPointerIsValid(MyProc->pendingDeletesList))
	{
		ls_dsa = MyProc->pendingDeletesList;
	}
	else
	{
		ls_dsa = lock_free_list_create();
		MyProc->pendingDeletesList = ls_dsa;
	}

	Timestamp start = GetCurrentTimestamp();

	lock_free_list *ls = lock_free_list_get_local_list(ls_dsa);

	#define BATCH_SIZE 20
	lock_free_list_cell *c[BATCH_SIZE] = {0};

	int remaining_elements = elements;
	bool del_direction = true;
	while (remaining_elements > 0)
	{
		int batch_remaining_elements = remaining_elements < BATCH_SIZE ? remaining_elements : BATCH_SIZE;

		for (uint64 i = 0; i < batch_remaining_elements; i++)
		{
			c[i] = lock_free_list_push(ls, (void*)(i + GpIdentity.segindex * 1000));
		}

		/* delete elements */
		if (del_direction)
		{
			for (int i = 0; i < batch_remaining_elements; i++)
			{
				lock_free_list_delete(c[i]);
			}
		}
		else
		{
			for (int i = batch_remaining_elements - 1; i >= 0; i--)
			{
				lock_free_list_delete(c[i]);
			}
		}
		del_direction = !del_direction;

		remaining_elements -= batch_remaining_elements;
	}

	Timestamp duration = GetCurrentTimestamp() - start;

	return duration;
}

Datum
lfl_test_perf(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	SLflPerfTestCtx *ctx;

	char *target_list_type = TextDatumGetCString(PG_GETARG_DATUM(0));
	int test_elem_num = PG_GETARG_INT32(1);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		int			nattr = 6;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(nattr);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "start_ts", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "elem_num", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "duration_us", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "list_type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "sess_id", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "segment", INT4OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		int cluster_segment_num = getgpsegmentCount();

		int ctxsize = sizeof(SLflPerfTestCtx) +
				sizeof(SLflPerfTestSegmentResult) * (cluster_segment_num - 1);

		funcctx->user_fctx = palloc(ctxsize);
		ctx = (SLflPerfTestCtx *) funcctx->user_fctx;

		funcctx->max_calls = cluster_segment_num;

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			CdbPgResults cdb_pgresults = {NULL, 0};
			StringInfoData buffer;

			initStringInfo(&buffer);
			appendStringInfo(&buffer,
							 "SELECT * FROM lfl_test_perf('%s', %d)",
							 target_list_type, test_elem_num);

			CdbDispatchCommand(buffer.data, DF_WITH_SNAPSHOT, &cdb_pgresults);

			if (cdb_pgresults.numResults == 0)
				elog(ERROR, "segments didn't return results for lfl test");

			for (int i = 0; i < cdb_pgresults.numResults; i++)
			{
				struct pg_result *pg_result = cdb_pgresults.pg_results[i];

				if (PQresultStatus(pg_result) != PGRES_TUPLES_OK)
				{
					cdbdisp_clearCdbPgResults(&cdb_pgresults);
					elog(ERROR, "lfl_test_perf(): resultStatus not tuples_Ok");
				}

				Assert(PQntuples(pg_result) == 1);
				Assert(PQnfields(pg_result) == 6);

				ctx->results[i].start_ts 	= atoi(PQgetvalue(pg_result, 0, 0));
				ctx->results[i].elem_num 	= atoi(PQgetvalue(pg_result, 0, 1));
				ctx->results[i].duration_us = atoi(PQgetvalue(pg_result, 0, 2));
				ctx->results[i].list_type 	= makeStringInfo();
				appendStringInfoString(ctx->results[i].list_type, PQgetvalue(pg_result, 0, 3));
				ctx->results[i].sess_id 	= atoi(PQgetvalue(pg_result, 0, 4));
				ctx->results[i].segment 	= atoi(PQgetvalue(pg_result, 0, 5));
			}

			cdbdisp_clearCdbPgResults(&cdb_pgresults);
		}
		else
			funcctx->max_calls = 1;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	ctx = (SLflPerfTestCtx *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum		values[7];
		bool		nulls[7];
		HeapTuple	tuple;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		if (Gp_role == GP_ROLE_EXECUTE)
		{
			int64 start_ts = GetCurrentTimestamp();// TODO: fix this mess with 64/32bits
			int32 elem_num = test_elem_num;
			int64 duration = DoLflPerfTest(elem_num);
			int32 sess_id = gp_session_id;
			int32 segment = GpIdentity.segindex;

			values[0] = Int32GetDatum(start_ts);
			values[1] = Int32GetDatum(elem_num);
			values[2] = Int32GetDatum(duration);
			if (strcmp(target_list_type, "lfl") == 0)
				values[3] = CStringGetTextDatum("lock free list");
			else
				values[3] = CStringGetTextDatum("list with locks");
			values[4] = Int32GetDatum(sess_id);
			values[5] = Int32GetDatum(segment);
		}
		else
		{
			SLflPerfTestSegmentResult *result = &ctx->results[funcctx->call_cntr];

			values[0] = Int32GetDatum(result->start_ts);
			values[1] = Int32GetDatum(result->elem_num);
			values[2] = Int32GetDatum(result->duration_us);
			values[3] = CStringGetTextDatum(result->list_type->data);
			values[4] = Int32GetDatum(result->sess_id);
			values[5] = Int32GetDatum(result->segment);
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

