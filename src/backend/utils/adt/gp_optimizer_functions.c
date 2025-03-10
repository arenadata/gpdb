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


//TODO: remove ==============================================================================

dsa_pointer
PendingDeleteShmemLinkNode(void * value);

void
PendingDeleteShmemUnlinkNode(dsa_pointer cur);

static Timestamp
DoListWithLocksPerfTest(int elements, int *batch_size)
{
	if (*batch_size < 1)
		*batch_size = 1;

	dsa_pointer *c = (dsa_pointer *)palloc(sizeof(dsa_pointer) * (*batch_size));

	Timestamp start = GetCurrentTimestamp();

	int remaining_elements = elements;
	bool del_direction = true;
	while (remaining_elements > 0)
	{
		int batch_remaining_elements = remaining_elements < (*batch_size) ? remaining_elements : (*batch_size);

		for (uint64 i = 0; i < batch_remaining_elements; i++)
		{
			c[i] = PendingDeleteShmemLinkNode((void*)(i + GpIdentity.segindex * 1000));
		}

		/* delete elements */
		if (del_direction)
		{
			for (int i = 0; i < batch_remaining_elements; i++)
			{
				PendingDeleteShmemUnlinkNode(c[i]);
			}
		}
		else
		{
			for (int i = batch_remaining_elements - 1; i >= 0; i--)
			{
				PendingDeleteShmemUnlinkNode(c[i]);
			}
		}
		del_direction = !del_direction;

		remaining_elements -= batch_remaining_elements;
	}

	Timestamp duration = GetCurrentTimestamp() - start;

	pfree(c);

	return duration;
}

//==============================================================================

typedef struct
{
	uint64 start_ts;
	int elem_num;
	int duration_us;
	StringInfo list_type;
	int sess_id;
	int segment;
	int batch_size;
} SLflPerfTestSegmentResult;

typedef struct
{
	SLflPerfTestSegmentResult results[1];
} SLflPerfTestCtx;

static Timestamp
DoLflPerfTest(int elements, int *batch_size)
{
	dsa_pointer ls_dsa = InvalidDsaPointer;

	if (*batch_size < 1)
		*batch_size = 1;

	if (DsaPointerIsValid(MyProc->pendingDeletesList))
	{
		ls_dsa = MyProc->pendingDeletesList;
	}
	else
	{
		ls_dsa = lock_free_list_create();
		MyProc->pendingDeletesList = ls_dsa;
	}

	lock_free_list_cell **c = (lock_free_list_cell**)palloc(sizeof(lock_free_list_cell *) * (*batch_size));

	Timestamp start = GetCurrentTimestamp();

	lock_free_list *ls = lock_free_list_get_local_list(ls_dsa);

	int remaining_elements = elements;
	bool del_direction = true;
	while (remaining_elements > 0)
	{
		int batch_remaining_elements = remaining_elements < (*batch_size) ? remaining_elements : (*batch_size);

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

	pfree(c);

	return duration;
}

Datum
lfl_test_perf(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	SLflPerfTestCtx *ctx;

	char *target_list_type = TextDatumGetCString(PG_GETARG_DATUM(0));
	int test_elem_num = PG_GETARG_INT32(1);
	int batch_size = PG_GETARG_INT32(2);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		int			nattr = 7;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(nattr);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "start_ts", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "elem_num", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "duration_us", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "list_type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "sess_id", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "segment", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "batch_size", INT4OID, -1, 0);

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
							 "SELECT * FROM lfl_test_perf('%s', %d, %d)",
							 target_list_type, test_elem_num, batch_size);

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
				Assert(PQnfields(pg_result) == 7);

				//ctx->results[i].start_ts 	= atoi(PQgetvalue(pg_result, 0, 0));
				sscanf(PQgetvalue(pg_result, 0, 0), "%lu", &(ctx->results[i].start_ts));
				ctx->results[i].elem_num 	= atoi(PQgetvalue(pg_result, 0, 1));
				ctx->results[i].duration_us = atoi(PQgetvalue(pg_result, 0, 2));
				ctx->results[i].list_type 	= makeStringInfo();
				appendStringInfoString(ctx->results[i].list_type, PQgetvalue(pg_result, 0, 3));
				ctx->results[i].sess_id 	= atoi(PQgetvalue(pg_result, 0, 4));
				ctx->results[i].segment 	= atoi(PQgetvalue(pg_result, 0, 5));
				ctx->results[i].batch_size	= atoi(PQgetvalue(pg_result, 0, 6));
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
		Datum		values[8];
		bool		nulls[8];
		HeapTuple	tuple;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		if (Gp_role == GP_ROLE_EXECUTE)
		{
			int64 start_ts = GetCurrentTimestamp();// TODO: fix this mess with 64/32bits
			int32 elem_num = test_elem_num;
			int64 duration = 0; 
			int32 sess_id = gp_session_id;
			int32 segment = GpIdentity.segindex;

			values[0] = Int64GetDatum(start_ts);
			values[1] = Int32GetDatum(elem_num);

			if (strcmp(target_list_type, "lfl") == 0)
			{
				duration = DoLflPerfTest(elem_num, &batch_size);
				values[3] = CStringGetTextDatum("lock free list");
			}
			else if (strcmp(target_list_type, "non-lfl") == 0)
			{
				duration = DoListWithLocksPerfTest(elem_num, &batch_size);
				values[3] = CStringGetTextDatum("list with locks");
			}
			else
				elog(ERROR, "unknown target list type to test");
			values[2] = Int32GetDatum(duration);
			values[4] = Int32GetDatum(sess_id);
			values[5] = Int32GetDatum(segment);
			values[6] = Int32GetDatum(batch_size);
		}
		else
		{
			SLflPerfTestSegmentResult *result = &ctx->results[funcctx->call_cntr];

			values[0] = Int64GetDatum(result->start_ts);
			values[1] = Int32GetDatum(result->elem_num);
			values[2] = Int32GetDatum(result->duration_us);
			values[3] = CStringGetTextDatum(result->list_type->data);
			values[4] = Int32GetDatum(result->sess_id);
			values[5] = Int32GetDatum(result->segment);
			values[6] = Int32GetDatum(result->batch_size);
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

