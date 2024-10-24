#include "postgres.h"


#include "access/genam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_db_role_setting.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "utils/relcache.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include "arenadata_toolkit_guc.h"
#include "arenadata_toolkit_worker.h"
#include "drops_track.h"
#include "dbsize.h"
#include "file_hook.h"
#include "tf_shmem.h"

PG_FUNCTION_INFO_V1(tracking_register_db);
PG_FUNCTION_INFO_V1(tracking_unregister_db);
PG_FUNCTION_INFO_V1(tracking_set_snapshot_on_recovery);
PG_FUNCTION_INFO_V1(tracking_register_schema);
PG_FUNCTION_INFO_V1(tracking_unregister_schema);
PG_FUNCTION_INFO_V1(tracking_set_relkinds);
PG_FUNCTION_INFO_V1(tracking_set_relstorages);
PG_FUNCTION_INFO_V1(tracking_is_segment_initialized);
PG_FUNCTION_INFO_V1(tracking_trigger_initial_snapshot);
PG_FUNCTION_INFO_V1(tracking_is_initial_snapshot_triggered);
PG_FUNCTION_INFO_V1(tracking_get_track);
PG_FUNCTION_INFO_V1(tracking_get_track_main);

typedef struct
{
	Relation	pg_class_rel;
	SysScanDesc scan;
}	tf_main_func_state_t;

typedef struct
{
	bloom_t    *bloom;
	bloom_t    *rollback_bloom;
	List	   *drops;
	ListCell   *next_drop;
	List	   *relkinds;
	List	   *relstorages;
	List	   *schema_oids;
}	tf_get_global_state_t;

typedef struct
{
	CdbPgResults cdb_results;
	int			current_result;
	int			current_row;

	SPITupleTable *entry_result;
	uint64		entry_processed;
	int			entry_current_row;

	FmgrInfo   *inputFuncInfos;
	Oid		   *typIOParams;
}	tf_get_func_state_t;

tf_get_global_state_t tf_get_global_state = {NULL, NULL, NIL, NULL, NIL, NIL, NIL};

static inline void
tf_check_shmem_error(void)
{
	if (tf_shared_state == NULL)
		ereport(ERROR,
				(errmsg("Failed to access shared memory due to wrong extension initialization"),
				 errhint("Load extension's code through shared_preload_library configuration")));
}

/*
 * If get function complete with commit, just free resources;
 * In case of abort bloom is merged back as well as drops track.
 */
static void
xact_end_get_callback(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT)
		return;

	if (tf_get_global_state.bloom == NULL)
		return;

	if (event == XACT_EVENT_ABORT)
	{
		if (tf_get_global_state.rollback_bloom)
			bloom_set_merge(&tf_shared_state->bloom_set, MyDatabaseId, tf_get_global_state.rollback_bloom);
		else
			bloom_set_merge(&tf_shared_state->bloom_set, MyDatabaseId, tf_get_global_state.bloom);
		drops_track_move_undo(tf_get_global_state.drops, MyDatabaseId);
	}

	if (tf_get_global_state.bloom)
	{
		pfree(tf_get_global_state.bloom);
		tf_get_global_state.bloom = NULL;
	}

	if (tf_get_global_state.rollback_bloom)
	{
		pfree(tf_get_global_state.rollback_bloom);
		tf_get_global_state.rollback_bloom = NULL;
	}

	if (tf_get_global_state.drops != NIL)
	{
		pfree(tf_get_global_state.drops);
		tf_get_global_state.drops = NIL;
		tf_get_global_state.next_drop = NULL;
	}

	if (tf_get_global_state.relkinds != NIL)
	{
		pfree(tf_get_global_state.relkinds);
		tf_get_global_state.relkinds = NIL;
	}

	if (tf_get_global_state.relstorages != NIL)
	{
		pfree(tf_get_global_state.relstorages);
		tf_get_global_state.relstorages = NIL;
	}

	if (tf_get_global_state.schema_oids != NIL)
	{
		pfree(tf_get_global_state.schema_oids);
		tf_get_global_state.schema_oids = NIL;
	}

}

static List *
split_string_to_list(const char *input)
{
	List	   *result = NIL;
	char	   *input_copy;
	char	   *token;

	if (input == NULL)
		return NIL;

	input_copy = pstrdup(input);

	token = strtok(input_copy, ",");

	while (token != NULL)
	{
		if (*token != '\0')
		{
			result = lappend(result, pstrdup(token));
		}

		token = strtok(NULL, ",");
	}

	pfree(input_copy);

	return result;
}

static void
get_filters_from_guc()
{
	Relation	rel;
	ScanKeyData skey[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	char	   *current_schemas = NULL;
	char	   *current_relkinds = NULL;
	char	   *current_relstorages = NULL;
	List	   *schema_names = NIL;
	ListCell   *lc;

	rel = heap_open(DbRoleSettingRelationId, RowExclusiveLock);
	ScanKeyInit(&skey[0],
				Anum_pg_db_role_setting_setdatabase,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(MyDatabaseId));

	/*
	 * Lookup for not role specific configuration
	 */
	ScanKeyInit(&skey[1],
				Anum_pg_db_role_setting_setrole,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(InvalidOid));
	scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true, NULL, 2, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		bool		isnull;
		Datum		str_datum;

		str_datum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
								 RelationGetDescr(rel), &isnull);
		if (!isnull)
		{
			ArrayType  *array;
			Datum	   *elems;
			bool	   *nulls;
			int			nelems;

			array = DatumGetArrayTypeP(str_datum);
			deconstruct_array(array, TEXTOID, -1, false, 'i',
							  &elems, &nulls, &nelems);
			for (int i = 0; i < nelems; i++)
			{
				if (nulls[i])
					continue;
				char	   *str = TextDatumGetCString(elems[i]);

				if (strncmp(str, "arenadata_toolkit.tracking_schemas=", 35) == 0)
					current_schemas = pstrdup(str + 35);
				else if (strncmp(str, "arenadata_toolkit.tracking_relstorages=", 39) == 0)
					current_relstorages = pstrdup(str + 39);
				else if (strncmp(str, "arenadata_toolkit.tracking_relkinds=", 36) == 0)
					current_relkinds = pstrdup(str + 36);
				pfree(str);
			}
		}
	}
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	if (current_schemas)
		schema_names = split_string_to_list(current_schemas);
	else
		schema_names = split_string_to_list(tracked_schemas);
	if (current_relstorages)
		tf_get_global_state.relstorages = split_string_to_list(current_relstorages);
	else
		tf_get_global_state.relstorages = split_string_to_list(tracked_rel_storages);
	if (current_relkinds)
		tf_get_global_state.relkinds = split_string_to_list(current_relkinds);
	else
		tf_get_global_state.relkinds = split_string_to_list(tracked_rel_kinds);

	foreach(lc, schema_names)
	{
		Oid			nspOid;
		char	   *name = (char *)lfirst(lc);

		nspOid = GetSysCacheOid1(NAMESPACENAME, CStringGetDatum(name));

		if (!OidIsValid(nspOid))
		{
			elog(DEBUG1, "[tracking_get_track] schema \"%s\" does not exist", name);
			continue;
		}

		tf_get_global_state.schema_oids = lappend_oid(tf_get_global_state.schema_oids, nspOid);
	}

	if (schema_names)
		pfree(schema_names);
}


static bool
schema_is_tracked(Oid schema)
{
	ListCell   *lc;

	if (tf_get_global_state.schema_oids == NIL)
		return false;

	foreach(lc, tf_get_global_state.schema_oids)
	{
		Oid			tracked_schema = lfirst_oid(lc);

		if (tracked_schema == schema)
			return true;
	}

	return false;
}

static bool
relkind_is_tracked(char relkind)
{
	ListCell   *lc;

	if (tf_get_global_state.relkinds == NIL)
		return false;

	foreach(lc, tf_get_global_state.relkinds)
	{
		char	   *tracked_relkind = (char *)lfirst(lc);

		if (tracked_relkind != NULL && *tracked_relkind == relkind)
			return true;
	}

	return false;
}

static bool
relstorage_is_tracked(char relstorage)
{
	ListCell   *lc;

	if (tf_get_global_state.relstorages == NIL)
		return false;

	foreach(lc, tf_get_global_state.relstorages)
	{
		char	   *tracked_relstorage = (char *)lfirst(lc);

		if (tracked_relstorage != NULL && *tracked_relstorage == relstorage)
			return true;
	}

	return false;
}

/*
 * Main logic for getting the size track.
 */
Datum
tracking_get_track_main(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	tf_main_func_state_t *state;
	HeapTuple	result;
	Datum		datums[9];
	bool		nulls[9] = {0};

	tf_check_shmem_error();

	if (!pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_error))
		ereport(ERROR,
				(errmsg("Can't perform tracking for database %u properly due to internal error", MyDatabaseId)));

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();

		RegisterXactCallbackOnce(xact_end_get_callback, NULL);

		oldcontext = MemoryContextSwitchTo(CurTransactionContext);

		if (tf_get_global_state.bloom == NULL)
		{
			tf_get_global_state.bloom = palloc(FULL_BLOOM_SIZE(bloom_size));
			if (!bloom_set_move(&tf_shared_state->bloom_set, MyDatabaseId, tf_get_global_state.bloom))
				ereport(ERROR,
						(errcode(ERRCODE_GP_COMMAND_ERROR),
						 errmsg("database %u is not tracked", MyDatabaseId),
						 errhint("Call 'arenadata_toolkit.tracking_register_db()'"
							 "to enable tracking")));
		}
		else
		{
			/*
			 * This code is needed for the cases when there are several track
			 * requests within the same transaction. rollback_bloom stands for
			 * preserving initial filter state at the moment of the first
			 * function call within the transaction.
			 */
			if (tf_get_global_state.rollback_bloom == NULL)
			{
				tf_get_global_state.rollback_bloom = palloc(FULL_BLOOM_SIZE(bloom_size));
				bloom_copy(tf_get_global_state.bloom, tf_get_global_state.rollback_bloom);
			}
			bloom_clear(tf_get_global_state.bloom);
			if (!bloom_set_move(&tf_shared_state->bloom_set, MyDatabaseId, tf_get_global_state.bloom))
				ereport(ERROR,
						(errcode(ERRCODE_GP_COMMAND_ERROR),
						 errmsg("database %u is not tracked", MyDatabaseId),
						 errhint("Call 'arenadata_toolkit.tracking_register_db()'"
							 "to enable tracking")));
		}
		/* initial snapshot shouldn't return drops */
		if (tf_get_global_state.bloom && !tf_get_global_state.bloom->is_set_all)
		{
			tf_get_global_state.drops = drops_track_move(MyDatabaseId);
			tf_get_global_state.next_drop = list_head(tf_get_global_state.drops);
		}

		/*
		 * Let's retrieve tracking information only once for the transaction.
		 */
		if (tf_get_global_state.schema_oids == NIL)
			get_filters_from_guc();

		if (tf_get_global_state.relstorages == NIL ||
			tf_get_global_state.relkinds == NIL ||
			tf_get_global_state.schema_oids == NIL)
			ereport(ERROR,
					(errmsg("Cannot get tracking configuration (schemas, relkinds, reltorage) for database %u", MyDatabaseId)));

		MemoryContextSwitchTo(oldcontext);

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		funcctx->tuple_desc = CreateTemplateTupleDesc(9, false);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)1, "relid", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)2, "name", NAMEOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)3, "relfilenode", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)4, "size", INT8OID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)5, "state", CHAROID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)6, "gp_segment_id", INT4OID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)7, "relnamespace", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)8, "relkind", CHAROID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)9, "relstorage", CHAROID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);

		state = (tf_main_func_state_t *) palloc0(sizeof(tf_main_func_state_t));
		funcctx->user_fctx = (void *)state;

		if (tf_get_global_state.bloom)
		{
			state->pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
			state->scan = systable_beginscan(state->pg_class_rel, InvalidOid, false, NULL, 0, NULL);
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	state = funcctx->user_fctx;

	if (pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_is_initialized))
	{
		systable_endscan(state->scan);
		heap_close(state->pg_class_rel, AccessShareLock);
		state->scan = NULL;
		state->pg_class_rel = NULL;
		elog(LOG, "Nothing to return from segment %d due to uninitialized status of Bloom filter", GpIdentity.segindex);
		SRF_RETURN_DONE(funcctx);
	}

	while (true)
	{
		Oid			filenode;
		Oid			relnamespace;
		char		relkind;
		char		relstorage;
		HeapTuple	pg_class_tuple;
		uint64_t	hash;
		Form_pg_class relp;
		int64 size;

		if (!state->scan)
			break;

		pg_class_tuple = systable_getnext(state->scan);

		if (!HeapTupleIsValid(pg_class_tuple))
		{
			systable_endscan(state->scan);
			heap_close(state->pg_class_rel, AccessShareLock);
			state->scan = NULL;
			state->pg_class_rel = NULL;
			break;
		}

		datums[6] = heap_getattr(pg_class_tuple, Anum_pg_class_relnamespace, RelationGetDescr(state->pg_class_rel), &nulls[6]);
		relnamespace = DatumGetObjectId(datums[6]);

		if (!schema_is_tracked(relnamespace))
			continue;

		datums[7] = heap_getattr(pg_class_tuple, Anum_pg_class_relkind, RelationGetDescr(state->pg_class_rel), &nulls[7]);
		relkind = DatumGetChar(datums[7]);

		if (!relkind_is_tracked(relkind))
			continue;

		datums[8] = heap_getattr(pg_class_tuple, Anum_pg_class_relstorage, RelationGetDescr(state->pg_class_rel), &nulls[8]);
		relstorage = DatumGetChar(datums[8]);

		if (!relstorage_is_tracked(relstorage))
			continue;

		datums[0] = ObjectIdGetDatum(HeapTupleGetOid(pg_class_tuple));

		datums[1] = heap_getattr(pg_class_tuple, Anum_pg_class_relname, RelationGetDescr(state->pg_class_rel), &nulls[1]);

		datums[2] = heap_getattr(pg_class_tuple, Anum_pg_class_relfilenode, RelationGetDescr(state->pg_class_rel), &nulls[2]);
		filenode = DatumGetObjectId(datums[2]);

		if (nulls[2])
			continue;

		/* Bloom filter check */
		hash = bloom_set_calc_hash(&filenode, sizeof(filenode));
		if (!bloom_isset(tf_get_global_state.bloom, hash))
			continue;

		relp = (Form_pg_class) GETSTRUCT(pg_class_tuple);
		size = dbsize_calc_size(relp);
		datums[3] = Int64GetDatum(size);
		datums[4] = CharGetDatum(tf_get_global_state.bloom->is_set_all ? 'i' : 'a');
		datums[5] = Int32GetDatum(GpIdentity.segindex);

		result = heap_form_tuple(funcctx->tuple_desc, datums, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result));
	}

	while (true)
	{
		Oid			filenode;

		if (!tf_get_global_state.next_drop)
			break;

		filenode = lfirst_oid(tf_get_global_state.next_drop);
		tf_get_global_state.next_drop = lnext(tf_get_global_state.next_drop);

		nulls[0] = true;
		nulls[1] = true;
		datums[2] = filenode;
		datums[3] = Int64GetDatum(0);
		datums[4] = CharGetDatum('d');
		datums[5] = Int32GetDatum(GpIdentity.segindex);
		nulls[6] = true;
		nulls[7] = true;
		nulls[8] = true;

		result = heap_form_tuple(funcctx->tuple_desc, datums, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Function used in "arenadata_toolkit.tables_track" view. In order to keep bloom filter
 * in consistent state across segments this function dispatches main tracking logic to the
 * segments in a distributed transaction.
 */
Datum
tracking_get_track(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	tf_get_func_state_t *state;
	HeapTuple	result;
	Datum		values[9];
	bool		nulls[9] = {0};

	tf_check_shmem_error();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext = CurrentMemoryContext;

		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * If we use CdbDispatchCommandToSegments, we will face the problem
		 * that entry db slice won't be part of global transaction and
		 * immediately commits, killing the chance for bloom filter to
		 * restore. Therefore, the spi approach for retrieving track at -1
		 * segment is chosen.
		 */
		if (SPI_connect() != SPI_OK_CONNECT)
			ereport(ERROR, (errmsg("SPI_connect failed")));
		if (SPI_execute("SELECT * FROM arenadata_toolkit.tracking_get_track_main()", true, 0) != SPI_OK_SELECT)
			ereport(ERROR, (errmsg("SPI_execute failed")));

		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		state = (tf_get_func_state_t *) palloc0(sizeof(tf_get_func_state_t));
		funcctx->user_fctx = (void *)state;

		state->entry_result = SPI_tuptable;
		state->entry_processed = SPI_processed;
		state->entry_current_row = 0;

		CdbDispatchCommand("SELECT * FROM arenadata_toolkit.tracking_get_track_main()", DF_NEED_TWO_PHASE | DF_CANCEL_ON_ERROR,
						   &state->cdb_results);

		state->current_result = 0;
		state->current_row = 0;

		funcctx->tuple_desc = CreateTemplateTupleDesc(9, false);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)1, "relid", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)2, "name", NAMEOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)3, "relfilenode", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)4, "size", INT8OID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)5, "state", CHAROID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)6, "gp_segment_id", INT4OID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)7, "relnamespace", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)8, "relkind", CHAROID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)9, "relstorage", CHAROID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);

		if (state->cdb_results.numResults > 0)
		{
			int			natts = funcctx->tuple_desc->natts;

			state->inputFuncInfos = (FmgrInfo *)palloc0(natts * sizeof(FmgrInfo));
			state->typIOParams = (Oid *)palloc0(natts * sizeof(Oid));
			for (int i = 0; i < natts; i++)
			{
				Oid			type = TupleDescAttr(funcctx->tuple_desc, i)->atttypid;

				getTypeInputInfo(type, &state->inputFuncInfos[i].fn_oid, &state->typIOParams[i]);
				fmgr_info(state->inputFuncInfos[i].fn_oid, &state->inputFuncInfos[i]);
			}
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	state = funcctx->user_fctx;

	if (state->entry_current_row < state->entry_processed)
	{
		HeapTuple	inputTuple = state->entry_result->vals[state->entry_current_row];
		TupleDesc	inputTupleDesc = state->entry_result->tupdesc;

		for (int i = 0; i < funcctx->tuple_desc->natts; i++)
		{
			values[i] = SPI_getbinval(inputTuple, inputTupleDesc, i + 1, &nulls[i]);
		}
		HeapTuple	resultTuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		state->entry_current_row++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(resultTuple));
	}

	SPI_finish();

	while (state->current_result < state->cdb_results.numResults)
	{
		struct pg_result *pgresult = state->cdb_results.pg_results[state->current_result];

		if (pgresult)
		{
			int			nrows = PQntuples(pgresult);
			int			ncols = PQnfields(pgresult);

			if (state->current_row < nrows)
			{
				for (int col = 0; col < ncols; col++)
				{
					if (PQgetisnull(pgresult, state->current_row, col))
					{
						values[col] = (Datum)0;
						nulls[col] = true;
					}
					else
					{
						char	   *value = PQgetvalue(pgresult, state->current_row, col);

						values[col] = InputFunctionCall(&state->inputFuncInfos[col], value, state->typIOParams[col], -1);
					}
				}
				result = heap_form_tuple(funcctx->tuple_desc, values, nulls);
				state->current_row++;
				SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result));
			}
			else
			{
				state->current_row = 0;
				state->current_result++;
			}
		}
		else
			state->current_result++;
	}

	SRF_RETURN_DONE(funcctx);
}

static void
track_db(Oid dbid, bool reg)
{
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		AlterDatabaseSetStmt stmt;
		VariableSetStmt v_stmt;
		A_Const		aconst =
		{.type = T_A_Const,.val = {.type = T_String,.val.str = reg ? "t" : "f"}};

		stmt.type = T_AlterDatabaseSetStmt;
		stmt.dbname = get_database_name(dbid);

		if (stmt.dbname == NULL)
			ereport(ERROR,
					(errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

		stmt.setstmt = &v_stmt;

		v_stmt.type = T_VariableSetStmt;
		v_stmt.kind = VAR_SET_VALUE;
		v_stmt.name = "arenadata_toolkit.tracking_is_db_tracked";
		v_stmt.args = lappend(NIL, &aconst);
		v_stmt.is_local = false;

		tf_guc_unlock_tracked_once();

		AlterDatabaseSet(&stmt);
	}

	if (!reg)
		bloom_set_unbind(&tf_shared_state->bloom_set, dbid);
	else if (!bloom_set_bind(&tf_shared_state->bloom_set, dbid))
		ereport(ERROR,
				(errmsg("[arenadata_toolkit] exceeded maximum number of tracked databases")));
}

/*
 * Registers current (if dbid is 0) or specific database as tracked by arenadata_toolkit tables tracking.
 * Dispatches call to segments by itself. Binds a bloom filter to the registered database if possible.
 */
Datum
tracking_register_db(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;
	elog(LOG, "[arenadata_toolkit] registering database %u for tracking", dbid);

	track_db(dbid, true);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *cmd =
		psprintf("select arenadata_toolkit.tracking_register_db(%u)", dbid);

		CdbDispatchCommand(cmd, 0, NULL);
	}

	PG_RETURN_BOOL(true);
}

/*
 * Stop tracking given database and unbind from bloom.
 */
Datum
tracking_unregister_db(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;
	elog(LOG, "[arenadata_toolkit] unregistering database %u from tracking", dbid);

	track_db(dbid, false);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *cmd =
		psprintf("select arenadata_toolkit.tracking_unregister_db(%u)", dbid);

		CdbDispatchCommand(cmd, 0, NULL);
	}

	PG_RETURN_BOOL(true);
}

Datum
tracking_set_snapshot_on_recovery(PG_FUNCTION_ARGS)
{
	bool		set = PG_GETARG_OID(0);
	Oid			dbid = PG_GETARG_OID(1);

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;

	A_Const		aconst =
	{.type = T_A_Const,.val = {.type = T_String,.val.str = set ? "t" : "f"}};

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		AlterDatabaseSetStmt stmt;
		VariableSetStmt v_stmt;

		stmt.type = T_AlterDatabaseSetStmt;
		stmt.dbname = get_database_name(dbid);
		stmt.setstmt = &v_stmt;

		if (stmt.dbname == NULL)
			ereport(ERROR,
					(errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

		v_stmt.type = T_VariableSetStmt;
		v_stmt.kind = VAR_SET_VALUE;
		v_stmt.name = "arenadata_toolkit.tracking_snapshot_on_recovery";
		v_stmt.args = lappend(NIL, &aconst);
		v_stmt.is_local = false;

		tf_guc_unlock_full_snapshot_on_recovery_once();

		AlterDatabaseSet(&stmt);
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *cmd =
		psprintf("select arenadata_toolkit.tracking_set_snapshot_on_recovery(%s, %u)",
				 set ? "true" : "false", dbid);

		CdbDispatchCommand(cmd, 0, NULL);
	}

	PG_RETURN_BOOL(true);
}

/* Helper function to add or remove schema from configuration string */
static char *
add_or_remove_schema(const char *schema_string, const char *schemaName, bool add)
{
	StringInfoData buf;
	char	   *token;
	char	   *str;
	bool		found = false;

	initStringInfo(&buf);

	if (schema_string && schema_string[0] != '\0')
	{
		str = pstrdup(schema_string);
		token = strtok(str, ",");
		while (token != NULL)
		{
			if (strcmp(token, schemaName) == 0)
			{
				found = true;
				if (add)
				{
					appendStringInfo(&buf, "%s,", token);
				}
			}
			else
			{
				appendStringInfo(&buf, "%s,", token);
			}
			token = strtok(NULL, ",");
		}
		pfree(str);
	}

	if (add && !found)
	{
		appendStringInfo(&buf, "%s,", schemaName);
	}

	if (buf.len > 0 && buf.data[buf.len - 1] == ',')
	{
		buf.data[buf.len - 1] = '\0';
		buf.len--;
	}

	if (buf.len == 0)
	{
		pfree(buf.data);
		return NULL;
	}

	return buf.data;
}

static void
track_schema(const char *schemaName, Oid dbid, bool reg)
{
	Relation	rel;
	ScanKeyData skey[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	char	   *current_schemas = NULL;
	char	   *new_schemas = NULL;
	AlterDatabaseSetStmt stmt;
	VariableSetStmt v_stmt;
	A_Const		arg;

	rel = heap_open(DbRoleSettingRelationId, RowExclusiveLock);
	ScanKeyInit(&skey[0],
				Anum_pg_db_role_setting_setdatabase,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dbid));

	/*
	 * Lookup for not role specific configuration
	 */
	ScanKeyInit(&skey[1],
				Anum_pg_db_role_setting_setrole,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(InvalidOid));
	scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true, NULL, 2, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		bool		isnull;
		Datum		str_datum;

		str_datum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
								 RelationGetDescr(rel), &isnull);
		if (!isnull)
		{
			ArrayType  *array;
			Datum	   *elems;
			int			nelems;

			array = DatumGetArrayTypeP(str_datum);
			deconstruct_array(array, TEXTOID, -1, false, 'i',
							  &elems, NULL, &nelems);
			for (int i = 0; i < nelems; i++)
			{
				char	   *str = TextDatumGetCString(elems[i]);

				if (strncmp(str, "arenadata_toolkit.tracking_schemas=", 35) == 0)
				{
					current_schemas = pstrdup(str + 35);
					break;
				}
				pfree(str);
			}
		}
	}
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	new_schemas = add_or_remove_schema(current_schemas, schemaName, reg);

	stmt.type = T_AlterDatabaseSetStmt;
	stmt.dbname = get_database_name(dbid);

	if (stmt.dbname == NULL)
		ereport(ERROR,
				(errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

	stmt.setstmt = &v_stmt;

	v_stmt.type = T_VariableSetStmt;
	v_stmt.name = "arenadata_toolkit.tracking_schemas";
	v_stmt.is_local = false;

	arg.type = T_A_Const;
	arg.val.type = T_String;
	arg.val.val.str = new_schemas;
	arg.location = -1;

	if (new_schemas == NULL)
	{
		/*
		 * If new_schemas is NULL, we're removing the last schema, so let's
		 * just RESET the variable
		 */
		v_stmt.kind = VAR_RESET;
		v_stmt.args = NIL;
	}
	else
	{
		v_stmt.kind = VAR_SET_VALUE;
		v_stmt.args = list_make1(&arg);
	}

	tf_guc_unlock_schemas_once();

	AlterDatabaseSet(&stmt);

	if (current_schemas)
		pfree(current_schemas);
	if (new_schemas)
		pfree(new_schemas);
}

Datum
tracking_register_schema(PG_FUNCTION_ARGS)
{
	const char *schema_name = NameStr(*PG_GETARG_NAME(0));
	Oid			dbid = PG_GETARG_OID(1);

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;

	if (!SearchSysCacheExists1(NAMESPACENAME, CStringGetDatum(schema_name)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema %s does not exist", schema_name)));

	elog(LOG, "[arenadata_toolkit] registering schema %s in database %u for tracking", schema_name, dbid);

	track_schema(schema_name, dbid, true);

	PG_RETURN_BOOL(true);
}

Datum
tracking_unregister_schema(PG_FUNCTION_ARGS)
{
	const char *schema_name = NameStr(*PG_GETARG_NAME(0));
	Oid			dbid = PG_GETARG_OID(1);

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;

	if (!SearchSysCacheExists1(NAMESPACENAME, CStringGetDatum(schema_name)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema with OID %s does not exist", schema_name)));

	elog(LOG, "[arenadata_toolkit] registering schema %s in database %u for tracking", schema_name, dbid);

	track_schema(schema_name, dbid, false);

	PG_RETURN_BOOL(true);
}

static bool
is_valid_relkind(char relkind)
{
	return (relkind == 'r' || relkind == 'i' || relkind == 'S' ||
			relkind == 't' || relkind == 'v' || relkind == 'c' ||
			relkind == 'f' || relkind == 'u' || relkind == 'm' ||
			relkind == 'o' || relkind == 'b' || relkind == 'M');
}

Datum
tracking_set_relkinds(PG_FUNCTION_ARGS)
{
	char	   *relkinds_str = NameStr(*PG_GETARG_NAME(0));
	Oid			dbid = PG_GETARG_OID(1);
	char	   *token;
	char	   *str_copy;
	bool		seen_relkinds[256] = {false};
	StringInfoData buf;
	AlterDatabaseSetStmt stmt;
	VariableSetStmt v_stmt;
	A_Const		arg;

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;

	initStringInfo(&buf);
	str_copy = pstrdup(relkinds_str);
	token = strtok(str_copy, ",");
	while (token != NULL)
	{
		if (strlen(token) != 1 || !is_valid_relkind(token[0]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Invalid relkind: %s", token),
					 errhint("Valid relkinds are: 'r', 'i', 'S', 't', 'v', 'c', 'f', 'u', 'm', 'o', 'b', 'M'")));

		if (!seen_relkinds[(unsigned char)token[0]])
		{
			appendStringInfoChar(&buf, token[0]);
			appendStringInfoChar(&buf, ',');
			seen_relkinds[(unsigned char)token[0]] = true;
		}
		token = strtok(NULL, ",");
	}
	pfree(str_copy);

	stmt.type = T_AlterDatabaseSetStmt;
	stmt.dbname = get_database_name(dbid);
	stmt.setstmt = &v_stmt;

	if (stmt.dbname == NULL)
		ereport(ERROR,
				(errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

	v_stmt.type = T_VariableSetStmt;
	v_stmt.name = "arenadata_toolkit.tracking_relkinds";
	v_stmt.is_local = false;

	arg.type = T_A_Const;
	arg.val.type = T_String;
	arg.val.val.str = buf.data;
	arg.location = -1;

	if (buf.len > 0 && buf.data[buf.len - 1] == ',')
	{
		buf.data[buf.len - 1] = '\0';
		buf.len--;
	}

	if (buf.len == 0)
	{
		v_stmt.kind = VAR_RESET;
		v_stmt.args = NIL;
	}
	else
	{
		v_stmt.kind = VAR_SET_VALUE;
		v_stmt.args = list_make1(&arg);
		elog(LOG, "[arenadata_toolkit] setting relkinds %s in database %u for tracking", buf.data, dbid);
	}

	tf_guc_unlock_relkinds_once();

	AlterDatabaseSet(&stmt);
	pfree(buf.data);

	PG_RETURN_BOOL(true);
}

static bool
is_valid_relstorage(char relstorage)
{
	return (relstorage == 'h' || relstorage == 'a' || relstorage == 'c' ||
			relstorage == 'x' || relstorage == 'v' || relstorage == 'f');
}

Datum
tracking_set_relstorages(PG_FUNCTION_ARGS)
{
	char	   *relstorages_str = NameStr(*PG_GETARG_NAME(0));
	Oid			dbid = PG_GETARG_OID(1);
	char	   *token;
	char	   *str_copy;
	bool		seen_relstorages[256] = {false};
	StringInfoData buf;
	AlterDatabaseSetStmt stmt;
	VariableSetStmt v_stmt;
	A_Const		arg;

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;

	initStringInfo(&buf);
	str_copy = pstrdup(relstorages_str);
	token = strtok(str_copy, ",");
	while (token != NULL)
	{
		if (strlen(token) != 1 || !is_valid_relstorage(token[0]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Invalid relstorage type: %s", token),
			errhint("Valid relstorages are: 'h', 'x', 'a', 'v', 'c', 'f'")));

		if (!seen_relstorages[(unsigned char)token[0]])
		{
			appendStringInfoChar(&buf, token[0]);
			appendStringInfoChar(&buf, ',');
			seen_relstorages[(unsigned char)token[0]] = true;
		}
		token = strtok(NULL, ",");
	}
	pfree(str_copy);

	stmt.type = T_AlterDatabaseSetStmt;
	stmt.dbname = get_database_name(dbid);

	if (stmt.dbname == NULL)
		ereport(ERROR,
				(errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

	stmt.setstmt = &v_stmt;

	v_stmt.type = T_VariableSetStmt;
	v_stmt.name = "arenadata_toolkit.tracking_relstorages";
	v_stmt.is_local = false;

	arg.type = T_A_Const;
	arg.val.type = T_String;
	arg.val.val.str = buf.data;
	arg.location = -1;

	if (buf.len > 0 && buf.data[buf.len - 1] == ',')
	{
		buf.data[buf.len - 1] = '\0';
		buf.len--;
	}

	if (buf.len == 0)
	{
		v_stmt.kind = VAR_RESET;
		v_stmt.args = NIL;
	}
	else
	{
		v_stmt.kind = VAR_SET_VALUE;
		v_stmt.args = list_make1(&arg);
		elog(LOG, "[arenadata_toolkit] setting relstorages %s in database %u for tracking", buf.data, dbid);
	}

	tf_guc_unlock_relstorages_once();

	AlterDatabaseSet(&stmt);

	pfree(buf.data);

	PG_RETURN_BOOL(true);
}

Datum
tracking_trigger_initial_snapshot(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;
	elog(LOG, "[arenadata_toolkit] tracking_trigger_initial_snapshot dbid: %u", dbid);

	if (!bloom_set_trigger_bits(&tf_shared_state->bloom_set, dbid, true))
		ereport(ERROR,
				(errmsg("Failed to find corresponding filter to database %u", dbid)));

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *cmd = psprintf("select arenadata_toolkit.tracking_trigger_initial_snapshot(%u)", dbid);

		CdbDispatchCommand(cmd, 0, NULL);
	}

	PG_RETURN_BOOL(true);
}

Datum
tracking_is_initial_snapshot_triggered(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);
	bool		is_triggered = false;

	tf_check_shmem_error();

	dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;

	is_triggered = bloom_set_is_all_bits_triggered(&tf_shared_state->bloom_set, dbid);

	elog(LOG, "[arenadata_toolkit] is_initial_snapshot_triggered:%d dbid: %u", is_triggered, dbid);

	PG_RETURN_BOOL(is_triggered);
}

Datum
tracking_is_segment_initialized(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsi;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[2];
	bool nulls[2] = {false, false};
	Datum		result;

	tf_check_shmem_error();

	rsi = (ReturnSetInfo *)fcinfo->resultinfo;
	tupdesc = rsi->expectedDesc;

	/* Populate an output tuple. */
	values[0] = Int32GetDatum(GpIdentity.segindex);
	values[1] = BoolGetDatum(pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_is_initialized) == false);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}
