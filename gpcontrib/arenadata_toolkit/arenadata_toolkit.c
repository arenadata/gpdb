#include <dirent.h>
#include <sys/stat.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "nodes/execnodes.h"
#include "cdb/cdbvars.h"
#include "utils/builtins.h"
#include "utils/relfilenodemap.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

typedef struct
{
	char     *datpath;
	DIR      *dirdesc;
	TupleDesc tupdesc;
	char      filename[MAXPGPATH * 2];
} user_fctx_data;

PG_FUNCTION_INFO_V1(adb_get_relfilenodes);
Datum adb_get_relfilenodes(PG_FUNCTION_ARGS)
{
	Oid              datoid = MyDatabaseId;
	Oid              tablespace_oid = MyDatabaseTableSpace;

	struct dirent   *direntry;
	user_fctx_data  *fctx_data;
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		fctx_data = palloc(sizeof(user_fctx_data));
		fctx_data->datpath = GetDatabasePath(datoid, tablespace_oid);
		fctx_data->dirdesc = AllocateDir(fctx_data->datpath);

		if (get_call_result_type(fcinfo, NULL, &fctx_data->tupdesc)
				!= TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		if (!fctx_data->dirdesc)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read directory \"%s\": %m",
							fctx_data->datpath)));

		funcctx->attinmeta = TupleDescGetAttInMetadata(fctx_data->tupdesc);
		funcctx->user_fctx = fctx_data;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	fctx_data = (user_fctx_data *) funcctx->user_fctx;

	while ((direntry = ReadDir(fctx_data->dirdesc, fctx_data->datpath)) != NULL)
	{
		struct stat fst;
		Datum       values[10];
		bool        nulls[10];
		char       *relfilenode;
		Oid         reloid;
		Oid         relfilenode_oid;
		HeapTuple   tuple;
		int         filenamelen;

		CHECK_FOR_INTERRUPTS();

		if (direntry->d_type == DT_DIR)
			continue;
		if (pg_strncasecmp(direntry->d_name, "pg", 2) == 0 ||
		    pg_strncasecmp(direntry->d_name, "t_", 2) == 0)
			continue;

		filenamelen = strlen(direntry->d_name);
		if ((filenamelen >= 3 &&
		     pg_strcasecmp(direntry->d_name + filenamelen - 3, "_vm") == 0) ||
		    (filenamelen >= 4 &&
		     pg_strcasecmp(direntry->d_name + filenamelen - 4, "_fsm") == 0) ||
		    (filenamelen >= 5 &&
		     pg_strcasecmp(direntry->d_name + filenamelen - 5, "_init") == 0))
			continue;

		snprintf(fctx_data->filename, sizeof(fctx_data->filename), "%s/%s",
				 fctx_data->datpath, direntry->d_name);

		if (stat(fctx_data->filename, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
							errmsg("could not stat file \"%s\": %m",
								   fctx_data->filename)));
		}

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int16GetDatum(GpIdentity.segindex);
		values[1] = Int16GetDatum(GpIdentity.dbid);
		values[2] = DatumGetObjectId(datoid);
		values[3] = DatumGetObjectId(tablespace_oid);
		values[4] = CStringGetTextDatum(fctx_data->filename);

		relfilenode = strchr(direntry->d_name, '.');
		if (relfilenode)
		{
			relfilenode += 1;
			relfilenode_oid = DirectFunctionCall1(oidin,
												  CStringGetDatum(relfilenode));
			reloid = RelidByRelfilenode(tablespace_oid, relfilenode_oid);

			if (OidIsValid(reloid))
			{
				values[5] = DatumGetObjectId(relfilenode_oid);
				values[6] = DatumGetObjectId(reloid);
			}
			else
			{
				nulls[5] = true;
				nulls[6] = true;
			}
		}
		else
		{
			nulls[5] = true;
			nulls[6] = true;
		}

		values[7] = Int64GetDatum(fst.st_size);

		values[8] = TimestampGetDatum(time_t_to_timestamptz(fst.st_mtime));
		values[9] = TimestampGetDatum(time_t_to_timestamptz(fst.st_ctime));

		tuple = heap_form_tuple(fctx_data->tupdesc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	FreeDir(fctx_data->dirdesc);
	SRF_RETURN_DONE(funcctx);

	return (Datum) 0;
}
