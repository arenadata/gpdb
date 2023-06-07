#include <dirent.h>
#include <sys/stat.h>
#include <ctype.h>

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
#include "catalog/pg_tablespace.h"

PG_MODULE_MAGIC;

typedef struct
{
	char     *datpath;
	DIR      *dirdesc;
	TupleDesc tupdesc;
} user_fctx_data;

static int should_skip_file(struct dirent *direntry)
{
	int filenamelen;

	if (direntry->d_type == DT_DIR)
		return 1;

	filenamelen = strlen(direntry->d_name);

	/* Check prefix */
	if (filenamelen >= 2)
	{
		if (tolower(direntry->d_name[0]) == 'p' &&
		    tolower(direntry->d_name[1]) == 'g')
			return 1;

		if (tolower(direntry->d_name[0]) == 't' &&
		    direntry->d_name[1]          == '_')
			return 1;
	}

	/* Check postfix */
	if (filenamelen >= 3)
	{
		if (direntry->d_name[filenamelen - 3]          == '_' &&
		    tolower(direntry->d_name[filenamelen - 2]) == 'v' &&
		    tolower(direntry->d_name[filenamelen - 1]) == 'm')
			return 1;
	}
	if (filenamelen >= 4)
	{
		if (direntry->d_name[filenamelen - 4]          == '_' &&
		    tolower(direntry->d_name[filenamelen - 3]) == 'f' &&
		    tolower(direntry->d_name[filenamelen - 2]) == 's' &&
		    tolower(direntry->d_name[filenamelen - 1]) == 'm')
			return 1;
	}
	if (filenamelen >= 5)
	{
		if (direntry->d_name[filenamelen - 5]          == '_' &&
		    tolower(direntry->d_name[filenamelen - 4]) == 'i' &&
		    tolower(direntry->d_name[filenamelen - 3]) == 'n' &&
		    tolower(direntry->d_name[filenamelen - 2]) == 'i' &&
		    tolower(direntry->d_name[filenamelen - 1]) == 't')
			return 1;
	}

	return 0;
}

PG_FUNCTION_INFO_V1(adb_get_relfilenodes);
Datum adb_get_relfilenodes(PG_FUNCTION_ARGS)
{
	Oid              datoid = MyDatabaseId;
	Oid              tablespace_oid = PG_GETARG_OID(0);

	struct dirent   *direntry;
	user_fctx_data  *fctx_data;
	FuncCallContext *funcctx;

	if (tablespace_oid == GLOBALTABLESPACE_OID)
		datoid = 0;

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
		char       *relfilenode, *ptr, *filename;
		Oid         reloid;
		Oid         relfilenode_oid;
		HeapTuple   tuple;

		CHECK_FOR_INTERRUPTS();

		if (should_skip_file(direntry) == 1)
			continue;

		filename = psprintf("%s/%s", fctx_data->datpath, direntry->d_name);

		if (stat(filename, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
							errmsg("could not stat file \"%s\": %m",
								   filename)));
		}

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int16GetDatum(GpIdentity.segindex);
		values[1] = Int16GetDatum(GpIdentity.dbid);
		values[2] = DatumGetObjectId(datoid);
		values[3] = DatumGetObjectId(tablespace_oid);
		values[4] = CStringGetTextDatum(filename);

		relfilenode = pstrdup(direntry->d_name);
		ptr = strchr(relfilenode, '.');
		if (ptr)
			*ptr = '\0';

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
