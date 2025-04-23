/*-------------------------------------------------------------------------
 *
 * cdbrelsize.c
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbrelsize.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/spi.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "catalog/catalog.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "libpq-fe.h"
#include "lib/stringinfo.h"
#include "utils/guc.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"

#include "cdb/cdbrelsize.h"

/*
 * Get the max size of the relation across segments
 */
int64
cdbRelMaxSegSize(Relation rel)
{
	int64		size = 0;
	int			i;
	CdbPgResults cdb_pgresults = {NULL, 0};
	char	   *sql;

	/*
	 * Let's ask the QEs for the size of the relation
	 *
	 * Relation Oids are assumed to be in sync in all nodes.
	 */
	sql = psprintf("select pg_catalog.pg_relation_size(%u)",
				   RelationGetRelid(rel));

	CdbDispatchCommand(sql, DF_WITH_SNAPSHOT, &cdb_pgresults);

	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			elog(ERROR, "cdbRelMaxSegSize: resultStatus not tuples_Ok: %s %s",
				 PQresStatus(PQresultStatus(pgresult)), PQresultErrorMessage(pgresult));
		}
		else
		{
			Assert(PQntuples(pgresult) == 1);
			int64		tempsize = 0;

			(void) scanint8(PQgetvalue(pgresult, 0, 0), false, &tempsize);
			if (tempsize > size)
				size = tempsize;
		}
	}

	pfree(sql);

	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return size;
}


/*
 * Get the size of the relation after decompression.
 */
int64
cdbRelUncompressedSize(Relation rel)
{
	StringInfoData sql;
	Oid			reloid = RelationGetRelid(rel);
	int64		result = 0;
	volatile bool connected = false;	/* volatile is required by PG_TRY */

	initStringInfo(&sql);
	if (RelationIsAppendOptimized(rel))
		appendStringInfo(&sql,
						 "select (pg_catalog.pg_relation_size(%u) * "
						 "get_ao_compression_ratio(%u))::int8",
						 reloid,
						 reloid);
	else
		appendStringInfo(&sql, "select pg_catalog.pg_relation_size(%u)",
						 reloid);

	/*
	 * This function may be called during Orca's planning and
	 * get_ao_compression_ratio may also involve query execution on auxiliary
	 * relations of the AO table - that will involve Orca again. But Orca
	 * doesn't support such nested planning. So, use standard planner when
	 * invoking get_ao_compression_ratio.
	 */
	bool		save_optimizer_guc_value = optimizer;

	optimizer = false;

	PG_TRY();
	{

		if (SPI_OK_CONNECT != SPI_connect())
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unable to obtain relation size information"),
					 errdetail("SPI_connect failed in cdbRelMaxSegSize.")));

		connected = true;

		if ((SPI_execute(sql.data, false, 0) <= 0) || (SPI_tuptable == NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unable to obtain relation size information"),
					 errdetail("SPI_execute failed in cdbRelMaxSegSize.")));
		else
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			SPITupleTable *tuptable = SPI_tuptable;
			HeapTuple	tuple = tuptable->vals[0];

			/* we expect only 1 tuple */
			Assert(SPI_processed == 1);

			char	   *val = SPI_getvalue(tuple, tupdesc, 1);

			Assert(NULL != val);

			if (!scanint8(val, true, &result))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unable to parse result string to int8.")));
		}

		connected = false;
		SPI_finish();
	}
	/* Clean up in case of error. */
	PG_CATCH();
	{
		if (connected)
			SPI_finish();

		optimizer = save_optimizer_guc_value;

		pfree(sql.data);

		/* Carry on with error handling. */
		PG_RE_THROW();
	}
	PG_END_TRY();

	optimizer = save_optimizer_guc_value;

	pfree(sql.data);

	return result;
}
