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

#include "access/aosegfiles.h"
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

	/* AO relation may be compressed, so need to adjust size calculation */
	if (RelationIsAppendOptimized(rel))
	{
		/*
		 * This function may be called during Orca's planning and
		 * get_ao_compression_ratio may also involve query execution on
		 * auxiliary relations of the AO table - that will involve Orca again.
		 * But Orca doesn't support such nested planning. So, use standard
		 * planner when invoking get_ao_compression_ratio.
		 */
		bool save_optimizer_guc_value = optimizer;
		optimizer = false;

		float8 compression_ratio = DatumGetFloat8(DirectFunctionCall1(
			get_ao_compression_ratio,
			ObjectIdGetDatum(RelationGetRelid(rel))));

		optimizer = save_optimizer_guc_value;

		/*
		 * get_ao_compression_ratio can return -1 if compression information
		 * is not available. So, for any value below 1.0 (which is the minimum
		 * reasonable start of compression ratio), we consider there is no
		 * compression.
		 */
		if (compression_ratio < 1.0)
			compression_ratio = 1.0;

		size = size * compression_ratio;
	}

	return size;
}
