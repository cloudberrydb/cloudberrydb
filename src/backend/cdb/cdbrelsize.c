/*-------------------------------------------------------------------------
 *
 * cdbrelsize.c
 *
 * Copyright (c) 2006-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "catalog/catalog.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "gp-libpq-fe.h"
#include "lib/stringinfo.h"
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
	int64 size = 0;
	int i;
	CdbPgResults cdb_pgresults = {NULL, 0};
	StringInfoData buffer;

	char *schemaName;
	char *relName;

	/*
	 * Let's ask the QEs for the size of the relation
	 */
	initStringInfo(&buffer);

	schemaName = get_namespace_name(RelationGetNamespace(rel));
	if (schemaName == NULL)
		elog(ERROR, "cache lookup failed for namespace %d",
			 RelationGetNamespace(rel));
	relName = RelationGetRelationName(rel);

	/*
	 * Safer to pass names than oids, just in case they get out of sync between QD and QE,
	 * which might happen with a toast table or index, I think (but maybe not)
	 */
	appendStringInfo(&buffer, "select pg_relation_size('%s.%s')",
					 quote_identifier(schemaName), quote_identifier(relName));

	CdbDispatchCommand(buffer.data, DF_WITH_SNAPSHOT, &cdb_pgresults);

	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		struct pg_result * pgresult = cdb_pgresults.pg_results[i];
		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			elog(ERROR,"cdbRelMaxSegSize: resultStatus not tuples_Ok: %s %s",
				 PQresStatus(PQresultStatus(pgresult)),PQresultErrorMessage(pgresult));
		}
		else
		{
			Assert(PQntuples(pgresult) == 1);
			int64 tempsize = 0;
			(void) scanint8(PQgetvalue(pgresult, 0, 0), false, &tempsize);
			if (tempsize > size)
				size = tempsize;
		}
	}

	pfree(buffer.data);

	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return size;
}
