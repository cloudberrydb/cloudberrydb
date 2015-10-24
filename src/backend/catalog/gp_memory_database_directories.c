/*-------------------------------------------------------------------------
 *
 * gp_memory_database_directories.c
 *		Set-returning function to view gp_memory_database_directories table.
 *
 * IDENTIFICATION
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"			/* MyDatabaseId */

#include "funcapi.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "cdb/cdbpersistentdatabase.h"
#include "access/transam.h"
#include "cdb/cdbvars.h"                /* Gp_segment */

Datum		gp_memory_database_directories(PG_FUNCTION_ARGS);

/*
 * pgdatabasev - produce a view of gp_memory_database_directories that combines 
 * information from the local clog and the distributed log.
 */
Datum
gp_memory_database_directories(PG_FUNCTION_ARGS)
{
	typedef struct Context
	{
		bool		haveDir;
		
		DatabaseEntry	databaseEntry;
	} Context;
	
	FuncCallContext *funcctx;
	Context *context;
	bool found;
	DbDirNode dbDirNode;
	PersistentFileSysState state;
	Datum		values[4];
	bool		nulls[4];
	HeapTuple	tuple;
	Datum		result;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function
		 * calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		/* this had better match gp_memory_database_directories view in system_views.sql */
		tupdesc = CreateTemplateTupleDesc(4, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "segment_id",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "tablespace",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "database",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "state",
						   TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/*
		 * Collect all the locking information that we will format and send
		 * out as a result set.
		 */
		context = (Context *) palloc(sizeof(Context));
		funcctx->user_fctx = (void *) context;

		PersistentDatabase_IterateInit();
		context->haveDir = false;
		context->databaseEntry = NULL;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	context = (Context *) funcctx->user_fctx;

	while (true)
	{
		if (!context->haveDir)
		{
			PersistentDatabase_IterateNext(&context->databaseEntry);
			if (context->databaseEntry == NULL)
				SRF_RETURN_DONE(funcctx);

			context->haveDir = true;
			PersistentDatabase_DirIterateInit(context->databaseEntry);
		}

		found = PersistentDatabase_DirIterateNext(
										&dbDirNode,
										&state);
		if (found)
			break;

		context->haveDir = false;
	}

	/*
	 * Form tuple with appropriate data.
	 */
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));

	values[0] = Int16GetDatum((int16)Gp_segment);
	values[1] = ObjectIdGetDatum(dbDirNode.tablespace);
	values[2] = ObjectIdGetDatum(dbDirNode.database);

	values[3] = 
		DirectFunctionCall1(textin,
			                CStringGetDatum(
			                		PersistentFileSysObj_StateName(state)));

	tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	SRF_RETURN_NEXT(funcctx, result);
}
