/*-------------------------------------------------------------------------
 *
 * matview.h
 *	  prototypes for matview.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_H
#define MATVIEW_H

#include "catalog/objectaddress.h"
#include "executor/execdesc.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/relcache.h"

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Oid			transientoid;	/* OID of new heap into which to store */
	Oid			oldreloid;
	bool		concurrent;
	bool		skipData;
	char 		relpersistence;
	/* These fields are filled by transientrel_startup: */
	Relation	transientrel;	/* relation to write to */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
	uint64		processed;		/* GPDB: number of tuples inserted */
} DR_transientrel;

/* Hook for plugins to get control in transientrel_init */
typedef void (*transientrel_init_hook_type)(QueryDesc *queryDesc);
extern PGDLLIMPORT transientrel_init_hook_type transientrel_init_hook;

extern void SetMatViewPopulatedState(Relation relation, bool newstate);

extern ObjectAddress ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
										ParamListInfo params, QueryCompletion *qc);

extern DestReceiver *CreateTransientRelDestReceiver(Oid oid, Oid oldreloid, bool concurrent,
													char relpersistence, bool skipdata);

extern bool MatViewIncrementalMaintenanceIsEnabled(void);

extern void transientrel_init_internal(QueryDesc *queryDesc);

extern void transientrel_init(QueryDesc *queryDesc);

#endif							/* MATVIEW_H */
