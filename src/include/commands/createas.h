/*-------------------------------------------------------------------------
 *
 * createas.h
 *	  prototypes for createas.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/createas.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CREATEAS_H
#define CREATEAS_H

#include "access/heapam.h"
#include "catalog/objectaddress.h"
#include "executor/execdesc.h"
#include "nodes/params.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"
#include "utils/queryenvironment.h"

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	IntoClause *into;			/* target relation specification */
	/* These fields are filled by intorel_startup: */
	Relation	rel;			/* relation to write to */
	ObjectAddress reladdr;		/* address of rel, for ExecCreateTableAs */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
} DR_intorel;

/* Hook for plugins to get control in intorel_initplan */
typedef void (*intorel_initplan_hook_type) (QueryDesc *queryDesc, int eflags);
extern PGDLLIMPORT intorel_initplan_hook_type intorel_initplan_hook;

extern ObjectAddress ExecCreateTableAs(ParseState *pstate, CreateTableAsStmt *stmt,
									   ParamListInfo params, QueryEnvironment *queryEnv,
									   QueryCompletion *qc);

extern int	GetIntoRelEFlags(IntoClause *intoClause);

extern DestReceiver *CreateIntoRelDestReceiver(IntoClause *intoClause);


struct QueryDesc;
extern void intorel_initplan_internal(struct QueryDesc *queryDesc, int eflags);
extern void intorel_initplan(struct QueryDesc *queryDesc, int eflags);

extern bool CreateTableAsRelExists(CreateTableAsStmt *ctas);


#endif							/* CREATEAS_H */
