/*-------------------------------------------------------------------------
 *
 * cdbdisp_query.h
 * routines for dispatching command string or plan to the qExec processes.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISP_QUERY_H
#define CDBDISP_QUERY_H

#include "lib/stringinfo.h" /* StringInfo */
#include "cdb/cdbtm.h"

#define DF_NONE 0x0
#define DF_CANCEL_ON_ERROR 0x1
#define DF_NEED_TWO_PHASE 0x2
#define DF_WITH_SNAPSHOT  0x4

struct QueryDesc;
struct CdbDispatcherState;
struct CdbPgResults;

/* Compose and dispatch the MPPEXEC commands corresponding to a plan tree
 * within a complete parallel plan.
 *
 * The CdbDispatchResults objects allocated for the plan are
 * returned in *pPrimaryResults
 * The caller, after calling CdbCheckDispatchResult(), can
 * examine the CdbDispatchResults objects, can keep them as
 * long as needed, and ultimately must free them with
 * cdbdisp_destroyDispatchState() prior to deallocation
 * of the caller's memory context.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchState() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
void
cdbdisp_dispatchPlan(struct QueryDesc *queryDesc,
					 bool planRequiresTxn,
					 bool cancelOnError,
					 struct CdbDispatcherState *ds);

/*
 * Special for sending SET commands that change GUC variables, so they go to all
 * gangs, both reader and writer
 */
void
CdbSetGucOnAllGangs(const char *strCommand, bool cancelOnError, bool needTwoPhase);

/*
 * CdbDoCommand:
 * Combination of cdbdisp_dispatchCommand and cdbdisp_finishCommand.
 * Called by general users, this method includes global transaction control.
 * If not all QEs execute the command successfully, throws an error and
 * does not return.
 *
 * needTwoPhase specifies whether to dispatch within a distributed 
 * transaction or not.
 */
void
CdbDoCommand(const char *strCommand, bool cancelOnError, bool needTwoPhase);

/* Dispatch a command - already parsed and in the form of a Node
 * tree - to all primary segdbs.  Does not wait for
 * completion.  Does not start a global transaction.
 *
 *
 * The needTwoPhase flag indicates whether you want the dispatched 
 * statement to participate in a distributed transaction or not.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchState() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
void
cdbdisp_dispatchUtilityStatement(struct Node *stmt,
								 bool cancelOnError,
								 bool needTwoPhase,
								 bool withSnapshot,
								 struct CdbDispatcherState *ds,
								 char* debugCaller __attribute__((unused)));

/*
 * cdbdisp_dispatchRMCommand:
 * Sends a non-cancelable command to all segment dbs, primary
 *
 * Returns a malloc'ed array containing the PGresult objects thus
 * produced; the caller must PQclear() them and free() the array.
 * A NULL entry follows the last used entry in the array.
 *
 * Any error messages - whether or not they are associated with
 * PGresult objects - are appended to a StringInfo buffer provided
 * by the caller.
 */
struct pg_result **
cdbdisp_dispatchRMCommand(const char *strCommand,
						  bool withSnapshot,
						  StringInfo errmsgbuf,
						  int *numresults);

/*
 * CdbDispatchUtilityStatement
 *
 * Dispatch an already parsed statement to all primary writer QEs, wait until
 * all QEs finished successfully. If one or more QEs got error,
 * throw an Error.
 *
 * -flags:
 * 	Is the combination of DF_NEED_TWO_PHASE, DF_WITH_SNAPSHOT,DF_CANCEL_ON_ERROR
 *
 * -cdb_pgresults:
 * 	Indicate whether return the pg_result for each QE connection.
 */
void
CdbDispatchUtilityStatement(struct Node *stmt,
							int flags,
							struct CdbPgResults* cdb_pgresults);

#endif   /* CDBDISP_QUERY_H */
