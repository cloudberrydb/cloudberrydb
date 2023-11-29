/*
 * FIXME: This file will be deleted in the future
 */

#ifndef HOOK_H 
#define HOOK_H


#include "postgres.h"
#include "nodes/parsenodes.h"
#include "commands/explain.h"
#include "tcop/tcopprot.h"

extern void VecExplainOneQuery(Query *query, int cursorOptions,
                               IntoClause *into, ExplainState *es,
                               const char *queryString, ParamListInfo params,
                               QueryEnvironment *queryEnv);
extern void VecExecutorStart(QueryDesc *queryDesc, int eflags);
extern void VecExecutorEnd(QueryDesc *queryDesc);
extern bool
donothingVecReceive(TupleTableSlot *slot, DestReceiver *self);
extern void
donothingVecStartup(DestReceiver *self, int operation, TupleDesc typeinfo);
extern void
donothingVecCleanup(DestReceiver *self);
#endif
