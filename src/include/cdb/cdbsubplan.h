/*-------------------------------------------------------------------------
 *
 * cdbsubplan.h
 * routines for preprocessing initPlan subplans
 *		and executing queries with initPlans *
 * Copyright (c) 2003-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBSUBPLAN_H
#define CDBSUBPLAN_H

#include "executor/execdesc.h"
#include "nodes/params.h"
#include "nodes/plannodes.h"

extern void preprocess_initplans(QueryDesc *queryDesc);
extern ParamListInfo addRemoteExecParamsToParamList(PlannedStmt *stmt, ParamListInfo p, ParamExecData *prm);

#endif   /* CDBSUBPLAN_H */
