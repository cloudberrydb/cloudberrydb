/*-------------------------------------------------------------------------
 *
 * execAmi.h
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_EXECAMI_H
#define VEC_EXECAMI_H

#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "vecnodes/plannodes.h"
#include "vecexecutor/execnodes.h"

extern void ExecVecReScan(PlanState *node);
extern void ExecVecMarkPos(PlanState *node);
extern void ExecVecSquelchNode(PlanState *node);
extern void ExecVecRestrPos(PlanState *node);
extern bool ExecVecSupportsMarkRestore(Path *pathnode);
extern bool ExecVecSupportsBackwardScan(Plan *node);
extern bool ExecVecMaterializesOutput(NodeTag plantype);

#endif							/* EXECAMI_H */
