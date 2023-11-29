/*-------------------------------------------------------------------------
 *
 * nodeResult.h
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeResult.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODERESULT_H
#define VEC_NODERESULT_H

#include "nodes/execnodes.h"

#include "vecnodes/plannodes.h"
#include "vecexecutor/execnodes.h"

extern ResultState *ExecInitVecResult(Result *node, EState *estate, int eflags);
extern void ExecEndVecResult(ResultState *node);
extern void ExecVecResultMarkPos(ResultState *node);
extern void ExecVecResultRestrPos(ResultState *node);
extern void ExecReScanVecResult(ResultState *node);
extern void ExecSquelchVecResult(ResultState *node);
#endif							/* NODERESULT_H */
