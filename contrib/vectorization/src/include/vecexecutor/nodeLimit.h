/*-------------------------------------------------------------------------
 *
 * nodeLimit.h
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeLimit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODELIMIT_H
#define VEC_NODELIMIT_H

#include "nodes/execnodes.h"

#include "vecexecutor/execnodes.h"

extern LimitState *ExecInitVecLimit(Limit *node, EState *estate, int eflags);
extern void ExecEndVecLimit(LimitState *node);
extern void ExecReScanVecLimit(LimitState *node);

#endif							/* NODELIMIT_H */
