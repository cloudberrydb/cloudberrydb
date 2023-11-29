/*-------------------------------------------------------------------------
 *
 * nodeVecAppend.h
 *
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/vecexecutor/nodeAppend.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEVECAPPEND_H
#define NODEVECAPPEND_H

#include "nodes/execnodes.h"
#include "vecexecutor/executor.h"

extern AppendState *ExecInitVecAppend(Append *node, EState *estate, int eflags);
extern void ExecEndVecAppend(AppendState *node);
extern void ExecReScanVecAppend(AppendState *node);
extern void ExecSquelchVecAppend(AppendState *node);

#endif   /* NODEVECAPPEND_H */
