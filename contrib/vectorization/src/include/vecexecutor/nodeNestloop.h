/*-------------------------------------------------------------------------
 *
 * nodeNestloop.h
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/vecexecutor/nodeNestloop.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODE_NEST_LOOP_H
#define VEC_NODE_NEST_LOOP_H

#include "nodes/execnodes.h"

extern NestLoopState *ExecInitVecNestLoop(NestLoop *node, EState *estate, int eflags);
extern void ExecEndVecNestLoop(NestLoopState *node);
extern void ExecSquelchVecNestLoop(NestLoopState *node);
#endif							/* VEC_NODE_NEST_LOOP_H */
