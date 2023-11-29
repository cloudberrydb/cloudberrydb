/*-------------------------------------------------------------------------
 *
 * nodeHash.h
 *	  prototypes for nodeHashjoin.c
 * 
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/nodeHashjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODE_HASHJOIN_H
#define VEC_NODE_HASHJOIN_H

#include "nodes/execnodes.h"
#include "storage/buffile.h"
#include "vecexecutor/execnodes.h"

extern HashJoinState *ExecInitVecHashJoin(HashJoin *node, EState *estate, int eflags);
extern TupleTableSlot *ExecVecHashJoin(PlanState *pstate);
extern void ExecEndVecHashJoin(HashJoinState *node);
extern void ExecSquelchVecHashJoin(HashJoinState *node);
#endif   /* VEC_NODE_HASHJOIN_H */