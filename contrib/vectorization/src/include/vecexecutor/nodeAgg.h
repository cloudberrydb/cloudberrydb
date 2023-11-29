/*-------------------------------------------------------------------------
 *
 * nodeAgg.h
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/nodeAgg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODE_AGG_H
#define VEC_NODE_AGG_H

#include "nodes/execnodes.h"
#include "executor/tuptable.h"

extern TupleTableSlot *ExecVecAgg_arrow(AggState *node);

extern AggState *ExecInitVecAgg(Agg *node, EState *estate, int eflags);
extern void ExecEndVecAgg(AggState *node);
extern void ExecSquelchVecAgg(AggState *aggstate);


#endif /* VEC_NODE_AGG_H */