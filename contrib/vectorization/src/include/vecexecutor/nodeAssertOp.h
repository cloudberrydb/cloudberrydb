#ifndef VEC_NODE_ASSERT_OP_H
#define VEC_NODE_ASSERT_OP_H

#include "executor/tuptable.h"
#include "nodes/execnodes.h"

#include "vecexecutor/execnodes.h"

extern AssertOpState* ExecInitVecAssertOp(AssertOp *node, EState *estate, int eflags); 
extern TupleTableSlot* ExecVecAssertOp(PlanState *pstate);
extern void ExecEndVecAssertOp(AssertOpState *node);
extern void ExecReScanAssertOp(AssertOpState *node);
extern void ExecSquelchVecAssertOp(AssertOpState *node);

#endif /* VEC_NODE_ASSERT_OP_H */