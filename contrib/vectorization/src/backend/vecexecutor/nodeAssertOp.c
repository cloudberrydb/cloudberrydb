#include "postgres.h"

#include "executor/executor.h"

#include "vecexecutor/nodeAssertOp.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execAmi.h"

AssertOpState *
ExecInitVecAssertOp(AssertOp *node, EState *estate, int eflags)
{
	VecAssertOpState *vassertOpState;
	AssertOpState *assertOpState;

	/* Check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
	Assert(outerPlan(node) != NULL);

	vassertOpState = (VecAssertOpState *)palloc0(sizeof(VecAssertOpState));
	assertOpState = (AssertOpState *)vassertOpState;
	NodeSetTag(assertOpState, T_AssertOpState);
	assertOpState->ps.plan = (Plan *)node;
	assertOpState->ps.state = estate;
	assertOpState->ps.ExecProcNode = ExecVecAssertOp;

	ExecAssignExprContext(estate, &assertOpState->ps);

	/*
	 * Initialize outer plan
	 */
	outerPlanState(assertOpState) = VecExecInitNode(outerPlan(node), estate, eflags);

	ExecInitResultTupleSlotTL(&(assertOpState->ps), &TTSOpsVecTuple);

	BuildVecPlan((PlanState *)vassertOpState, &vassertOpState->estate);

	return assertOpState;
}

TupleTableSlot *
ExecVecAssertOp(PlanState *pstate)
{
	return ExecuteVecPlan(&((VecAssertOpState*)pstate)->estate);
}

static void
ExecEagerFreeVecAssertOp(VecAssertOpState *vnode)
{
	FreeVecExecuteState(&vnode->estate);
}

/* Release Resources Requested by AssertOp node. */
void ExecEndVecAssertOp(AssertOpState *node)
{
	VecAssertOpState *vnode = (VecAssertOpState *) node;
	
	ExecFreeExprContext(&node->ps);

	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	ExecEagerFreeVecAssertOp(vnode);
	/*
	 * shut down subplans
	 */
	VecExecEndNode(outerPlanState(node));
}

void ExecReScanAssertOp(AssertOpState *node)
{
	/*
	 * If chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree && node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}

void
ExecSquelchVecAssertOp(AssertOpState *node)
{
	VecAssertOpState *vnode = (VecAssertOpState *) node;
	ExecEagerFreeVecAssertOp(vnode);
	ExecVecSquelchNode(outerPlanState(node));
}