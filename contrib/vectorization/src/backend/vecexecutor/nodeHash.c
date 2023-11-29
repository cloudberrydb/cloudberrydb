#include "postgres.h"

#include <math.h>
#include <limits.h>

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/tablespace.h"
#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/dynahash.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/faultinjector.h"
#include "utils/syscache.h"

#include "utils/arrow.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/nodeHash.h"

/* ----------------------------------------------------------------
 *		ExecInitHash
 *
 *		Init routine for Hash node
 * ----------------------------------------------------------------
 */
HashState *
ExecInitVecHash(Hash *node, EState *estate, int eflags)
{
    HashState *hashstate;
    VecHashState *vhashstate;

    /* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	vhashstate = (VecHashState*) palloc0(sizeof(VecHashState));
	hashstate = (HashState *) vhashstate;
	NodeSetTag(hashstate, T_HashState);
	hashstate->ps.plan = (Plan *) node;
	hashstate->ps.state = estate;
	hashstate->ps.ExecProcNode = ExecVecHash;
	hashstate->hashtable = NULL;
	hashstate->hashkeys = NIL;	/* will be set by parent HashJoin */

	hashstate->rfstate = NULL;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hashstate->ps);

	/*
	 * initialize child nodes
	 */
	outerPlanState(hashstate) = VecExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize our result slot and type. No need to build projection
	 * because this node doesn't do projections.
	 */
	ExecInitResultTupleSlotTL(&hashstate->ps, &TTSOpsVecTuple);
	hashstate->ps.ps_ProjInfo = NULL;

	/*
	 * initialize child expressions
	 */
	Assert(node->plan.qual == NIL);
	hashstate->hashkeys =
		ExecInitExprList(node->hashkeys, (PlanState *) hashstate);

	return hashstate;
}

/* ----------------------------------------------------------------
 *		ExecVecHash
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecVecHash(PlanState *pstate)
{
	return ExecProcNode(outerPlanState(pstate));
}

/* ---------------------------------------------------------------
 *		ExecEndHash
 *
 *		clean up routine for Hash node
 * ----------------------------------------------------------------
 */
void
ExecEndVecHash(HashState *node)
{
	PlanState  *outerPlan;

	/*
	 * free exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * shut down the subplan
	 */
	outerPlan = outerPlanState(node);
	VecExecEndNode(outerPlan);
}
