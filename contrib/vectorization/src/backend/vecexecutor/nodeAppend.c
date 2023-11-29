/*-------------------------------------------------------------------------
 *
 * nodeAppend.c
 *	  routines to handle append nodes.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/nodeAppend.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitAppend	- initialize the append node
 *		ExecAppend		- retrieve the next tuple from the node
 *		ExecEndAppend	- shut down the append node
 *		ExecReScanAppend - rescan the append node
 *
 *	 NOTES
 *		Each append node contains a list of one or more subplans which
 *		must be iteratively processed (forwards or backwards).
 *		Tuples are retrieved by executing the 'whichplan'th subplan
 *		until the subplan stops returning tuples, at which point that
 *		plan is shut down and the next started up.
 *
 *		Append nodes don't make use of their left and right
 *		subtrees, rather they maintain a list of subplans so
 *		a typical append node looks like this in the plan tree:
 *
 *				   ...
 *				   /
 *				Append -------+------+------+--- nil
 *				/	\		  |		 |		|
 *			  nil	nil		 ...    ...    ...
 *								 subplans
 *
 *		Append nodes are currently used for unions, and to support
 *		inheritance queries, where several relations need to be scanned.
 *		For example, in our standard person/student/employee/student-emp
 *		example, where student and employee inherit from person
 *		and student-emp inherits from student and employee, the
 *		query:
 *
 *				select name from person
 *
 *		generates the plan:
 *
 *				  |
 *				Append -------+-------+--------+--------+
 *				/	\		  |		  |		   |		|
 *			  nil	nil		 Scan	 Scan	  Scan	   Scan
 *							  |		  |		   |		|
 *							person employee student student-emp
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "vecexecutor/nodeAppend.h"

#include "vecexecutor/execAmi.h"
static bool exec_append_initialize_next(AppendState *appendstate);
static TupleTableSlot *ExecVecAppend(PlanState *pstate);

/* ----------------------------------------------------------------
 *		exec_append_initialize_next
 *
 *		Sets up the append state node for the "next" scan.
 *
 *		Returns t iff there is a "next" scan to process.
 * ----------------------------------------------------------------
 */
static bool
exec_append_initialize_next(AppendState *appendstate)
{
	int			whichplan;

	/*
	 * get information from the append node
	 */
	whichplan = appendstate->as_whichplan;

	if (whichplan < 0)
	{
		/*
		 * if scanning in reverse, we start at the last scan in the list and
		 * then proceed back to the first.. in any case we inform ExecAppend
		 * that we are at the end of the line by returning FALSE
		 */
		appendstate->as_whichplan = 0;
		return FALSE;
	}
	else if (whichplan >= appendstate->as_nplans)
	{
		/*
		 * as above, end the scan if we go beyond the last scan in our list..
		 */
		appendstate->as_whichplan = appendstate->as_nplans - 1;
		return FALSE;
	}
	else
	{
		return TRUE;
	}
}

AppendState *
ExecInitVecAppend(Append *node, EState *estate, int eflags)
{
    VecAppendState *vecappendstate;
    AppendState *appendstate;
	PlanState **appendplanstates;
	int			nplans;
	int			i;
	ListCell   *lc;

    vecappendstate = (VecAppendState*) palloc0(sizeof(VecAppendState));
    appendstate = (AppendState *)vecappendstate;
    NodeSetTag(appendstate, T_AppendState);

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * Set up empty vector of subplan states
	 */
	nplans = list_length(node->appendplans);


	/* node returns slots from each of its subnodes, therefore not fixed */
	appendstate->ps.resultopsset = true;
	appendstate->ps.resultopsfixed = false;
	appendplanstates = (PlanState **) palloc0(nplans * sizeof(PlanState *));

	/*
	 * create new AppendState for our append node
	 */
	appendstate->ps.plan = (Plan *) node;
	appendstate->ps.ExecProcNode = ExecVecAppend;
	appendstate->ps.state = estate;
	appendstate->appendplans = appendplanstates;
	appendstate->as_nplans = nplans;

	/*
	 * Initialize result tuple type and slot.
	 */
	ExecInitResultTupleSlotTL(&appendstate->ps, &TTSOpsVecTuple);

	/*
	 * call ExecInitNode on each of the plans to be executed and save the
	 * results into the array "appendplans".
	 */
	i = 0;
	foreach(lc, node->appendplans)
	{
		Plan	   *initNode = (Plan *) lfirst(lc);

		appendplanstates[i] = VecExecInitNode(initNode, estate, eflags);
		i++;
	}

	/*
	 * initialize output tuple type
	 */
	appendstate->ps.ps_ProjInfo = NULL;

	/*
	 * initialize to scan first subplan
	 */
	appendstate->as_whichplan = 0;
	exec_append_initialize_next(appendstate);
	vecappendstate->schema = NULL;

	return appendstate;
}

/* ----------------------------------------------------------------
 *	   ExecVecAppend
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecVecAppend(PlanState *pstate)
{
	AppendState *node = (AppendState *) pstate;
	VecAppendState *vnode = (VecAppendState *)node;
	for (;;)
	{
		PlanState  *subnode;
		TupleTableSlot *result;
		g_autoptr(GError) error = NULL;

		/*
		 * figure out which subplan we are currently processing
		 */
		subnode = node->appendplans[node->as_whichplan];

		/*
		 * get a tuple from the subplan
		 */
		result = ExecProcNode(subnode);

		if (!TupIsNull(result))
		{
			if (!vnode->schema)
			{
				vnode->schema = garrow_record_batch_get_schema(VECSLOT(result)->tts_recordbatch);
			} 
			else 
			{
				garrow_record_batch_rewrite_schema(VECSLOT(result)->tts_recordbatch, vnode->schema, &error);
				if (error != NULL) 
					elog(ERROR, "append rewrite schema failed.");
			}
			
			/*
			 * If the subplan gave us something then return it as-is. We do
			 * NOT make use of the result slot that was set up in
			 * ExecInitVecAppend; there's no need for it.
			 */
			return result;
		}

		/*
		 * Go on to the "next" subplan in the appropriate direction. If no
		 * more subplans, return the empty slot set up for us by
		 * ExecInitVecAppend.
		 */
		if (ScanDirectionIsForward(node->ps.state->es_direction))
			node->as_whichplan++;
		else
			node->as_whichplan--;
		if (!exec_append_initialize_next(node))
			return ExecClearTuple(node->ps.ps_ResultTupleSlot);

		/* Else loop back and try to get a tuple from the new subplan */
	}
}

/* ----------------------------------------------------------------
 *		ExecEndVecAppend
 *
 *		Shuts down the subscans of the append node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndVecAppend(AppendState *node)
{
	PlanState **appendplans;
	VecAppendState *vnode;
	int			nplans;
	int			i;

	vnode = (VecAppendState *) node;
	/*
	 * get information from the node
	 */
	appendplans = node->appendplans;
	nplans = node->as_nplans;

	ARROW_FREE(GArrowSchema, &vnode->schema);

	vnode->schema = NULL;

	/*
	 * shut down each of the subscans
	 */
	for (i = nplans-1; i >= 0; --i) 
	{
		if (appendplans[i])
			VecExecEndNode(appendplans[i]);
	}
}

void
ExecReScanVecAppend(AppendState *node)
{
	int			i;

	for (i = 0; i < node->as_nplans; i++)
	{
		PlanState  *subnode = node->appendplans[i];

		/*
		 * ExecReScan doesn't know about my subplans, so I have to do
		 * changed-parameter signaling myself.
		 */
		if (node->ps.chgParam != NULL)
			UpdateChangedParamSet(subnode, node->ps.chgParam);

		/*
		 * If chgParam of subnode is not null then plan will be re-scanned by
		 * first ExecProcNode.
		 */
		if (subnode->chgParam == NULL)
			ExecReScan(subnode);
	}
	node->as_whichplan = 0;
	exec_append_initialize_next(node);
}

void
ExecSquelchVecAppend(AppendState *node)
{
	int			i;

	for (i = 0; i < node->as_nplans; i++)
		ExecVecSquelchNode(node->appendplans[i]);
}