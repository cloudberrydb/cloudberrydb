/*-------------------------------------------------------------------------
 *
 * nodeMaterial.c
 *	  Routines to handle materialization nodes.
 *
 * Portions Copyright (c) 2005-2008, Cloudberry inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMaterial.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecMaterial			- materialize the result of a subplan
 *		ExecInitMaterial		- initialize node and subnodes
 *		ExecEndMaterial			- shutdown node and subnodes
 *
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeMaterial.h"
#include "miscadmin.h"

#include "cdb/cdbvars.h"
#include "cdb/tupser.h"
#include "cdb/vectupser.h"
#include "executor/instrument.h"        /* Instrumentation */

#include "vecexecutor/executor.h"
#include "vecexecutor/execAmi.h"
#include "vecexecutor/nodeMaterial.h"
#include "vecexecutor/execslot.h"
#include "utils/tuplestore_vec.h"

static void ExecChildVecRescan(MaterialState *node);

static void ExecEagerFreeVecMaterial(MaterialState *node);

/* ----------------------------------------------------------------
 *		ExecMaterial
 *
 *		As long as we are at the end of the data collected in the tuplestore,
 *		we collect one new row from the subplan on each call, and stash it
 *		aside in the tuplestore before returning it.  The tuplestore is
 *		only read if we are asked to scan backwards, rescan, or mark/restore.
 *
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* result tuple from subplan */
ExecVecMaterial(PlanState *pstate)
{
	MaterialState *node = (MaterialState*) pstate;
	VecMaterialState *vnode = (VecMaterialState*) pstate;

	CHECK_FOR_INTERRUPTS();

	/* if is_skip is true, the material node do nothing but pass tuple slots */
	if (vnode->is_skip)
	{
		TupleTableSlot *outerslot = ExecProcNode(outerPlanState(node));

		return outerslot;
	}

	return ExecuteVecPlan(&((VecMaterialState*)pstate)->estate);
}

/* ----------------------------------------------------------------
 *		ExecInitMaterial
 * ----------------------------------------------------------------
 */
MaterialState *
ExecInitVecMaterial(Material *node, EState *estate, int eflags)
{
	VecMaterialState *vmatstate;
	MaterialState *matstate;
	Plan	   *outerPlan;
	/*
	 * create state structure
	 */
	vmatstate = (VecMaterialState*) palloc0(sizeof(VecMaterialState));
	NodeSetTag(vmatstate, T_MaterialState);
	vmatstate->is_skip = false;
	vmatstate->cdb_strict = node->cdb_strict;

	matstate = (MaterialState*)vmatstate;
	matstate->ss.ps.plan = (Plan *) node;
	matstate->ss.ps.state = estate;
	matstate->ss.ps.ExecProcNode = ExecVecMaterial;

	if (node->cdb_strict)
		eflags |= EXEC_FLAG_REWIND;

	/*
	 * If the Material node was inserted to protect the child node from rescanning, don't
	 * eager free.
	 *
	 * XXX: The planner doesn't always set the flag for Material nodes that are put
	 * directly on top of Motion nodes, so check for that, too. (Or is this for ORCA?)
	 */
	if (node->cdb_shield_child_from_rescans ||
		IsA(outerPlan((Plan *) node), Motion))
	{
		eflags |= EXEC_FLAG_REWIND;
	}

	/*
	 * We must have a tuplestore buffering the subplan output to do backward
	 * scan or mark/restore.  We also prefer to materialize the subplan output
	 * if we might be called on to rewind and replay it many times. However,
	 * if none of these cases apply, we can skip storing the data.
	 */
	matstate->eflags = (eflags & (EXEC_FLAG_REWIND |
								  EXEC_FLAG_BACKWARD |
								  EXEC_FLAG_MARK));

	/*
	 * Tuplestore's interpretation of the flag bits is subtly different from
	 * the general executor meaning: it doesn't think BACKWARD necessarily
	 * means "backwards all the way to start".  If told to support BACKWARD we
	 * must include REWIND in the tuplestore eflags, else tuplestore_trim
	 * might throw away too much.
	 */
	if (eflags & EXEC_FLAG_BACKWARD)
		matstate->eflags |= EXEC_FLAG_REWIND;

	matstate->eof_underlying = false;
	matstate->tuplestorestate = NULL;
	matstate->ts_destroyed = false;

	/*
	 * Miscellaneous initialization
	 *
	 * Materialization nodes don't need ExprContexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * If eflag contains EXEC_FLAG_REWIND or EXEC_FLAG_BACKWARD or EXEC_FLAG_MARK,
	 * then this node is not eager free safe.
	 */
	matstate->delayEagerFree =
		((eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) != 0);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	/*
	 * If Materialize does not have any external parameters, then it
	 * can shield the child node from being rescanned as well, hence
	 * we can clear the EXEC_FLAG_REWIND as well. If there are parameters,
	 * don't clear the REWIND flag, as the child will be rewound.
	 */
	if (node->plan.allParam == NULL || node->plan.extParam == NULL)
	{
		eflags &= ~EXEC_FLAG_REWIND;
	}

	outerPlan = outerPlan(node);
	/*
	 * A very basic check to see if the optimizer requires the material to do a projection.
	 * Ideally, this check would recursively compare all the target list expressions. However,
	 * such a check is tricky because of the varno mismatch (outer plan may have a varno that
	 * index into range table, while the material may refer to the same relation as "outer" varno)
	 * [JIRA: MPP-25365]
	 */
	if (list_length(node->plan.targetlist) != list_length(outerPlan->targetlist))
		elog(ERROR, "Material operator does not support projection");
	outerPlanState(matstate) = VecExecInitNode(outerPlan, estate, eflags);

	/*
	 * Initialize result type and slot. No need to initialize projection info
	 * because this node doesn't do projections.
	 *
	 * material nodes only return tuples from their materialized relation.
	 */
	ExecInitResultTupleSlotTL(&matstate->ss.ps, &TTSOpsVecTuple);
	matstate->ss.ps.ps_ProjInfo = NULL;

	/*
	 * initialize tuple type.
	 */
	ExecCreateScanSlotFromOuterPlan(estate, &matstate->ss, &TTSOpsVecTuple);
	BuildVecPlan((PlanState *)vmatstate, &vmatstate->estate);
	return matstate;
}


/* ----------------------------------------------------------------
 *		ExecEndMaterial
 * ----------------------------------------------------------------
 */
void
ExecEndVecMaterial(MaterialState *node)
{
	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate != NULL)
	{
		tuplestore_end_vec(node->tuplestorestate);
		node->ts_destroyed = true;
	}
	node->tuplestorestate = NULL;

	/*
	 * shut down the subplan
	 */
	VecExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecMaterialMarkPos
 *
 *		Calls tuplestore to save the current position in the stored file.
 * ----------------------------------------------------------------
 */
void
ExecVecMaterialMarkPos(MaterialState *node)
{
	Assert(node->eflags & EXEC_FLAG_MARK);

	/*
	 * if we haven't materialized yet, just return.
	 */
	if (!node->tuplestorestate)
		return;

	/*
	 * copy the active read pointer to the mark.
	 */
	tuplestore_copy_read_pointer(node->tuplestorestate, 0, 1);

	/*
	 * since we may have advanced the mark, try to truncate the tuplestore.
	 */
	tuplestore_trim_vec(node->tuplestorestate);
}

/* ----------------------------------------------------------------
 *		ExecMaterialRestrPos
 *
 *		Calls tuplestore to restore the last saved file position.
 * ----------------------------------------------------------------
 */
void
ExecVecMaterialRestrPos(MaterialState *node)
{
	Assert(node->eflags & EXEC_FLAG_MARK);

	/*
	 * if we haven't materialized yet, just return.
	 */
	if (!node->tuplestorestate)
		return;

	/*
	 * copy the mark to the active read pointer.
	 */
	tuplestore_copy_read_pointer(node->tuplestorestate, 1, 0);
}

/*
 * ExecChildVecRescan
 *      Helper function for rescanning child of materialize node
 */
static void
ExecChildVecRescan(MaterialState *node)
{
	Assert(node);
	/*
	 * if parameters of subplan have changed, then subplan will be rescanned by
	 * first ExecProcNode. Otherwise, we need to rescan subplan here
	 */
	if (node->ss.ps.lefttree->chgParam == NULL)
		ExecVecReScan(node->ss.ps.lefttree);

	node->eof_underlying = false;
}


/* ----------------------------------------------------------------
 *		ExecReScanMaterial
 *
 *		Rescans the materialized relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanVecMaterial(MaterialState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (node->eflags != 0)
	{
		/*
		 * If tuple store is empty, then either we have not materialized yet
		 * or tuple store was destroyed after a previous execution of materialize.
		 */
		if (!node->tuplestorestate)
		{
			/*
			 *  If tuple store was destroyed before, then materialize is part of subquery
			 *  execution, and we need to rescan child (MPP-15087).
			 */
			if (node->ts_destroyed)
			{
				ExecChildVecRescan(node);
			}
			return;
		}

		/*
		 * If subnode is to be rescanned then we forget previous stored
		 * results; we have to re-read the subplan and re-store.  Also, if we
		 * told tuplestore it needn't support rescan, we lose and must
		 * re-read.  (This last should not happen in common cases; else our
		 * caller lied by not passing EXEC_FLAG_REWIND to us.)
		 *
		 * Otherwise we can just rewind and rescan the stored output. The
		 * state of the subnode does not change.
		 */
		if (outerPlan->chgParam != NULL ||
			(node->eflags & EXEC_FLAG_REWIND) == 0)
		{
			tuplestore_end_vec(node->tuplestorestate);
			node->tuplestorestate = NULL;
			node->ts_destroyed = true;
			if (outerPlan->chgParam == NULL)
				ExecVecReScan(outerPlan);
			node->eof_underlying = false;
		}
		else
			tuplestore_rescan(node->tuplestorestate);
	}
	else
	{
		/* In this case we are just passing on the subquery's output */

		/*
		 * if chgParam of subnode is not null then plan will be re-scanned by
		 * first ExecProcNode.
		 */
		if (outerPlan->chgParam == NULL)
			ExecVecReScan(outerPlan);
		node->eof_underlying = false;
	}
}

static void
ExecEagerFreeVecMaterial(MaterialState *node)
{
	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate)
	{
		tuplestore_end_vec(node->tuplestorestate);
		node->ts_destroyed = true;
	}
	node->tuplestorestate = NULL;
}

void
ExecSquelchVecMaterial(MaterialState *node)
{
	/*
	 * If this Material is shielding the underlying nodes from rescanning (for
	 * example, if there is a Motion node below), then keep the tuplestore.
	 * Also, don't recurse to the subtree in that case, because we might need
	 * to read more tuples from it after a ReScan. Most likely we have already
	 * read all the tuples from the underlying node in that case, but it's
	 * possible that ExecMaterial hasn't been called even once yet, and we
	 * haven't created the tuplestore yet.
	 */
	if (!node->delayEagerFree)
	{
		ExecEagerFreeVecMaterial(node);
		ExecVecSquelchNode(outerPlanState(node));
	}
}
