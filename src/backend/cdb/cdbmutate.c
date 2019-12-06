/*-------------------------------------------------------------------------
 *
 * cdbmutate.c
 *	  Parallelize a PostgreSQL sequential plan tree.
 *
 * Portions Copyright (c) 2004-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbmutate.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "parser/parsetree.h"	/* for rt_fetch() */
#include "utils/relcache.h"		/* RelationGetPartitioningKey() */
#include "optimizer/planmain.h"
#include "optimizer/var.h"
#include "parser/parse_relation.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/datum.h"
#include "utils/syscache.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "nodes/makefuncs.h"

#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "catalog/gp_policy.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"

#include "cdb/cdbhash.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbmutate.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbpullup.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbtargeteddispatch.h"

#include "executor/executor.h"

/*
 * ApplyMotionState holds state for the recursive apply_motion_mutator().
 */
typedef struct ApplyMotionSubPlanState
{
	int			sliceDepth;
	Flow	   *parentFlow;
	Flow	   *outerQueryFlow;
	bool		is_initplan;
	bool		useHashTable;
	bool		processed;

} ApplyMotionSubPlanState;

typedef struct ApplyMotionState
{
	plan_tree_base_prefix base; /* Required prefix for
								 * plan_tree_walker/mutator */
	int			nextMotionID;
	int			sliceDepth;

	ApplyMotionSubPlanState *subplans;

	List	   *subplan_workingQueue;

	Flow	   *currentPlanFlow;
	Flow	   *outer_query_flow;
} ApplyMotionState;

typedef struct
{
	plan_tree_base_prefix base; /* Required prefix for
								 * plan_tree_walker/mutator */
	EState	   *estate;
	bool		single_row_insert;
	List	   *cursorPositions;
} pre_dispatch_function_evaluation_context;

/*
 * Forward Declarations
 */
static Node *pre_dispatch_function_evaluation_mutator(Node *node,
										 pre_dispatch_function_evaluation_context *context);
static Node *apply_motion_mutator(Node *node, ApplyMotionState *context);
static bool replace_shareinput_targetlists_walker(Node *node, PlannerInfo *root, bool fPop);

static void
directDispatchCalculateHash(Plan *plan, GpPolicy *targetPolicy, Oid *hashfuncs)
{
	int			i;
	ListCell   *cell = NULL;
	bool		directDispatch;
	Datum	   *values;
	bool	   *nulls;

	values = (Datum *) palloc(targetPolicy->nattrs * sizeof(Datum));
	nulls = (bool *) palloc(targetPolicy->nattrs * sizeof(bool));

	/*
	 * the nested loops here seem scary -- especially since we've already
	 * walked them before -- but I think this is still required since they may
	 * not be in the same order. (also typically we don't distribute by more
	 * than a handful of attributes).
	 */
	directDispatch = true;
	for (i = 0; i < targetPolicy->nattrs; i++)
	{
		foreach(cell, plan->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(cell);
			Const	   *c;

			Assert(tle->expr);

			if (tle->resno != targetPolicy->attrs[i])
				continue;

			if (!IsA(tle->expr, Const))
			{
				/* the planner could not simplify this */
				directDispatch = false;
				break;
			}

			c = (Const *) tle->expr;

			values[i] = c->constvalue;
			nulls[i] = c->constisnull;
			break;
		}

		if (!directDispatch)
			break;
	}

	plan->directDispatch.isDirectDispatch = directDispatch;
	if (directDispatch)
	{
		uint32		hashcode;
		CdbHash    *h;

		h = makeCdbHash(targetPolicy->numsegments, targetPolicy->nattrs, hashfuncs);

		cdbhashinit(h);
		for (i = 0; i < targetPolicy->nattrs; i++)
		{
			cdbhash(h, i + 1, values[i], nulls[i]);
		}

		/* We now have the hash-partition that this row belong to */
		hashcode = cdbhashreduce(h);
		plan->directDispatch.contentIds = list_make1_int(hashcode);

		elog(DEBUG1, "sending single row constant insert to content %d", hashcode);
	}
	pfree(values);
	pfree(nulls);
}

/* -------------------------------------------------------------------------
 * Function apply_motion() adds Motion nodes on top of plan trees for
 * SubPlans, so that the subplan results are available at the correct nodes
 * for the outer query. It also assigns motionIDs to all the motions in the
 * plan tree.
 *
 * The result is a deep copy of the argument Plan tree with added/modified
 * Motion nodes.
 * -------------------------------------------------------------------------
 */
Plan *
apply_motion(PlannerInfo *root, Plan *plan)
{
	Query	   *query = root->parse;
	Plan	   *result;
	int			nInitPlans;
	ApplyMotionState state;
	int			numsegments = getgpsegmentCount();
	int			nsubplans;

	/* Initialize mutator context. */
	planner_init_plan_tree_base(&state.base, root); /* error on attempt to
													 * descend into subplan
													 * plan */
	state.nextMotionID = 1;		/* Start at 1 so zero will mean "unassigned". */
	state.sliceDepth = 0;
	state.subplan_workingQueue = NIL;

	nsubplans = list_length(root->glob->subplans);
	state.subplans = (ApplyMotionSubPlanState *) palloc((nsubplans + 1) * sizeof(ApplyMotionSubPlanState));

	for (int i = 1; i <= nsubplans; i++)
	{
		ApplyMotionSubPlanState *sstate = &state.subplans[i];

		sstate->sliceDepth = -1;
		sstate->parentFlow = NULL;
		sstate->outerQueryFlow = NULL;
		sstate->is_initplan = false;
		sstate->processed = false;
	}

	/*
	 * Process the main plan tree.
	 */
	state.outer_query_flow = plan->flow;
	state.currentPlanFlow = plan->flow;
	result = (Plan *) apply_motion_mutator((Node *) plan, &state);

	/*
	 * Process any SubPlans. While we process the SubPlans, we might
	 * encounter more SubPlans. They will be added to the working queue,
	 * so keep going until the working queue is empty.
	 */
	while (state.subplan_workingQueue)
	{
		int			plan_id = linitial_int(state.subplan_workingQueue);
		ApplyMotionSubPlanState *sstate = &state.subplans[plan_id];
		ListCell   *planlist_cell = list_nth_cell(root->glob->subplans, plan_id - 1);
		Plan	   *subplan = (Plan *) lfirst(planlist_cell);

		state.subplan_workingQueue = list_delete_first(state.subplan_workingQueue);

		if (sstate->is_initplan)
		{
			/*
			 * InitPlans are dispatched separately, before the main plan,
			 * and the result is brought to the QD.
			 */
			Flow	   *topFlow = makeFlow(FLOW_SINGLETON, getgpsegmentCount());

			topFlow->segindex = -1;
			state.sliceDepth = 0;
			state.outer_query_flow = topFlow;
			state.currentPlanFlow = topFlow;
		}
		else
		{
			state.sliceDepth = sstate->sliceDepth;
			state.outer_query_flow = sstate->outerQueryFlow;
			state.currentPlanFlow = sstate->parentFlow;
		}

		if (sstate->processed)
			elog(ERROR, "SubPlan %d already processed", plan_id);

		/*
		 * If the subquery result is not available where the outer query needs it,
		 * we have to add a Motion node to redistribute it.
		 */
		if (subplan->flow->locustype != CdbLocusType_OuterQuery &&
			subplan->flow->locustype != CdbLocusType_General)
		{
			numsegments = state.outer_query_flow->numsegments;
			switch (state.outer_query_flow->flotype)
			{
				case FLOW_SINGLETON:
					subplan = focusPlan(subplan, false);
					break;
				case FLOW_REPLICATED:
				case FLOW_PARTITIONED:
					if (subplan->flow->locustype != CdbLocusType_SegmentGeneral)
						subplan = broadcastPlan(subplan, false, numsegments);
					break;
				default:
					elog(ERROR, "unexpected flow type %d in parent of SubPlan expression",
						 state.outer_query_flow->flotype);
			}

			/*
			 * If we created a Motion, protect it from rescanning. Init Plans
			 * and hashed SubPlans are never rescanned.
			 */
			if (IsA(subplan, Motion) && !sstate->is_initplan &&
				!sstate->useHashTable)
				subplan = (Plan *) make_material(subplan);
		}

		subplan = (Plan *) apply_motion_mutator((Node *) subplan, &state);

		lfirst(planlist_cell) = subplan;
	}

	root->glob->nMotionNodes = state.nextMotionID - 1;

	/*
	 * Assign slice numbers to the initplans.
	 *
	 * The callers should've allocated an array of correct size for us.
	 */
	Assert(root->glob->subplan_sliceIds);
	nInitPlans = 0;
	for (int plan_id = 1; plan_id <= nsubplans; plan_id++)
	{
		ApplyMotionSubPlanState *sstate = &state.subplans[plan_id];

		Assert(plan_id <= list_length(root->glob->subplans));

		if (sstate->is_initplan)
		{
			root->glob->subplan_sliceIds[plan_id] = state.nextMotionID++;
			nInitPlans++;
		}
	}
	root->glob->nInitPlans = nInitPlans;

	/*
	 * Discard subtrees of Query node that aren't needed for execution. Note
	 * the targetlist (query->targetList) is used in execution of prepared
	 * statements, so we leave that alone.
	 */
	query->jointree = NULL;
	query->groupClause = NIL;
	query->havingQual = NULL;
	query->distinctClause = NIL;
	query->sortClause = NIL;
	query->limitCount = NULL;
	query->limitOffset = NULL;
	query->setOperations = NULL;

	return result;
}


/*
 * Function apply_motion_mutator() is the workhorse for apply_motion().
 */
static Node *
apply_motion_mutator(Node *node, ApplyMotionState *context)
{
	Node	   *newnode;
	Plan	   *plan;
	Flow	   *saveCurrentPlanFlow;

#ifdef USE_ASSERT_CHECKING
	PlannerInfo *root = (PlannerInfo *) context->base.node;
	Assert(root && IsA(root, PlannerInfo));
#endif

	if (node == NULL)
		return NULL;

	/* An expression node might have subtrees containing plans to be mutated. */
	if (!is_plan_node(node))
	{
		node = plan_tree_mutator(node, apply_motion_mutator, context, false);

		/*
		 * If we see a SubPlan, remember the context where we saw it. We memorize
		 * the parent's node's Flow, so that we know where the SubPlan needs to
		 * bring the result. There might be multiple references to the same SubPlan,
		 * if e.g. the result of the SubPlan is passed up the plan for use in later
		 * joins or for the final target list. Remember the *deepest* slice level
		 * where we saw it. We cannot pass SubPlan's result down the tree, but we
		 * can propagate it upwards if we put it to the target list. So it needs
		 * to be calculated at the bottom level, and propagated up from there.
		 */
		if (IsA(node, SubPlan))
		{
			SubPlan	   *spexpr = (SubPlan *) node;
			ApplyMotionSubPlanState *sstate = &context->subplans[spexpr->plan_id];

			sstate->is_initplan = spexpr->is_initplan;
			sstate->useHashTable = spexpr->useHashTable;

			if (sstate->processed)
				elog(ERROR, "found reference to already-processed SubPlan");
			if (sstate->sliceDepth == -1)
				context->subplan_workingQueue = lappend_int(context->subplan_workingQueue,
															spexpr->plan_id);

			if (!spexpr->is_initplan && context->sliceDepth > sstate->sliceDepth)
			{
				sstate->sliceDepth = context->sliceDepth;
				if (context->currentPlanFlow->locustype == CdbLocusType_OuterQuery)
					sstate->outerQueryFlow = context->outer_query_flow;
				else
					sstate->outerQueryFlow = context->currentPlanFlow;
				sstate->parentFlow = context->currentPlanFlow;
			}
		}
		return node;
	}

	plan = (Plan *) node;

	if (IsA(plan, Motion))
	{
		Motion	   *motion = (Motion *) plan;

		/* Sanity check */
		/* Sub plan must have flow */
		Assert(plan->flow && motion->plan.lefttree->flow);

		/* If top slice marked as singleton, make it a dispatcher singleton. */
		if ((motion->motionType == MOTIONTYPE_GATHER || motion->motionType == MOTIONTYPE_GATHER_SINGLE)
			&& context->sliceDepth == 0)
		{
			plan->flow->segindex = -1;
		}

		/*
		 * Recurse into the Motion's initPlans and other fields, and the
		 * subtree.
		 *
		 * All the fields on the Motion node are considered part of the sending
		 * slice.
		 */
		saveCurrentPlanFlow = context->currentPlanFlow;
		if (plan->flow != NULL && plan->flow->flotype != FLOW_UNDEFINED)
			context->currentPlanFlow = plan->lefttree->flow;
		context->sliceDepth++;

		plan = (Plan *) plan_tree_mutator((Node *) plan,
										  apply_motion_mutator,
										  context,
										  false);
		motion = (Motion *) plan;
		context->sliceDepth--;
		context->currentPlanFlow = saveCurrentPlanFlow;

		/*
		 * If this is a Motion node in a correlated SubPlan, where we bring
		 * the result to the parent, the Flow was marked with
		 * CdbLocusType_OuterQuery. Because we didn't know whether the parent
		 * is distributed, replicated, or a single QD/QE process, when the
		 * subquery was planned, we fix that up here. Modify the Motion node
		 * so that it brings the result to the parent.
		 */
		if (plan->flow->locustype == CdbLocusType_OuterQuery)
		{
			Assert(context->outer_query_flow);
			Assert(motion->motionType == MOTIONTYPE_GATHER ||
				   motion->motionType == MOTIONTYPE_BROADCAST);
			Assert(!motion->sendSorted);

			if (context->outer_query_flow->flotype == FLOW_SINGLETON)
			{
				motion->motionType = MOTIONTYPE_GATHER;
			}
			else if (context->outer_query_flow->flotype == FLOW_REPLICATED ||
					 context->outer_query_flow->flotype == FLOW_PARTITIONED)
			{
				motion->motionType = MOTIONTYPE_BROADCAST;
			}
			else
				elog(ERROR, "unexpected Flow type in parent of a SubPlan");

			motion->plan.flow = context->outer_query_flow;
		}

		/*
		 * For non-top slice, if this motion is QE singleton and subplan's locus
		 * is CdbLocusType_SegmentGeneral, omit this motion.
		 */
		if (context->sliceDepth > 0 &&
			plan->flow->flotype == FLOW_SINGLETON &&
			plan->flow->segindex == 0 &&
			motion->plan.lefttree->flow->locustype == CdbLocusType_SegmentGeneral)
		{
			/* omit this motion */
			newnode = (Node *) motion->plan.lefttree;
		}
		else
		{
			/* Assign unique node number to the new node. */
			motion->motionID = context->nextMotionID++;
			newnode = (Node *) motion;
		}
	}
	else
	{
		/*
		 * Copy this node and mutate its children. Afterwards, this node should be
		 * an exact image of the input node, except that contained nodes requiring
		 * parallelization will have had it applied.
		 */
		saveCurrentPlanFlow = context->currentPlanFlow;
		if (plan->flow != NULL && plan->flow->flotype != FLOW_UNDEFINED)
			context->currentPlanFlow = plan->flow;
		newnode = plan_tree_mutator(node, apply_motion_mutator, context, false);
		context->currentPlanFlow = saveCurrentPlanFlow;

		/* Update flow if reassigning singleton top slice to qDisp. */
		if (plan->flow &&
			plan->flow->flotype == FLOW_SINGLETON &&
			plan->flow->segindex >= 0 &&
			context->sliceDepth == 0)
		{
			plan->flow->segindex = -1;
		}
	}

	return newnode;
}								/* apply_motion_mutator */

Motion *
make_union_motion(Plan *lefttree, int numsegments)
{
	Motion	   *motion;

	Assert(numsegments > 0);
	Assert(numsegments != GP_POLICY_INVALID_NUMSEGMENTS());

	motion = make_motion(NULL, lefttree,
						 0, NULL, NULL, NULL, NULL /* no ordering */);

	motion->motionType = MOTIONTYPE_GATHER;
	motion->hashExprs = NIL;
	motion->hashFuncs = NULL;
	motion->plan.flow = makeFlow(FLOW_SINGLETON, numsegments);
	motion->plan.flow->locustype = CdbLocusType_SingleQE;

	return motion;
}

Motion *
make_sorted_union_motion(PlannerInfo *root, Plan *lefttree, int numSortCols,
						 AttrNumber *sortColIdx, Oid *sortOperators,
						 Oid *collations, bool *nullsFirst,
						 int numsegments)
{
	Motion	   *motion;

	Assert(numsegments > 0);
	Assert(numsegments != GP_POLICY_INVALID_NUMSEGMENTS());

	motion = make_motion(root, lefttree,
						 numSortCols, sortColIdx, sortOperators, collations, nullsFirst);
	motion->motionType = MOTIONTYPE_GATHER;
	motion->hashExprs = NIL;
	motion->hashFuncs = NULL;
	motion->plan.flow = makeFlow(FLOW_SINGLETON, numsegments);
	motion->plan.flow->locustype = CdbLocusType_SingleQE;

	return motion;
}

Motion *
make_hashed_motion(Plan *lefttree,
				   List *hashExprs,
				   List *hashOpfamilies,
				   int numsegments)
{
	Motion	   *motion;
	Oid		   *hashFuncs;
	ListCell   *expr_cell;
	ListCell   *opf_cell;
	int			i;

	Assert(numsegments > 0);
	Assert(numsegments != GP_POLICY_INVALID_NUMSEGMENTS());
	Assert(list_length(hashExprs) == list_length(hashOpfamilies));

	/* Look up the right hash functions for the hash expressions */
	hashFuncs = palloc(list_length(hashExprs) * sizeof(Oid));
	i = 0;
	forboth(expr_cell, hashExprs, opf_cell, hashOpfamilies)
	{
		Node	   *expr = lfirst(expr_cell);
		Oid			opfamily = lfirst_oid(opf_cell);
		Oid			typeoid = exprType(expr);

		hashFuncs[i++] = cdb_hashproc_in_opfamily(opfamily, typeoid);
	}

	motion = make_motion(NULL, lefttree,
						 0, NULL, NULL, NULL, NULL /* no ordering */);
	motion->motionType = MOTIONTYPE_HASH;
	motion->hashExprs = hashExprs;
	motion->hashFuncs = hashFuncs;
	motion->plan.flow = makeFlow(FLOW_PARTITIONED, numsegments);
	motion->plan.flow->locustype = CdbLocusType_Hashed;

	return motion;
}

Motion *
make_broadcast_motion(Plan *lefttree, int numsegments)
{
	Motion	   *motion;

	Assert(numsegments > 0);
	Assert(numsegments != GP_POLICY_INVALID_NUMSEGMENTS());

	motion = make_motion(NULL, lefttree,
						 0, NULL, NULL, NULL, NULL /* no ordering */);
	motion->motionType = MOTIONTYPE_BROADCAST;
	motion->hashExprs = NIL;
	motion->hashFuncs = NULL;
	motion->plan.flow = makeFlow(FLOW_REPLICATED, numsegments);
	motion->plan.flow->locustype = CdbLocusType_Replicated;

	return motion;
}

Plan *
make_explicit_motion(PlannerInfo *root, Plan *lefttree, AttrNumber segidColIdx)
{
	Motion	   *motion;
	int			numsegments;
	plan_tree_base_prefix base;

	base.node = (Node *) root;

	/*
	 * For explicit motion data come back to the source segments,
	 * so numsegments is also the same with source.
	 */
	numsegments = lefttree->flow->numsegments;

	Assert(numsegments > 0);
	Assert(numsegments != GP_POLICY_INVALID_NUMSEGMENTS());
	Assert(segidColIdx > 0 && segidColIdx <= list_length(lefttree->targetlist));

	motion = make_motion(NULL, lefttree,
						 0, NULL, NULL, NULL, NULL /* no ordering */);
	motion->motionType = MOTIONTYPE_EXPLICIT;
	motion->hashExprs = NIL;
	motion->hashFuncs = NULL;
	motion->segidColIdx = segidColIdx;
	motion->plan.flow = makeFlow(FLOW_PARTITIONED, numsegments);
	/*
	 * TODO: antova - Nov 18, 2010; add a special locus type for
	 * ExplicitRedistribute flows
	 */
	motion->plan.flow->locustype = CdbLocusType_Strewn;

	return (Plan *) motion;
}

/* --------------------------------------------------------------------
 *
 *	Static Helper routines
 * --------------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 * getExprListFromTargetList
 *
 * Creates a VAR that references the indicated entries from the target list,
 * sets the restype and restypmod fields from the target list info,
 * and puts them into a list.
 *
 * The AttrNumber indexes actually refer to the 1 based index into the
 * target list.
 *
 * The entries have the varno field replaced by references in OUTER_VAR.
 * ----------------------------------------------------------------
 */
List *
getExprListFromTargetList(List *tlist,
						  int numCols,
						  AttrNumber *colIdx)
{
	int			i;
	List	   *elist = NIL;

	for (i = 0; i < numCols; i++)
	{
		/* Find expr in TargetList */
		AttrNumber	n = colIdx[i];

		TargetEntry *target = get_tle_by_resno(tlist, n);

		if (target == NULL)
			elog(ERROR, "no tlist entry for key %d", n);

		elist = lappend(elist, copyObject(target->expr));
	}

	return elist;
}

/* ----------------------------------------------------------------------- *
 * cdbmutate_warn_ctid_without_segid() warns the user if the plan refers to a
 * partitioned table's ctid column without also referencing its
 * gp_segment_id column.
 * ----------------------------------------------------------------------- *
 */
typedef struct ctid_inventory_context
{
	plan_tree_base_prefix base; /* Required prefix for
								 * plan_tree_walker/mutator */
	bool		uses_ctid;
	bool		uses_segid;
	Index		relid;
} ctid_inventory_context;

static bool
ctid_inventory_walker(Node *node, ctid_inventory_context *inv)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varattno < 0 &&
			var->varno == inv->relid &&
			var->varlevelsup == 0)
		{
			if (var->varattno == SelfItemPointerAttributeNumber)
				inv->uses_ctid = true;
			else if (var->varattno == GpSegmentIdAttributeNumber)
				inv->uses_segid = true;
		}
		return false;
	}
	return plan_tree_walker(node, ctid_inventory_walker, inv, true);
}

void
cdbmutate_warn_ctid_without_segid(struct PlannerInfo *root, struct RelOptInfo *rel)
{
	ctid_inventory_context inv;
	Relids		relids_to_ignore;
	ListCell   *cell;
	AttrNumber	attno;
	int			ndx;

	planner_init_plan_tree_base(&inv.base, root);
	inv.uses_ctid = false;
	inv.uses_segid = false;
	inv.relid = rel->relid;

	/* Rel not distributed?  Then segment id doesn't matter. */
	if (!rel->cdbpolicy ||
		rel->cdbpolicy->ptype == POLICYTYPE_ENTRY)
		return;

	/* Ignore references occurring in the Query's final output targetlist. */
	relids_to_ignore = bms_make_singleton(0);

	/* Is 'ctid' referenced in join quals? */
	attno = SelfItemPointerAttributeNumber;
	Assert(attno >= rel->min_attr && attno <= rel->max_attr);
	ndx = attno - rel->min_attr;
	if (bms_nonempty_difference(rel->attr_needed[ndx], relids_to_ignore))
		inv.uses_ctid = true;

	/* Is 'gp_segment_id' referenced in join quals? */
	attno = GpSegmentIdAttributeNumber;
	Assert(attno >= rel->min_attr && attno <= rel->max_attr);
	ndx = attno - rel->min_attr;
	if (bms_nonempty_difference(rel->attr_needed[ndx], relids_to_ignore))
		inv.uses_segid = true;

	/* Examine the single-table quals on this rel. */
	foreach(cell, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(cell);

		Assert(IsA(rinfo, RestrictInfo));

		ctid_inventory_walker((Node *) rinfo->clause, &inv);
	}

	/* Notify client if found a reference to ctid only, without gp_segment_id */
	if (inv.uses_ctid &&
		!inv.uses_segid)
	{
		RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
		const char *cmd;
		int			elevel;

		/* Reject if UPDATE or DELETE.  Otherwise just give info msg. */
		switch (root->parse->commandType)
		{
			case CMD_UPDATE:
				cmd = "UPDATE";
				elevel = ERROR;
				break;

			case CMD_DELETE:
				cmd = "DELETE";
				elevel = ERROR;
				break;

			default:
				cmd = "SELECT";
				elevel = NOTICE;
		}

		ereport(elevel,
				(errmsg("%s uses system-defined column \"%s.ctid\" without the necessary companion column \"%s.gp_segment_id\"",
						cmd, rte->eref->aliasname, rte->eref->aliasname),
				 errhint("To uniquely identify a row within a distributed table, use the \"gp_segment_id\" column together with the \"ctid\" column.")));
	}
	bms_free(relids_to_ignore);
}								/* cdbmutate_warn_ctid_without_segid */


/*
 * Code that mutate the tree for share input
 *
 * After the planner, the plan is really a DAG.  SISCs will have valid
 * pointer to the underlying share.  However, other code (setrefs etc)
 * depends on the fact that the plan is a tree.  We first mutate the DAG
 * to a tree.
 *
 * Next, we will need to decide if the share is cross slices.  If the share
 * is not cross slice, we do not need the syncrhonization, and it is possible to
 * keep the Material/Sort in memory to save a sort.
 *
 * It is essential that we walk the tree in the same order as the ExecProcNode start
 * execution, otherwise, deadlock may rise.
 */

/* Walk the tree for shareinput.
 * Shareinput fix shared_as_id and underlying_share_id of nodes in place.  We do not want to use
 * the ordinary tree walker as it is unnecessary to make copies etc.
 */
typedef bool (*SHAREINPUT_MUTATOR) (Node *node, PlannerInfo *root, bool fPop);
static void
shareinput_walker(SHAREINPUT_MUTATOR f, Node *node, PlannerInfo *root)
{
	Plan	   *plan = NULL;
	bool		recursive_down;

	if (node == NULL)
		return;

	if (IsA(node, List))
	{
		List	   *l = (List *) node;
		ListCell   *lc;

		foreach(lc, l)
		{
			Node	   *n = lfirst(lc);

			shareinput_walker(f, n, root);
		}
		return;
	}

	if (!is_plan_node(node))
		return;

	plan = (Plan *) node;
	recursive_down = (*f) (node, root, false);

	if (recursive_down)
	{
		if (IsA(node, Append))
		{
			ListCell   *cell;
			Append	   *app = (Append *) node;

			foreach(cell, app->appendplans)
				shareinput_walker(f, (Node *) lfirst(cell), root);
		}
		else if (IsA(node, ModifyTable))
		{
			ListCell   *cell;
			ModifyTable *mt = (ModifyTable *) node;

			foreach(cell, mt->plans)
				shareinput_walker(f, (Node *) lfirst(cell), root);
		}
		else if (IsA(node, SubqueryScan))
		{
			SubqueryScan  *subqscan = (SubqueryScan *) node;
			PlannerGlobal *glob = root->glob;
			PlannerInfo   *subroot;
			List	      *save_rtable;
			RelOptInfo    *rel;

			/*
			 * If glob->finalrtable is not NULL, rtables have been flatten,
			 * thus we should use glob->finalrtable instead.
			 */
			save_rtable = glob->share.curr_rtable;
			if (root->glob->finalrtable == NULL)
			{
				rel = find_base_rel(root, subqscan->scan.scanrelid);
				/*
				 * The Assert() on RelOptInfo's subplan being
				 * same as the subqueryscan's subplan, is valid
				 * in Upstream but for not for GPDB, since we
				 * create a new copy of the subplan if two
				 * SubPlans refer to the same initplan.
				 */
				subroot = rel->subroot;
				glob->share.curr_rtable = subroot->parse->rtable;
			}
			else
			{
				subroot = root;
				glob->share.curr_rtable = glob->finalrtable;
			}
			shareinput_walker(f, (Node *) subqscan->subplan, subroot);
			glob->share.curr_rtable = save_rtable;
		}
		else if (IsA(node, TableFunctionScan))
		{
			TableFunctionScan  *tfscan = (TableFunctionScan *) node;
			PlannerGlobal *glob = root->glob;
			PlannerInfo   *subroot;
			List	      *save_rtable;
			RelOptInfo    *rel;

			/*
			 * If glob->finalrtable is not NULL, rtables have been flatten,
			 * thus we should use glob->finalrtable instead.
			 */
			save_rtable = glob->share.curr_rtable;
			if (root->glob->finalrtable == NULL)
			{
				rel = find_base_rel(root, tfscan->scan.scanrelid);
				subroot = rel->subroot;
				glob->share.curr_rtable = subroot->parse->rtable;
			}
			else
			{
				subroot = root;
				glob->share.curr_rtable = glob->finalrtable;
			}
			shareinput_walker(f, (Node *)  tfscan->scan.plan.lefttree, subroot);
			glob->share.curr_rtable = save_rtable;
		}
		else if (IsA(node, BitmapAnd))
		{
			ListCell   *cell;
			BitmapAnd  *ba = (BitmapAnd *) node;

			foreach(cell, ba->bitmapplans)
				shareinput_walker(f, (Node *) lfirst(cell), root);
		}
		else if (IsA(node, BitmapOr))
		{
			ListCell   *cell;
			BitmapOr   *bo = (BitmapOr *) node;

			foreach(cell, bo->bitmapplans)
				shareinput_walker(f, (Node *) lfirst(cell), root);
		}
		else if (IsA(node, NestLoop))
		{
			/*
			 * Nest loop join is strange.  Exec order depends on
			 * prefetch_inner
			 */
			NestLoop   *nl = (NestLoop *) node;

			if (nl->join.prefetch_inner)
			{
				shareinput_walker(f, (Node *) plan->righttree, root);
				shareinput_walker(f, (Node *) plan->lefttree, root);
			}
			else
			{
				shareinput_walker(f, (Node *) plan->lefttree, root);
				shareinput_walker(f, (Node *) plan->righttree, root);
			}
		}
		else if (IsA(node, HashJoin))
		{
			/* Hash join the hash table is at inner */
			shareinput_walker(f, (Node *) plan->righttree, root);
			shareinput_walker(f, (Node *) plan->lefttree, root);
		}
		else if (IsA(node, MergeJoin))
		{
			MergeJoin  *mj = (MergeJoin *) node;

			if (mj->unique_outer)
			{
				shareinput_walker(f, (Node *) plan->lefttree, root);
				shareinput_walker(f, (Node *) plan->righttree, root);
			}
			else
			{
				shareinput_walker(f, (Node *) plan->righttree, root);
				shareinput_walker(f, (Node *) plan->lefttree, root);
			}
		}
		else if (IsA(node, Sequence))
		{
			ListCell   *cell = NULL;
			Sequence   *sequence = (Sequence *) node;

			foreach(cell, sequence->subplans)
			{
				shareinput_walker(f, (Node *) lfirst(cell), root);
			}
		}
		else
		{
			shareinput_walker(f, (Node *) plan->lefttree, root);
			shareinput_walker(f, (Node *) plan->righttree, root);
			shareinput_walker(f, (Node *) plan->initPlan, root);
		}
	}

	(*f) (node, root, true);
}

/*
 * Create a fake CTE range table entry that reflects the target list of a
 * shared input.
 */
static RangeTblEntry *
create_shareinput_producer_rte(ApplyShareInputContext *ctxt, int share_id,
							   int refno)
{
	int			attno = 1;
	ListCell   *lc;
	Plan	   *subplan;
	char		buf[100];
	RangeTblEntry *rte;
	List	   *colnames = NIL;
	List	   *coltypes = NIL;
	List	   *coltypmods = NIL;
	List	   *colcollations = NIL;
	ShareInputScan *producer;

	Assert(ctxt->producer_count > share_id);
	producer = ctxt->producers[share_id];
	subplan = producer->scan.plan.lefttree;

	foreach(lc, subplan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Oid			vartype;
		int32		vartypmod;
		Oid			varcollid;
		char	   *resname;

		vartype = exprType((Node *) tle->expr);
		vartypmod = exprTypmod((Node *) tle->expr);
		varcollid = exprCollation((Node *) tle->expr);

		/*
		 * We should've filled in tle->resname in shareinput_save_producer().
		 * Note that it's too late to call get_tle_name() here, because this
		 * runs after all the varnos in Vars have already been changed to
		 * INNER_VAR/OUTER_VAR.
		 */
		resname = tle->resname;
		if (!resname)
			resname = pstrdup("unnamed_attr");

		colnames = lappend(colnames, makeString(resname));
		coltypes = lappend_oid(coltypes, vartype);
		coltypmods = lappend_int(coltypmods, vartypmod);
		colcollations = lappend_oid(colcollations, varcollid);
		attno++;
	}

	/*
	 * Create a new RTE. Note that we use a different RTE for each reference,
	 * because we want to give each reference a different name.
	 */
	snprintf(buf, sizeof(buf), "share%d_ref%d", share_id, refno);

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_CTE;
	rte->ctename = pstrdup(buf);
	rte->ctelevelsup = 0;
	rte->self_reference = false;
	rte->alias = NULL;

	rte->eref = makeAlias(rte->ctename, colnames);
	rte->ctecoltypes = coltypes;
	rte->ctecoltypmods = coltypmods;
	rte->ctecolcollations = colcollations;

	rte->inh = false;
	rte->inFromCl = false;

	rte->requiredPerms = 0;
	rte->checkAsUser = InvalidOid;

	return rte;
}

/*
 * Memorize the producer of a shared input in an array of producers, one
 * producer per share_id.
 */
static void
shareinput_save_producer(ShareInputScan *plan, ApplyShareInputContext *ctxt)
{
	int			share_id = plan->share_id;
	int			new_producer_count = (share_id + 1);

	Assert(plan->share_id >= 0);

	if (ctxt->producers == NULL)
	{
		ctxt->producers = palloc0(sizeof(ShareInputScan *) * new_producer_count);
		ctxt->producer_count = new_producer_count;
	}
	else if (ctxt->producer_count < new_producer_count)
	{
		ctxt->producers = repalloc(ctxt->producers, new_producer_count * sizeof(ShareInputScan *));
		memset(&ctxt->producers[ctxt->producer_count], 0, (new_producer_count - ctxt->producer_count) * sizeof(ShareInputScan *));
		ctxt->producer_count = new_producer_count;
	}

	Assert(ctxt->producers[share_id] == NULL);
	ctxt->producers[share_id] = plan;
}

/*
 * When a plan comes out of the planner, all the ShareInputScan nodes belonging
 * to the same "share" have the same child node. apply_shareinput_dag_to_tree()
 * turns the DAG into a proper tree. The first occurrence of a ShareInput scan,
 * with a particular child tree, becomes the "producer" of the share, and the
 * others becomes consumers. The subtree is removed from all the consumer nodes.
 *
 * Also, a share_id is assigned to each ShareInputScan node, as well as the
 * Material/Sort nodes below the producers. The producers and its consumers
 * are linked together by the same share_id.
 */
static bool
shareinput_mutator_dag_to_tree(Node *node, PlannerInfo *root, bool fPop)
{
	PlannerGlobal *glob = root->glob;
	ApplyShareInputContext *ctxt = &glob->share;
	Plan	   *plan = (Plan *) node;

	if (fPop)
		return true;

	if (IsA(plan, ShareInputScan))
	{
		ShareInputScan *siscan = (ShareInputScan *) plan;
		Plan	   *subplan = plan->lefttree;
		int			i;
		int			attno;
		ListCell   *lc;

		/* Is there a producer for this sub-tree already? */
		for (i = 0; i < ctxt->producer_count; i++)
		{
			if (ctxt->producers[i] && ctxt->producers[i]->scan.plan.lefttree == subplan)
			{
				/*
				 * Yes. This is a consumer. Remove the subtree, and assign the
				 * same share_id as the producer.
				 */
				Assert(get_plan_share_id((Plan *) ctxt->producers[i]) == i);
				set_plan_share_id((Plan *) plan, ctxt->producers[i]->share_id);
				siscan->scan.plan.lefttree = NULL;
				return false;
			}
		}

		/*
		 * Couldn't find a match in existing list of producers, so this is a
		 * producer. Add this to the list of producers, and assign a new
		 * share_id.
		 */
		Assert(get_plan_share_id(subplan) == SHARE_ID_NOT_ASSIGNED);
		set_plan_share_id((Plan *) plan, ctxt->producer_count);
		set_plan_share_id(subplan, ctxt->producer_count);

		shareinput_save_producer(siscan, ctxt);

		/*
		 * Also make sure that all the entries in the subplan's target list
		 * have human-readable column names. They are used for EXPLAIN.
		 */
		attno = 1;
		foreach(lc, subplan->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			if (tle->resname == NULL)
			{
				char		default_name[100];
				char	   *resname;

				snprintf(default_name, sizeof(default_name), "col_%d", attno);

				resname = strVal(get_tle_name(tle, ctxt->curr_rtable, default_name));
				tle->resname = pstrdup(resname);
			}
			attno++;
		}
	}

	return true;
}

Plan *
apply_shareinput_dag_to_tree(PlannerInfo *root, Plan *plan)
{
	PlannerGlobal *glob = root->glob;

	glob->share.curr_rtable = root->parse->rtable;
	shareinput_walker(shareinput_mutator_dag_to_tree, (Node *) plan, root);
	return plan;
}

/*
 * Collect all the producer ShareInput nodes into an array, for later use by
 * replace_shareinput_targetlists().
 *
 * This is a stripped-down version of apply_shareinput_dag_to_tree(), for use
 * on ORCA-produced plans. ORCA assigns share_ids to all ShareInputScan nodes,
 * and only producer nodes have a subtree, so we don't need to do the DAG to
 * tree conversion or assign share_ids here.
 */
static bool
collect_shareinput_producers_walker(Node *node, PlannerInfo *root, bool fPop)
{
	PlannerGlobal *glob = root->glob;
	ApplyShareInputContext *ctxt = &glob->share;

	if (fPop)
		return true;

	if (IsA(node, ShareInputScan))
	{
		ShareInputScan *siscan = (ShareInputScan *) node;
		Plan	   *subplan = siscan->scan.plan.lefttree;

		Assert(get_plan_share_id((Plan *) siscan) >= 0);

		if (subplan)
		{
			shareinput_save_producer(siscan, ctxt);
		}
	}
	return true;
}

void
collect_shareinput_producers(PlannerInfo *root, Plan *plan)
{
	PlannerGlobal *glob = root->glob;

	glob->share.curr_rtable = glob->finalrtable;
	shareinput_walker(collect_shareinput_producers_walker, (Node *) plan, root);
}

/* Some helper: implements a stack using List. */
static void
shareinput_pushmot(ApplyShareInputContext *ctxt, int motid)
{
	ctxt->motStack = lcons_int(motid, ctxt->motStack);
}
static void
shareinput_popmot(ApplyShareInputContext *ctxt)
{
	list_delete_first(ctxt->motStack);
}
static int
shareinput_peekmot(ApplyShareInputContext *ctxt)
{
	return linitial_int(ctxt->motStack);
}


/*
 * Replace the target list of ShareInputScan nodes, with references
 * to CTEs that we build on the fly.
 *
 * Only one of the ShareInputScan nodes in a plan tree contains the real
 * child plan, while others contain just a "share id" that binds all the
 * ShareInputScan nodes sharing the same input together. The missing
 * child plan is a problem for EXPLAIN, as any OUTER Vars in the
 * ShareInputScan's target list cannot be resolved without the child
 * plan.
 *
 * To work around that issue, create a CTE for each shared input node, with
 * columns that match the target list of the SharedInputScan's subplan,
 * and replace the target list entries of the SharedInputScan with
 * Vars that point to the CTE instead of the child plan.
 */
Plan *
replace_shareinput_targetlists(PlannerInfo *root, Plan *plan)
{
	shareinput_walker(replace_shareinput_targetlists_walker, (Node *) plan, root);
	return plan;
}

static bool
replace_shareinput_targetlists_walker(Node *node, PlannerInfo *root, bool fPop)
{
	PlannerGlobal *glob = root->glob;
	ApplyShareInputContext *ctxt = &glob->share;

	if (fPop)
		return true;

	if (IsA(node, ShareInputScan))
	{
		ShareInputScan *sisc = (ShareInputScan *) node;
		int			share_id = sisc->share_id;
		ListCell   *lc;
		int			attno;
		List	   *newtargetlist;
		RangeTblEntry *rte;

		/*
		 * Note that even though the planner assigns sequential share_ids for
		 * each shared node, so that share_id is always below
		 * list_length(ctxt->sharedNodes), ORCA has a different assignment
		 * scheme. So we have to be prepared for any share_id, at least when
		 * ORCA is in use.
		 */
		if (ctxt->share_refcounts == NULL)
		{
			int			new_sz = share_id + 1;

			ctxt->share_refcounts = palloc0(new_sz * sizeof(int));
			ctxt->share_refcounts_sz = new_sz;
		}
		else if (share_id >= ctxt->share_refcounts_sz)
		{
			int			old_sz = ctxt->share_refcounts_sz;
			int			new_sz = share_id + 1;

			ctxt->share_refcounts = repalloc(ctxt->share_refcounts, new_sz * sizeof(int));
			memset(&ctxt->share_refcounts[old_sz], 0, (new_sz - old_sz) * sizeof(int));
			ctxt->share_refcounts_sz = new_sz;
		}

		ctxt->share_refcounts[share_id]++;

		/*
		 * Create a new RTE. Note that we use a different RTE for each
		 * reference, because we want to give each reference a different name.
		 */
		rte = create_shareinput_producer_rte(ctxt, share_id,
											 ctxt->share_refcounts[share_id]);

		glob->finalrtable = lappend(glob->finalrtable, rte);
		sisc->scan.scanrelid = list_length(glob->finalrtable);

		/*
		 * Replace all the target list entries.
		 *
		 * SharedInputScan nodes are not projection-capable, so the target
		 * list of the SharedInputScan matches the subplan's target list.
		 */
		newtargetlist = NIL;
		attno = 1;
		foreach(lc, sisc->scan.plan.targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			TargetEntry *newtle = flatCopyTargetEntry(tle);

			newtle->expr = (Expr *) makeVar(sisc->scan.scanrelid, attno,
											exprType((Node *) tle->expr),
											exprTypmod((Node *) tle->expr),
											exprCollation((Node *) tle->expr),
											0);
			newtargetlist = lappend(newtargetlist, newtle);
			attno++;
		}
		sisc->scan.plan.targetlist = newtargetlist;
	}

	return true;
}


typedef struct ShareNodeWithSliceMark
{
	Plan	   *plan;
	int			slice_mark;
} ShareNodeWithSliceMark;

static bool
shareinput_find_sharenode(ApplyShareInputContext *ctxt, int share_id, ShareNodeWithSliceMark *result)
{
	ShareInputScan *siscan;
	Plan	   *plan;

	Assert(share_id < ctxt->producer_count);
	if (share_id >= ctxt->producer_count)
		return false;

	siscan = ctxt->producers[share_id];
	if (!siscan)
		return false;

	plan = siscan->scan.plan.lefttree;

	Assert(get_plan_share_id(plan) == share_id);
	Assert(IsA(plan, Material) ||IsA(plan, Sort));

	if (result)
	{
		result->plan = plan;
		result->slice_mark = ctxt->sliceMarks[share_id];
	}

	return true;
}

/*
 * First walk on shareinput xslice.  It does the following:
 *
 * 1. Build the sliceMarks in context.
 * 2. Build a list a share on QD
 */
static bool
shareinput_mutator_xslice_1(Node *node, PlannerInfo *root, bool fPop)
{
	PlannerGlobal *glob = root->glob;
	ApplyShareInputContext *ctxt = &glob->share;
	Plan	   *plan = (Plan *) node;

	if (fPop)
	{
		if (IsA(plan, Motion))
			shareinput_popmot(ctxt);
		return false;
	}

	if (IsA(plan, Motion))
	{
		Motion	   *motion = (Motion *) plan;

		shareinput_pushmot(ctxt, motion->motionID);
		return true;
	}

	if (IsA(plan, ShareInputScan))
	{
		ShareInputScan *sisc = (ShareInputScan *) plan;
		int			motId = shareinput_peekmot(ctxt);
		Plan	   *shared = plan->lefttree;

		Assert(sisc->scan.plan.flow);
		if (sisc->scan.plan.flow->flotype == FLOW_SINGLETON)
		{
			if (sisc->scan.plan.flow->segindex < 0)
				ctxt->qdShares = list_append_unique_int(ctxt->qdShares, sisc->share_id);
		}

		if (shared)
		{
			Assert(get_plan_share_id(plan) == get_plan_share_id(shared));
			set_plan_driver_slice(shared, motId);

			/*
			 * We need to repopulate the producers array. cdbparallelize() was
			 * run on the plan tree between shareinput_mutator_dag_to_tree()
			 * and here, which copies all the nodes, and the destroys the
			 * producers array in the process.
			 */
			ctxt->producers[sisc->share_id] = sisc;
			ctxt->sliceMarks[sisc->share_id] = motId;
		}
	}

	return true;
}

/*
 * Second pass on shareinput xslice.  It marks the shared node xslice,
 * if a 'shared' is cross-slice.
 */
static bool
shareinput_mutator_xslice_2(Node *node, PlannerInfo *root, bool fPop)
{
	PlannerGlobal *glob = root->glob;
	ApplyShareInputContext *ctxt = &glob->share;
	Plan	   *plan = (Plan *) node;

	if (fPop)
	{
		if (IsA(plan, Motion))
			shareinput_popmot(ctxt);
		return false;
	}

	if (IsA(plan, Motion))
	{
		Motion	   *motion = (Motion *) plan;

		shareinput_pushmot(ctxt, motion->motionID);
		return true;
	}

	if (IsA(plan, ShareInputScan))
	{
		ShareInputScan *sisc = (ShareInputScan *) plan;
		int			motId = shareinput_peekmot(ctxt);
		Plan	   *shared = plan->lefttree;

		if (!shared)
		{
			ShareNodeWithSliceMark plan_slicemark = {NULL /* plan */ , 0 /* slice_mark */ };
			int			shareSliceId = 0;

			shareinput_find_sharenode(ctxt, sisc->share_id, &plan_slicemark);
			if (plan_slicemark.plan == NULL)
				elog(ERROR, "could not find shared input node with id %d", sisc->share_id);

			shareSliceId = get_plan_driver_slice(plan_slicemark.plan);

			if (shareSliceId != motId)
			{
				ShareType	stype = get_plan_share_type(plan_slicemark.plan);

				if (stype == SHARE_MATERIAL || stype == SHARE_SORT)
					set_plan_share_type_xslice(plan_slicemark.plan);

				incr_plan_nsharer_xslice(plan_slicemark.plan);
				sisc->driver_slice = motId;
			}
		}
	}

	return true;
}

/*
 * Third pass:
 * 	1. Mark shareinput scan xslice,
 * 	2. Bulid a list of QD slices
 */
static bool
shareinput_mutator_xslice_3(Node *node, PlannerInfo *root, bool fPop)
{
	PlannerGlobal *glob = root->glob;
	ApplyShareInputContext *ctxt = &glob->share;
	Plan	   *plan = (Plan *) node;

	if (fPop)
	{
		if (IsA(plan, Motion))
			shareinput_popmot(ctxt);
		return false;
	}

	if (IsA(plan, Motion))
	{
		Motion	   *motion = (Motion *) plan;

		shareinput_pushmot(ctxt, motion->motionID);
		return true;
	}

	if (IsA(plan, ShareInputScan))
	{
		ShareInputScan *sisc = (ShareInputScan *) plan;
		int			motId = shareinput_peekmot(ctxt);

		ShareNodeWithSliceMark plan_slicemark = {NULL, 0};
		ShareType	stype = SHARE_NOTSHARED;

		shareinput_find_sharenode(ctxt, sisc->share_id, &plan_slicemark);

		if (!plan_slicemark.plan)
			elog(ERROR, "sub-plan for share_id %d cannot be NULL", sisc->share_id);

		stype = get_plan_share_type(plan_slicemark.plan);

		switch (stype)
		{
			case SHARE_MATERIAL_XSLICE:
				Assert(sisc->share_type == SHARE_MATERIAL);
				set_plan_share_type_xslice(plan);
				break;
			case SHARE_SORT_XSLICE:
				Assert(sisc->share_type == SHARE_SORT);
				set_plan_share_type_xslice(plan);
				break;
			default:
				Assert(sisc->share_type == stype);
				break;
		}

		if (list_member_int(ctxt->qdShares, sisc->share_id))
		{
			Assert(sisc->scan.plan.flow);
			Assert(sisc->scan.plan.flow->flotype == FLOW_SINGLETON);
			ctxt->qdSlices = list_append_unique_int(ctxt->qdSlices, motId);
		}
	}
	return true;
}

/*
 * The fourth pass.  If a shareinput is running on QD, then all slices in
 * this share must be on QD.  Move them to QD.
 */
static bool
shareinput_mutator_xslice_4(Node *node, PlannerInfo *root, bool fPop)
{
	PlannerGlobal *glob = root->glob;
	ApplyShareInputContext *ctxt = &glob->share;
	Plan	   *plan = (Plan *) node;
	int			motId = shareinput_peekmot(ctxt);

	if (fPop)
	{
		if (IsA(plan, Motion))
			shareinput_popmot(ctxt);
		return false;
	}

	if (IsA(plan, Motion))
	{
		Motion	   *motion = (Motion *) plan;

		shareinput_pushmot(ctxt, motion->motionID);
		/* Do not return.  Motion need to be adjusted as well */
	}

	/*
	 * Well, the following test can be optimized if we record the test result
	 * so we test just once for all node in one slice.  But this code is not
	 * perf critical so be lazy.
	 */
	if (list_member_int(ctxt->qdSlices, motId))
	{
		if (plan->flow)
		{
			Assert(plan->flow->flotype == FLOW_SINGLETON);
			plan->flow->segindex = -1;
		}
	}
	return true;
}

Plan *
apply_shareinput_xslice(Plan *plan, PlannerInfo *root)
{
	PlannerGlobal *glob = root->glob;
	ApplyShareInputContext *ctxt = &glob->share;
	ListCell   *lp, *lr;

	ctxt->motStack = NULL;
	ctxt->qdShares = NULL;
	ctxt->qdSlices = NULL;
	ctxt->nextPlanId = 0;

	ctxt->sliceMarks = palloc0(ctxt->producer_count * sizeof(int));

	shareinput_pushmot(ctxt, 0);

	/*
	 * Walk the tree.  See comment for each pass for what each pass will do.
	 * The context is used to carry information from one pass to another, as
	 * well as within a pass.
	 */

	/*
	 * A subplan might have a SharedScan consumer while the SharedScan
	 * producer is in the main plan, or vice versa. So in the first pass, we
	 * walk through all plans and collect all producer subplans into the
	 * context, before processing the consumers.
	 */
	forboth(lp, glob->subplans, lr, glob->subroots)
	{
		Plan	   *subplan = (Plan *) lfirst(lp);
		PlannerInfo *subroot =  (PlannerInfo *) lfirst(lr);

		shareinput_walker(shareinput_mutator_xslice_1, (Node *) subplan, subroot);
	}
	shareinput_walker(shareinput_mutator_xslice_1, (Node *) plan, root);

	/* Now walk the tree again, and process all the consumers. */
	forboth(lp, glob->subplans, lr, glob->subroots)
	{
		Plan	   *subplan = (Plan *) lfirst(lp);
		PlannerInfo *subroot =  (PlannerInfo *) lfirst(lr);

		shareinput_walker(shareinput_mutator_xslice_2, (Node *) subplan, subroot);
	}
	shareinput_walker(shareinput_mutator_xslice_2, (Node *) plan, root);

	forboth(lp, glob->subplans, lr, glob->subroots)
	{
		Plan	   *subplan = (Plan *) lfirst(lp);
		PlannerInfo *subroot =  (PlannerInfo *) lfirst(lr);

		shareinput_walker(shareinput_mutator_xslice_3, (Node *) subplan, subroot);
	}
	shareinput_walker(shareinput_mutator_xslice_3, (Node *) plan, root);

	forboth(lp, glob->subplans, lr, glob->subroots)
	{
		Plan	   *subplan = (Plan *) lfirst(lp);
		PlannerInfo *subroot =  (PlannerInfo *) lfirst(lr);

		shareinput_walker(shareinput_mutator_xslice_4, (Node *) subplan, subroot);
	}
	shareinput_walker(shareinput_mutator_xslice_4, (Node *) plan, root);

	return plan;
}

/*
 * Hash a list of const values with GPDB's hash function
 */
int32
cdbhash_const_list(List *plConsts, int iSegments, Oid *hashfuncs)
{
	ListCell   *lc;
	CdbHash    *pcdbhash;
	int			i;

	Assert(0 < list_length(plConsts));

	pcdbhash = makeCdbHash(iSegments, list_length(plConsts), hashfuncs);

	cdbhashinit(pcdbhash);

	Assert(0 < list_length(plConsts));

	i = 0;
	foreach(lc, plConsts)
	{
		Const	   *pconst = (Const *) lfirst(lc);

		cdbhash(pcdbhash, i + 1, pconst->constvalue, pconst->constisnull);
		i++;
	}

	return cdbhashreduce(pcdbhash);
}

/*
 * Construct an expression that checks whether the current segment is
 * 'segid'.
 */
Node *
makeSegmentFilterExpr(int segid)
{
	/* Build an expression: gp_execution_segment() = <segid> */
	return (Node *)
		make_opclause(Int4EqualOperator,
					  BOOLOID,
					  false,	/* opretset */
					  (Expr *) makeFuncExpr(F_MPP_EXECUTION_SEGMENT,
											INT4OID,
											NIL,	/* args */
											InvalidOid,
											InvalidOid,
											COERCE_EXPLICIT_CALL),
					  (Expr *) makeConst(INT4OID,
										 -1,		/* consttypmod */
										 InvalidOid, /* constcollid */
										 sizeof(int32),
										 Int32GetDatum(segid),
										 false,		/* constisnull */
										 true),		/* constbyval */
					  InvalidOid,	/* opcollid */
					  InvalidOid	/* inputcollid */
			);
}

typedef struct ParamWalkerContext
{
	plan_tree_base_prefix base; /* Required prefix for
								 * plan_tree_walker/mutator */
	Bitmapset  *paramids;		/* Bitmapset for Param */
	Bitmapset  *scanrelids;		/* Bitmapset for scanrelid */
} ParamWalkerContext;

static bool
param_walker(Node *node, ParamWalkerContext *context)
{
	PlannerInfo *root = (PlannerInfo *) context->base.node;
	Param	   *param;
	Aggref	   *aggref;
	Scan	   *scan;
	ListCell   *lc;

	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_Param:
			param = (Param *) node;
			if (param->paramkind == PARAM_EXEC)
			{
				if (!bms_is_member(param->paramid, context->paramids))
					context->paramids = bms_add_member(context->paramids,
													   param->paramid);
			}
			return false;

		case T_Aggref:
			/*
			 * See if it's an Aggref that will be replaced by a Param in
			 * set_plan_references()
			 */
			aggref = (Aggref *) node;
			if (root->minmax_aggs != NIL &&
				list_length(aggref->args) == 1)
			{
				TargetEntry *curTarget = (TargetEntry *) linitial(aggref->args);
				ListCell   *lc;

				foreach(lc, root->minmax_aggs)
				{
					MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);

					if (mminfo->aggfnoid == aggref->aggfnoid &&
						equal(mminfo->target, curTarget->expr))
						param_walker((Node *) mminfo->param, context);
				}
			}
			break;

		case T_SubqueryScan:
			{
				SubqueryScan *sscan = (SubqueryScan *) node;
				RelOptInfo *rel;
				Node	   *save_root;

				/* Need to look up the subquery's RelOptInfo, since we need its subroot */
				rel = find_base_rel(root, sscan->scan.scanrelid);

				/* Recurse into any expressions on the SubqueryScan node itself. */
				if (walk_plan_node_fields(&sscan->scan.plan, param_walker, context))
					return true;

				/* recurse into the subquery, with the new 'root' */
				save_root = context->base.node;
				context->base.node = (Node *) rel->subroot;

				if (param_walker((Node *) sscan->subplan, context))
					return true;

				context->base.node = save_root;
				return false;
			}
			break;

		case T_ValuesScan:
		case T_FunctionScan:
		case T_TableFunctionScan:
			scan = (Scan *) node;
			if (!bms_is_member(scan->scanrelid, context->scanrelids))
				context->scanrelids = bms_add_member(context->scanrelids,
													 scan->scanrelid);
			break;

		case T_SubPlan:
			{
				PlannerInfo *root = (PlannerInfo *) context->base.node;
				SubPlan	   *spexpr = (SubPlan *) node;
				Plan	   *subplan_plan = planner_subplan_get_plan(root, spexpr);
				PlannerInfo *subplan_root = planner_subplan_get_root(root, spexpr);
				Node	   *save_root;

				if (spexpr->subLinkType == MULTIEXPR_SUBLINK &&
					spexpr->is_initplan)
				{
					foreach (lc, spexpr->setParam)
					{
						int			paramid = lfirst_int(lc);

						if (!bms_is_member(paramid, context->paramids))
							context->paramids = bms_add_member(context->paramids,
															   paramid);
					}
				}

				/* recurse into the subplan */
				save_root = context->base.node;
				context->base.node = (Node *) subplan_root;
				if (param_walker((Node *) subplan_plan, context))
					return true;

				context->base.node = save_root;

				/*
				 * fall through to let plan_tree_walker() handle any expressions in
				 * testexpr and args
				 */
			}
			break;

		default:
			break;
	}

	return plan_tree_walker(node, param_walker, context, false);
}

/*
 * Retrieve param ids that are referenced in RTEs.
 * We can't simply use range_table_walker() here, because we only
 * want to walk through RTEs that are referenced in the plan.
 */
static void
rte_param_walker(List *rtable, ParamWalkerContext *context)
{
	ListCell   *lc;
	int			rteid = 0;
	ListCell   *func_lc;

	foreach(lc, rtable)
	{
		rteid++;
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (!bms_is_member(rteid, context->scanrelids))
		{
			/* rte not referenced in the plan */
			continue;
		}

		switch (rte->rtekind)
		{
			case RTE_RELATION:
			case RTE_VOID:
			case RTE_CTE:
				/* nothing to do */
				break;
			case RTE_SUBQUERY:
				param_walker((Node *) rte->subquery, context);
				break;
			case RTE_JOIN:
				param_walker((Node *) rte->joinaliasvars, context);
				break;
			case RTE_FUNCTION:
				foreach(func_lc, rte->functions)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(func_lc);
					param_walker(rtfunc->funcexpr, context);
				}
				break;
			case RTE_TABLEFUNCTION:
				param_walker((Node *) rte->subquery, context);
				foreach(func_lc, rte->functions)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(func_lc);
					param_walker(rtfunc->funcexpr, context);
				}
				break;
			case RTE_VALUES:
				param_walker((Node *) rte->values_lists, context);
				break;
		}
	}
}

static bool
initplan_walker(Node *node, ParamWalkerContext *context)
{
	PlannerInfo *root;
	List	   *new_initplans = NIL;
	ListCell   *lc,
			   *lp;
	bool		anyused;		/* Are any of this Init Plan's output
								 * parameters actually used? */

	if (node == NULL)
		return false;

	if (is_plan_node(node))
	{
		root = (PlannerInfo *) context->base.node;
		Plan	   *plan = (Plan *) node;

		foreach(lc, plan->initPlan)
		{
			SubPlan    *subplan = (SubPlan *) lfirst(lc);

			Assert(subplan->is_initplan);

			anyused = false;
			foreach(lp, subplan->setParam)
			{
				int			paramid = lfirst_int(lp);

				if (bms_is_member(paramid, context->paramids))
				{
					anyused = true;
					break;
				}
			}

			/* If none of its params are used, leave out from the new list */
			if (anyused)
				new_initplans = lappend(new_initplans, subplan);
			else
			{
				/*
				 * This init plan is unused. Leave it out of this plan node's
				 * initPlan list, and also replace it in the global list of
				 * subplans with a dummy. (We can't just remove it from the
				 * global list, because that would screw up the plan_id
				 * numbering of the subplans).
				 */
				Result	   *dummy = make_result(NIL,
												(Node *) list_make1(makeBoolConst(false, false)),
												NULL);

				planner_subplan_put_plan(root, subplan, (Plan *) dummy);
			}
		}

		/* remove unused params */
		plan->allParam = bms_intersect(plan->allParam, context->paramids);

		list_free(plan->initPlan);
		plan->initPlan = new_initplans;
	}

	return plan_tree_walker(node, initplan_walker, context, true);
}

/*
 * Remove unused initplans from the given plan object
 */
void
remove_unused_initplans(Plan *top_plan, PlannerInfo *root)
{
	ParamWalkerContext context;

	if (!root->glob->subplans)
		return;

	context.base.node = (Node *) root;
	context.paramids = NULL;
	context.scanrelids = NULL;

	/*
	 * Collect param ids of all the Params that are referenced in the plan,and
	 * the IDs of all the range table entries that are referenced in the Plan.
	 */
	param_walker((Node *) top_plan, &context);

	/*
	 * Now that we know which range table entries are referenced in the plan,
	 * also collect Params from those range table entries.
	 */
	rte_param_walker(root->glob->finalrtable, &context);

	/* workhorse to remove unused initplans */
	initplan_walker((Node *) top_plan, &context);

	bms_free(context.paramids);
	bms_free(context.scanrelids);
}

/*
 * Evaluate functions to constants.
 */
Node *
exec_make_plan_constant(struct PlannedStmt *stmt, EState *estate, bool is_SRI,
						List **cursorPositions)
{
	pre_dispatch_function_evaluation_context pcontext;
	Node	   *result;

	Assert(stmt);
	exec_init_plan_tree_base(&pcontext.base, stmt);
	pcontext.single_row_insert = is_SRI;
	pcontext.cursorPositions = NIL;
	pcontext.estate = estate;

	result = pre_dispatch_function_evaluation_mutator((Node *) stmt->planTree, &pcontext);

	*cursorPositions = pcontext.cursorPositions;
	return result;
}

/*
 * Remove subquery field in RTE's with subquery kind.
 */
void
remove_subquery_in_RTEs(Node *node)
{
	if (node == NULL)
	{
		return;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (RTE_SUBQUERY == rte->rtekind && NULL != rte->subquery)
		{
			/*
			 * Replace subquery with a dummy subquery.
			 *
			 * XXX: We could save a lot more memory by deep-freeing the many
			 * fields in the Query too. But I'm not sure which of them might
			 * be shared by other objects in the tree.
			 */
			pfree(rte->subquery);
			rte->subquery = NULL;
		}

		return;
	}

	if (IsA(node, List))
	{
		List	   *list = (List *) node;
		ListCell   *lc = NULL;

		foreach(lc, list)
		{
			remove_subquery_in_RTEs((Node *) lfirst(lc));
		}
	}
}

/*
 * Let's evaluate all STABLE functions that have constant args before
 * dispatch, so we get a consistent view across QEs
 *
 * Also, if this is a single_row insert, let's evaluate nextval() and
 * currval() before dispatching
 */
static Node *
pre_dispatch_function_evaluation_mutator(Node *node,
										 pre_dispatch_function_evaluation_context *context)
{
	Node	   *new_node = 0;

	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		/* Not replaceable, so just copy the Param (no need to recurse) */
		return (Node *) copyObject(param);
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;
		List	   *args;
		ListCell   *arg;
		Expr	   *simple;
		FuncExpr   *newexpr;
		bool		has_nonconst_input;

		Form_pg_proc funcform;
		EState	   *estate;
		ExprState  *exprstate;
		MemoryContext oldcontext;
		Datum		const_val;
		bool		const_is_null;
		int16		resultTypLen;
		bool		resultTypByVal;

		Oid			funcid;
		HeapTuple	func_tuple;

		/*
		 * Reduce constants in the FuncExpr's arguments. We know args is
		 * either NIL or a List node, so we can call expression_tree_mutator
		 * directly rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
												pre_dispatch_function_evaluation_mutator,
												(void *) context);

		funcid = expr->funcid;

		newexpr = makeNode(FuncExpr);
		newexpr->funcid = expr->funcid;
		newexpr->funcresulttype = expr->funcresulttype;
		newexpr->funcretset = expr->funcretset;
		newexpr->funcvariadic = expr->funcvariadic;
		newexpr->funcformat = expr->funcformat;
		newexpr->funccollid = expr->funccollid;
		newexpr->inputcollid = expr->inputcollid;
		newexpr->args = args;
		newexpr->location = expr->location;
		newexpr->is_tablefunc = expr->is_tablefunc;

		/*
		 * Check for constant inputs
		 */
		has_nonconst_input = false;

		foreach(arg, args)
		{
			if (!IsA(lfirst(arg), Const))
			{
				has_nonconst_input = true;
				break;
			}
		}

		if (!has_nonconst_input)
		{
			bool		is_seq_func = false;
			bool		tup_or_set;

			func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
			if (!HeapTupleIsValid(func_tuple))
				elog(ERROR, "cache lookup failed for function %u", funcid);

			funcform = (Form_pg_proc) GETSTRUCT(func_tuple);

			/* can't handle set returning or row returning functions */
			tup_or_set = (funcform->proretset || type_is_rowtype(funcform->prorettype));

			ReleaseSysCache(func_tuple);

			/* can't handle it */
			if (tup_or_set)
			{
				/*
				 * We haven't mutated this node, but we still return the
				 * mutated arguments.
				 *
				 * If we don't do this, we'll miss out on transforming
				 * function arguments which are themselves functions we need
				 * to mutated. For example, select foo(now()).
				 */
				return (Node *) newexpr;
			}

			/*
			 * Here we want to mark any statement that is going to use a
			 * sequence as dirty.  Doing this means that the QD will flush the
			 * xlog which will also flush any xlog writes that the sequence
			 * server might do.
			 */
			if (funcid == F_NEXTVAL_OID || funcid == F_CURRVAL_OID ||
				funcid == F_SETVAL_OID)
			{
				ExecutorMarkTransactionUsesSequences();
				is_seq_func = true;
			}

			if (funcform->provolatile == PROVOLATILE_IMMUTABLE)
				 /* okay */ ;
			else if (funcform->provolatile == PROVOLATILE_STABLE)
				 /* okay */ ;
			else if (context->single_row_insert && is_seq_func)
				;				/* Volatile, but special sequence function */
			else
				return (Node *) newexpr;

			/*
			 * Ok, we have a function that is STABLE (or IMMUTABLE), with
			 * constant args. Let's try to evaluate it.
			 */

			/*
			 * To use the executor, we need an EState.
			 */
			estate = CreateExecutorState();

			/* We can use the estate's working context to avoid memory leaks. */
			oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

			/*
			 * Prepare expr for execution.
			 */
			exprstate = ExecPrepareExpr((Expr *) newexpr, estate);

			/*
			 * And evaluate it.
			 *
			 * It is OK to use a default econtext because none of the
			 * ExecEvalExpr() code used in this situation will use econtext.
			 * That might seem fortuitous, but it's not so unreasonable --- a
			 * constant expression does not depend on context, by definition,
			 * n'est-ce pas?
			 */
			const_val = ExecEvalExprSwitchContext(exprstate,
												  GetPerTupleExprContext(estate),
												  &const_is_null, NULL);

			/* Get info needed about result datatype */
			get_typlenbyval(expr->funcresulttype, &resultTypLen, &resultTypByVal);

			/* Get back to outer memory context */
			MemoryContextSwitchTo(oldcontext);

			/* Must copy result out of sub-context used by expression eval */
			if (!const_is_null)
				const_val = datumCopy(const_val, resultTypByVal, resultTypLen);

			/* Release all the junk we just created */
			FreeExecutorState(estate);

			/*
			 * Make the constant result node.
			 */
			simple = (Expr *) makeConst(expr->funcresulttype,
										-1,
										expr->funccollid,
										resultTypLen,
										const_val, const_is_null,
										resultTypByVal);

			/* successfully simplified it */
			if (simple)
				return (Node *) simple;
		}

		/*
		 * The expression cannot be simplified any further, so build and
		 * return a replacement FuncExpr node using the possibly-simplified
		 * arguments.
		 */
		return (Node *) newexpr;
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;
		List	   *args;

		OpExpr	   *newexpr;

		/*
		 * Reduce constants in the OpExpr's arguments.  We know args is either
		 * NIL or a List node, so we can call expression_tree_mutator directly
		 * rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
												pre_dispatch_function_evaluation_mutator,
												(void *) context);

		/*
		 * Need to get OID of underlying function.	Okay to scribble on input
		 * to this extent.
		 */
		set_opfuncid(expr);

		newexpr = makeNode(OpExpr);
		newexpr->opno = expr->opno;
		newexpr->opfuncid = expr->opfuncid;
		newexpr->opresulttype = expr->opresulttype;
		newexpr->opretset = expr->opretset;
		newexpr->opcollid = expr->opcollid;
		newexpr->inputcollid = expr->inputcollid;
		newexpr->args = args;
		newexpr->location = expr->location;

		return (Node *) newexpr;
	}
	else if (IsA(node, CurrentOfExpr))
	{
		/*
		 * updatable cursors
		 *
		 * During constant folding, we collect the current position of each
		 * cursor mentioned in the plan into a list, and dispatch them to the
		 * QEs.
		 *
		 * We should not get here if called from planner_make_plan_constant().
		 * That is only used for simple Result plans, which should not contain
		 * CURRENT OF expressions.
		 */
		if (context->estate)
		{
			CurrentOfExpr *expr = (CurrentOfExpr *) node;
			CursorPosInfo *cpos;

			cpos = makeNode(CursorPosInfo);

			getCurrentOf(expr,
						 GetPerTupleExprContext(context->estate),
						 expr->target_relid,
						 &cpos->ctid,
						 &cpos->gp_segment_id,
						 &cpos->table_oid,
						 &cpos->cursor_name);

			context->cursorPositions = lappend(context->cursorPositions, cpos);
		}
	}

	/*
	 * For any node type not handled above, we recurse using
	 * plan_tree_mutator, which will copy the node unchanged but try to
	 * simplify its arguments (if any) using this routine.
	 */
	new_node = plan_tree_mutator(node,
								 pre_dispatch_function_evaluation_mutator,
								 (void *) context,
								 true);

	return new_node;
}

/*
 * cdbpathtoplan_create_sri_path
 *
 * Optimize for single-row-insertion for const result. If result is const, and
 * result relation is partitioned, we could decide partition relation during
 * plan time and replace targetPolicy by partition relation's targetPolicy.
 * In addition, we don't need tuple distribution, but do filter on each writer
 * segment.
 *
 * Inputs:
 *
 * root		PlannerInfo passed by caller
 * plan		should always be result node
 * rte		is the target relation entry
 *
 * Inputs/Outputs:
 *
 * targetPolicy(in/out) is the target relation policy, and would be replaced
 * by partition relation.
 * hashExpr is distribution expression of target relation, and would be
 * replaced by partition relation.
 */
Plan *
cdbpathtoplan_create_sri_plan(RangeTblEntry *rte, PlannerInfo *subroot, Path *subpath,
							  int createplan_flags)
{
	CdbMotionPath *motionpath;
	ResultPath *resultpath;
	Result	   *resultplan;
	Relation	rel;
	PartitionNode *pn;
	GpPolicy   *targetPolicy;
	List	   *hashExprs;
	List	   *hashOpfamilies;
	int			numHashAttrs;
	AttrNumber *hashAttrs;
	Oid		   *hashFuncs;
	int			i;
	ListCell   *cell;

	if (!gp_enable_fast_sri)
		return NULL;

	if (!IsA(subpath, CdbMotionPath))
		return NULL;
	motionpath = (CdbMotionPath *) subpath;

	if (!IsA(motionpath->subpath, ResultPath))
		return NULL;

	resultpath = (ResultPath *) motionpath->subpath;

	if (contain_mutable_functions((Node *) resultpath->path.pathtarget->exprs))
		return NULL;

	resultplan = (Result *) create_plan_recurse(subroot, (Path *) resultpath, createplan_flags);
	if (!IsA(resultplan, Result))
		return NULL;

	/* Suppose caller already hold proper locks for relation. */
	rel = relation_open(rte->relid, NoLock);
	targetPolicy = rel->rd_cdbpolicy;

	/* 1: See if it's partitioned */
	pn = RelationBuildPartitionDesc(rel, false);

	/* Look up the distribution policy for the target partition. */
	if (pn && !partition_policies_equal(targetPolicy, pn))
	{
		/*
		 * 2: See if partitioning columns are constant
		 */
		List	   *partatts = get_partition_attrs(pn);
		ListCell   *lc;
		bool	   *nulls;
		Datum	   *values;
		EState	   *estate = CreateExecutorState();
		ResultRelInfo *rri;

		/*
		 * 4: build tuple, look up partitioning key
		 */
		nulls = palloc0(sizeof(bool) * rel->rd_att->natts);
		values = palloc(sizeof(Datum) * rel->rd_att->natts);

		foreach(lc, partatts)
		{
			AttrNumber	attnum = lfirst_int(lc);
			List	   *tl = resultplan->plan.targetlist;
			ListCell   *cell;

			foreach(cell, tl)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(cell);

				Assert(tle->expr);

				if (tle->resno == attnum)
				{
					/*
					 * GPDB_96_MERGE_FIXME: it's bogus to assume that the
					 * entries are all Consts. We used to verify that explicitly,
					 * but now we just call contain_mutable_functions(), which
					 * will return true also for more complicated expressions
					 * that only contain immutable functions. The right way would
					 * be to evaluate the expression here, with ExecPrepareExpr() +
					 * ExecEvalExpr(). In practice this works, because the planner
					 * has already "preprocessed" the target list, which does
					 * replace more complicated expressions with Consts.
					 */
					Assert(IsA(tle->expr, Const));

					nulls[attnum - 1] = ((Const *) tle->expr)->constisnull;
					if (!nulls[attnum - 1])
						values[attnum - 1] = ((Const *) tle->expr)->constvalue;
				}
			}
		}
		estate->es_result_partitions = pn;
		estate->es_partition_state =
			createPartitionState(estate->es_result_partitions, 1 /* resultPartSize */ );

		rri = makeNode(ResultRelInfo);
		rri->ri_RangeTableIndex = 1;	/* dummy */
		rri->ri_RelationDesc = rel;

		estate->es_result_relations = rri;
		estate->es_num_result_relations = 1;
		estate->es_result_relation_info = rri;
		rri = values_get_partition(values, nulls, RelationGetDescr(rel),
								   estate, false);

		/*
		 * 5: get target policy for destination table
		 */
		targetPolicy = RelationGetPartitioningKey(rri->ri_RelationDesc);

		if (targetPolicy->ptype != POLICYTYPE_PARTITIONED)
			elog(ERROR, "policy must be partitioned");

		heap_close(rri->ri_RelationDesc, NoLock);
		FreeExecutorState(estate);
	}

	hashExprs = getExprListFromTargetList(resultplan->plan.targetlist,
										  targetPolicy->nattrs,
										  targetPolicy->attrs);
	hashOpfamilies = NIL;
	for (int i = 0; i < targetPolicy->nattrs; i++)
	{
		Oid			opfamily = get_opclass_family(targetPolicy->opclasses[i]);

		hashOpfamilies = lappend_oid(hashOpfamilies, opfamily);
	}

	/*
	 * If there is no distribution key, don't do direct dispatch.
	 *
	 * GPDB_90_MERGE_FIXME: Is that the right thing to do? Couldn't we
	 * direct dispatch to any arbitrarily chosen segment, in that case?
	 */
	numHashAttrs = targetPolicy->nattrs;

	if (numHashAttrs > 0)
	{
		/* Get hash functions for the columns. */
		hashFuncs = palloc(numHashAttrs * sizeof(Oid));
		i = 0;
		foreach(cell, hashExprs)
		{
			Expr	   *elem = (Expr *) lfirst(cell);
			Oid			att_type = exprType((Node *) elem);
			Oid			opclass = targetPolicy->opclasses[i];

			hashFuncs[i++] =
				cdb_hashproc_in_opfamily(get_opclass_family(opclass),
										 att_type);
		}

		/*
		 * all constants in values clause -- no need to repartition.
		 */

		/* copy the attributes array */
		hashAttrs = palloc(numHashAttrs * sizeof(AttrNumber));
		for (i = 0; i < numHashAttrs; i++)
			hashAttrs[i] = targetPolicy->attrs[i];

		if (subroot->config->gp_enable_direct_dispatch)
		{
			directDispatchCalculateHash(&resultplan->plan, targetPolicy, hashFuncs);

			/*
			 * we now either have a hash-code, or we've marked the plan
			 * non-directed.
			 */
		}

		resultplan->numHashFilterCols = numHashAttrs;
		resultplan->hashFilterColIdx = hashAttrs;
		resultplan->hashFilterFuncs = hashFuncs;

		/* Build a partitioned flow */
		resultplan->plan.flow->flotype = FLOW_PARTITIONED;
		resultplan->plan.flow->locustype = CdbLocusType_Hashed;
	}
	else
		resultplan = NULL;

	relation_close(rel, NoLock);

	return (Plan *) resultplan;
}

/*
 * Does the given expression contain Params that are passed down from
 * outer query?
 */
bool
contains_outer_params(Node *node, void *context)
{
	PlannerInfo *root = (PlannerInfo *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXEC)
		{
			/* Does this Param refer to a value that an outer query provides? */
			PlannerInfo *parent = root->parent_root;

			while (parent)
			{
				ListCell   *lc;

				foreach (lc, parent->plan_params)
				{
					PlannerParamItem *ppi = (PlannerParamItem *) lfirst(lc);

					if (ppi->paramId == param->paramid)
						return true;		/* abort the tree traversal and return true */
				}

				parent = parent->parent_root;
			}
		}
	}
	return expression_tree_walker(node, contains_outer_params, context);
}
