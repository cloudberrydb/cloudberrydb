/*-------------------------------------------------------------------------
 *
 * planshare.c
 *	  Plan shared plan
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planshare.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/subselect.h"
#include "optimizer/planshare.h"

ShareType
get_plan_share_type(Plan *p)
{
	if(IsA(p, Material))
		return ((Material *) p)->share_type;

	if(IsA(p, Sort))
		return ((Sort *) p)->share_type ;

	Assert(IsA(p, ShareInputScan));
	return ((ShareInputScan *) p)->share_type;
}

static ShareInputScan *
make_shareinputscan(PlannerInfo *root, Plan *inputplan)
{
	ShareInputScan *sisc;
	Path		sipath;

	Assert(IsA(inputplan, Material) || IsA(inputplan, Sort));

	sisc = makeNode(ShareInputScan);

	sisc->scan.plan.targetlist = copyObject(inputplan->targetlist);
	sisc->scan.plan.lefttree = inputplan;
	sisc->scan.plan.flow = copyObject(inputplan->flow);

	sisc->share_type = get_plan_share_type(inputplan);
	sisc->producer_slice_id = -1;
	sisc->this_slice_id = -1;
	sisc->nconsumers = 0;

	sisc->scan.plan.qual = NIL;
	sisc->scan.plan.righttree = NULL;

	cost_shareinputscan(&sipath, root, inputplan->total_cost, inputplan->plan_rows, inputplan->plan_width);

	sisc->scan.plan.startup_cost = sipath.startup_cost;
	sisc->scan.plan.total_cost = sipath.total_cost; 
	sisc->scan.plan.plan_rows = inputplan->plan_rows;
	sisc->scan.plan.plan_width = inputplan->plan_width;

	sisc->scan.plan.extParam = bms_copy(inputplan->extParam);
	sisc->scan.plan.allParam = bms_copy(inputplan->allParam);

	return sisc;
}

/*
 * Prepare a subplan for sharing. This creates a Materialize node,
 * or marks the existing Materialize or Sort node as shared. After
 * this, you can call share_prepared_plan() as many times as you
 * want to share this plan.
 */
Plan *
prepare_plan_for_sharing(PlannerInfo *root, Plan *common)
{
	Plan *shared = common;

	if (IsA(common, ShareInputScan))
	{
		shared = common->lefttree;
	}
	
	else if(IsA(common, Material))
	{
		Material *m = (Material *) common;

		Assert(m->share_type == SHARE_NOTSHARED);
		Assert(m->share_id == SHARE_ID_NOT_SHARED);

		m->share_id = SHARE_ID_NOT_ASSIGNED;
		m->share_type = SHARE_MATERIAL;
	}
	else if (IsA(common, Sort))
	{
		Sort *s = (Sort *) common;

		Assert(s->share_type == SHARE_NOTSHARED);
		s->share_id = SHARE_ID_NOT_ASSIGNED;
		s->share_type = SHARE_SORT;
	}
	else
	{
		Path matpath;
		Material *m = make_material(common);
		shared = (Plan *) m;

		cost_material(&matpath, root,
					  common->startup_cost,
					  common->total_cost,
					  common->plan_rows,
					  common->plan_width);
		shared->startup_cost = matpath.startup_cost;
		shared->total_cost = matpath.total_cost;
		shared->plan_rows = common->plan_rows;
		shared->plan_width = common->plan_width;
		shared->flow = copyObject(common->flow); 
		shared->extParam = bms_copy(common->extParam);
		shared->allParam = bms_copy(common->allParam);

		m->share_id = SHARE_ID_NOT_ASSIGNED;
		m->share_type = SHARE_MATERIAL;
	}

	return shared;
}

/*
 * Create a ShareInputScan to scan the given subplan. The subplan
 * must've been prepared for sharing by calling
 * prepare_plan_for_sharing().
 */
Plan *
share_prepared_plan(PlannerInfo *root, Plan *common)
{
	return (Plan *) make_shareinputscan(root, common);
}

/*
 * A shorthand for prepare_plan_for_sharing() followed by
 * 'numpartners' calls to share_prepared_plan().
 */
List *
share_plan(PlannerInfo *root, Plan *common, int numpartners)
{
	List	   *shared_nodes = NIL;
	Plan	   *shared;
	int			i;

	Assert(numpartners > 0);

	if (numpartners == 1)
		return list_make1(common);

	shared = prepare_plan_for_sharing(root, common);

	for (i = 0; i < numpartners; i++)
	{
		Plan	   *p;

		p = (Plan *) make_shareinputscan(root, shared);
		shared_nodes = lappend(shared_nodes, p);
	}

	return shared_nodes;
}


/*
 * Return the total cost of sharing common numpartner times.
 * If the planner need to make a decision whether the common subplan should be shared
 * or should be duplicated, planner should compare the cost returned by this function 
 * against common->total_cost * numpartners.
 */
Cost cost_share_plan(Plan *common, PlannerInfo *root, int numpartners)
{
	Path sipath;
	Path mapath;

	if(IsA(common, Material) || IsA(common, Sort))
	{
		cost_shareinputscan(&sipath, root, common->total_cost, common->plan_rows, common->plan_width);

		return common->total_cost + (sipath.total_cost - common->total_cost) * numpartners;
	}

	cost_material(&mapath, root,
				  common->startup_cost,
				  common->total_cost,
				  common->plan_rows,
				  common->plan_width);
	cost_shareinputscan(&sipath, root, mapath.total_cost, common->plan_rows, common->plan_width);

	return mapath.total_cost + (sipath.total_cost - mapath.total_cost) * numpartners;
}

