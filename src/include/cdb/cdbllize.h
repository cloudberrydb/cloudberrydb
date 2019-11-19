/*-------------------------------------------------------------------------
 *
 * cdbllize.h
 *	  definitions for parallelizing a PostgreSQL sequential plan tree.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbllize.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBLLIZE_H
#define CDBLLIZE_H

#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"

extern Path *create_motion_for_top_plan(PlannerInfo *root, Path *best_path,
										bool *needToAssignDirectDispatchContentIds);

extern Plan *cdbparallelize(struct PlannerInfo *root, Plan *plan);

extern bool is_plan_node(Node *node);

extern Flow *makeFlow(FlowType flotype, int numsegments);

extern Flow *pull_up_Flow(Plan *plan, Plan *subplan);

extern Plan *focusPlan(Plan *plan, bool stable);
extern Plan *broadcastPlan(Plan *plan, bool stable,
						  int numsegments);

#endif   /* CDBLLIZE_H */
