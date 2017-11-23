/*-------------------------------------------------------------------------
 *
 * cdbplan.h
 *	  definitions for cdbplan.c utilities
 *
 * Portions Copyright (c) 2004-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbplan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBPLAN_H
#define CDBPLAN_H

#include "optimizer/walkers.h"

extern Node * plan_tree_mutator(Node *node, Node *(*mutator) (), void *context);

extern RangeTblEntry *package_plan_as_rte(Query *query, Plan *plan, Alias *eref, List *pathkeys);

extern Value *get_tle_name(TargetEntry *tle, List *rtable, const char *default_name);

extern Plan *wrap_plan(PlannerInfo *root, Plan *plan, Query *query, 
		  List **p_pathkeys, 
		  const char *alias_name, List *col_names, 
		  Query **query_p);

#endif   /* CDBPLAN_H */
