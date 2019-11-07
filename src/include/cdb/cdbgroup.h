/*-------------------------------------------------------------------------
 *
 * cdbgroup.h
 *	  prototypes for cdbgroup.c.
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbgroup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBGROUP_H
#define CDBGROUP_H

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"

#include "optimizer/clauses.h" /* AggClauseCosts */

extern bool cdbpathlocus_collocates_pathkeys(PlannerInfo *root, CdbPathLocus locus, List *pathkeys, bool exact_match);
extern bool cdbpathlocus_collocates_expressions(PlannerInfo *root, CdbPathLocus locus, List *exprs,
											   bool exact_match);

extern void UpdateScatterClause(Query *query, List *newtlist);

#endif   /* CDBGROUP_H */
