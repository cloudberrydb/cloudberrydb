/*-------------------------------------------------------------------------
 *
 * cdbgroup.c
 *	  Some helper routines for planning.
 *
 * This used to contain the code to implement MPP-aware GROUP BY planning,
 * hence the filename. But that's now handled in cdbgroupingpaths.c, only
 * a few functions remain here that are used elsewhere in the planner.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbgroup.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbgroup.h"
#include "optimizer/tlist.h"

/* Coefficients for cost calculation adjustments: These are candidate GUCs
 * or, perhaps, replacements for the gp_eager_... series.  We wouldn't
 * need these if our statistics and cost calculations were correct, but
 * as of 3.2, they not.
 *
 * Early testing suggested that (1.0, 0.45, 1.7) was about right, but the
 * risk of introducing skew in the initial redistribution of a 1-phase plan
 * is great (especially given the 3.2 tendency to way underestimate the
 * cardinality of joins), so we penalize 1-phase and normalize to the
 * 2-phase cost (approximately).
 */
/* GPDB_96_MERGE_FIXME: should we apply coefficient like this in the new pathified
 * MPP grouping implementation, in cdbgroupingpaths.c, still?
 */
#ifdef NOT_USED
static const double gp_coefficient_1phase_agg = 20.0;	/* penalty */
static const double gp_coefficient_2phase_agg = 1.0;	/* normalized */
static const double gp_coefficient_3phase_agg = 3.3;	/* increase systematic
														 * underestimate */
#endif

/*
 * Function: cdbpathlocus_collocates_pathkeys
 *
 * Is a relation with the given locus guaranteed to collocate tuples with
 * non-distinct values of the key.  The key is a list of PathKeys.
 *
 * Collocation is guaranteed if the locus specifies a single process or
 * if the result is partitioned on a subset of the keys that must be
 * collocated.
 *
 * We ignore other sorts of collocation, e.g., replication or partitioning
 * on a range since these cannot occur at the moment (MPP 2.3).
 */
bool
cdbpathlocus_collocates_pathkeys(PlannerInfo *root, CdbPathLocus locus, List *pathkeys,
								 bool exact_match)
{
	ListCell   *i;
	List	   *pk_eclasses;

	if (CdbPathLocus_IsBottleneck(locus))
		return true;

	if (!CdbPathLocus_IsHashed(locus))
	{
		/*
		 * Note: HashedOJ can *not* be used for grouping. In HashedOJ, NULL
		 * values can be located on any segment, so we would end up with
		 * multiple NULL groups.
		 */
		return false;
	}

	if (exact_match && list_length(pathkeys) != list_length(locus.distkey))
		return false;

	/*
	 * Extract a list of eclasses from the pathkeys.
	 */
	pk_eclasses = NIL;
	foreach(i, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(i);
		EquivalenceClass *ec;

		ec = pathkey->pk_eclass;
		while (ec->ec_merged != NULL)
			ec = ec->ec_merged;

		pk_eclasses = lappend(pk_eclasses, ec);
	}

	/*
	 * Check for containment of locus in pk_eclasses.
	 *
	 * We ignore constants in the locus hash key. A constant has the same
	 * value everywhere, so it doesn't affect collocation.
	 */
	return cdbpathlocus_is_hashed_on_eclasses(locus, pk_eclasses, true);
}


/*
 * Function: cdbpathlocus_collocates_expressions
 *
 * Like cdbpathlocus_collocates_pathkeys, but the key list is given as a list
 * of plain expressions, instead of PathKeys.
 */
bool
cdbpathlocus_collocates_expressions(PlannerInfo *root, CdbPathLocus locus, List *exprs,
								   bool exact_match)
{
	if (CdbPathLocus_IsBottleneck(locus))
		return true;

	if (!CdbPathLocus_IsHashed(locus))
	{
		/*
		 * Note: HashedOJ can *not* be used for grouping. In HashedOJ, NULL
		 * values can be located on any segment, so we would end up with
		 * multiple NULL groups.
		 */
		return false;
	}

	if (exact_match && list_length(exprs) != list_length(locus.distkey))
		return false;

	/*
	 * Check for containment of locus in pk_eclasses.
	 *
	 * We ignore constants in the locus hash key. A constant has the same
	 * value everywhere, so it doesn't affect collocation.
	 */
	return cdbpathlocus_is_hashed_on_exprs(locus, exprs, true);
}

/**
 * Update the scatter clause before a query tree's targetlist is about to
 * be modified. The scatter clause of a query tree will correspond to
 * old targetlist entries. If the query tree is modified and the targetlist
 * is to be modified, we must call this method to ensure that the scatter clause
 * is kept in sync with the new targetlist.
 */
void
UpdateScatterClause(Query *query, List *newtlist)
{
	Assert(query);
	Assert(query->targetList);
	Assert(newtlist);

	if (query->scatterClause
		&& list_nth(query->scatterClause, 0) != NULL	/* scattered randomly */
		)
	{
		Assert(list_length(query->targetList) == list_length(newtlist));
		List	   *scatterClause = NIL;
		ListCell   *lc = NULL;

		foreach(lc, query->scatterClause)
		{
			Expr	   *o = (Expr *) lfirst(lc);

			Assert(o);
			TargetEntry *tle = tlist_member((Node *) o, query->targetList);

			Assert(tle);
			TargetEntry *ntle = list_nth(newtlist, tle->resno - 1);

			scatterClause = lappend(scatterClause, copyObject(ntle->expr));
		}
		query->scatterClause = scatterClause;
	}
}
