/*-------------------------------------------------------------------------
 *
 * tlist.c
 *	  Target list manipulation routines
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/optimizer/util/tlist.c,v 1.74 2007/01/05 22:19:33 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_expr.h"
#include "utils/lsyscache.h"

typedef struct maxSortGroupRef_context
{
	Index maxsgr;
	bool include_orderedagg;
} maxSortGroupRef_context;

static 
bool maxSortGroupRef_walker(Node *node, maxSortGroupRef_context *cxt);

/*****************************************************************************
 *		Target list creation and searching utilities
 *****************************************************************************/

/*
 * tlist_member
 *	  Finds the (first) member of the given tlist whose expression is
 *	  equal() to the given expression.	Result is NULL if no such member.
 */
TargetEntry *
tlist_member(Node *node, List *targetlist)
{
	ListCell   *temp;

	foreach(temp, targetlist)
	{
		TargetEntry *tlentry = (TargetEntry *) lfirst(temp);

        Assert(IsA(tlentry, TargetEntry));

		if (equal(node, tlentry->expr))
			return tlentry;
	}
	return NULL;
}

/*
 * tlist_members
 *	  Finds all members of the given tlist whose expression is
 *	  equal() to the given expression.	Result is NIL if no such member.
 *	  Note: We do not make a copy of the tlist entries that match. 
 *	  The caller is responsible for cleaning up the memory allocated 
 *	  to the List returned.
 */
List *
tlist_members(Node *node, List *targetlist)
{
	List *tlist = NIL;
	ListCell   *temp = NULL;

	foreach(temp, targetlist)
	{
		TargetEntry *tlentry = (TargetEntry *) lfirst(temp);

        Assert(IsA(tlentry, TargetEntry));

		if (equal(node, tlentry->expr))
		{
			tlist = lappend(tlist, tlentry);
		}
	}
	
	return tlist;
}

/*
 * tlist_member_ignoring_RelabelType
 *	  Finds the (first) member of the given tlist whose expression is
 *	  equal() to the given expression.	Result is NULL if no such member.
 *
 * Disregards the presence or absence of a RelabelType node atop the
 * given expression and/or the tlist expressions.
 */
TargetEntry *
tlist_member_ignoring_RelabelType(Expr *expr, List *targetlist)
{
    ListCell   *temp;

    if (IsA(expr, RelabelType))
        expr = ((RelabelType *)expr)->arg;

    foreach(temp, targetlist)
    {
        TargetEntry *tlentry = (TargetEntry *)lfirst(temp);
        Expr        *tlexpr = tlentry->expr;

        Assert(IsA(tlentry, TargetEntry));

        if (IsA(tlexpr, RelabelType))
            tlexpr = ((RelabelType *)tlexpr)->arg;

        if (equal(expr, tlexpr))
            return tlentry;
	}
	return NULL;
}                               /* tlist_member_ignoring_RelabelType */


/*
 * flatten_tlist
 *	  Create a target list that only contains unique variables.
 *
 * Note that Vars with varlevelsup > 0 are not included in the output
 * tlist.  We expect that those will eventually be replaced with Params,
 * but that probably has not happened at the time this routine is called.
 *
 * 'tlist' is the current target list
 *
 * Returns the "flattened" new target list.
 *
 * The result is entirely new structure sharing no nodes with the original.
 * Copying the Var nodes is probably overkill, but be safe for now.
 */
List *
flatten_tlist(List *tlist)
{
	List	   *vlist = pull_var_clause((Node *) tlist, false);
	List	   *new_tlist;

	new_tlist = add_to_flat_tlist(NIL, vlist, false /* resjunk */);
	list_free(vlist);
	return new_tlist;
}

/*
 * add_to_flat_tlist
 *		Add more expressions to a flattened tlist (if they're not already in it)
 *
 * 'tlist' is the flattened tlist
 * 'exprs' is a list of expression nodes
 *
 * Returns the extended tlist.
 */
List *
add_to_flat_tlist(List *tlist, List *exprs, bool resjunk)
{
	int			next_resno = list_length(tlist) + 1;
	ListCell   *v;

	foreach(v, exprs)
	{
		Expr		   *expr = (Expr *) lfirst(v);

		if (!tlist_member_ignoring_RelabelType(expr, tlist))
		{
			TargetEntry *tle;

			tle = makeTargetEntry(copyObject(expr),		/* copy needed?? */
								  next_resno++,
								  NULL,
								  resjunk);
			tlist = lappend(tlist, tle);
		}
	}
	return tlist;
}

/*
 * get_sortgroupclause_tle
 *		Find the targetlist entry matching the given SortClause
 *		(or GroupClause) by ressortgroupref, and return it.
 *
 * Because GroupClause is typedef'd as SortClause, either kind of
 * node can be passed without casting.
 */
TargetEntry *
get_sortgroupclause_tle(SortClause *sortClause,
						List *targetList)
{
	Index		refnumber = sortClause->tleSortGroupRef;
	ListCell   *l;

	foreach(l, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->ressortgroupref == refnumber)
			return tle;
	}

	elog(ERROR, "ORDER/GROUP BY expression not found in targetlist");
	return NULL;				/* keep compiler quiet */
}

/*
 * get_sortgroupclauses_tles
 *      Find a list of unique targetlist entries matching the given list of
 *      SortClause, GroupClause, or GroupingClauses.
 *
 * In each grouping set, targets that do not appear in a GroupingClause
 * will be put in the front of those that appear in a GroupingClauses.
 * The targets within the same clause will be appended in the order
 * of their appearance.
 */
List *
get_sortgroupclauses_tles(List *clauses, List *targetList)
{
	List *result = NIL;
	ListCell *l;

	List *sub_grouping_result = NIL;

	foreach(l, clauses)
	{
		Node *node = lfirst(l);

		if (node == NULL)
			continue;

		if (IsA(node, GroupClause) || IsA(node, SortClause))
		{
			TargetEntry *tle =
				get_sortgroupclause_tle((SortClause*)node, targetList);

			if (!list_member(result, tle))
				result = lappend(result, tle);
		}

		else if (IsA(node, List))
		{
			result = list_concat_unique(result,
										get_sortgroupclauses_tles((List *)node,
																  targetList));
		}

		else
		{
			List *sub_list;
			Assert (IsA(node, GroupingClause));

			sub_list =
				get_sortgroupclauses_tles(((GroupingClause*)node)->groupsets,
										  targetList);

			sub_grouping_result =
				list_concat_unique(sub_grouping_result, sub_list);
		}
	}

	/* Put GroupClauses before GroupingClauses. */
	result = list_concat_unique(result, sub_grouping_result);

	return result;
}

/*
 * get_sortgroupclause_expr
 *		Find the targetlist entry matching the given SortClause
 *		(or GroupClause) by ressortgroupref, and return its expression.
 *
 * Because GroupClause is typedef'd as SortClause, either kind of
 * node can be passed without casting.
 */
Node *
get_sortgroupclause_expr(SortClause *sortClause, List *targetList)
{
	TargetEntry *tle = get_sortgroupclause_tle(sortClause, targetList);

	return (Node *) tle->expr;
}

/*
 * get_sortgrouplist_exprs
 *		Given a list of SortClauses (or GroupClauses), build a list
 *		of the referenced targetlist expressions.
 */
List *
get_sortgrouplist_exprs(List *sortClauses, List *targetList)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, sortClauses)
	{
		SortClause *sortcl = (SortClause *) lfirst(l);
		Node	   *sortexpr;

		sortexpr = get_sortgroupclause_expr(sortcl, targetList);
		result = lappend(result, sortexpr);
	}
	return result;
}

/*
 * get_grouplist_colidx
 *		Given a list of GroupClauses, build an array of the referenced
 *		targetlist resno.  If numCols is not NULL, it is filled by the
 *		length of returned array.  Results are allocated by palloc.
 */
AttrNumber *
get_grouplist_colidx(List *groupClauses, List *targetList, int *numCols)
{
	AttrNumber	   *result;
	List		   *tles;
	ListCell	   *l;
	int				i, len;

	len = num_distcols_in_grouplist(groupClauses);
	if (numCols)
		*numCols = len;

	if (len == 0)
		return NULL;

	result = (AttrNumber *) palloc(sizeof(AttrNumber) * len);
	tles = get_sortgroupclauses_tles(groupClauses, targetList);
	foreach_with_count (l, tles, i)
	{
		TargetEntry	   *tle = lfirst(l);

		result[i] = tle->resno;
	}

	return result;
}

/*
 * get_grouplist_exprs
 *     Find a list of unique referenced targetlist expressions used in a given
 *     list of GroupClauses or a GroupingClauses.
 *
 * All expressions will appear in the same order as they appear in the
 * given list of GroupClauses or a GroupingClauses.
 *
 * Note that the top-level empty sets will be removed here.
 */
List *
get_grouplist_exprs(List *groupClauses, List *targetList)
{
	List *result = NIL;
	ListCell *l;

	foreach (l, groupClauses)
	{
		Node *groupClause = lfirst(l);

		if (groupClause == NULL)
			continue;

		Assert(IsA(groupClause, GroupClause) ||
			   IsA(groupClause, GroupingClause) ||
			   IsA(groupClause, List));

		if (IsA(groupClause, GroupClause))
		{
			Node *expr = get_sortgroupclause_expr((GroupClause*)groupClause,
												  targetList);
			
			if (!list_member(result, expr))
				result = lappend(result, expr);
		}

		else if (IsA(groupClause, List))
			result = list_concat_unique(result,
								 get_grouplist_exprs((List *)groupClause, targetList));

		else
			result = list_concat_unique(result,
								 get_grouplist_exprs(((GroupingClause*)groupClause)->groupsets,
													 targetList));
	}

	return result;
}


/*
 * Does tlist have same output datatypes as listed in colTypes?
 *
 * Resjunk columns are ignored if junkOK is true; otherwise presence of
 * a resjunk column will always cause a 'false' result.
 *
 * Note: currently no callers care about comparing typmods.
 */
bool
tlist_same_datatypes(List *tlist, List *colTypes, bool junkOK)
{
	ListCell   *l;
	ListCell   *curColType = list_head(colTypes);

	foreach(l, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->resjunk)
		{
			if (!junkOK)
				return false;
		}
		else
		{
			if (curColType == NULL)
				return false;	/* tlist longer than colTypes */
			if (exprType((Node *) tle->expr) != lfirst_oid(curColType))
				return false;
			curColType = lnext(curColType);
		}
	}
	if (curColType != NULL)
		return false;			/* tlist shorter than colTypes */
	return true;
}


/*
 * Return the largest sortgroupref value in use in the given 
 * target list.  
 *
 * If include_orderedagg is false, consider only the top-level 
 * entries in the target list, i.e., those that might be occur 
 * in a groupClause, distinctClause, or sortClause of the Query 
 * node that immediately contains the target list.
 *
 * If include_orderedagg is true, also consider AggOrder entries 
 * embedded in Aggref nodes within the target list.  Though 
 * such entries will only occur in the aggregation sub_tlist
 * (input) they affect sortgroupref numbering for both sub_tlist
 * and tlist (aggregate).
 */
Index maxSortGroupRef(List *targetlist, bool include_orderedagg)
{
	maxSortGroupRef_context context;
	context.maxsgr = 0;
	context.include_orderedagg = include_orderedagg;
	
	if (targetlist != NIL)
	{
		if ( !IsA(targetlist, List) || !IsA(linitial(targetlist), TargetEntry ) )
			elog(ERROR, "non-targetlist argument supplied");
			
		maxSortGroupRef_walker((Node*)targetlist, &context);
	}
	
	return context.maxsgr;
}

bool maxSortGroupRef_walker(Node *node, maxSortGroupRef_context *cxt)
{
	if ( node == NULL )
		return false;
	
	if ( IsA(node, TargetEntry) )
	{
		TargetEntry *tle = (TargetEntry*)node;
		if ( tle->ressortgroupref > cxt->maxsgr )
			cxt->maxsgr = tle->ressortgroupref;
		
		return maxSortGroupRef_walker((Node*)tle->expr, cxt);
	}
	
	/* Aggref nodes don't nest, so we can treat them here without recurring
	 * further.
	 */
	
	if ( IsA(node, Aggref) )
	{
		Aggref *ref = (Aggref*)node;
		if ( ref->aggorder && cxt->include_orderedagg )
		{
			ListCell *lc;
			AggOrder *aggorder = ref->aggorder;
			
			foreach (lc, aggorder->sortClause)
			{
				SortClause *sort = (SortClause *)lfirst(lc);
				Assert(IsA(sort, SortClause));
				Assert( sort->tleSortGroupRef != 0 );
				if (sort->tleSortGroupRef > cxt->maxsgr )
					cxt->maxsgr = sort->tleSortGroupRef;
			}
			
		}
		return false;
	}
	
	return expression_tree_walker(node, maxSortGroupRef_walker, cxt);
}
	
/**
 * Returns the width of the row by traversing through the
 * target list and adding up the width of each target entry.
 */
int get_row_width(List *tlist)
{
	int width = 0;
	ListCell *plc = NULL;

    foreach(plc, tlist)
    {
    	TargetEntry *pte = (TargetEntry*) lfirst(plc);
	    Expr *pexpr = pte->expr;

	    Assert(NULL != pexpr);

	    Oid oidType = exprType( (Node *) pexpr);
	    int32 iTypmod = exprTypmod( (Node *) pexpr);

	    width += get_typavgwidth(oidType, iTypmod);
	}

    return width;
}


/*
 * insist_target_lists_aligned
 * 		Check that two target lists have the same number and type of entries
 */
void
insist_target_lists_aligned(List *tlistFst, List *tlistSnd)
{
	Assert(list_length(tlistFst) == list_length(tlistSnd));
	
	ListCell *lcFst = NULL;
	ListCell *lcSnd = NULL;
	
	forboth (lcFst, tlistFst, lcSnd, tlistSnd)
	{
		TargetEntry *teFst = lfirst(lcFst);
		TargetEntry *teSnd = lfirst(lcSnd);
		
		Insist(exprType((Node*) teFst->expr) == exprType((Node *) teSnd->expr));
	}
}
