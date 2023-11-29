/*-------------------------------------------------------------------------
 *
 * deparse.c
 *		  Query deparser for dlproxy
 *
 * This file includes functions that examine query WHERE clauses to see
 * whether they're safe to send to the remote server for execution.
 *
 * IDENTIFICATION
 *		  dlproxy/deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/sysattr.h"
#include "nodes/pg_list.h"
#include "nodes/pathnodes.h"
#include "utils/rel.h"
#include "deparse.h"

/*
 * We create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
void
deparseTargetList(Relation rel, Bitmapset *attrs_used, List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	int			i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

	if (have_wholerow)
		return;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}
}

/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void
classifyConditions(List *input_conds,
				   List **remote_conds,
				   List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* for now, just assume that all WHERE clauses are OK on remote */
		*remote_conds = lappend(*remote_conds, ri);
		*local_conds = lappend(*local_conds, ri);
	}
}
