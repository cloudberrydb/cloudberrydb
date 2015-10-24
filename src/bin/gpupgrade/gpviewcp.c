/*
 * gpviewcp.c - Greenplum View Copy
 *
 * As part of upgrade we need to be able to modify existing catalog views,
 * but there are no postgres operations for view modification.
 *
 * We handle this by creating a NEW view with the view definition that we
 * want and then copy it into the OLD view.  This is complicated by the fact
 * that a view definition typically refers to its own OID, so it's not a
 * true copy but a transformed version of the original view.
 *
 * Based on the code in backend/utils/adt/ruleutils.c
 */

#include "postgres.h"

#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "catalog/pg_rewrite.h"
#include "catalog/indexing.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/relcache.h"

/* Extensibility Magic */
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(gpviewcopy);
PG_FUNCTION_INFO_V1(gpviewreparse);

Datum gpviewcopy(PG_FUNCTION_ARGS);
Datum gpviewreparse(PG_FUNCTION_ARGS);

/* ----------
 * void gpviewcopy(oid source, oid target)
 *   1) Deparse source view.
 *   2) Replace source view oid with target view oid in view def.
 *   3) Update target view with new definition.
 * ----------
 */
Datum
gpviewcopy(PG_FUNCTION_ARGS)
{
	Oid               source_oid, target_oid;
	HeapTuple         source_tup, target_tup, new_tuple;
	Form_pg_rewrite   source_rec, target_rec;
	Form_pg_class     source_class, target_class;
	Form_pg_attribute source_att, target_att;
	Datum             source_action;
	char             *source_name, *target_name;
	Relation          rel;
	AttrNumber        attnum;
	int               natts;
	Datum             values[Natts_pg_rewrite];
	bool              nulls[Natts_pg_rewrite];
	bool              replaces[Natts_pg_rewrite];
	bool              isnull;
	char             *ev_action;
	char             *new_action;
	List             *actions;
	List             *range_table;
	RangeTblEntry    *rte;
	Query	         *query;

	/* Validate input arguments */
	if (PG_NARGS() != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpviewcopy(): called with %hd arguments",
						PG_NARGS())));
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpviewcopy(): called with null arguments")));

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied"),
				 errhint("gpviewcopy(): requires superuser privileges.")));

	/* Get the oids from the arguments */
	source_oid = PG_GETARG_OID(0);  
	target_oid = PG_GETARG_OID(1);

	/* 
	 * Validate that the source view and the target view have the same
	 * result type.
	 */
	rel = heap_open(RelationRelationId, RowShareLock);
	source_tup = SearchSysCache(RELOID, ObjectIdGetDatum(source_oid),
								0,0,0);
	if (!HeapTupleIsValid(source_tup))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpviewcopy(): oid %d not found", source_oid)));
	source_class = (Form_pg_class) GETSTRUCT(source_tup);
	source_name = pstrdup(NameStr(source_class->relname));
	if (source_class->relkind != 'v')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpviewcopy(): \"%s\" is not a view", source_name)));
	target_tup = SearchSysCache(RELOID, ObjectIdGetDatum(target_oid),
								0,0,0);
	if (!HeapTupleIsValid(target_tup))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpviewcopy(): oid %d not found", target_oid)));
	target_class = (Form_pg_class) GETSTRUCT(target_tup);
	target_name = pstrdup(NameStr(target_class->relname));
	if (target_class->relkind != 'v')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpviewcopy(): \"%s\" is not a view", target_name)));

	/* Both views should have the same number of columns */
	if (source_class->relnatts != target_class->relnatts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpviewcopy(): views are not compatible"),
				 errhint("\"%s\" has %d columns, \"%s\" has %d columns",
						 source_name, source_class->relnatts, 
						 target_name, target_class->relnatts)));
	natts = source_class->relnatts;

	/* Keep the lock on the pg_class tuples until we are done */
	ReleaseSysCache(source_tup);
	ReleaseSysCache(target_tup);
	heap_close(rel, NoLock);

	/* Now verify that pg_attribute matches for the two views */
	rel = heap_open(AttributeRelationId, RowShareLock);
	for (attnum = 1; attnum <= natts; attnum++)
	{
		char *source_attname, *target_attname;

		source_tup = SearchSysCache(ATTNUM, 
									ObjectIdGetDatum(source_oid),
									Int16GetDatum(attnum),
									0, 0);
		if (!HeapTupleIsValid(source_tup))
			elog(ERROR, "cache lookup failed for attribute %d of relation %u",
				 attnum, source_oid);
		source_att = (Form_pg_attribute) GETSTRUCT(source_tup);

		target_tup = SearchSysCache(ATTNUM, 
									ObjectIdGetDatum(target_oid),
									Int16GetDatum(attnum),
									0, 0);
		if (!HeapTupleIsValid(target_tup))
			elog(ERROR, "cache lookup failed for attribute %d of relation %u",
				 attnum, target_oid);
		target_att = (Form_pg_attribute) GETSTRUCT(target_tup);
		
		/* The should be identical except for attrelid */
		source_attname = NameStr(source_att->attname);
		target_attname = NameStr(target_att->attname);
		if ( strcmp(source_attname, target_attname) )
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: attname mismatch \"%s\" != \"%s\"", 
							 attnum, source_attname, target_attname)));
		}
		if (source_att->atttypid != target_att->atttypid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: typid mismatch %d != %d", attnum, 
							 source_att->atttypid, target_att->atttypid)));
		if (source_att->attlen != target_att->attlen)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: attlen mismatch %d != %d", attnum, 
							 source_att->attlen, target_att->attlen)));
		if (source_att->attnum != target_att->attnum)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: attnum mismatch %d != %d", attnum, 
							 source_att->attnum, target_att->attnum)));
		if (source_att->attndims != target_att->attndims)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: attndims mismatch %d != %d", attnum, 
							 source_att->attndims, target_att->attndims)));
		if (source_att->atttypmod != target_att->atttypmod)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: typmod mismatch %d != %d", attnum, 
							 source_att->atttypmod, target_att->atttypmod)));
		if (source_att->attbyval != target_att->attbyval)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: attbyval mismatch %d != %d", attnum, 
							 source_att->attbyval, target_att->attbyval)));
		if (source_att->attstorage != target_att->attstorage)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: storage mismatch %c != %c", attnum, 
							 source_att->attstorage, target_att->attstorage)));
		if (source_att->attalign != target_att->attalign)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("gpviewcopy(): views are not compatible"),
					 errhint("attnum %d: alignent mismatch %c != %c", attnum, 
							 source_att->attalign, target_att->attalign)));

		ReleaseSysCache(source_tup);
		ReleaseSysCache(target_tup);
	}
	heap_close(rel, RowShareLock);

	/* Lookup source and target tuples from pg_rewrite */
	rel = heap_open(RewriteRelationId, RowExclusiveLock);

	/* Find the source action */
	source_tup = SearchSysCache(RULERELNAME,
								ObjectIdGetDatum(source_oid),
								PointerGetDatum("_RETURN"), 
								0, 0);
	if (!HeapTupleIsValid(source_tup))
		elog(ERROR, "gpviewcopy(): \"%s\" not found in pg_rewrite", source_name);
	source_rec = (Form_pg_rewrite) GETSTRUCT(source_tup);
	Assert(source_rec->ev_class == source_oid);
	if (source_rec->ev_type != '1' || 
		source_rec->ev_attr >= 0 ||
		!source_rec->is_instead)
	{
		elog(ERROR, "gpviewcopy(): pg_rewrite entry is invalid for \"%s\"", 
			 source_name);
	}
	source_action = heap_getattr(source_tup, 
								 Anum_pg_rewrite_ev_action, 
								 RelationGetDescr(rel), 
								 &isnull);
	if (isnull)
		elog(ERROR, "gpviewcopy(): ev_action is null for \"%s\"", source_name);
	ev_action = TextDatumGetCString(source_action);  /* palloced */

	/* Find the target */
	target_tup = SearchSysCache(RULERELNAME, 
								ObjectIdGetDatum(target_oid),
								PointerGetDatum("_RETURN"), 
								0, 0);
	if (!HeapTupleIsValid(target_tup))
		elog(ERROR, "gpviewcopy(): \"%s\" not found in pg_rewrite", target_name);
	target_rec = (Form_pg_rewrite) GETSTRUCT(target_tup);
	Assert(target_rec->ev_class == target_oid);
	if (target_rec->ev_type != '1' || 
		target_rec->ev_attr >= 0 ||
		!target_rec->is_instead)
	{
		elog(ERROR, "gpviewcopy(): pg_rewrite entry is invalid for \"%s\"",
			 target_name);
	}

	/*
	 * Modify the source ev_action to replace the source_oid with
	 * the target oid, this is used to handle the two special 
	 * range table entries present in views that refer to itself.
	 * 
	 * These are the *OLD* and *NEW* range table entries that exist
	 * as the first two range table entries of the view. 
	 *
	 * see backend/commands/view.c
	 */
	actions = (List *) stringToNode(ev_action);
	if (list_length(actions) != 1)
		elog(ERROR, "gpviewcopy(): ev_action is invalid for \"%s\"", source_name);
	query = (Query *) linitial(actions);
	if (query->commandType != CMD_SELECT)
		elog(ERROR, "gpviewcopy(): ev_action is invalid for \"%s\"", source_name);
	range_table = query->rtable;
	if (list_length(range_table) < 2)
		elog(ERROR, "gpviewcopy(): ev_action is invalid for \"%s\"", source_name);
	rte = (RangeTblEntry*) linitial(range_table);
	Assert(rte->rtekind == 0 && rte->relid == source_oid);
	rte->relid = target_oid;
	rte = (RangeTblEntry*) lsecond(range_table);
	Assert(rte->rtekind == 0 && rte->relid == source_oid);
	rte->relid = target_oid;

	/* Convert the modified parsetree back into a string */
	new_action = nodeToString(actions);
	
	/* Modify the target tuple */
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));
	values[Anum_pg_rewrite_ev_action - 1] = CStringGetTextDatum(new_action);
	replaces[Anum_pg_rewrite_ev_action - 1] = true;
	new_tuple = heap_modify_tuple(target_tup, RelationGetDescr(rel),
								  values, nulls, replaces);
	simple_heap_update(rel, &new_tuple->t_self, new_tuple);
	CatalogUpdateIndexes(rel, new_tuple);
	heap_freetuple(new_tuple);

	/* Cleanup */
	ReleaseSysCache(source_tup);
	ReleaseSysCache(target_tup);
	heap_close(rel, RowExclusiveLock);

	PG_RETURN_VOID();
}


/* ----------
 * void gpviewreparse(text source)
 *   1) Deparse source view
 *   2) Return a reparse of the source view
 *
 * The above should prove that the new code can read the old view format
 * and when it writes it out again it should be in the new format.
 * ----------
 */
Datum
gpviewreparse(PG_FUNCTION_ARGS)
{
	char *source_text;
	char *result_text;
	List *parsetree;         


	/* Validate input arguments */
	if (PG_NARGS() != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpviewreparse(): called with %hd arguments",
						PG_NARGS())));
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* 
	 * Convert input to a parsetree, and the parsetree back to text in
	 * the new format.
	 */
	source_text = TextDatumGetCString(PG_GETARG_DATUM(0));
	parsetree = (List *) stringToNode(source_text);
	result_text = nodeToString(parsetree);

	PG_RETURN_DATUM(CStringGetTextDatum(result_text));
}
