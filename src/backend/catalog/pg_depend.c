/*-------------------------------------------------------------------------
 *
 * pg_depend.c
 *	  routines to support manipulation of the pg_depend relation
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/catalog/pg_depend.c,v 1.23 2006/10/04 00:29:50 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/tqual.h"


static bool isObjectPinned(const ObjectAddress *object, Relation rel);


/*
 * Record a dependency between 2 objects via their respective objectAddress.
 * The first argument is the dependent object, the second the one it
 * references.
 *
 * This simply creates an entry in pg_depend, without any other processing.
 */
void
recordDependencyOn(const ObjectAddress *depender,
				   const ObjectAddress *referenced,
				   DependencyType behavior)
{
	recordMultipleDependencies(depender, referenced, 1, behavior);
}

/*
 * Record multiple dependencies (of the same kind) for a single dependent
 * object.	This has a little less overhead than recording each separately.
 */
void
recordMultipleDependencies(const ObjectAddress *depender,
						   const ObjectAddress *referenced,
						   int nreferenced,
						   DependencyType behavior)
{
	Relation	dependDesc;
	HeapTuple	tup;
	int			i;
	bool		nulls[Natts_pg_depend];
	Datum		values[Natts_pg_depend];
	cqContext	cqc;
	cqContext  *pcqCtx;

	if (nreferenced <= 0)
		return;					/* nothing to do */

	/*
	 * During bootstrap, do nothing since pg_depend may not exist yet. initdb
	 * will fill in appropriate pg_depend entries after bootstrap.
	 */
	if (IsBootstrapProcessingMode())
		return;

	dependDesc = heap_open(DependRelationId, RowExclusiveLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), dependDesc),
			cql("INSERT INTO pg_depend ",
				NULL));

	memset(nulls, false, sizeof(nulls));

	for (i = 0; i < nreferenced; i++, referenced++)
	{
		/*
		 * If the referenced object is pinned by the system, there's no real
		 * need to record dependencies on it.  This saves lots of space in
		 * pg_depend, so it's worth the time taken to check.
		 */
		if (!isObjectPinned(referenced, dependDesc))
		{
			/*
			 * Record the Dependency.  Note we don't bother to check for
			 * duplicate dependencies; there's no harm in them.
			 */
			values[Anum_pg_depend_classid - 1] = ObjectIdGetDatum(depender->classId);
			values[Anum_pg_depend_objid - 1] = ObjectIdGetDatum(depender->objectId);
			values[Anum_pg_depend_objsubid - 1] = Int32GetDatum(depender->objectSubId);

			values[Anum_pg_depend_refclassid - 1] = ObjectIdGetDatum(referenced->classId);
			values[Anum_pg_depend_refobjid - 1] = ObjectIdGetDatum(referenced->objectId);
			values[Anum_pg_depend_refobjsubid - 1] = Int32GetDatum(referenced->objectSubId);

			values[Anum_pg_depend_deptype - 1] = CharGetDatum((char) behavior);

			tup = caql_form_tuple(pcqCtx, values, nulls);

			caql_insert(pcqCtx, tup);
			/* and Update indexes (implicit) */

			heap_freetuple(tup);
		}
	}

	caql_endscan(pcqCtx);

	heap_close(dependDesc, RowExclusiveLock);
}

/*
 * deleteDependencyRecordsFor -- delete all records with given depender
 * classId/objectId.  Returns the number of records deleted.
 *
 * This is used when redefining an existing object.  Links leading to the
 * object do not change, and links leading from it will be recreated
 * (possibly with some differences from before).
 */
long
deleteDependencyRecordsFor(Oid classId, Oid objectId)
{
	long		count = 0;

	count = caql_getcount(
			NULL,
			cql("DELETE FROM pg_depend "
				" WHERE classid = :1 "
				" AND objid = :2 ",
				ObjectIdGetDatum(classId),
				ObjectIdGetDatum(objectId)));

	return count;
}

/*
 * Adjust dependency record(s) to point to a different object of the same type
 *
 * classId/objectId specify the referencing object.
 * refClassId/oldRefObjectId specify the old referenced object.
 * newRefObjectId is the new referenced object (must be of class refClassId).
 *
 * Note the lack of objsubid parameters.  If there are subobject references
 * they will all be readjusted.
 *
 * Returns the number of records updated.
 */
long
changeDependencyFor(Oid classId, Oid objectId,
					Oid refClassId, Oid oldRefObjectId,
					Oid newRefObjectId)
{
	long		count = 0;
	Relation	depRel;
	cqContext  *pcqCtx;
	cqContext	cqc;
	HeapTuple	tup;
	ObjectAddress objAddr;
	bool		newIsPinned;

	depRel = heap_open(DependRelationId, RowExclusiveLock);

	/*
	 * If oldRefObjectId is pinned, there won't be any dependency entries on
	 * it --- we can't cope in that case.  (This isn't really worth expending
	 * code to fix, in current usage; it just means you can't rename stuff out
	 * of pg_catalog, which would likely be a bad move anyway.)
	 */
	objAddr.classId = refClassId;
	objAddr.objectId = oldRefObjectId;
	objAddr.objectSubId = 0;

	if (isObjectPinned(&objAddr, depRel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("cannot remove dependency on %s because it is a system object",
			   getObjectDescription(&objAddr))));

	/*
	 * We can handle adding a dependency on something pinned, though, since
	 * that just means deleting the dependency entry.
	 */
	objAddr.objectId = newRefObjectId;

	newIsPinned = isObjectPinned(&objAddr, depRel);

	/* Now search for dependency records */
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), depRel),
			cql("SELECT * FROM pg_depend "
				" WHERE classid = :1 "
				" AND objid = :2 "
				" FOR UPDATE ",
				ObjectIdGetDatum(classId),
				ObjectIdGetDatum(objectId)));

	while (HeapTupleIsValid((tup = caql_getnext(pcqCtx))))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == refClassId &&
			depform->refobjid == oldRefObjectId)
		{
			if (newIsPinned)
				caql_delete_current(pcqCtx);
			else
			{
				/* make a modifiable copy */
				tup = heap_copytuple(tup);
				depform = (Form_pg_depend) GETSTRUCT(tup);

				depform->refobjid = newRefObjectId;

				caql_update_current(pcqCtx, tup);

				heap_freetuple(tup);
			}

			count++;
		}
	}

	caql_endscan(pcqCtx);

	heap_close(depRel, RowExclusiveLock);

	return count;
}

/*
 * isObjectPinned()
 *
 * Test if an object is required for basic database functionality.
 * Caller must already have opened pg_depend.
 *
 * The passed subId, if any, is ignored; we assume that only whole objects
 * are pinned (and that this implies pinning their components).
 */
static bool
isObjectPinned(const ObjectAddress *object, Relation rel)
{
	bool		ret = false;
	cqContext	cqc;
	HeapTuple	tup;

	tup = caql_getfirst(
			caql_addrel(cqclr(&cqc), rel),
			cql("SELECT * FROM pg_depend "
				" WHERE refclassid = :1 "
				" AND refobjid = :2 ",
				ObjectIdGetDatum(object->classId),
				ObjectIdGetDatum(object->objectId)));

	/*
	 * Since we won't generate additional pg_depend entries for pinned
	 * objects, there can be at most one entry referencing a pinned object.
	 * Hence, it's sufficient to look at the first returned tuple; we don't
	 * need to loop.
	 */

	if (HeapTupleIsValid(tup))
	{
		Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);

		if (foundDep->deptype == DEPENDENCY_PIN)
			ret = true;
	}

	return ret;
}


/*
 * Various special-purpose lookups and manipulations of pg_depend.
 */


/*
 * Detect whether a sequence is marked as "owned" by a column
 *
 * An ownership marker is an AUTO dependency from the sequence to the
 * column.	If we find one, store the identity of the owning column
 * into *tableId and *colId and return TRUE; else return FALSE.
 *
 * Note: if there's more than one such pg_depend entry then you get
 * a random one of them returned into the out parameters.  This should
 * not happen, though.
 */
bool
sequenceIsOwned(Oid seqId, Oid *tableId, int32 *colId)
{
	bool		ret = false;
	cqContext  *pcqCtx;
	HeapTuple	tup;


	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_depend "
				" WHERE classid = :1 "
				" AND objid = :2 ",
				ObjectIdGetDatum(RelationRelationId),
				ObjectIdGetDatum(seqId)));

	while (HeapTupleIsValid((tup = caql_getnext(pcqCtx))))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == RelationRelationId &&
			depform->deptype == DEPENDENCY_AUTO)
		{
			*tableId = depform->refobjid;
			*colId = depform->refobjsubid;
			ret = true;
			break;				/* no need to keep scanning */
		}
	}

	caql_endscan(pcqCtx);

	return ret;
}

/*
 * Remove any existing "owned" markers for the specified sequence.
 *
 * Note: we don't provide a special function to install an "owned"
 * marker; just use recordDependencyOn().
 */
void
markSequenceUnowned(Oid seqId)
{
	cqContext  *pcqCtx;
	HeapTuple	tup;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_depend "
				" WHERE classid = :1 "
				" AND objid = :2 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationRelationId),
				ObjectIdGetDatum(seqId)));

	while (HeapTupleIsValid((tup = caql_getnext(pcqCtx))))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == RelationRelationId &&
			depform->deptype == DEPENDENCY_AUTO)
		{
			caql_delete_current(pcqCtx);
		}
	}

	caql_endscan(pcqCtx);
}


/*
 * get_constraint_index
 *		Given the OID of a unique or primary-key constraint, return the
 *		OID of the underlying unique index.
 *
 * Return InvalidOid if the index couldn't be found; this suggests the
 * given OID is bogus, but we leave it to caller to decide what to do.
 */
Oid
get_constraint_index(Oid constraintId)
{
	Oid			indexId = InvalidOid;
	HeapTuple	tup;
	cqContext  *pcqCtx;

	/* Search the dependency table for the dependent index */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_depend "
				" WHERE refclassid = :1 "
				" AND refobjid = :2 "
				" AND refobjsubid = :3 ",
				ObjectIdGetDatum(ConstraintRelationId),
				ObjectIdGetDatum(constraintId),
				Int32GetDatum(0)));

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		/*
		 * We assume any internal dependency of an index on the constraint
		 * must be what we are looking for.  (The relkind test is just
		 * paranoia; there shouldn't be any such dependencies otherwise.)
		 */
		if (deprec->classid == RelationRelationId &&
			deprec->objsubid == 0 &&
			deprec->deptype == DEPENDENCY_INTERNAL &&
			get_rel_relkind(deprec->objid) == RELKIND_INDEX)
		{
			indexId = deprec->objid;
			break;
		}
	}

	caql_endscan(pcqCtx);

	return indexId;
}

/*
 * get_index_constraint
 *		Given the OID of an index, return the OID of the owning unique or
 *		primary-key constraint, or InvalidOid if no such constraint.
 */
Oid
get_index_constraint(Oid indexId)
{
	Oid			constraintId = InvalidOid;
	HeapTuple	tup;
	cqContext  *pcqCtx;

	/* Search the dependency table for the index */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_depend "
				" WHERE classid = :1 "
				" AND objid = :2 "
				" AND objsubid = :3 ",
				ObjectIdGetDatum(RelationRelationId),
				ObjectIdGetDatum(indexId),
				Int32GetDatum(0)));

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		/*
		 * We assume any internal dependency on a constraint must be what we
		 * are looking for.
		 */
		if (deprec->refclassid == ConstraintRelationId &&
			deprec->refobjsubid == 0 &&
			deprec->deptype == DEPENDENCY_INTERNAL)
		{
			constraintId = deprec->refobjid;
			break;
		}
	}

	caql_endscan(pcqCtx);

	return constraintId;
}
