/*-------------------------------------------------------------------------
 *
 * collationcmds.c
 *	  collation-related commands support code
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/collationcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_collation_fn.h"
#include "commands/alter.h"
#include "commands/collationcmds.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/syscache.h"

static void AlterCollationOwner_internal(Relation rel, Oid collationOid,
							 Oid newOwnerId);

/*
 * CREATE COLLATION
 */
void
DefineCollation(List *names, List *parameters)
{
	char	   *collName;
	Oid			collNamespace;
	AclResult	aclresult;
	ListCell   *pl;
	DefElem    *fromEl = NULL;
	DefElem    *localeEl = NULL;
	DefElem    *lccollateEl = NULL;
	DefElem    *lcctypeEl = NULL;
	char	   *collcollate = NULL;
	char	   *collctype = NULL;
	Oid			newoid;

	collNamespace = QualifiedNameGetCreationNamespace(names, &collName);

	aclresult = pg_namespace_aclcheck(collNamespace, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(collNamespace));

	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);
		DefElem   **defelp;

		if (pg_strcasecmp(defel->defname, "from") == 0)
			defelp = &fromEl;
		else if (pg_strcasecmp(defel->defname, "locale") == 0)
			defelp = &localeEl;
		else if (pg_strcasecmp(defel->defname, "lc_collate") == 0)
			defelp = &lccollateEl;
		else if (pg_strcasecmp(defel->defname, "lc_ctype") == 0)
			defelp = &lcctypeEl;
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("collation attribute \"%s\" not recognized",
							defel->defname)));
			break;
		}

		*defelp = defel;
	}

	if ((localeEl && (lccollateEl || lcctypeEl))
		|| (fromEl && list_length(parameters) != 1))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("conflicting or redundant options")));

	if (fromEl)
	{
		Oid			collid;
		HeapTuple	tp;

		collid = get_collation_oid(defGetQualifiedName(fromEl), false);
		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation %u", collid);

		collcollate = pstrdup(NameStr(((Form_pg_collation) GETSTRUCT(tp))->collcollate));
		collctype = pstrdup(NameStr(((Form_pg_collation) GETSTRUCT(tp))->collctype));

		ReleaseSysCache(tp);
	}

	if (localeEl)
	{
		collcollate = defGetString(localeEl);
		collctype = defGetString(localeEl);
	}

	if (lccollateEl)
		collcollate = defGetString(lccollateEl);

	if (lcctypeEl)
		collctype = defGetString(lcctypeEl);

	if (!collcollate)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
			errmsg("parameter \"lc_collate\" parameter must be specified")));

	if (!collctype)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("parameter \"lc_ctype\" must be specified")));

	check_encoding_locale_matches(GetDatabaseEncoding(), collcollate, collctype);

	newoid = CollationCreate(collName,
							 collNamespace,
							 GetUserId(),
							 GetDatabaseEncoding(),
							 collcollate,
							 collctype,
							 false);

	if (!OidIsValid(newoid))
		return;

	/* check that the locales can be loaded */
	CommandCounterIncrement();
	(void) pg_newlocale_from_collation(newoid);
}

/*
 * DROP COLLATION
 */
void
DropCollationsCommand(DropStmt *drop)
{
	ObjectAddresses *objects;
	ListCell   *cell;

	/*
	 * First we identify all the collations, then we delete them in a single
	 * performMultipleDeletions() call.  This is to avoid unwanted DROP
	 * RESTRICT errors if one of the collations depends on another. (Not that
	 * that is very likely, but we may as well do this consistently.)
	 */
	objects = new_object_addresses();

	foreach(cell, drop->objects)
	{
		List	   *name = (List *) lfirst(cell);
		Oid			collationOid;
		HeapTuple	tuple;
		Form_pg_collation coll;
		ObjectAddress object;

		collationOid = get_collation_oid(name, drop->missing_ok);

		if (!OidIsValid(collationOid))
		{
			ereport(NOTICE,
					(errmsg("collation \"%s\" does not exist, skipping",
							NameListToString(name))));
			continue;
		}

		tuple = SearchSysCache1(COLLOID, ObjectIdGetDatum(collationOid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for collation %u",
				 collationOid);
		coll = (Form_pg_collation) GETSTRUCT(tuple);

		/* Permission check: must own collation or its namespace */
		if (!pg_collation_ownercheck(collationOid, GetUserId()) &&
			!pg_namespace_ownercheck(coll->collnamespace, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_COLLATION,
						   NameStr(coll->collname));

		object.classId = CollationRelationId;
		object.objectId = collationOid;
		object.objectSubId = 0;

		add_exact_object_address(&object, objects);

		ReleaseSysCache(tuple);
	}

	performMultipleDeletions(objects, drop->behavior);

	free_object_addresses(objects);
}

/*
 * Rename collation
 */
void
RenameCollation(List *name, const char *newname)
{
	Oid			collationOid;
	Oid			namespaceOid;
	HeapTuple	tup;
	Relation	rel;
	AclResult	aclresult;

	rel = heap_open(CollationRelationId, RowExclusiveLock);

	collationOid = get_collation_oid(name, false);

	tup = SearchSysCacheCopy1(COLLOID, ObjectIdGetDatum(collationOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for collation %u", collationOid);

	namespaceOid = ((Form_pg_collation) GETSTRUCT(tup))->collnamespace;

	/* make sure the new name doesn't exist */
	if (SearchSysCacheExists3(COLLNAMEENCNSP,
							  CStringGetDatum(newname),
							  Int32GetDatum(GetDatabaseEncoding()),
							  ObjectIdGetDatum(namespaceOid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("collation \"%s\" for encoding \"%s\" already exists in schema \"%s\"",
						newname,
						GetDatabaseEncodingName(),
						get_namespace_name(namespaceOid))));

	/* mustn't match an any-encoding entry, either */
	if (SearchSysCacheExists3(COLLNAMEENCNSP,
							  CStringGetDatum(newname),
							  Int32GetDatum(-1),
							  ObjectIdGetDatum(namespaceOid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("collation \"%s\" already exists in schema \"%s\"",
						newname,
						get_namespace_name(namespaceOid))));

	/* must be owner */
	if (!pg_collation_ownercheck(collationOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_COLLATION,
					   NameListToString(name));

	/* must have CREATE privilege on namespace */
	aclresult = pg_namespace_aclcheck(namespaceOid, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(namespaceOid));

	/* rename */
	namestrcpy(&(((Form_pg_collation) GETSTRUCT(tup))->collname), newname);
	simple_heap_update(rel, &tup->t_self, tup);
	CatalogUpdateIndexes(rel, tup);

	heap_freetuple(tup);

	heap_close(rel, RowExclusiveLock);
}

/*
 * Change collation owner, by name
 */
void
AlterCollationOwner(List *name, Oid newOwnerId)
{
	Oid			collationOid;
	Relation	rel;

	rel = heap_open(CollationRelationId, RowExclusiveLock);

	collationOid = get_collation_oid(name, false);

	AlterCollationOwner_internal(rel, collationOid, newOwnerId);

	heap_close(rel, RowExclusiveLock);
}

/*
 * Change collation owner, by oid
 */
void
AlterCollationOwner_oid(Oid collationOid, Oid newOwnerId)
{
	Relation	rel;

	rel = heap_open(CollationRelationId, RowExclusiveLock);

	AlterCollationOwner_internal(rel, collationOid, newOwnerId);

	heap_close(rel, RowExclusiveLock);
}

/*
 * AlterCollationOwner_internal
 *
 * Internal routine for changing the owner.  rel must be pg_collation, already
 * open and suitably locked; it will not be closed.
 */
static void
AlterCollationOwner_internal(Relation rel, Oid collationOid, Oid newOwnerId)
{
	Form_pg_collation collForm;
	HeapTuple	tup;

	Assert(RelationGetRelid(rel) == CollationRelationId);

	tup = SearchSysCacheCopy1(COLLOID, ObjectIdGetDatum(collationOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for collation %u", collationOid);

	collForm = (Form_pg_collation) GETSTRUCT(tup);

	/*
	 * If the new owner is the same as the existing owner, consider the
	 * command to have succeeded.  This is for dump restoration purposes.
	 */
	if (collForm->collowner != newOwnerId)
	{
		AclResult	aclresult;

		/* Superusers can always do it */
		if (!superuser())
		{
			/* Otherwise, must be owner of the existing object */
			if (!pg_collation_ownercheck(HeapTupleGetOid(tup), GetUserId()))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_COLLATION,
							   NameStr(collForm->collname));

			/* Must be able to become new owner */
			check_is_member_of_role(GetUserId(), newOwnerId);

			/* New owner must have CREATE privilege on namespace */
			aclresult = pg_namespace_aclcheck(collForm->collnamespace,
											  newOwnerId,
											  ACL_CREATE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
							   get_namespace_name(collForm->collnamespace));
		}

		/*
		 * Modify the owner --- okay to scribble on tup because it's a copy
		 */
		collForm->collowner = newOwnerId;

		simple_heap_update(rel, &tup->t_self, tup);

		CatalogUpdateIndexes(rel, tup);

		/* Update owner dependency reference */
		changeDependencyOnOwner(CollationRelationId, collationOid,
								newOwnerId);
	}

	heap_freetuple(tup);
}

/*
 * Execute ALTER COLLATION SET SCHEMA
 */
void
AlterCollationNamespace(List *name, const char *newschema)
{
	Oid			collOid,
				nspOid;

	collOid = get_collation_oid(name, false);

	nspOid = LookupCreationNamespace(newschema);

	AlterCollationNamespace_oid(collOid, nspOid);
}

/*
 * Change collation schema, by oid
 */
Oid
AlterCollationNamespace_oid(Oid collOid, Oid newNspOid)
{
	Oid			oldNspOid;
	Relation	rel;
	char	   *collation_name;

	rel = heap_open(CollationRelationId, RowExclusiveLock);

	/*
	 * We have to check for name collision ourselves, because
	 * AlterObjectNamespace doesn't know how to deal with the encoding
	 * considerations.
	 */
	collation_name = get_collation_name(collOid);
	if (!collation_name)
		elog(ERROR, "cache lookup failed for collation %u", collOid);

	/* make sure the name doesn't already exist in new schema */
	if (SearchSysCacheExists3(COLLNAMEENCNSP,
							  CStringGetDatum(collation_name),
							  Int32GetDatum(GetDatabaseEncoding()),
							  ObjectIdGetDatum(newNspOid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("collation \"%s\" for encoding \"%s\" already exists in schema \"%s\"",
						collation_name,
						GetDatabaseEncodingName(),
						get_namespace_name(newNspOid))));

	/* mustn't match an any-encoding entry, either */
	if (SearchSysCacheExists3(COLLNAMEENCNSP,
							  CStringGetDatum(collation_name),
							  Int32GetDatum(-1),
							  ObjectIdGetDatum(newNspOid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("collation \"%s\" already exists in schema \"%s\"",
						collation_name,
						get_namespace_name(newNspOid))));

	/* OK, do the work */
	oldNspOid = AlterObjectNamespace(rel, COLLOID, -1,
									 collOid, newNspOid,
									 Anum_pg_collation_collname,
									 Anum_pg_collation_collnamespace,
									 Anum_pg_collation_collowner,
									 ACL_KIND_COLLATION);

	heap_close(rel, RowExclusiveLock);

	return oldNspOid;
}


/*
 * "Normalize" a locale name, stripping off encoding tags such as
 * ".utf8" (e.g., "en_US.utf8" -> "en_US", but "br_FR.iso885915@euro"
 * -> "br_FR@euro").  Return true if a new, different name was
 * generated.
 */
pg_attribute_unused()
static bool
normalize_locale_name(char *new, const char *old)
{
	char	   *n = new;
	const char *o = old;
	bool		changed = false;

	while (*o)
	{
		if (*o == '.')
		{
			/* skip over encoding tag such as ".utf8" or ".UTF-8" */
			o++;
			while ((*o >= 'A' && *o <= 'Z')
				   || (*o >= 'a' && *o <= 'z')
				   || (*o >= '0' && *o <= '9')
				   || (*o == '-'))
				o++;
			changed = true;
		}
		else
			*n++ = *o++;
	}
	*n = '\0';

	return changed;
}


Datum
pg_import_system_collations(PG_FUNCTION_ARGS)
{
#if defined(HAVE_LOCALE_T) && !defined(WIN32)
	bool		if_not_exists = PG_GETARG_BOOL(0);
	Oid         nspid = PG_GETARG_OID(1);

	FILE	   *locale_a_handle;
	char		localebuf[NAMEDATALEN]; /* we assume ASCII so this is fine */
	int			count = 0;
#endif

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to import system collations"))));

#if defined(HAVE_LOCALE_T) && !defined(WIN32)
	//GPDB_93_MERGE_FIXME: put back use of OpenPipeStream after it has been merged in
	locale_a_handle = popen("locale -a", "r");
	if (locale_a_handle == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						"locale -a")));

	while (fgets(localebuf, sizeof(localebuf), locale_a_handle))
	{
		int			i;
		size_t		len;
		int			enc;
		bool		skip;
		char		alias[NAMEDATALEN];

		len = strlen(localebuf);

		if (len == 0 || localebuf[len - 1] != '\n')
		{
			elog(DEBUG1, "locale name too long, skipped: \"%s\"", localebuf);
			continue;
		}
		localebuf[len - 1] = '\0';

		/*
		 * Some systems have locale names that don't consist entirely of ASCII
		 * letters (such as "bokm&aring;l" or "fran&ccedil;ais").  This is
		 * pretty silly, since we need the locale itself to interpret the
		 * non-ASCII characters. We can't do much with those, so we filter
		 * them out.
		 */
		skip = false;
		for (i = 0; i < len; i++)
		{
			if (IS_HIGHBIT_SET(localebuf[i]))
			{
				skip = true;
				break;
			}
		}
		if (skip)
		{
			elog(DEBUG1, "locale name has non-ASCII characters, skipped: \"%s\"", localebuf);
			continue;
		}

		enc = pg_get_encoding_from_locale(localebuf, false);
		if (enc < 0)
		{
			/* error message printed by pg_get_encoding_from_locale() */
			continue;
		}
		if (!PG_VALID_BE_ENCODING(enc))
			continue;			/* ignore locales for client-only encodings */
		if (enc == PG_SQL_ASCII)
			continue;			/* C/POSIX are already in the catalog */

		count++;

		CollationCreate(localebuf, nspid, GetUserId(), enc,
						localebuf, localebuf, if_not_exists);

		CommandCounterIncrement();

		/*
		 * Generate aliases such as "en_US" in addition to "en_US.utf8" for
		 * ease of use.  Note that collation names are unique per encoding
		 * only, so this doesn't clash with "en_US" for LATIN1, say.
		 *
		 * This always runs in "if not exists" mode, to skip aliases that
		 * conflict with an existing locale name for the same encoding.  For
		 * example, "br_FR.iso88591" is normalized to "br_FR", both for
		 * encoding LATIN1.  But the unnormalized locale "br_FR" already
		 * exists for LATIN1.
		 */
		if (normalize_locale_name(alias, localebuf))
		{
			CollationCreate(alias, nspid, GetUserId(), enc,
							localebuf, localebuf, true);
			CommandCounterIncrement();
		}
	}

	//GPDB_93_MERGE_FIXME: put back use of ClosePipestream after it has been merged in
	pclose(locale_a_handle);

	if (count == 0)
		ereport(ERROR,
				(errmsg("no usable system locales were found")));
#endif	/* not HAVE_LOCALE_T && not WIN32 */

	PG_RETURN_VOID();
}
