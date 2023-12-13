#include "postgres.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "access/heapam.h"
#include "catalog/pg_tablespace.h"
#include "catalog/dependency.h"
#include "catalog/catalog.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/objectaccess.h"
#include "catalog/heap.h"
#include "commands/tablespace.h"
#include "commands/comment.h"
#include "commands/seclabel.h"
#include "postmaster/bgwriter.h"
#include "miscadmin.h"
#include "foreign/foreign.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"

#include "dfs_tablespace.h"
#include "dfs_utils.h"
#include "ufs.h"

typedef struct TableSpaceCacheEntry
{
	Oid					  oid;   /* lookup key - must be first */
	DfsTableSpaceOptions *options;  /* options, or NULL if none */
} TableSpaceCacheEntry;

static void validateDfsTablespaceOption(const char *value);

static relopt_kind dfsTablespaceOptionKind;
static relopt_parse_elt dfsTablespaceOptionTable[3];

/* Hash table for information about each tablespace */
static HTAB *TableSpaceCacheHash = NULL;

static void
validateDfsTablespaceOption(const char *value)
{
	static bool initialized = false;

	if (!initialized)
	{
		initialized = true;
		return;
	}

	if (value == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for \"server\" option"),
				 errdetail("Valid value must not be null.")));
}

static void
invalidateTableSpaceCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	TableSpaceCacheEntry *spc;

	hash_seq_init(&status, TableSpaceCacheHash);
	while ((spc = (TableSpaceCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		if (spc->options)
			pfree(spc->options);
		if (hash_search(TableSpaceCacheHash,
						(void *) &spc->oid,
						HASH_REMOVE,
						NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}
}

static void
initializeTableSpaceCache(void)
{
	HASHCTL		ctl;

	/* Initialize the hash table. */
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(TableSpaceCacheEntry);
	TableSpaceCacheHash =
		hash_create("TableSpace cache", 16, &ctl,
					HASH_ELEM | HASH_BLOBS);

	/* Make sure we've initialized CacheMemoryContext. */
	if (!CacheMemoryContext)
		CreateCacheMemoryContext();

	/* Watch for invalidation events. */
	CacheRegisterSyscacheCallback(TABLESPACEOID,
								  invalidateTableSpaceCacheCallback,
								  (Datum) 0);
}

static bytea *
dfsTablespaceOptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"stage", RELOPT_TYPE_BOOL, offsetof(DfsTableSpaceOptions, stage)},
		{"server", RELOPT_TYPE_STRING, offsetof(DfsTableSpaceOptions, serverOffset)},
		{"prefix", RELOPT_TYPE_STRING, offsetof(DfsTableSpaceOptions, prefixOffset)}
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  dfsTablespaceOptionKind,
									  sizeof(DfsTableSpaceOptions),
									  tab, lengthof(tab));
}

static Datum
transformTablespaceOptions(Datum withOptions, char *location)
{
	Datum	   *withDatums = NULL;
	Datum		d;
	int			i,
				nWithOpts = 0;
	ArrayType  *withArr;
	ArrayBuildState *astate = NULL;

	withArr = DatumGetArrayTypeP(withOptions);
	Assert(ARR_ELEMTYPE(withArr) == TEXTOID);

	deconstruct_array(withArr, TEXTOID, -1, false, 'i', &withDatums, NULL, &nWithOpts);

	for (i = 0; i < nWithOpts; ++i)
		astate = accumArrayResult(astate, withDatums[i], false, TEXTOID, CurrentMemoryContext);

	if (location)
	{
		d = CStringGetTextDatum(psprintf("%s=%s", "prefix", location));
		astate = accumArrayResult(astate, d, false, TEXTOID, CurrentMemoryContext);
	}

	return makeArrayResult(astate, CurrentMemoryContext);
}

static TableSpaceCacheEntry *
getTablespaceEntry(Oid spcid)
{
	TableSpaceCacheEntry *spc;
	HeapTuple	tp;
	DfsTableSpaceOptions *options;

	/*
	 * Since spcid is always from a pg_class tuple, InvalidOid implies the
	 * default.
	 */
	if (spcid == InvalidOid)
		spcid = MyDatabaseTableSpace;

	/* Find existing cache entry, if any. */
	if (!TableSpaceCacheHash)
		initializeTableSpaceCache();

	spc = (TableSpaceCacheEntry *) hash_search(TableSpaceCacheHash,
											   (void *) &spcid,
											   HASH_FIND,
											   NULL);
	if (spc)
		return spc;

	/*
	 * Not found in TableSpace cache.  Check catcache.  If we don't find a
	 * valid HeapTuple, it must mean someone has managed to request tablespace
	 * details for a non-existent tablespace.  We'll just treat that case as
	 * if no options were specified.
	 */
	tp = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcid));
	if (!HeapTupleIsValid(tp))
		options = NULL;
	else
	{
		Datum		datum;
		bool		isNull;

		datum = SysCacheGetAttr(TABLESPACEOID,
								tp,
								Anum_pg_tablespace_spcoptions,
								&isNull);
		if (isNull)
			options = NULL;
		else
		{
			bytea	   *bytea_opts = dfsTablespaceOptions(datum, false);

			options = MemoryContextAlloc(CacheMemoryContext, VARSIZE(bytea_opts));
			memcpy(options, bytea_opts, VARSIZE(bytea_opts));
		}
		ReleaseSysCache(tp);
	}

	/*
	 * Now create the cache entry.  It's important to do this only after
	 * reading the pg_tablespace entry, since doing so could cause a cache
	 * flush.
	 */
	spc = (TableSpaceCacheEntry *) hash_search(TableSpaceCacheHash,
											   (void *) &spcid,
											   HASH_ENTER,
											   NULL);
	spc->options = options;
	return spc;
}

static bool
isDfsTableSpaceStmt(CreateTableSpaceStmt *stmt)
{
	ListCell   *option;

	foreach(option, stmt->options)
	{
		DefElem	   *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "server") == 0)
			return true;
	}

	return false;
}

static bool
isDfsTablespace(const char *name)
{
	return IsDfsTablespace(get_tablespace_oid(name, false));
}

static Oid
dfsCreateTableSpace(CreateTableSpaceStmt *stmt)
{
	Relation	rel;
	Datum		values[Natts_pg_tablespace];
	bool		nulls[Natts_pg_tablespace];
	HeapTuple	tuple;
	Oid			tablespaceoid;
	char	   *location;
	Oid			ownerId;
	ListCell   *option;
	Datum		newOptions;
	ObjectAddress   myself;
	ObjectAddress   referenced;
	ForeignServer  *server;
	DfsTableSpaceOptions *stdOptions;

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create tablespace \"%s\"",
						stmt->tablespacename),
				 errhint("Must be superuser to create a tablespace.")));

	/* However, the eventual owner of the tablespace need not be */
	if (stmt->owner)
		ownerId = get_rolespec_oid(stmt->owner, false);
	else
		ownerId = GetUserId();

	foreach(option, stmt->options)
	{
		DefElem	   *defel = (DefElem *) lfirst(option);

		/* Segment content ID specific locations */
		if (strlen(defel->defname) > strlen("content") &&
			strncmp(defel->defname, "content", strlen("content")) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("parameter \"content_id\" is not supported on dfs tablespace")));
	}

	location = pstrdup(stmt->location);

	/* Unix-ify the offered path, and strip any trailing slashes */
	canonicalize_path(location);

	/*
	 * Allowing relative paths seems risky
	 *
	 * This also helps us ensure that location is not empty or whitespace,
	 * unless specifying a developer-only in-place tablespace.
	 */
	if (!is_absolute_path(location))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("tablespace location must be an absolute path")));

	if (location[1] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("root directory can't be used as location")));

	if (strlen(location) > MAX_DFS_PATH_SIZE - NAMEDATALEN - 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("tablespace location \"%s\" is too long",
						location)));

	/*
	 * Disallow creation of tablespaces named "pg_xxx"; we reserve this
	 * namespace for system purposes.
	 */
	if (!allowSystemTableMods && IsReservedName(stmt->tablespacename))
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("unacceptable tablespace name \"%s\"",
						stmt->tablespacename),
				 errdetail("The prefix \"pg_\" is reserved for system tablespaces.")));

	/*
	 * Check that there is no other tablespace by this name.  (The unique
	 * index would catch this anyway, but might as well give a friendlier
	 * message.)
	 */
	if (OidIsValid(get_tablespace_oid(stmt->tablespacename, true)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("tablespace \"%s\" already exists",
						stmt->tablespacename)));

	/*
	 * Insert tuple into pg_tablespace.  The purpose of doing this first is to
	 * lock the proposed tablename against other would-be creators. The
	 * insertion will roll back if we find problems below.
	 */
	rel = table_open(TableSpaceRelationId, RowExclusiveLock);

	MemSet(nulls, false, sizeof(nulls));

	tablespaceoid = GetNewOidForTableSpace(rel, TablespaceOidIndexId,
										   Anum_pg_tablespace_oid,
										   stmt->tablespacename);
	values[Anum_pg_tablespace_oid - 1] = ObjectIdGetDatum(tablespaceoid);
	values[Anum_pg_tablespace_spcname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->tablespacename));
	values[Anum_pg_tablespace_spcowner - 1] =
		ObjectIdGetDatum(ownerId);
	nulls[Anum_pg_tablespace_spcacl - 1] = true;

	/* Generate new proposed spcoptions (text array) */
	newOptions = transformRelOptions((Datum) 0,
									 stmt->options,
									 NULL, NULL, false, false);
	stdOptions = (DfsTableSpaceOptions *) dfsTablespaceOptions(newOptions, true);
	newOptions = transformTablespaceOptions(newOptions, location);

	if (newOptions != (Datum) 0)
		values[Anum_pg_tablespace_spcoptions - 1] = newOptions;
	else
		nulls[Anum_pg_tablespace_spcoptions - 1] = true;

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	/* Record dependency on owner */
	recordDependencyOnOwner(TableSpaceRelationId, tablespaceoid, ownerId);

	/* Add pg_class dependency on the server */
	server = GetForeignServerByName((char *) stdOptions + stdOptions->serverOffset, false);
	myself.classId = TableSpaceRelationId;
	myself.objectId = tablespaceoid;
	myself.objectSubId = 0;

	referenced.classId = ForeignServerRelationId;
	referenced.objectId = server->serverid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* Post creation hook for new tablespace */
	InvokeObjectPostCreateHook(TableSpaceRelationId, tablespaceoid, 0);

	pfree(location);

	/* We keep the lock on pg_tablespace until commit */
	table_close(rel, NoLock);

	if (Gp_role == GP_ROLE_DISPATCH && ENABLE_DISPATCH())
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);

		/* MPP-6929: metadata tracking */
		MetaTrackAddObject(TableSpaceRelationId,
						   tablespaceoid,
						   GetUserId(),
						   "CREATE", "TABLESPACE");
	}

	return tablespaceoid;
}

static void
dfsDropTableSpace(DropTableSpaceStmt *stmt)
{
	char	   *tablespacename = stmt->tablespacename;
	TableScanDesc scandesc;
	Relation	rel;
	HeapTuple	tuple;
	Form_pg_tablespace spcform;
	ScanKeyData entry[1];
	Oid			tablespaceoid;
	char	   *detail;
	char	   *detail_log;
	ForeignServer *server;

	/*
	 * Find the target tuple
	 */
	rel = table_open(TableSpaceRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0],
				Anum_pg_tablespace_spcname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(tablespacename));
	scandesc = table_beginscan_catalog(rel, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	if (!HeapTupleIsValid(tuple))
	{
		if (!stmt->missing_ok)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tablespace \"%s\" does not exist",
							tablespacename)));
		}
		else
		{
			ereport(NOTICE,
					(errmsg("tablespace \"%s\" does not exist, skipping",
							tablespacename)));
			/* XXX I assume I need one or both of these next two calls */
			table_endscan(scandesc);
			table_close(rel, NoLock);
		}
		return;
	}

	spcform = (Form_pg_tablespace) GETSTRUCT(tuple);
	tablespaceoid = spcform->oid;

	server = GetForeignServerByName(GetDfsTablespaceServer(tablespaceoid), false);

	/* Must be tablespace owner */
	if (!pg_tablespace_ownercheck(tablespaceoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLESPACE,
					   tablespacename);

	/* Disallow drop of the standard tablespaces, even by superuser */
	if (tablespaceoid == GLOBALTABLESPACE_OID ||
		tablespaceoid == DEFAULTTABLESPACE_OID)
		aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_TABLESPACE,
					   tablespacename);

	/* Check for pg_shdepend entries depending on this tablespace */
	if (checkSharedDependencies(TableSpaceRelationId, tablespaceoid,
								&detail, &detail_log))
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("tablespace \"%s\" cannot be dropped because some objects depend on it",
						tablespacename),
				 errdetail_internal("%s", detail),
				 errdetail_log("%s", detail_log)));

	/* DROP hook for the tablespace being removed */
	InvokeObjectDropHook(TableSpaceRelationId, tablespaceoid, 0);

	/*
	 * Remove the pg_tablespace tuple (this will roll back if we fail below)
	 */
	CatalogTupleDelete(rel, &tuple->t_self);

	table_endscan(scandesc);

	/*
	 * Remove any comments or security labels on this tablespace.
	 */
	DeleteSharedComments(tablespaceoid, TableSpaceRelationId);
	DeleteSharedSecurityLabel(tablespaceoid, TableSpaceRelationId);

	/*
	 * Remove dependency on owner.
	 */
	deleteSharedDependencyRecordsFor(TableSpaceRelationId, tablespaceoid, 0);

	deleteDependencyRecordsForSpecific(TableSpaceRelationId, tablespaceoid,
									   DEPENDENCY_NORMAL,
									   ForeignServerRelationId, server->serverid);

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackDropObject(TableSpaceRelationId,
							tablespaceoid);

	/* We keep the lock on pg_tablespace until commit */
	table_close(rel, NoLock);

	/*
	 * If we are the QD, dispatch this DROP command to all the QEs
	 */
	if (Gp_role == GP_ROLE_DISPATCH && ENABLE_DISPATCH())
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									NIL,
									NULL);
	}
}

static void
validateTablespaceOptions(AlterTableSpaceOptionsStmt *stmt)
{
	int			 i;
	ListCell	*option;

	foreach(option, stmt->options)
	{
		DefElem	   *defel = (DefElem *) lfirst(option);

		for (i = 0; i < lengthof(dfsTablespaceOptionTable); i++)
		{
			if (strcmp(dfsTablespaceOptionTable[i].optname, defel->defname) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not change value for \"%s\" option", defel->defname)));
		}
	}
}

static Oid
dfsAlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt)
{
	Relation	rel;
	ScanKeyData entry[1];
	TableScanDesc scandesc;
	HeapTuple	tup;
	Oid			tablespaceoid;
	Datum		datum;
	Datum		newOptions;
	Datum		repl_val[Natts_pg_tablespace];
	bool		isnull;
	bool		repl_null[Natts_pg_tablespace];
	bool		repl_repl[Natts_pg_tablespace];
	HeapTuple	newtuple;
	DfsTableSpaceOptions *stdOptions;

	validateTablespaceOptions(stmt);

	/* Search pg_tablespace */
	rel = table_open(TableSpaceRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0],
				Anum_pg_tablespace_spcname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->tablespacename));
	scandesc = table_beginscan_catalog(rel, 1, entry);
	tup = heap_getnext(scandesc, ForwardScanDirection);
	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace \"%s\" does not exist",
						stmt->tablespacename)));

	tablespaceoid = ((Form_pg_tablespace) GETSTRUCT(tup))->oid;

	/* Must be owner of the existing object */
	if (!pg_tablespace_ownercheck(tablespaceoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLESPACE,
					   stmt->tablespacename);

	/* Generate new proposed spcoptions (text array) */
	datum = heap_getattr(tup, Anum_pg_tablespace_spcoptions,
						 RelationGetDescr(rel), &isnull);
	newOptions = transformRelOptions(isnull ? (Datum) 0 : datum,
									 stmt->options, NULL, NULL, false,
									 stmt->isReset);
	stdOptions = (DfsTableSpaceOptions *) dfsTablespaceOptions(newOptions, false);
	newOptions = transformTablespaceOptions(newOptions, NULL);

	/* Build new tuple. */
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));
	if (newOptions != (Datum) 0)
		repl_val[Anum_pg_tablespace_spcoptions - 1] = newOptions;
	else
		repl_null[Anum_pg_tablespace_spcoptions - 1] = true;
	repl_repl[Anum_pg_tablespace_spcoptions - 1] = true;
	newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), repl_val,
								 repl_null, repl_repl);

	/* Update system catalog. */
	CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);

	InvokeObjectPostAlterHook(TableSpaceRelationId, tablespaceoid, 0);

	heap_freetuple(newtuple);

	/* Conclude heap scan. */
	table_endscan(scandesc);
	table_close(rel, NoLock);

	if (Gp_role == GP_ROLE_DISPATCH && ENABLE_DISPATCH())
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
	}

	return tablespaceoid;
}

void
dfsInitializeReloptions(void)
{
	dfsTablespaceOptionKind = add_reloption_kind();

	add_bool_reloption(dfsTablespaceOptionKind,
					   "stage",
					   "Specify whether the dfs tablespace is staged",
					   false,
					   AccessExclusiveLock);
	dfsTablespaceOptionTable[0].optname = "stage";
	dfsTablespaceOptionTable[0].opttype = RELOPT_TYPE_BOOL;
	dfsTablespaceOptionTable[0].offset = offsetof(DfsTableSpaceOptions, stage);

	add_string_reloption(dfsTablespaceOptionKind,
						 "server",
						 "Specify the server used by the dfs tablespace",
						 NULL,
						 validateDfsTablespaceOption,
						 AccessExclusiveLock);
	dfsTablespaceOptionTable[1].optname = "server";
	dfsTablespaceOptionTable[1].opttype = RELOPT_TYPE_STRING;
	dfsTablespaceOptionTable[1].offset = offsetof(DfsTableSpaceOptions, serverOffset);

	add_string_reloption(dfsTablespaceOptionKind,
						 "prefix",
						 "Specify the prefix of the dfs tablespace",
						 NULL,
						 NULL,
						 AccessExclusiveLock);
	dfsTablespaceOptionTable[2].optname = "prefix";
	dfsTablespaceOptionTable[2].opttype = RELOPT_TYPE_STRING;
	dfsTablespaceOptionTable[2].offset = offsetof(DfsTableSpaceOptions, prefixOffset);
}

const char *
GetDfsTablespaceServer(Oid id)
{
	TableSpaceCacheEntry *spc = getTablespaceEntry(id);

	if (!spc->options)
		return NULL;

	if (spc->options->serverOffset == 0)
		return NULL;

	return (char *) spc->options + spc->options->serverOffset;
}

const char *
GetDfsTablespacePath(Oid id)
{
	TableSpaceCacheEntry *spc = getTablespaceEntry(id);

	if (!spc->options)
		return NULL;

	if (spc->options->prefixOffset == 0)
		return NULL;

	return (char *) spc->options + spc->options->prefixOffset;
}

bool
IsDfsTablespace(Oid id)
{
	return GetDfsTablespaceServer(id) != NULL;
}

Oid
DfsCreateTableSpace(CreateTableSpaceStmt *stmt)
{
	if (isDfsTableSpaceStmt(stmt))
		return dfsCreateTableSpace(stmt);

	return CreateTableSpace(stmt);
}

void
DfsDropTableSpace(DropTableSpaceStmt *stmt)
{
	if (isDfsTablespace(stmt->tablespacename))
	{
		dfsDropTableSpace(stmt);
		return;
	}

	DropTableSpace(stmt);
}

Oid
DfsAlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt)
{
	if (isDfsTablespace(stmt->tablespacename))
		return dfsAlterTableSpaceOptions(stmt);

	return AlterTableSpaceOptions(stmt);
}
