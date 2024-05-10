#include "postgres.h"

#include "fmgr.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/heapam.h"
#include "catalog/pg_tablespace.h"
#include "catalog/dependency.h"
#include "catalog/catalog.h"
#include "catalog/oid_dispatch.h"
#include "catalog/gp_storage_server.h"
#include "catalog/objectaccess.h"
#include "catalog/heap.h"
#include "commands/tablespace.h"
#include "commands/comment.h"
#include "commands/seclabel.h"
#include "postmaster/bgwriter.h"
#include "miscadmin.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "access/htup.h"
#include "catalog/objectaddress.h"
#include "commands/defrem.h"
#include "commands/storagecmds.h"
#include "nodes/parsenodes.h"
#include "utils/acl.h"
#include "utils/relcache.h"
#include "utils/spccache.h"
#include "utils/varlena.h"

#include "dfs_tablespace.h"
#include "remotefile_connection.h"

static void
validateDfsTablespaceOptions(AlterTableSpaceOptionsStmt *stmt)
{
	ListCell *option;

	foreach(option, stmt->options)
	{
		DefElem *defel = (DefElem *) lfirst(option);

		if (pg_strcasecmp("stage", defel->defname) == 0 ||
			pg_strcasecmp("server", defel->defname) == 0 ||
			pg_strcasecmp("path", defel->defname) == 0)
		{
			ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not change value for \"%s\" option", defel->defname)));
		}
	}
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
	char 	*fileHandler = NULL;
	StorageServer  *server = NULL;
	ListCell	*lc = NULL;
	char 	*serverName = NULL;

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
	if (stmt->filehandler)
		fileHandler = pstrdup(stmt->filehandler);

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
	 * Check that there is no other tablespace by this name. (The unique
	 * index would catch this anyway, but might as well give a friendlier
	 * message.)
	 */
	if (OidIsValid(get_tablespace_oid(stmt->tablespacename, true)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("tablespace \"%s\" already exists",
						stmt->tablespacename)));

	/*
	 * Insert tuple into pg_tablespace. The purpose of doing this first is to
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

	if (fileHandler)
	{
		List *fileHandler_list;
		char *spcfilehandlerbin = NULL;
		char *spcfilehandlersrc = NULL;

		SplitIdentifierString(fileHandler, ',', &fileHandler_list);

		spcfilehandlerbin = (char *) linitial(fileHandler_list);
		spcfilehandlersrc = (char *) lsecond(fileHandler_list);

		values[Anum_pg_tablespace_spcfilehandlerbin - 1] = CStringGetTextDatum(spcfilehandlerbin);
		values[Anum_pg_tablespace_spcfilehandlersrc - 1] = CStringGetTextDatum(spcfilehandlersrc);
		
		pfree(fileHandler);
	}
	else
	{
		nulls[Anum_pg_tablespace_spcfilehandlersrc - 1] = true;
		nulls[Anum_pg_tablespace_spcfilehandlerbin - 1] = true;
	}

	/* Generate new proposed spcoptions (text array) */
	newOptions = transformRelOptions((Datum) 0,
									 stmt->options,
									 NULL, NULL, false, false);

	foreach(lc, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "server") == 0)
		{
			serverName = defGetString(def);
		}
	}
	server = GetStorageServerByName(serverName, false);

	if (newOptions != (Datum) 0)
		values[Anum_pg_tablespace_spcoptions - 1] = newOptions;
	else
		nulls[Anum_pg_tablespace_spcoptions - 1] = true;

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	recordStorageServerDependency(TableSpaceRelationId, tablespaceoid, server->serverid);

	/* Record dependency on owner */
	recordDependencyOnOwner(TableSpaceRelationId, tablespaceoid, ownerId);

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

static bool
isCreateDfsTableSpaceStmt(CreateTableSpaceStmt *stmt)
{
	ListCell	*option;

	foreach(option, stmt->options)
	{
		DefElem	   *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "server") == 0)
			return true;
	}

	return false;
}

static bool
isDropDfsTableSpaceStmt(DropTableSpaceStmt *stmt)
{
	Relation	rel;
	TableScanDesc	scandesc;
	HeapTuple	tuple;
	ScanKeyData	entry[1];
	bool	isnull;

	/*
	 * Search pg_tablespace.  We use a heapscan here even though there is an
	 * index on name, on the theory that pg_tablespace will usually have just
	 * a few entries and so an indexed lookup is a waste of effort.
	 */
	rel = table_open(TableSpaceRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_tablespace_spcname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->tablespacename));
	scandesc = table_beginscan_catalog(rel, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	if (HeapTupleIsValid(tuple))
		heap_getattr(tuple, Anum_pg_tablespace_spcfilehandlersrc,
					 RelationGetDescr(rel), &isnull);
	else
		return false;

	table_endscan(scandesc);
	table_close(rel, AccessShareLock);

	if (isnull)
		return false;
	else
		return true;
}

Oid
DfsCreateTableSpace(CreateTableSpaceStmt *stmt)
{
	if (isCreateDfsTableSpaceStmt(stmt))
		return dfsCreateTableSpace(stmt);

	return CreateTableSpace(stmt);
}

void
DfsDropTableSpace(DropTableSpaceStmt *stmt)
{
	if (isDropDfsTableSpaceStmt(stmt))
	{
		dfsDropTableSpace(stmt);
		return;
	}
	return DropTableSpace(stmt);
}

Oid
DfsAlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt)
{
        Relation        rel;
        ScanKeyData entry[1];
        TableScanDesc scandesc;
        HeapTuple       tup;
        Oid                     tablespaceoid;
        Datum           datum;
        Datum           newOptions;
        Datum           repl_val[Natts_pg_tablespace];
        bool            isnull;
        bool            repl_null[Natts_pg_tablespace];
        bool            repl_repl[Natts_pg_tablespace];
        HeapTuple       newtuple;

	validateDfsTablespaceOptions(stmt);

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
	(void) tablespace_reloptions(newOptions, true);

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

const char *
GetDfsTablespaceServer(Oid id)
{
	TableSpaceCacheEntry *spc = get_tablespace(id);

	if (!spc->opts)
		return NULL;

	if (spc->opts->serverOffset == 0)
		return NULL;

	return pstrdup((char *) spc->opts + spc->opts->serverOffset);
}

const char *
GetDfsTablespacePath(Oid id)
{
	TableSpaceCacheEntry *spc = get_tablespace(id);

	if (!spc->opts)
		return NULL;

	if (spc->opts->pathOffset == 0)
		return NULL;

	return pstrdup((char *) spc->opts + spc->opts->pathOffset);
}

bool
IsDfsTablespaceById(Oid spcId)
{
	return GetDfsTablespaceServer(spcId) != NULL;
}
