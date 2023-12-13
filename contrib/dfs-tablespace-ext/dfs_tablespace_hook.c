#include "postgres.h"

#include "access/xact.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/dependency.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_am.h"
#include "executor/executor.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "cdb/cdbvars.h"

#include "dfs_tablespace.h"

PG_MODULE_MAGIC;

#define RELATION_IS_SUPPORTED(relkind) \
	((relkind) == RELKIND_RELATION || \
	 (relkind) == RELKIND_INDEX || \
	 (relkind) == RELKIND_MATVIEW)

/*
 * Declarations
 */
void _PG_init(void);

static bool needInitialization = true;
static object_access_hook_type prevObjectAccessHook = NULL;
static ProcessUtility_hook_type prevProcessUtilityHook = NULL;

static int
classifyUtilityCommandAsReadOnly(Node *parsetree)
{
	switch (nodeTag(parsetree))
	{
		case T_AlterCollationStmt:
		case T_AlterDatabaseSetStmt:
		case T_AlterDatabaseStmt:
		case T_AlterDefaultPrivilegesStmt:
		case T_AlterDomainStmt:
		case T_AlterEnumStmt:
		case T_AlterEventTrigStmt:
		case T_AlterExtensionContentsStmt:
		case T_AlterExtensionStmt:
		case T_AlterFdwStmt:
		case T_AlterForeignServerStmt:
		case T_AlterFunctionStmt:
		case T_AlterObjectDependsStmt:
		case T_AlterObjectSchemaStmt:
		case T_AlterOpFamilyStmt:
		case T_AlterOperatorStmt:
		case T_AlterOwnerStmt:
		case T_AlterPolicyStmt:
		case T_AlterPublicationStmt:
		case T_AlterRoleSetStmt:
		case T_AlterRoleStmt:
		case T_AlterSeqStmt:
		case T_AlterStatsStmt:
		case T_AlterSubscriptionStmt:
		case T_AlterTSConfigurationStmt:
		case T_AlterTSDictionaryStmt:
		case T_AlterTableMoveAllStmt:
		case T_AlterTableSpaceOptionsStmt:
		case T_AlterTableStmt:
		case T_AlterTypeStmt:
		case T_AlterUserMappingStmt:
		case T_CommentStmt:
		case T_CompositeTypeStmt:
		case T_CreateAmStmt:
		case T_CreateCastStmt:
		case T_CreateConversionStmt:
		case T_CreateDomainStmt:
		case T_CreateEnumStmt:
		case T_CreateEventTrigStmt:
		case T_CreateExtensionStmt:
		case T_CreateFdwStmt:
		case T_CreateForeignServerStmt:
		case T_CreateForeignTableStmt:
		case T_CreateFunctionStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_CreatePLangStmt:
		case T_CreatePolicyStmt:
		case T_CreatePublicationStmt:
		case T_CreateRangeStmt:
		case T_CreateRoleStmt:
		case T_CreateSchemaStmt:
		case T_CreateSeqStmt:
		case T_CreateStatsStmt:
		case T_CreateStmt:
		case T_CreateSubscriptionStmt:
		case T_CreateTableAsStmt:
		case T_CreateTableSpaceStmt:
		case T_CreateTransformStmt:
		case T_CreateTrigStmt:
		case T_CreateUserMappingStmt:
		case T_CreatedbStmt:
		case T_DefineStmt:
		case T_DropOwnedStmt:
		case T_DropRoleStmt:
		case T_DropStmt:
		case T_DropSubscriptionStmt:
		case T_DropTableSpaceStmt:
		case T_DropUserMappingStmt:
		case T_DropdbStmt:
		case T_GrantRoleStmt:
		case T_GrantStmt:
		case T_ImportForeignSchemaStmt:
		case T_IndexStmt:
		case T_ReassignOwnedStmt:
		case T_RefreshMatViewStmt:
		case T_RenameStmt:
		case T_RuleStmt:
		case T_SecLabelStmt:
		case T_TruncateStmt:
		case T_ViewStmt:
		/* fallthrough */
		/* GPDB specific commands */
		case T_AlterQueueStmt:
		case T_AlterResourceGroupStmt:
		case T_CreateQueueStmt:
		case T_CreateResourceGroupStmt:
		//case T_CreateTaskStmt:
		//case T_AlterTaskStmt:
		//case T_DropTaskStmt:
		case T_DropQueueStmt:
		case T_DropResourceGroupStmt:
		case T_CreateExternalStmt:
		case T_RetrieveStmt:
			{
				/* DDL is not read-only, and neither is TRUNCATE. */
				return COMMAND_IS_NOT_READ_ONLY;
			}

		case T_AlterSystemStmt:
			{
				/*
				 * Surprisingly, ALTER SYSTEM meets all our definitions of
				 * read-only: it changes nothing that affects the output of
				 * pg_dump, it doesn't write WAL or imperil the application of
				 * future WAL, and it doesn't depend on any state that needs
				 * to be synchronized with parallel workers.
				 *
				 * So, despite the fact that it writes to a file, it's read
				 * only!
				 */
				return COMMAND_IS_STRICTLY_READ_ONLY;
			}

		case T_CallStmt:
		case T_DoStmt:
			{
				/*
				 * Commands inside the DO block or the called procedure might
				 * not be read only, but they'll be checked separately when we
				 * try to execute them.  Here we only need to worry about the
				 * DO or CALL command itself.
				 */
				return COMMAND_IS_STRICTLY_READ_ONLY;
			}

		case T_CheckPointStmt:
			{
				/*
				 * You might think that this should not be permitted in
				 * recovery, but we interpret a CHECKPOINT command during
				 * recovery as a request for a restartpoint instead. We allow
				 * this since it can be a useful way of reducing switchover
				 * time when using various forms of replication.
				 */
				return COMMAND_IS_STRICTLY_READ_ONLY;
			}

		case T_ClosePortalStmt:
		case T_ConstraintsSetStmt:
		case T_DeallocateStmt:
		case T_DeclareCursorStmt:
		case T_DiscardStmt:
		case T_ExecuteStmt:
		case T_FetchStmt:
		case T_LoadStmt:
		case T_PrepareStmt:
		case T_UnlistenStmt:
		case T_VariableSetStmt:
			{
				/*
				 * These modify only backend-local state, so they're OK to run
				 * in a read-only transaction or on a standby. However, they
				 * are disallowed in parallel mode, because they either rely
				 * upon or modify backend-local state that might not be
				 * synchronized among cooperating backends.
				 */
				return COMMAND_OK_IN_RECOVERY | COMMAND_OK_IN_READ_ONLY_TXN;
			}

		case T_ClusterStmt:
		case T_ReindexStmt:
		case T_VacuumStmt:
			{
				/*
				 * These commands write WAL, so they're not strictly
				 * read-only, and running them in parallel workers isn't
				 * supported.
				 *
				 * However, they don't change the database state in a way that
				 * would affect pg_dump output, so it's fine to run them in a
				 * read-only transaction. (CLUSTER might change the order of
				 * rows on disk, which could affect the ordering of pg_dump
				 * output, but that's not semantically significant.)
				 */
				return COMMAND_OK_IN_READ_ONLY_TXN;
			}

		case T_CopyStmt:
			{
				CopyStmt   *stmt = (CopyStmt *) parsetree;

				/*
				 * You might think that COPY FROM is not at all read only, but
				 * it's OK to copy into a temporary table, because that
				 * wouldn't change the output of pg_dump.  If the target table
				 * turns out to be non-temporary, DoCopy itself will call
				 * PreventCommandIfReadOnly.
				 */
				if (stmt->is_from)
					return COMMAND_OK_IN_READ_ONLY_TXN;
				else
					return COMMAND_IS_STRICTLY_READ_ONLY;
			}

		case T_ExplainStmt:
		case T_VariableShowStmt:
			{
				/*
				 * These commands don't modify any data and are safe to run in
				 * a parallel worker.
				 */
				return COMMAND_IS_STRICTLY_READ_ONLY;
			}

		case T_ListenStmt:
		case T_NotifyStmt:
			{
				/*
				 * NOTIFY requires an XID assignment, so it can't be permitted
				 * on a standby. Perhaps LISTEN could, since without NOTIFY it
				 * would be OK to just do nothing, at least until promotion,
				 * but we currently prohibit it lest the user get the wrong
				 * idea.
				 *
				 * (We do allow T_UnlistenStmt on a standby, though, because
				 * it's a no-op.)
				 */
				return COMMAND_OK_IN_READ_ONLY_TXN;
			}

		case T_LockStmt:
			{
				LockStmt   *stmt = (LockStmt *) parsetree;

				/*
				 * Only weaker locker modes are allowed during recovery. The
				 * restrictions here must match those in
				 * LockAcquireExtended().
				 */
				if (stmt->mode > RowExclusiveLock)
					return COMMAND_OK_IN_READ_ONLY_TXN;
				else
					return COMMAND_IS_STRICTLY_READ_ONLY;
			}

		case T_TransactionStmt:
			{
				TransactionStmt *stmt = (TransactionStmt *) parsetree;

				/*
				 * PREPARE, COMMIT PREPARED, and ROLLBACK PREPARED all write
				 * WAL, so they're not read-only in the strict sense; but the
				 * first and third do not change pg_dump output, so they're OK
				 * in a read-only transactions.
				 *
				 * We also consider COMMIT PREPARED to be OK in a read-only
				 * transaction environment, by way of exception.
				 */
				switch (stmt->kind)
				{
					case TRANS_STMT_BEGIN:
					case TRANS_STMT_START:
					case TRANS_STMT_COMMIT:
					case TRANS_STMT_ROLLBACK:
					case TRANS_STMT_SAVEPOINT:
					case TRANS_STMT_RELEASE:
					case TRANS_STMT_ROLLBACK_TO:
						return COMMAND_IS_STRICTLY_READ_ONLY;

					case TRANS_STMT_PREPARE:
					case TRANS_STMT_COMMIT_PREPARED:
					case TRANS_STMT_ROLLBACK_PREPARED:
						return COMMAND_OK_IN_READ_ONLY_TXN;
				}
				elog(ERROR, "unrecognized TransactionStmtKind: %d",
					 (int) stmt->kind);
				return 0;		/* silence stupider compilers */
			}

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(parsetree));
			return 0;			/* silence stupider compilers */
	}
}

static void preventCommandIfNeed(Node *parsetree)
{
	int readonly_flags;

	/* Prohibit read/write commands in read-only states. */
	readonly_flags = classifyUtilityCommandAsReadOnly(parsetree);
	if (readonly_flags != COMMAND_IS_STRICTLY_READ_ONLY &&
		(XactReadOnly || IsInParallelMode()))
	{
		CommandTag	commandtag = CreateCommandTag(parsetree);

		if ((readonly_flags & COMMAND_OK_IN_READ_ONLY_TXN) == 0)
			PreventCommandIfReadOnly(GetCommandTagName(commandtag));
		if ((readonly_flags & COMMAND_OK_IN_PARALLEL_MODE) == 0)
			PreventCommandIfParallelMode(GetCommandTagName(commandtag));
		if ((readonly_flags & COMMAND_OK_IN_RECOVERY) == 0)
			PreventCommandDuringRecovery(GetCommandTagName(commandtag));
	}
}

static Oid
getDatabaseTablespace(Oid id)
{
	Relation	pg_database;
	ScanKeyData entry[1];
	SysScanDesc scan;
	HeapTuple	dbtuple;
	Oid			oid;

	pg_database = table_open(DatabaseRelationId, AccessShareLock);
	ScanKeyInit(&entry[0],
				Anum_pg_database_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(id));

	scan = systable_beginscan(pg_database, DatabaseOidIndexId, true,
							  SnapshotSelf, 1, entry);

	dbtuple = systable_getnext(scan);

	/* We assume that there can be at most one matching tuple */
	if (!HeapTupleIsValid(dbtuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%u\" does not exist", id)));

	oid = ((Form_pg_database) GETSTRUCT(dbtuple))->dattablespace;

	systable_endscan(scan);
	table_close(pg_database, AccessShareLock);

	return oid;
}

static void
getRelationInfo(Oid relId, Oid *tablespaceId, char *relKind)
{
	Relation rel;
	ScanKeyData scankey[1];
	SysScanDesc scan;
	HeapTuple tuple;
	Form_pg_class relTuple;

	ScanKeyInit(&scankey[0],
				Anum_pg_class_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relId));
	rel = table_open(RelationRelationId, AccessShareLock);
	scan = systable_beginscan(rel, ClassOidIndexId, true,
							  SnapshotSelf, 1, scankey);
	tuple = systable_getnext(scan);
	if (!tuple)
		elog(ERROR, "unable to find relation entry in pg_class for %u", relId);

	relTuple = (Form_pg_class) GETSTRUCT(tuple);
	
	*tablespaceId = relTuple->reltablespace;
	*relKind = relTuple->relkind;

	systable_endscan(scan);
	table_close(rel, AccessShareLock);
}

static void
dfsObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg)
{
	Oid tablespaceId;
	char relKind;

	if (prevObjectAccessHook)
		prevObjectAccessHook(access, classId, objectId, subId, arg);

	if (access == OAT_POST_CREATE)
	{
		bool isInternal = arg ? ((ObjectAccessPostCreate *) arg)->is_internal : false;
		if (classId == DatabaseRelationId)
		{
			Assert(!isInternal);

			if (IsDfsTablespace(getDatabaseTablespace(objectId)))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not create database in dfs tablespace")));
		}

		if (classId == RelationRelationId && subId == 0 && !isInternal)
		{
			getRelationInfo(objectId, &tablespaceId, &relKind);
			if (RELATION_IS_SUPPORTED(relKind) && IsDfsTablespace(tablespaceId))
				recordDependencyOnTablespace(RelationRelationId, objectId, tablespaceId);
		}
	}
}

static void
dfsProcessUtility(PlannedStmt *pstmt,
				  const char *queryString,
				  bool readOnlyTree,
				  ProcessUtilityContext context,
				  ParamListInfo params,
				  QueryEnvironment *queryEnv,
				  DestReceiver *dest,
				  QueryCompletion *completionTag)
{
	Node *parseTree = pstmt->utilityStmt;
	bool  isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);

	switch (nodeTag(parseTree))
	{
		case T_CreateTableSpaceStmt:
			preventCommandIfNeed(parseTree);
			/* no event triggers for global objects */
			if (Gp_role != GP_ROLE_EXECUTE)
			{
				/*
				 * Don't allow master to call this in a transaction block. Segments
				 * are ok as distributed transaction participants.
				 */
				PreventInTransactionBlock(isTopLevel, "CREATE TABLESPACE");
			}
			DfsCreateTableSpace((CreateTableSpaceStmt *) parseTree);
			break;

		case T_DropTableSpaceStmt:
			preventCommandIfNeed(parseTree);
			/* no event triggers for global objects */
			if (Gp_role != GP_ROLE_EXECUTE)
			{
				/*
				 * Don't allow master to call this in a transaction block.  Segments are ok as
				 * distributed transaction participants.
				 */
				PreventInTransactionBlock(isTopLevel, "DROP TABLESPACE");
			}
			DfsDropTableSpace((DropTableSpaceStmt *) parseTree);
			break;

		case T_AlterTableSpaceOptionsStmt:
			preventCommandIfNeed(parseTree);
			/* no event triggers for global objects */
			DfsAlterTableSpaceOptions((AlterTableSpaceOptionsStmt *) parseTree);
			break;

		default:
			prevProcessUtilityHook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, completionTag);
	}
}

/* shared library initialization function */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR, (errmsg("dfs_tablespace can only be loaded via shared_preload_libraries"),
						errhint("Add dfs_tablespace to shared_preload_libraries configuration "
								"variable in postgresql.conf in coordinator and segments. "
								"Note that dfs_tablespace should be at the beginning of "
								"shared_preload_libraries.")));

	/*
	 * Perform checks before registering any hooks, to avoid erroring out in a
	 * partial state.
	 */
	if (ProcessUtility_hook != NULL)
		ereport(ERROR, (errmsg("dfs_tablespace has to be loaded first"),
						errhint("Place dfs_tablespace at the beginning of shared_preload_libraries.")));

	prevObjectAccessHook = object_access_hook;
	object_access_hook = dfsObjectAccessHook;

	prevProcessUtilityHook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = dfsProcessUtility;

	if (needInitialization)
	{
		dfsInitializeReloptions();
		needInitialization = false;
	}
}
