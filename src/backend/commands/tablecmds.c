/*-------------------------------------------------------------------------
 *
 * tablecmds.c
 *	  Commands for creating and altering table structures and settings
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/tablecmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/attmap.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/partition.h"
#include "catalog/pg_am.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_compression.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_directory_table.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "catalog/toasting.h"
#include "catalog/gp_fastsequence.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbappendonlyxlog.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbcat.h"
#include "cdb/memquota.h"
#include "commands/cluster.h"
#include "commands/copy.h"
#include "commands/comment.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/event_trigger.h"
#include "commands/policy.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/tag.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "commands/user.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "parser/parser.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "pgstat.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/sinval.h"
#include "tcop/utility.h"
#include "storage/lock.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/metrics_utils.h"
#include "utils/partcache.h"
#include "utils/relcache.h"
#include "utils/resgroup.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#include "access/appendonly_compaction.h"
#include "access/bitmap_private.h"
#include "access/external.h"
#include "catalog/aocatalog.h"
#include "catalog/gp_matview_aux.h"
#include "catalog/oid_dispatch.h"
#include "nodes/altertablenodes.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbrelsize.h"
#include "cdb/cdboidsync.h"
#include "postmaster/autostats.h"

const char *synthetic_sql = "(internally generated SQL command)";

/*
 * ON COMMIT action list
 */
typedef struct OnCommitItem
{
	Oid			relid;			/* relid of relation */
	OnCommitAction oncommit;	/* what to do at end of xact */

	/*
	 * If this entry was created during the current transaction,
	 * creating_subid is the ID of the creating subxact; if created in a prior
	 * transaction, creating_subid is zero.  If deleted during the current
	 * transaction, deleting_subid is the ID of the deleting subxact; if no
	 * deletion request is pending, deleting_subid is zero.
	 */
	SubTransactionId creating_subid;
	SubTransactionId deleting_subid;
} OnCommitItem;

static List *on_commits = NIL;


/*
 * State information for ALTER TABLE
 * In GPDB, these are in nodes/altertablenodes.h
 */

/*
 * Error-reporting support for RemoveRelations
 */
struct dropmsgstrings
{
	char		kind;
	int			nonexistent_code;
	const char *nonexistent_msg;
	const char *skipping_msg;
	const char *nota_msg;
	const char *drophint_msg;
};

static const struct dropmsgstrings dropmsgstringarray[] = {
	{RELKIND_RELATION,
		ERRCODE_UNDEFINED_TABLE,
		gettext_noop("table \"%s\" does not exist"),
		gettext_noop("table \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not a table"),
	gettext_noop("Use DROP TABLE to remove a table.")},
	{RELKIND_DIRECTORY_TABLE,
		ERRCODE_UNDEFINED_TABLE,
		gettext_noop("directory table \"%s\" does not exist"),
		gettext_noop("directory table \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not a directory table"),
		gettext_noop("Use DROP DIRECTORY TABLE to remove a directory table.")},
	{RELKIND_SEQUENCE,
		ERRCODE_UNDEFINED_TABLE,
		gettext_noop("sequence \"%s\" does not exist"),
		gettext_noop("sequence \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not a sequence"),
	gettext_noop("Use DROP SEQUENCE to remove a sequence.")},
	{RELKIND_VIEW,
		ERRCODE_UNDEFINED_TABLE,
		gettext_noop("view \"%s\" does not exist"),
		gettext_noop("view \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not a view"),
	gettext_noop("Use DROP VIEW to remove a view.")},
	{RELKIND_MATVIEW,
		ERRCODE_UNDEFINED_TABLE,
		gettext_noop("materialized view \"%s\" does not exist"),
		gettext_noop("materialized view \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not a materialized view"),
	gettext_noop("Use DROP MATERIALIZED VIEW to remove a materialized view.")},
	{RELKIND_INDEX,
		ERRCODE_UNDEFINED_OBJECT,
		gettext_noop("index \"%s\" does not exist"),
		gettext_noop("index \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not an index"),
	gettext_noop("Use DROP INDEX to remove an index.")},
	{RELKIND_COMPOSITE_TYPE,
		ERRCODE_UNDEFINED_OBJECT,
		gettext_noop("type \"%s\" does not exist"),
		gettext_noop("type \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not a type"),
	gettext_noop("Use DROP TYPE to remove a type.")},
	{RELKIND_FOREIGN_TABLE,
		ERRCODE_UNDEFINED_OBJECT,
		gettext_noop("foreign table \"%s\" does not exist"),
		gettext_noop("foreign table \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not a foreign table"),
	gettext_noop("Use DROP FOREIGN TABLE to remove a foreign table.")},
	{RELKIND_PARTITIONED_TABLE,
		ERRCODE_UNDEFINED_TABLE,
		gettext_noop("table \"%s\" does not exist"),
		gettext_noop("table \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not a table"),
	gettext_noop("Use DROP TABLE to remove a table.")},
	{RELKIND_PARTITIONED_INDEX,
		ERRCODE_UNDEFINED_OBJECT,
		gettext_noop("index \"%s\" does not exist"),
		gettext_noop("index \"%s\" does not exist, skipping"),
		gettext_noop("\"%s\" is not an index"),
	gettext_noop("Use DROP INDEX to remove an index.")},
	{'\0', 0, NULL, NULL, NULL, NULL}
};

/* communication between RemoveRelations and RangeVarCallbackForDropRelation */
struct DropRelationCallbackState
{
	/* These fields are set by RemoveRelations: */
	char		expected_relkind;
	LOCKMODE	heap_lockmode;
	/* These fields are state to track which subsidiary locks are held: */
	Oid			heapOid;
	Oid			partParentOid;
	/* These fields are passed back by RangeVarCallbackForDropRelation: */
	char		actual_relkind;
	char		actual_relpersistence;
};

/* Alter table target-type flags for ATSimplePermissions */
#define		ATT_TABLE				0x0001
#define		ATT_VIEW				0x0002
#define		ATT_MATVIEW				0x0004
#define		ATT_INDEX				0x0008
#define		ATT_COMPOSITE_TYPE		0x0010
#define		ATT_FOREIGN_TABLE		0x0020
#define		ATT_PARTITIONED_INDEX	0x0040
#define		ATT_DIRECTORY_TABLE		0x0080
#define		ATT_SEQUENCE			0x0100

/*
 * ForeignTruncateInfo
 *
 * Information related to truncation of foreign tables.  This is used for
 * the elements in a hash table. It uses the server OID as lookup key,
 * and includes a per-server list of all foreign tables involved in the
 * truncation.
 */
typedef struct ForeignTruncateInfo
{
	Oid			serverid;
	List	   *rels;
} ForeignTruncateInfo;

/*
 * Partition tables are expected to be dropped when the parent partitioned
 * table gets dropped. Hence for partitioning we use AUTO dependency.
 * Otherwise, for regular inheritance use NORMAL dependency.
 */
#define child_dependency_type(child_is_partition)	\
	((child_is_partition) ? DEPENDENCY_AUTO : DEPENDENCY_NORMAL)

static void truncate_check_rel(Oid relid, Form_pg_class reltuple);
static void truncate_check_perms(Oid relid, Form_pg_class reltuple);
static void truncate_check_activity(Relation rel);
static void RangeVarCallbackForTruncate(const RangeVar *relation,
										Oid relId, Oid oldRelId, void *arg);
static List *MergeAttributes(List *schema, List *supers, char relpersistence,
							 bool is_partition, List **supconstr);
static void MergeAttributesIntoExisting(Relation child_rel, Relation parent_rel);
static bool MergeCheckConstraint(List *constraints, char *name, Node *expr);
static void MergeConstraintsIntoExisting(Relation child_rel, Relation parent_rel);
static void StoreCatalogInheritance(Oid relationId, List *supers,
									bool child_is_partition);
static void StoreCatalogInheritance1(Oid relationId, Oid parentOid,
									 int32 seqNumber, Relation inhRelation,
									 bool child_is_partition);
static int	findAttrByName(const char *attributeName, List *schema);
static void AlterIndexNamespaces(Relation classRel, Relation rel,
								 Oid oldNspOid, Oid newNspOid, ObjectAddresses *objsMoved);
static void AlterSeqNamespaces(Relation classRel, Relation rel,
							   Oid oldNspOid, Oid newNspOid, ObjectAddresses *objsMoved,
							   LOCKMODE lockmode);
static ObjectAddress ATExecAlterConstraint(Relation rel, AlterTableCmd *cmd,
										   bool recurse, bool recursing, LOCKMODE lockmode);
static bool ATExecAlterConstrRecurse(Constraint *cmdcon, Relation conrel, Relation tgrel,
									 Relation rel, HeapTuple contuple, List **otherrelids,
									 LOCKMODE lockmode);
static ObjectAddress ATExecValidateConstraint(List **wqueue,
											  Relation rel, char *constrName,
											  bool recurse, bool recursing, LOCKMODE lockmode);
static int	transformColumnNameList(Oid relId, List *colList,
									int16 *attnums, Oid *atttypids);
static int	transformFkeyGetPrimaryKey(Relation pkrel, Oid *indexOid,
									   List **attnamelist,
									   int16 *attnums, Oid *atttypids,
									   Oid *opclasses);
static Oid	transformFkeyCheckAttrs(Relation pkrel,
									int numattrs, int16 *attnums,
									Oid *opclasses);
static void checkFkeyPermissions(Relation rel, int16 *attnums, int natts);
static CoercionPathType findFkeyCast(Oid targetTypeId, Oid sourceTypeId,
									 Oid *funcid);
static void validateForeignKeyConstraint(char *conname,
										 Relation rel, Relation pkrel,
										 Oid pkindOid, Oid constraintOid);
static void ATController(AlterTableStmt *parsetree,
						 Relation rel, List *cmds, bool recurse, LOCKMODE lockmode,
						 AlterTableUtilityContext *context);
static void ATPrepCmd(List **wqueue, Relation rel, AlterTableCmd *cmd,
					  bool recurse, bool recursing, LOCKMODE lockmode,
					  AlterTableUtilityContext *context);
static void ATRewriteCatalogs(List **wqueue, LOCKMODE lockmode,
							  AlterTableUtilityContext *context);
static void ATExecCmd(List **wqueue, AlteredTableInfo *tab,
					  AlterTableCmd *cmd, LOCKMODE lockmode, int cur_pass,
					  AlterTableUtilityContext *context);
static AlterTableCmd *ATParseTransformCmd(List **wqueue, AlteredTableInfo *tab,
										  Relation rel, AlterTableCmd *cmd,
										  bool recurse, LOCKMODE lockmode,
										  int cur_pass,
										  AlterTableUtilityContext *context);
static void ATRewriteTables(AlterTableStmt *parsetree,
							List **wqueue, LOCKMODE lockmode,
							AlterTableUtilityContext *context);
static void ATRewriteTable(AlteredTableInfo *tab, Oid OIDNewHeap, LOCKMODE lockmode);
static void ATAocsWriteSegFileNewColumns(
		AOCSAddColumnDesc idesc, AOCSHeaderScanDesc sdesc,
		AlteredTableInfo *tab, ExprContext *econtext, TupleTableSlot *slot, const char *relname);
static void ATAocsWriteNewColumns(AlteredTableInfo *tab);
static AlteredTableInfo *ATGetQueueEntry(List **wqueue, Relation rel);
static void ATSimplePermissions(Relation rel, int allowed_targets);
static void ATWrongRelkindError(Relation rel, int allowed_targets);
static void ATSimpleRecursion(List **wqueue, Relation rel,
							  AlterTableCmd *cmd, bool recurse, LOCKMODE lockmode,
							  AlterTableUtilityContext *context);
static void ATCheckPartitionsNotInUse(Relation rel, LOCKMODE lockmode);
static void ATTypedTableRecursion(List **wqueue, Relation rel, AlterTableCmd *cmd,
								  LOCKMODE lockmode,
								  AlterTableUtilityContext *context);
static List *find_typed_table_dependencies(Oid typeOid, const char *typeName,
										   DropBehavior behavior);
static void ATPrepAddColumn(List **wqueue, Relation rel, bool recurse, bool recursing,
							bool is_view, AlterTableCmd *cmd, LOCKMODE lockmode,
							AlterTableUtilityContext *context);
static ObjectAddress ATExecAddColumn(List **wqueue, AlteredTableInfo *tab,
									 Relation rel, AlterTableCmd **cmd,
									 bool recurse, bool recursing,
									 LOCKMODE lockmode, int cur_pass,
									 AlterTableUtilityContext *context);
static bool check_for_column_name_collision(Relation rel, const char *colname,
											bool if_not_exists);
static void add_column_datatype_dependency(Oid relid, int32 attnum, Oid typid);
static void add_column_collation_dependency(Oid relid, int32 attnum, Oid collid);
static void ATPrepDropNotNull(Relation rel, bool recurse, bool recursing);
static ObjectAddress ATExecDropNotNull(Relation rel, const char *colName, LOCKMODE lockmode);
static void ATPrepSetNotNull(List **wqueue, Relation rel,
							 AlterTableCmd *cmd, bool recurse, bool recursing,
							 LOCKMODE lockmode,
							 AlterTableUtilityContext *context);
static ObjectAddress ATExecSetNotNull(AlteredTableInfo *tab, Relation rel,
									  const char *colName, LOCKMODE lockmode);
static void ATExecCheckNotNull(AlteredTableInfo *tab, Relation rel,
							   const char *colName, LOCKMODE lockmode);
static bool NotNullImpliedByRelConstraints(Relation rel, Form_pg_attribute attr);
static bool ConstraintImpliedByRelConstraint(Relation scanrel,
											 List *testConstraint, List *provenConstraint);
static ObjectAddress ATExecColumnDefault(Relation rel, const char *colName,
										 Node *newDefault, LOCKMODE lockmode);
static ObjectAddress ATExecCookedColumnDefault(Relation rel, AttrNumber attnum,
											   Node *newDefault);
static ObjectAddress ATExecAddIdentity(Relation rel, const char *colName,
									   Node *def, LOCKMODE lockmode);
static ObjectAddress ATExecSetIdentity(Relation rel, const char *colName,
									   Node *def, LOCKMODE lockmode);
static ObjectAddress ATExecDropIdentity(Relation rel, const char *colName, bool missing_ok, LOCKMODE lockmode);
static void ATPrepDropExpression(Relation rel, AlterTableCmd *cmd, bool recurse, bool recursing, LOCKMODE lockmode);
static ObjectAddress ATExecDropExpression(Relation rel, const char *colName, bool missing_ok, LOCKMODE lockmode);
static ObjectAddress ATExecSetStatistics(Relation rel, const char *colName, int16 colNum,
										 Node *newValue, LOCKMODE lockmode);
static ObjectAddress ATExecSetOptions(Relation rel, const char *colName,
									  Node *options, bool isReset, LOCKMODE lockmode);
static ObjectAddress ATExecSetStorage(Relation rel, const char *colName,
									  Node *newValue, LOCKMODE lockmode);
static void ATPrepDropColumn(List **wqueue, Relation rel, bool recurse, bool recursing,
							 AlterTableCmd *cmd, LOCKMODE lockmode,
							 AlterTableUtilityContext *context);
static ObjectAddress ATExecDropColumn(List **wqueue, Relation rel, const char *colName,
									  DropBehavior behavior,
									  bool recurse, bool recursing,
									  bool missing_ok, LOCKMODE lockmode,
									  ObjectAddresses *addrs);
static ObjectAddress ATExecAddIndex(AlteredTableInfo *tab, Relation rel,
									IndexStmt *stmt, bool is_rebuild, LOCKMODE lockmode);
static ObjectAddress ATExecAddStatistics(AlteredTableInfo *tab, Relation rel,
										 CreateStatsStmt *stmt, bool is_rebuild, LOCKMODE lockmode);
static ObjectAddress ATExecAddConstraint(List **wqueue,
										 AlteredTableInfo *tab, Relation rel,
										 Constraint *newConstraint, bool recurse, bool is_readd,
										 LOCKMODE lockmode);
static char *ChooseForeignKeyConstraintNameAddition(List *colnames);
static ObjectAddress ATExecAddIndexConstraint(AlteredTableInfo *tab, Relation rel,
											  IndexStmt *stmt, LOCKMODE lockmode);
static ObjectAddress ATAddCheckConstraint(List **wqueue,
										  AlteredTableInfo *tab, Relation rel,
										  Constraint *constr,
										  bool recurse, bool recursing, bool is_readd,
										  LOCKMODE lockmode);
static ObjectAddress ATAddForeignKeyConstraint(List **wqueue, AlteredTableInfo *tab,
											   Relation rel, Constraint *fkconstraint,
											   bool recurse, bool recursing,
											   LOCKMODE lockmode);
static ObjectAddress addFkRecurseReferenced(List **wqueue, Constraint *fkconstraint,
											Relation rel, Relation pkrel, Oid indexOid, Oid parentConstr,
											int numfks, int16 *pkattnum, int16 *fkattnum,
											Oid *pfeqoperators, Oid *ppeqoperators, Oid *ffeqoperators,
											bool old_check_ok);
static void addFkRecurseReferencing(List **wqueue, Constraint *fkconstraint,
									Relation rel, Relation pkrel, Oid indexOid, Oid parentConstr,
									int numfks, int16 *pkattnum, int16 *fkattnum,
									Oid *pfeqoperators, Oid *ppeqoperators, Oid *ffeqoperators,
									bool old_check_ok, LOCKMODE lockmode);
static void CloneForeignKeyConstraints(List **wqueue, Relation parentRel,
									   Relation partitionRel);
static void CloneFkReferenced(Relation parentRel, Relation partitionRel);
static void CloneFkReferencing(List **wqueue, Relation parentRel,
							   Relation partRel);
static void createForeignKeyCheckTriggers(Oid myRelOid, Oid refRelOid,
										  Constraint *fkconstraint, Oid constraintOid,
										  Oid indexOid);
static void createForeignKeyActionTriggers(Relation rel, Oid refRelOid,
										   Constraint *fkconstraint, Oid constraintOid,
										   Oid indexOid);
static bool tryAttachPartitionForeignKey(ForeignKeyCacheInfo *fk,
										 Oid partRelid,
										 Oid parentConstrOid, int numfks,
										 AttrNumber *mapped_conkey, AttrNumber *confkey,
										 Oid *conpfeqop);
static void ATExecDropConstraint(Relation rel, const char *constrName,
								 DropBehavior behavior,
								 bool recurse, bool recursing,
								 bool missing_ok, LOCKMODE lockmode);
static void ATPrepAlterColumnType(List **wqueue,
								  AlteredTableInfo *tab, Relation rel,
								  bool recurse, bool recursing,
								  AlterTableCmd *cmd, LOCKMODE lockmode,
								  AlterTableUtilityContext *context);
static bool ATColumnChangeRequiresRewrite(Node *expr, AttrNumber varattno);
static ObjectAddress ATExecAlterColumnType(AlteredTableInfo *tab, Relation rel,
										   AlterTableCmd *cmd, LOCKMODE lockmode);
static void RememberConstraintForRebuilding(Oid conoid, AlteredTableInfo *tab);
static void RememberIndexForRebuilding(Oid indoid, AlteredTableInfo *tab);
static void RememberStatisticsForRebuilding(Oid indoid, AlteredTableInfo *tab);
static void ATPostAlterTypeCleanup(List **wqueue, AlteredTableInfo *tab,
								   LOCKMODE lockmode, AlterTableUtilityContext *context);
static void ATPostAlterTypeParse(Oid oldId, Oid oldRelId, Oid refRelId,
								 char *cmd, List **wqueue, LOCKMODE lockmode,
								 bool rewrite, AlterTableUtilityContext *context);
static void RebuildConstraintComment(AlteredTableInfo *tab, int pass,
									 Oid objid, Relation rel, List *domname,
									 const char *conname);
static void TryReuseIndex(Oid oldId, IndexStmt *stmt);
static void TryReuseForeignKey(Oid oldId, Constraint *con);
static ObjectAddress ATExecAlterColumnGenericOptions(Relation rel, const char *colName,
													 List *options, LOCKMODE lockmode);
static void change_owner_fix_column_acls(Oid relationOid,
										 Oid oldOwnerId, Oid newOwnerId);
static void change_owner_recurse_to_sequences(Oid relationOid,
											  Oid newOwnerId, LOCKMODE lockmode);
static ObjectAddress ATExecClusterOn(Relation rel, const char *indexName,
									 LOCKMODE lockmode);
static void ATExecDropCluster(Relation rel, LOCKMODE lockmode);
static bool ATPrepChangePersistence(Relation rel, bool toLogged);
static void ATPrepSetTableSpace(AlteredTableInfo *tab, Relation rel,
								const char *tablespacename, LOCKMODE lockmode);
static void ATExecSetTableSpace(Oid tableOid, Oid newTableSpace, LOCKMODE lockmode);
static void ATExecSetTableSpaceNoStorage(Relation rel, Oid newTableSpace);
static void ATExecSetRelOptions(Relation rel, List *defList,
								AlterTableType operation,
								LOCKMODE lockmode);
static void ATExecEnableDisableTrigger(Relation rel, const char *trigname,
									   char fires_when, bool skip_system, LOCKMODE lockmode);
static void ATExecEnableDisableRule(Relation rel, const char *rulename,
									char fires_when, LOCKMODE lockmode);
static void ATPrepAddInherit(Relation child_rel);
static ObjectAddress ATExecAddInherit(Relation child_rel, RangeVar *parent, LOCKMODE lockmode);
static ObjectAddress ATExecDropInherit(Relation rel, RangeVar *parent, LOCKMODE lockmode);
static void drop_parent_dependency(Oid relid, Oid refclassid, Oid refobjid,
								   DependencyType deptype);
static ObjectAddress ATExecAddOf(Relation rel, const TypeName *ofTypename, LOCKMODE lockmode);
static void ATExecDropOf(Relation rel, LOCKMODE lockmode);
static void ATExecReplicaIdentity(Relation rel, ReplicaIdentityStmt *stmt, LOCKMODE lockmode);
static void ATExecGenericOptions(Relation rel, List *options);
static void ATExecSetRowSecurity(Relation rel, bool rls);
static void ATExecForceNoForceRowSecurity(Relation rel, bool force_rls);
static ObjectAddress ATExecSetCompression(AlteredTableInfo *tab, Relation rel,
										  const char *column, Node *newValue, LOCKMODE lockmode);

static void index_copy_data(Relation rel, RelFileNode newrnode);
static const char *storage_name(char c);

static void RangeVarCallbackForDropRelation(const RangeVar *rel, Oid relOid,
											Oid oldRelOid, void *arg);
static void RangeVarCallbackForAlterRelation(const RangeVar *rv, Oid relid,
											 Oid oldrelid, void *arg);

static void ATExecExpandTable(List **wqueue, Relation rel, AlterTableCmd *cmd, int numsegments);
static void ATExecExpandPartitionTablePrepare(Relation rel, int numsegments);
static void ATExecExpandTableCTAS(AlterTableCmd *rootCmd, Relation rel, AlterTableCmd *cmd, int numsegments);

static void ATExecSetDistributedBy(Relation rel, Node *node,
								   AlterTableCmd *cmd);
static PartitionSpec *transformPartitionSpec(Relation rel, PartitionSpec *partspec, char *strategy);
static void CreateInheritance(Relation child_rel, Relation parent_rel);
static void RemoveInheritance(Relation child_rel, Relation parent_rel,
							  bool allow_detached);
static ObjectAddress ATExecAttachPartition(List **wqueue, Relation rel,
										   PartitionCmd *cmd,
										   AlterTableUtilityContext *context);
static void AttachPartitionEnsureIndexes(Relation rel, Relation attachrel);
static void QueuePartitionConstraintValidation(List **wqueue, Relation scanrel,
											   List *partConstraint,
											   bool validate_default);
static void CloneRowTriggersToPartition(Relation parent, Relation partition);
static void DetachAddConstraintIfNeeded(List **wqueue, Relation partRel);
static void DropClonedTriggersFromPartition(Oid partitionId);
static ObjectAddress ATExecDetachPartition(List **wqueue, AlteredTableInfo *tab,
										   Relation rel, RangeVar *name,
										   bool concurrent);
static void DetachPartitionFinalize(Relation rel, Relation partRel,
									bool concurrent, Oid defaultPartOid);
static ObjectAddress ATExecDetachPartitionFinalize(Relation rel, RangeVar *name);
static ObjectAddress ATExecAttachPartitionIdx(List **wqueue, Relation rel,
											  RangeVar *name);
static void validatePartitionedIndex(Relation partedIdx, Relation partedTbl);
static void refuseDupeIndexAttach(Relation parentIdx, Relation partIdx,
								  Relation partitionTbl);
static List *GetParentedForeignKeyRefs(Relation partition);
static void ATDetachCheckNoForeignKeyRefs(Relation partition);
static char GetAttributeCompression(Oid atttypid, char *compression);

static void ATSetTags(Relation rel, List *tags, bool unset);

static RangeVar *make_temp_table_name(Relation rel, BackendId id);
static bool prebuild_temp_table(Relation rel, RangeVar *tmpname, DistributedBy *distro,
								char *amname, List *opts,
								bool isTmpTableAo, bool useExistingColumnAttributes);


/* ----------------------------------------------------------------
 *		DefineRelation
 *				Creates a new relation.
 *
 * stmt carries parsetree information from an ordinary CREATE TABLE statement.
 * The other arguments are used to extend the behavior for other cases:
 * relkind: relkind to assign to the new relation
 * ownerId: if not InvalidOid, use this as the new relation's owner.
 * typaddress: if not null, it's set to the pg_type entry's address.
 * queryString: for error reporting
 *
 * Note that permissions checks are done against current user regardless of
 * ownerId.  A nonzero ownerId is used when someone is creating a relation
 * "on behalf of" someone else, so we still want to see that the current user
 * has permissions to do it.
 *
 * If successful, returns the address of the new relation.
 *
 * GPDB: If 'dispatch' is true (and we are running in QD), the statement is
 * also dispatched to the QE nodes. Otherwise it is the caller's
 * responsibility to dispatch.
 * ----------------------------------------------------------------
 */
ObjectAddress
DefineRelation(CreateStmt *stmt, char relkind, Oid ownerId,
			   ObjectAddress *typaddress, const char *queryString,
			   bool dispatch, bool useChangedOpts, GpPolicy *intoPolicy)
{
	char		relname[NAMEDATALEN];
	Oid			namespaceId;
	GpPolicy   *policy;
	Oid			relationId = InvalidOid;
	Oid			tablespaceId;
	Relation	rel;
	TupleDesc	descriptor;
	List	   *inheritOids;
	List	   *old_constraints;
	List	   *rawDefaults;
	List	   *cookedDefaults;
	Datum		reloptions;
	ListCell   *listptr;
	AttrNumber	attnum;
	bool		partitioned;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	Oid			ofTypeId;
	ObjectAddress address;
	LOCKMODE	parentLockmode;
	const char *accessMethod = NULL;
	Oid			accessMethodId = InvalidOid;
	Oid			amHandlerOid = InvalidOid;
	List	   *schema;
	List	   *cooked_constraints;
	bool		shouldDispatch = dispatch &&
								 Gp_role == GP_ROLE_DISPATCH &&
                                 IsNormalProcessingMode();

	/*
	 * In QE mode, tableElts contain not only the normal ColumnDefs, but also
	 * pre-made CookedConstraints. Separate them into different lists.
	 */
	schema = NIL;
	cooked_constraints = NIL;
	if (relkind == RELKIND_DIRECTORY_TABLE)
	{
		schema = GetDirectoryTableSchema();
	}
	else
	{
		foreach(listptr, stmt->tableElts)
		{
			Node	   *node = lfirst(listptr);

			if (IsA(node, CookedConstraint))
			{
				Assert(Gp_role == GP_ROLE_EXECUTE);
				cooked_constraints = lappend(cooked_constraints, node);
			}
			else
				schema = lappend(schema, node);
		}
	}

	/*
	 * Truncate relname to appropriate length (probably a waste of time, as
	 * parser should have done this already).
	 */
	strlcpy(relname, stmt->relation->relname, NAMEDATALEN);

	/*
	 * Check consistency of arguments
	 */
	if (stmt->oncommit != ONCOMMIT_NOOP
		&& stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("ON COMMIT can only be used on temporary tables")));

	if (stmt->partspec != NULL)
	{
		/*
		 * In QD, the caller calls us with RELKIND_RELATION, and we turn it int
		 * REKIND_PARTITIONED_TABLE here. In QE, we received the modified
		 * CreateStmt from QD where relkind has already been set to RELKIND_PARTITIONED_TABLE
		 */
		if (relkind != (Gp_role == GP_ROLE_EXECUTE ? RELKIND_PARTITIONED_TABLE : RELKIND_RELATION))
			elog(ERROR, "unexpected relkind: %d", (int) relkind);

		relkind = RELKIND_PARTITIONED_TABLE;
		partitioned = true;
	}
	else
		partitioned = false;

	/*
	 * Look up the namespace in which we are supposed to create the relation,
	 * check we have permission to create there, lock it against concurrent
	 * drop, and mark stmt->relation as RELPERSISTENCE_TEMP if a temporary
	 * namespace is selected.
	 */
	namespaceId =
		RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock, NULL);

	/*
	 * Security check: disallow creating temp tables from security-restricted
	 * code.  This is needed because calling code might not expect untrusted
	 * tables to appear in pg_temp at the front of its search path.
	 */
	if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP
		&& InSecurityRestrictedOperation())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("cannot create temporary table within security-restricted operation")));

	/*
	 * Determine the lockmode to use when scanning parents.  A self-exclusive
	 * lock is needed here.
	 *
	 * For regular inheritance, if two backends attempt to add children to the
	 * same parent simultaneously, and that parent has no pre-existing
	 * children, then both will attempt to update the parent's relhassubclass
	 * field, leading to a "tuple concurrently updated" error.  Also, this
	 * interlocks against a concurrent ANALYZE on the parent table, which
	 * might otherwise be attempting to clear the parent's relhassubclass
	 * field, if its previous children were recently dropped.
	 *
	 * If the child table is a partition, then we instead grab an exclusive
	 * lock on the parent because its partition descriptor will be changed by
	 * addition of the new partition.
	 */
	parentLockmode = (stmt->partbound != NULL ? AccessExclusiveLock :
					  ShareUpdateExclusiveLock);

	/* Determine the list of OIDs of the parents. */
	inheritOids = NIL;
	foreach(listptr, stmt->inhRelations)
	{
		RangeVar   *rv = (RangeVar *) lfirst(listptr);
		Oid			parentOid;

		parentOid = RangeVarGetRelid(rv, parentLockmode, false);

		/*
		 * Reject duplications in the list of parents.
		 */
		if (list_member_oid(inheritOids, parentOid))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" would be inherited from more than once",
							get_rel_name(parentOid))));

		inheritOids = lappend_oid(inheritOids, parentOid);
	}

	/*
	 * Directory table never has partitions.
	 */
	AssertImply(relkind == RELKIND_DIRECTORY_TABLE, !partitioned);
	AssertImply(relkind == RELKIND_DIRECTORY_TABLE, !stmt->inhRelations);

	/*
	 * Select tablespace to use: an explicitly indicated one, or (in the case
	 * of a partitioned table) the parent's, if it has one.
	 */
	if (stmt->tablespacename)
	{
		/*
		 * Tablespace specified on the command line, or was passed down by
		 * dispatch.
		 */
		tablespaceId = get_tablespace_oid(stmt->tablespacename, false);

		if (partitioned && tablespaceId == MyDatabaseTableSpace)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot specify default tablespace for partitioned relations")));
	}
	else if (stmt->partbound)
	{
		/*
		 * For partitions, when no other tablespace is specified, we default
		 * the tablespace to the parent partitioned table's.
		 */
		Assert(list_length(inheritOids) == 1);
		tablespaceId = get_rel_tablespace(linitial_oid(inheritOids));

		/* 
		 * MPP-8238 : inconsistent tablespaces between segments and master 
		 */
		stmt->tablespacename = get_tablespace_name(tablespaceId);
	}
	else
		tablespaceId = InvalidOid;

	/* still nothing? use the default */
	if (!OidIsValid(tablespaceId))
		tablespaceId = GetDefaultTablespace(stmt->relation->relpersistence,
											partitioned);

	/* Check permissions except when using database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(),
										   ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TABLESPACE,
						   get_tablespace_name(tablespaceId));
	}

	/* In all cases disallow placing user relations in pg_global */
	if (tablespaceId == GLOBALTABLESPACE_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only shared relations can be placed in pg_global tablespace")));

	/* Identify user ID that will own the table */
	if (!OidIsValid(ownerId))
		ownerId = GetUserId();

	/*
	 * Cloudberry: the accessMethod is necessary to extract, transform and
	 * validate the reloptions.
	 */

	/*
	 * If the statement hasn't specified an access method, but we're defining
	 * a type of relation that needs one, use the default.
	 */
	if (stmt->accessMethod != NULL && relkind != RELKIND_DIRECTORY_TABLE)
	{
		accessMethod = stmt->accessMethod;

		if (partitioned)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("specifying a table access method is not supported on a partitioned table")));

	}
	else if (relkind == RELKIND_DIRECTORY_TABLE)
		accessMethod = DEFAULT_TABLE_ACCESS_METHOD;
	else if (relkind == RELKIND_RELATION ||
			 relkind == RELKIND_TOASTVALUE ||
			 relkind == RELKIND_MATVIEW)
		accessMethod = default_table_access_method;

	/* look up the access method, verify it is for a table */
	if (accessMethod != NULL)
	{
		accessMethodId = get_table_am_oid(accessMethod, false);
		amHandlerOid = get_table_am_handler_oid(accessMethod, false);
	}

	/*
	 * Parse and validate reloptions, if any.
	 */
	reloptions = transformRelOptions((Datum) 0, stmt->options, NULL, validnsps,
									 true, false);

	/*
	 * Cloudberry: special case checks for reloptions that correspond to
	 * appendonly relations. This check can not be performed earlier because it
	 * is needed to know the access method.
	 */
	if (AMHandlerIsAO(amHandlerOid))
	{
		Assert(relkind == RELKIND_MATVIEW || relkind == RELKIND_RELATION || relkind == RELKIND_DIRECTORY_TABLE);

		/*
		 * Extract and process any WITH options supplied, otherwise use defaults
		 *
		 * The generated options will be added during heap_create_with_catalog
		 * for appendoptimized relations, so (view|heap)_reloptions should not
		 * be called yet.
		 */
		StdRdOptions *stdRdOptions = (StdRdOptions *)default_reloptions(reloptions,
																		true,
																		RELOPT_KIND_APPENDOPTIMIZED);

		/* Validate the StdRdOptions parsed or error out */
		validateAppendOnlyRelOptions(stdRdOptions->blocksize,
									 gp_safefswritesize,
									 stdRdOptions->compresslevel,
									 stdRdOptions->compresstype,
									 stdRdOptions->checksum,
									 AMHandlerIsAoCols(amHandlerOid));

		reloptions = transformAOStdRdOptions(stdRdOptions, reloptions);
	}
	else
	switch (relkind)
	{
		case RELKIND_VIEW:
			(void) view_reloptions(reloptions, true);
			break;
		case RELKIND_PARTITIONED_TABLE:
			(void) partitioned_table_reloptions(reloptions, true);
			break;
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_TOASTVALUE:
			(void) table_reloptions_am(accessMethodId, reloptions, relkind, true);
			break;
		default:
			break;
	}

	if (stmt->ofTypename)
	{
		AclResult	aclresult;

		ofTypeId = typenameTypeId(NULL, stmt->ofTypename);

		aclresult = pg_type_aclcheck(ofTypeId, GetUserId(), ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error_type(aclresult, ofTypeId);
	}
	else
		ofTypeId = InvalidOid;

	/*
	 * Look up inheritance ancestors and generate relation schema, including
	 * inherited attributes.  (Note that stmt->tableElts is destructively
	 * modified by MergeAttributes.)
	 */
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
	{
		schema =
			MergeAttributes(schema, inheritOids,
							stmt->relation->relpersistence,
							stmt->partbound != NULL,
							&old_constraints);
	}
	else
	{
		/*
		 * In QE mode, we already extracted all the constraints, inherited
		 * or not, from tableElts at the beginning of the function.
		 */
		old_constraints = NIL;
	}

	/*
	 * Create a tuple descriptor from the relation schema.  Note that this
	 * deals with column names, types, and NOT NULL constraints, but not
	 * default values or CHECK constraints; we handle those below.
	 */
	descriptor = BuildDescForRelation(schema);

	/*
	 * now that we have the final list of attributes, interpret DISTRIBUTED BY
	 * column names into a GpPolicy
	 */
	if (intoPolicy)
	{
		policy = intoPolicy;
	}
	else
		policy = getPolicyForDistributedBy(stmt->distributedBy, descriptor);

	if (partitioned && GpPolicyIsReplicated(policy))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("PARTITION BY clause cannot be used with DISTRIBUTED REPLICATED clause")));

	/*
	 * Find columns with default values and prepare for insertion of the
	 * defaults.  Pre-cooked (that is, inherited) defaults go into a list of
	 * CookedConstraint structs that we'll pass to heap_create_with_catalog,
	 * while raw defaults go into a list of RawColumnDefault structs that will
	 * be processed by AddRelationNewConstraints.  (We can't deal with raw
	 * expressions until we can do transformExpr.)
	 *
	 * We can set the atthasdef flags now in the tuple descriptor; this just
	 * saves StoreAttrDefault from having to do an immediate update of the
	 * pg_attribute rows.
	 */
	rawDefaults = NIL;
	cookedDefaults = NIL;
	attnum = 0;

	foreach(listptr, schema)
	{
		ColumnDef  *colDef = lfirst(listptr);
		Form_pg_attribute attr;

		attnum++;
		attr = TupleDescAttr(descriptor, attnum - 1);

		if (colDef->raw_default != NULL)
		{
			RawColumnDefault *rawEnt;

			Assert(colDef->cooked_default == NULL);

			rawEnt = (RawColumnDefault *) palloc(sizeof(RawColumnDefault));
			rawEnt->attnum = attnum;
			rawEnt->raw_default = colDef->raw_default;
			rawEnt->missingMode = false;
			rawEnt->generated = colDef->generated;
			rawDefaults = lappend(rawDefaults, rawEnt);
			attr->atthasdef = true;
		}
		else if (colDef->cooked_default != NULL)
		{
			CookedConstraint *cooked;

			cooked = (CookedConstraint *) palloc(sizeof(CookedConstraint));
			cooked->contype = CONSTR_DEFAULT;
			cooked->conoid = InvalidOid;	/* until created */
			cooked->name = NULL;
			cooked->attnum = attnum;
			cooked->expr = colDef->cooked_default;
			cooked->skip_validation = false;
			cooked->is_local = true;	/* not used for defaults */
			cooked->inhcount = 0;	/* ditto */
			cooked->is_no_inherit = false;
			cookedDefaults = lappend(cookedDefaults, cooked);
			attr->atthasdef = true;
		}

		if (colDef->identity)
			attr->attidentity = colDef->identity;

		if (colDef->generated)
			attr->attgenerated = colDef->generated;

		if (colDef->compression)
			attr->attcompression = GetAttributeCompression(attr->atttypid,
														   colDef->compression);
	}

	/*
	 * Analyze AOCS attribute encoding clauses.
	 *
	 * Ideally this could have happened even later confined in
	 * AddRelationAttributeEncodings(). However, since this function can
	 * legitimately error out, it is prefered to call it before updating the
	 * catalog in heap_create_with_catalog().
	 *
	 * For RELKIND_PARTITIONED_TABLE, let the transformation of attribute
	 * encoding happen. We don't store it for parent partition in
	 * pg_attribute_encoding table. Transformed encoding will be used to
	 * create child partition create stmts, hence avoid marking it NIL as
	 * well.
	 *
	 * This is done in dispatcher (and in utility mode). In QE, we receive
	 * the already-processed options from the QD.
	 */
	if ((relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW || relkind == RELKIND_DIRECTORY_TABLE) &&
		Gp_role != GP_ROLE_EXECUTE)
	{
		const TableAmRoutine *tam = GetTableAmRoutineByAmId(accessMethodId);

		stmt->attr_encodings = transformColumnEncoding(tam, /* TableAmRoutine */
								NULL /* Relation */, 
								schema,
								stmt->attr_encodings,
								stmt->options,
								AMHandlerIsAoCols(amHandlerOid) /* createDefaultOne*/);
		if (!AMHandlerSupportEncodingClause(tam) && relkind != RELKIND_PARTITIONED_TABLE)
			stmt->attr_encodings = NIL;
	}
	else if (relkind == RELKIND_PARTITIONED_TABLE &&
			 Gp_role != GP_ROLE_EXECUTE)
	{
		/*
		 * The parent table might be AOCS, while some of the partitions
		 * are not, or vice versa, so options can make sense for some
		 * parts of the partition hierarchy, even if it doesn't for this partition.
		 *
		 * Also in this time, we can't get the access method from root partition.
		 * But for other access method which support encoding clause, still can't
		 * pass the encoding clause from root partition.
		 */
		stmt->attr_encodings = transfromColumnEncodingAocoRootPartition(schema,
								stmt->attr_encodings,
								stmt->options,
								!AMHandlerIsAoCols(amHandlerOid)
										&& !stmt->partbound
										&& !stmt->partspec /* errorOnEncodingClause */);
	}

	/*
	 * In executor mode, we received all the defaults and constraints
	 * in pre-cooked form from the QD, so forget about the lists we
	 * constructed just above, and use the old_constraints we received
	 * from the QD.
	 */
	if (Gp_role != GP_ROLE_EXECUTE)
		cooked_constraints = list_concat(cookedDefaults, old_constraints);

	/*
	 * Store the deduced options back in the CreateStmt, for later dispatch.
	 *
	 * NOTE: We do this even if !shouldDispatch, because it means that the
	 * caller will dispatch the statement later, not that we won't need to
	 * dispatch at all.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		Relation		pg_class_desc;
		Relation		pg_type_desc;
		MemoryContext	oldContext;

		/*
		 * We use a RowExclusiveLock but hold it till end of transaction so
		 * that two DDL operations will not deadlock between QEs
		 */
		pg_class_desc = heap_open(RelationRelationId, RowExclusiveLock);
		pg_type_desc = heap_open(TypeRelationId, RowExclusiveLock);

		LockRelationOid(DependRelationId, RowExclusiveLock);

		heap_close(pg_class_desc, NoLock);  /* gonna update, so don't unlock */

		stmt->relKind = relkind;

		if (!OidIsValid(stmt->ownerid))
			stmt->ownerid = ownerId;

		oldContext = MemoryContextSwitchTo(CacheMemoryContext);
		stmt->relation->schemaname = get_namespace_name(namespaceId);
		MemoryContextSwitchTo(oldContext);

		heap_close(pg_type_desc, NoLock);
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		Assert(stmt->ownerid != InvalidOid);
	}

	if (shouldDispatch)
		cdb_sync_oid_to_segments();

	bool valid_opts = !useChangedOpts;

	/*
	 * Create the relation.  Inherited defaults and constraints are passed in
	 * for immediate handling --- since they don't need parsing, they can be
	 * stored immediately.
	 */
	relationId = heap_create_with_catalog(relname,
										  namespaceId,
										  tablespaceId,
										  InvalidOid,
										  InvalidOid,
										  ofTypeId,
										  ownerId,
										  accessMethodId,
										  descriptor,
										  cooked_constraints,
										  relkind,
										  stmt->relation->relpersistence,
										  tablespaceId==GLOBALTABLESPACE_OID,
										  false,
										  stmt->oncommit,
                                          policy,  /*CDB*/
										  reloptions,
										  stmt->partbound?false:true,
										  allowSystemTableMods,
										  false,
										  InvalidOid,
										  typaddress,
										  valid_opts);

	/*
	 * Create tag description.
	 */
	if (stmt->tags &&
		stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
	{
		
		AddTagDescriptions(stmt->tags,
						   MyDatabaseId,
						   RelationRelationId,
						   relationId,
						   relname);
	}
	
	/*
	 * We must bump the command counter to make the newly-created relation
	 * tuple visible for opening.
	 */
	CommandCounterIncrement();

	/*
	 * Open the new relation and acquire exclusive lock on it.  This isn't
	 * really necessary for locking out other backends (since they can't see
	 * the new rel anyway until we commit), but it keeps the lock manager from
	 * complaining about deadlock risks.
	 */
	rel = relation_open(relationId, AccessExclusiveLock);

	if (relkind == RELKIND_DIRECTORY_TABLE)
	{
		CreateDirectoryTableIndex(rel);
	}

	/*
	 * If this is an append-only relation, create the auxliary tables necessary
	 */
	if (RelationIsAppendOptimized(rel))
		NewRelationCreateAOAuxTables(RelationGetRelid(rel), stmt->buildAoBlkdir);

	/*
	 * Now add any newly specified column default and generation expressions
	 * to the new relation.  These are passed to us in the form of raw
	 * parsetrees; we need to transform them to executable expression trees
	 * before they can be added. The most convenient way to do that is to
	 * apply the parser's transformExpr routine, but transformExpr doesn't
	 * work unless we have a pre-existing relation. So, the transformation has
	 * to be postponed to this final step of CREATE TABLE.
	 *
	 * This needs to be before processing the partitioning clauses because
	 * those could refer to generated columns.
	 */
	if (Gp_role != GP_ROLE_EXECUTE && rawDefaults)
	{
		List	   *newCookedDefaults;

		newCookedDefaults =
			AddRelationNewConstraints(rel, rawDefaults, NIL,
									  true, true, false, queryString);

		cooked_constraints = list_concat(cooked_constraints, newCookedDefaults);
	}

	if (stmt->attr_encodings && (relkind != RELKIND_PARTITIONED_TABLE))
		AddRelationAttributeEncodings(rel, stmt->attr_encodings);

	/*
	 * Make column generation expressions visible for use by partitioning.
	 */
	CommandCounterIncrement();

	/* Process and store partition bound, if any. */
	if (stmt->partbound)
	{
		PartitionBoundSpec *bound;
		ParseState *pstate;
		Oid			parentId = linitial_oid(inheritOids),
					defaultPartOid;
		Relation	parent,
					defaultRel = NULL;
		ParseNamespaceItem *nsitem;

		/* Already have strong enough lock on the parent */
		parent = table_open(parentId, NoLock);

		/*
		 * We are going to try to validate the partition bound specification
		 * against the partition key of parentRel, so it better have one.
		 */
		if (parent->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("\"%s\" is not partitioned",
							RelationGetRelationName(parent))));

		/*
		 * The partition constraint of the default partition depends on the
		 * partition bounds of every other partition. It is possible that
		 * another backend might be about to execute a query on the default
		 * partition table, and that the query relies on previously cached
		 * default partition constraints. We must therefore take a table lock
		 * strong enough to prevent all queries on the default partition from
		 * proceeding until we commit and send out a shared-cache-inval notice
		 * that will make them update their index lists.
		 *
		 * Order of locking: The relation being added won't be visible to
		 * other backends until it is committed, hence here in
		 * DefineRelation() the order of locking the default partition and the
		 * relation being added does not matter. But at all other places we
		 * need to lock the default relation before we lock the relation being
		 * added or removed i.e. we should take the lock in same order at all
		 * the places such that lock parent, lock default partition and then
		 * lock the partition so as to avoid a deadlock.
		 */
		defaultPartOid =
			get_default_oid_from_partdesc(RelationGetPartitionDesc(parent,
																   true));
		if (OidIsValid(defaultPartOid))
			defaultRel = table_open(defaultPartOid, AccessExclusiveLock);

		/* Transform the bound values */
		pstate = make_parsestate(NULL);
		pstate->p_sourcetext = queryString;

		/*
		 * Add an nsitem containing this relation, so that transformExpr
		 * called on partition bound expressions is able to report errors
		 * using a proper context.
		 */
		nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
											   NULL, false, false);
		addNSItemToQuery(pstate, nsitem, false, true, true);

		bound = transformPartitionBound(pstate, parent, RelationGetPartitionKey(parent), stmt->partbound);

		/*
		 * Check first that the new partition's bound is valid and does not
		 * overlap with any of existing partitions of the parent.
		 */
		check_new_partition_bound(relname, parent, bound, pstate);

		/*
		 * If the default partition exists, its partition constraints will
		 * change after the addition of this new partition such that it won't
		 * allow any row that qualifies for this new partition. So, check that
		 * the existing data in the default partition satisfies the constraint
		 * as it will exist after adding this partition.
		 */
		if (OidIsValid(defaultPartOid))
		{
			check_default_partition_contents(parent, defaultRel, bound);
			/* Keep the lock until commit. */
			table_close(defaultRel, NoLock);
		}

		/* Update the pg_class entry. */
		StorePartitionBound(rel, parent, bound);

		table_close(parent, NoLock);

		/*
		 * GPDB inherits the ACLs from parent during creation.
		 */
		CopyRelationAcls(parentId, relationId);
	}

	/* Store inheritance information for new rel. */
	StoreCatalogInheritance(relationId, inheritOids, stmt->partbound != NULL);

	/*
	 * Process the partitioning specification (if any) and store the partition
	 * key information into the catalog.
	 */
	if (partitioned)
	{
		ParseState *pstate;
		char		strategy;
		int			partnatts;
		AttrNumber	partattrs[PARTITION_MAX_KEYS];
		Oid			partopclass[PARTITION_MAX_KEYS];
		Oid			partcollation[PARTITION_MAX_KEYS];
		List	   *partexprs = NIL;

		pstate = make_parsestate(NULL);
		pstate->p_sourcetext = queryString;

		partnatts = list_length(stmt->partspec->partParams);

		/* Protect fixed-size arrays here and in executor */
		if (partnatts > PARTITION_MAX_KEYS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("cannot partition using more than %d columns",
							PARTITION_MAX_KEYS)));

		/*
		 * We need to transform the raw parsetrees corresponding to partition
		 * expressions into executable expression trees.  Like column defaults
		 * and CHECK constraints, we could not have done the transformation
		 * earlier.
		 */
		stmt->partspec = transformPartitionSpec(rel, stmt->partspec,
												&strategy);

		ComputePartitionAttrs(pstate, rel, stmt->partspec->partParams,
							  partattrs, &partexprs, partopclass,
							  partcollation, strategy);

		StorePartitionKey(rel, strategy, partnatts, partattrs, partexprs,
						  partopclass, partcollation);

		/* make it all visible */
		CommandCounterIncrement();
	}

	/*
	 * If we're creating a partition, create now all the indexes, triggers,
	 * FKs defined in the parent.
	 *
	 * We can't do it earlier, because DefineIndex wants to know the partition
	 * key which we just stored.
	 */
	if (stmt->partbound)
	{
		Oid			parentId = linitial_oid(inheritOids);
		Relation	parent;
		List	   *idxlist;
		ListCell   *cell;

		/* Already have strong enough lock on the parent */
		parent = table_open(parentId, NoLock);
		idxlist = RelationGetIndexList(parent);

		/*
		 * For each index in the parent table, create one in the partition
		 */
		foreach(cell, idxlist)
		{
			Relation	idxRel = index_open(lfirst_oid(cell), AccessShareLock);
			AttrMap    *attmap;
			IndexStmt  *idxstmt;
			Oid			constraintOid;

			if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			{
				if (idxRel->rd_index->indisunique)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot create foreign partition of partitioned table \"%s\"",
									RelationGetRelationName(parent)),
							 errdetail("Table \"%s\" contains indexes that are unique.",
									   RelationGetRelationName(parent))));
				else
				{
					index_close(idxRel, AccessShareLock);
					continue;
				}
			}

			attmap = build_attrmap_by_name(RelationGetDescr(rel),
										   RelationGetDescr(parent));
			idxstmt =
				generateClonedIndexStmt(NULL, idxRel,
										attmap, &constraintOid);

			/*
			 * In QE, we cannot independently choose index names. We must use
			 * the same names as were chosen in the QD. The QD stashed the
			 * names in the CreateStmt->part_idx_oids/names lists, dig them
			 * up from there.
			 */
			if (Gp_role == GP_ROLE_EXECUTE)
			{
				ListCell   *lc_oid,
						   *lc_name;

				forboth(lc_oid, stmt->part_idx_oids,
						lc_name, stmt->part_idx_names)
				{
					if (lfirst_oid(lc_oid) == RelationGetRelid(idxRel))
					{
						idxstmt->idxname = strVal(lfirst(lc_name));
						break;
					}
				}
				if (!idxstmt->idxname)
					elog(ERROR, "did not receive index name from QD for index %s on partition %s",
						 RelationGetRelationName(idxRel),
						 RelationGetRelationName(rel));
			}

			DefineIndex(RelationGetRelid(rel),
						idxstmt,
						InvalidOid,
						RelationGetRelid(idxRel),
						constraintOid,
						false, false, false, false, false);

			if (Gp_role == GP_ROLE_DISPATCH)
			{
				stmt->part_idx_oids = lappend_oid(stmt->part_idx_oids, RelationGetRelid(idxRel));
				stmt->part_idx_names = lappend(stmt->part_idx_names, makeString(idxstmt->idxname));
			}

			index_close(idxRel, AccessShareLock);
		}

		list_free(idxlist);

		/*
		 * If there are any row-level triggers, clone them to the new
		 * partition.
		 */
		if (parent->trigdesc != NULL)
			CloneRowTriggersToPartition(parent, rel);

		/*
		 * And foreign keys too.  Note that because we're freshly creating the
		 * table, there is no need to verify these new constraints.
		 */
		CloneForeignKeyConstraints(NULL, parent, rel);

		table_close(parent, NoLock);
	}

	/*
	 * Now add any newly specified CHECK constraints to the new relation. Same
	 * as for defaults above, but these need to come after partitioning is set
	 * up.
	 */
	if (Gp_role != GP_ROLE_EXECUTE && stmt->constraints)
	{
		List	   *newCookedDefaults;

		newCookedDefaults =
			AddRelationNewConstraints(rel, NIL, stmt->constraints,
									  true, true, false, queryString);

		cooked_constraints = list_concat(cooked_constraints, newCookedDefaults);
	}

	ObjectAddressSet(address, RelationRelationId, relationId);

	/*
	 * Transfer any inherited CHECK constraints back to the statement, so
	 * that they are dispatched to QE nodes along with the statement
	 * itself. This way, the QE nodes don't need to repeat the processing
	 * above, which reduces the risk that they would interpret the defaults
	 * or constraints somehow differently.
	 *
	 * NOTE: We do this even if !shouldDispatch, because it means that the
	 * caller will dispatch the statement later, not that we won't need to
	 * dispatch at all.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		stmt->tableElts = schema;

		foreach(listptr, cooked_constraints)
		{
			CookedConstraint *cooked = (CookedConstraint *) lfirst(listptr);

			/*
			 * In PostgreSQL, CookedConstraint is not a regular struct, not
			 * "node", so MergeAttributes and friends that above created
			 * the CookedConstraints have not set the node tag. Set it now.
			 */
			cooked->type = T_CookedConstraint;

			stmt->tableElts = lappend(stmt->tableElts, cooked);
		}
	}

	/* It is now safe to dispatch */
	if (shouldDispatch)
	{
		/*
		 * Dispatch the statement tree to all primary and mirror segdbs.
		 * Doesn't wait for the QEs to finish execution.
		 *
		 * The OIDs are carried out-of-band.
		 */

		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR |
									DF_NEED_TWO_PHASE |
									DF_WITH_SNAPSHOT,
									GetAssignedOidsForDispatch(),
									NULL);
	}

	/* MPP-6929: metadata tracking */
	if (stmt->partbound && Gp_role == GP_ROLE_DISPATCH)
	{
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "PARTITION", "ATTACH");
	}

	/*
	 * Clean up.  We keep lock on new relation (although it shouldn't be
	 * visible to anyone else anyway, until commit).
	 */
	relation_close(rel, NoLock);

	return address;
}

/* Don't track internal namespaces for toast, bitmap, aoseg */
#define METATRACK_VALIDNAMESPACE(namespaceId) \
	(namespaceId != PG_TOAST_NAMESPACE &&	\
	 namespaceId != PG_BITMAPINDEX_NAMESPACE && \
	 namespaceId != PG_EXTAUX_NAMESPACE && \
	 namespaceId != PG_AOSEGMENT_NAMESPACE )

/* check for valid namespace and valid relkind */
static bool
MetaTrackValidKindNsp(Form_pg_class rd_rel)
{
	Oid nsp = rd_rel->relnamespace;

	if (PG_CATALOG_NAMESPACE == nsp)
	{
		/*
		 * MPP-7773: don't track objects in system namespace
		 * if modifying system tables (eg during upgrade)  
		 */
		if (allowSystemTableMods)
			return false;
	}

	/* MPP-7599: watch out for toast indexes */
	return (METATRACK_VALIDNAMESPACE(nsp)
			&& MetaTrackValidRelkind(rd_rel->relkind)
			/* MPP-7572: not valid if in any temporary namespace */
			&& (!(isAnyTempNamespace(nsp))));
}

/*
 * Emit the right error or warning message for a "DROP" command issued on a
 * non-existent relation
 */
static void
DropErrorMsgNonExistent(RangeVar *rel, char rightkind, bool missing_ok)
{
	const struct dropmsgstrings *rentry;

	if (rel->schemaname != NULL &&
		!OidIsValid(LookupNamespaceNoError(rel->schemaname)))
	{
		if (!missing_ok)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_SCHEMA),
					 errmsg("schema \"%s\" does not exist", rel->schemaname)));
		}
		else
		{
			ereport(NOTICE,
					(errmsg("schema \"%s\" does not exist, skipping",
							rel->schemaname)));
		}
		return;
	}

	for (rentry = dropmsgstringarray; rentry->kind != '\0'; rentry++)
	{
		if (rentry->kind == rightkind)
		{
			if (!missing_ok)
			{
				ereport(ERROR,
						(errcode(rentry->nonexistent_code),
						 errmsg(rentry->nonexistent_msg, rel->relname)));
			}
			else
			{
				if (Gp_role != GP_ROLE_EXECUTE)
					ereport(NOTICE, (errmsg(rentry->skipping_msg, rel->relname)));
				break;
			}
		}
	}

	Assert(rentry->kind != '\0');	/* Should be impossible */
}

/*
 * Emit the right error message for a "DROP" command issued on a
 * relation of the wrong type
 */
static void
DropErrorMsgWrongType(const char *relname, char wrongkind, char rightkind)
{
	const struct dropmsgstrings *rentry;
	const struct dropmsgstrings *wentry;

	for (rentry = dropmsgstringarray; rentry->kind != '\0'; rentry++)
		if (rentry->kind == rightkind)
			break;
	Assert(rentry->kind != '\0');

	for (wentry = dropmsgstringarray; wentry->kind != '\0'; wentry++)
		if (wentry->kind == wrongkind)
			break;
	/* wrongkind could be something we don't have in our table... */

	ereport(ERROR,
			(errcode(ERRCODE_WRONG_OBJECT_TYPE),
			 errmsg(rentry->nota_msg, relname),
			 (wentry->kind != '\0') ? errhint("%s", _(wentry->drophint_msg)) : 0));
}

/*
 * RemoveRelations
 *		Implements DROP TABLE, DROP INDEX, DROP SEQUENCE, DROP VIEW,
 *		DROP MATERIALIZED VIEW, DROP FOREIGN TABLE
 */
void
RemoveRelations(DropStmt *drop)
{
	ObjectAddresses *objects;
	char		relkind;
	ListCell   *cell;
	int			flags = 0;
	LOCKMODE	lockmode = AccessExclusiveLock;
	HeapTuple	indexTuple;
	Form_pg_index index;

	/* DROP CONCURRENTLY uses a weaker lock, and has some restrictions */
	if (drop->concurrent)
	{
		/*
		 * Note that for temporary relations this lock may get upgraded later
		 * on, but as no other session can access a temporary relation, this
		 * is actually fine.
		 */
		lockmode = ShareUpdateExclusiveLock;
		Assert(drop->removeType == OBJECT_INDEX);
		if (list_length(drop->objects) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DROP INDEX CONCURRENTLY does not support dropping multiple objects")));
		if (drop->behavior == DROP_CASCADE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DROP INDEX CONCURRENTLY does not support CASCADE")));
	}

	/*
	 * First we identify all the relations, then we delete them in a single
	 * performMultipleDeletions() call.  This is to avoid unwanted DROP
	 * RESTRICT errors if one of the relations depends on another.
	 */

	/* Determine required relkind */
	switch (drop->removeType)
	{
		case OBJECT_TABLE:
			relkind = RELKIND_RELATION;
			break;

		case OBJECT_INDEX:
			relkind = RELKIND_INDEX;
			break;

		case OBJECT_SEQUENCE:
			relkind = RELKIND_SEQUENCE;
			break;

		case OBJECT_VIEW:
			relkind = RELKIND_VIEW;
			break;

		case OBJECT_MATVIEW:
			relkind = RELKIND_MATVIEW;
			break;

		case OBJECT_FOREIGN_TABLE:
			relkind = RELKIND_FOREIGN_TABLE;
			break;

		case OBJECT_DIRECTORY_TABLE:
			relkind = RELKIND_DIRECTORY_TABLE;
			break;

		default:
			elog(ERROR, "unrecognized drop object type: %d",
				 (int) drop->removeType);
			relkind = 0;		/* keep compiler quiet */
			break;
	}

	/* Lock and validate each relation; build a list of object addresses */
	objects = new_object_addresses();

	foreach(cell, drop->objects)
	{
		if (relkind == RELKIND_DIRECTORY_TABLE)
		{
			DropDirectoryTableStmt *dirtable_drop =  (DropDirectoryTableStmt *) drop;
			if (dirtable_drop->with_content)
				flags |= PERFORM_DELETION_WITH_CONTENT;
		}

		RangeVar   *rel = makeRangeVarFromNameList((List *) lfirst(cell));
		Oid			relOid;
		ObjectAddress obj;
		struct DropRelationCallbackState state;

		/*
		 * These next few steps are a great deal like relation_openrv, but we
		 * don't bother building a relcache entry since we don't need it.
		 *
		 * Check for shared-cache-inval messages before trying to access the
		 * relation.  This is needed to cover the case where the name
		 * identifies a rel that has been dropped and recreated since the
		 * start of our transaction: if we don't flush the old syscache entry,
		 * then we'll latch onto that entry and suffer an error later.
		 */
		AcceptInvalidationMessages();

		/* Look up the appropriate relation using namespace search. */
		state.expected_relkind = relkind;
		state.heap_lockmode = drop->concurrent ?
			ShareUpdateExclusiveLock : AccessExclusiveLock;
		/* We must initialize these fields to show that no locks are held: */
		state.heapOid = InvalidOid;
		state.partParentOid = InvalidOid;

		relOid = RangeVarGetRelidExtended(rel, lockmode, RVR_MISSING_OK,
										  RangeVarCallbackForDropRelation,
										  (void *) &state);

		/* Not there? */
		if (!OidIsValid(relOid))
		{
			DropErrorMsgNonExistent(rel, relkind, drop->missing_ok);
			continue;
		}

		if (relkind == RELKIND_INDEX)
		{
			indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(relOid));
			if (!HeapTupleIsValid(indexTuple) && !drop->missing_ok)	/* should not happen */
				elog(ERROR, "cache lookup failed for index %u", relOid);
			if (HeapTupleIsValid(indexTuple))
			{
				index = (Form_pg_index) GETSTRUCT(indexTuple);
				if (RelationIsDirectoryTable(index->indrelid) && index->indisprimary)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("Disallowed to drop primary index \"%s\" on directory table \"%s\"",
							 get_rel_name(index->indexrelid), get_rel_name(index->indrelid))));
				ReleaseSysCache(indexTuple);
			}
		}

		/*
		 * Decide if concurrent mode needs to be used here or not.  The
		 * callback retrieved the rel's persistence for us.
		 */
		if (drop->concurrent &&
			state.actual_relpersistence != RELPERSISTENCE_TEMP)
		{
			Assert(list_length(drop->objects) == 1 &&
				   drop->removeType == OBJECT_INDEX);
			flags |= PERFORM_DELETION_CONCURRENTLY;
		}

		/*
		 * Concurrent index drop cannot be used with partitioned indexes,
		 * either.
		 */
		if ((flags & PERFORM_DELETION_CONCURRENTLY) != 0 &&
			state.actual_relkind == RELKIND_PARTITIONED_INDEX)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot drop partitioned index \"%s\" concurrently",
							rel->relname)));

		/*
		 * If we're told to drop a partitioned index, we must acquire lock on
		 * all the children of its parent partitioned table before proceeding.
		 * Otherwise we'd try to lock the child index partitions before their
		 * tables, leading to potential deadlock against other sessions that
		 * will lock those objects in the other order.
		 */
		if (state.actual_relkind == RELKIND_PARTITIONED_INDEX)
			(void) find_all_inheritors(state.heapOid,
									   state.heap_lockmode,
									   NULL);

		/* OK, we're ready to delete this one */
		obj.classId = RelationRelationId;
		obj.objectId = relOid;
		obj.objectSubId = 0;

		add_exact_object_address(&obj, objects);
	}

	performMultipleDeletions(objects, drop->behavior, flags);

	free_object_addresses(objects);
}

static void
relid_set_new_relfilenode(Oid relid)
{
	if (OidIsValid(relid))
	{
		Relation rel;

		rel = relation_open(relid, AccessExclusiveLock);
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
		heap_close(rel, NoLock);
	}
}

static void
ao_aux_tables_safe_truncate(Relation rel)
{
	if (!RelationIsAppendOptimized(rel))
		return;

	Oid relid = RelationGetRelid(rel);

	Oid aoseg_relid = InvalidOid;
	Oid aoblkdir_relid = InvalidOid;
	Oid aovisimap_relid = InvalidOid;

	GetAppendOnlyEntryAuxOids(rel, &aoseg_relid,
							  &aoblkdir_relid, NULL, &aovisimap_relid,
							  NULL);

	relid_set_new_relfilenode(aoseg_relid);
	relid_set_new_relfilenode(aoblkdir_relid);
	relid_set_new_relfilenode(aovisimap_relid);

	/*
	 * Reset existing gp_fastsequence entries for the segrel to an initial entry.
	 * This mimics the state of the gp_fastsequence row when an empty AO/AOCS
	 * table is created.
	 */
	RemoveFastSequenceEntry(aoseg_relid);
	InsertInitialFastSequenceEntries(aoseg_relid);

	/* GPDB truncate should also update pg_appendonly.segfilecount */
	Relation	aorel;
	HeapTuple	aotup;
	Form_pg_appendonly aoform;

	aotup = SearchSysCache1(AORELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(aotup))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			errmsg("appendonly table relid %u does not exist in pg_appendonly", relid)));

	aoform = (Form_pg_appendonly) GETSTRUCT(aotup);
	if (aoform->segfilecount != 0)
	{
		aorel = table_open(AppendOnlyRelationId, RowExclusiveLock);
		aoform->segfilecount = 0;
		heap_inplace_update(aorel, aotup);
		table_close(aorel, RowExclusiveLock);
	}
	ReleaseSysCache(aotup);
}

/*
 * Before acquiring a table lock, check whether we have sufficient rights.
 * In the case of DROP INDEX, also try to lock the table before the index.
 * Also, if the table to be dropped is a partition, we try to lock the parent
 * first.
 */
static void
RangeVarCallbackForDropRelation(const RangeVar *rel, Oid relOid, Oid oldRelOid,
								void *arg)
{
	HeapTuple	tuple;
	struct DropRelationCallbackState *state;
	char		expected_relkind;
	bool		is_partition;
	Form_pg_class classform;
	LOCKMODE	heap_lockmode;
	bool		invalid_system_index = false;

	state = (struct DropRelationCallbackState *) arg;
	heap_lockmode = state->heap_lockmode;

	/*
	 * If we previously locked some other index's heap, and the name we're
	 * looking up no longer refers to that relation, release the now-useless
	 * lock.
	 */
	if (relOid != oldRelOid && OidIsValid(state->heapOid))
	{
		UnlockRelationOid(state->heapOid, heap_lockmode);
		state->heapOid = InvalidOid;
	}

	/*
	 * Similarly, if we previously locked some other partition's heap, and the
	 * name we're looking up no longer refers to that relation, release the
	 * now-useless lock.
	 */
	if (relOid != oldRelOid && OidIsValid(state->partParentOid))
	{
		UnlockRelationOid(state->partParentOid, AccessExclusiveLock);
		state->partParentOid = InvalidOid;
	}

	/* Didn't find a relation, so no need for locking or permission checks. */
	if (!OidIsValid(relOid))
		return;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
	if (!HeapTupleIsValid(tuple))
		return;					/* concurrently dropped, so nothing to do */
	classform = (Form_pg_class) GETSTRUCT(tuple);
	is_partition = classform->relispartition;

	/* Pass back some data to save lookups in RemoveRelations */
	state->actual_relkind = classform->relkind;
	state->actual_relpersistence = classform->relpersistence;

	/*
	 * Both RELKIND_RELATION and RELKIND_PARTITIONED_TABLE are OBJECT_TABLE,
	 * but RemoveRelations() can only pass one relkind for a given relation.
	 * It chooses RELKIND_RELATION for both regular and partitioned tables.
	 * That means we must be careful before giving the wrong type error when
	 * the relation is RELKIND_PARTITIONED_TABLE.  An equivalent problem
	 * exists with indexes.
	 */
	if (classform->relkind == RELKIND_PARTITIONED_TABLE)
		expected_relkind = RELKIND_RELATION;
	else if (classform->relkind == RELKIND_PARTITIONED_INDEX)
		expected_relkind = RELKIND_INDEX;
	else
		expected_relkind = classform->relkind;

	if (state->expected_relkind != expected_relkind)
		DropErrorMsgWrongType(rel->relname, classform->relkind,
							  state->expected_relkind);

	/* Allow DROP to either table owner or schema owner */
	if (!pg_class_ownercheck(relOid, GetUserId()) &&
		!pg_namespace_ownercheck(classform->relnamespace, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(classform->relkind),
					   rel->relname);

	/*
	 * Check the case of a system index that might have been invalidated by a
	 * failed concurrent process and allow its drop. For the time being, this
	 * only concerns indexes of toast relations that became invalid during a
	 * REINDEX CONCURRENTLY process.
	 */
	if (IsSystemClass(relOid, classform) && classform->relkind == RELKIND_INDEX)
	{
		HeapTuple	locTuple;
		Form_pg_index indexform;
		bool		indisvalid;

		locTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(relOid));
		if (!HeapTupleIsValid(locTuple))
		{
			ReleaseSysCache(tuple);
			return;
		}

		indexform = (Form_pg_index) GETSTRUCT(locTuple);
		indisvalid = indexform->indisvalid;
		ReleaseSysCache(locTuple);

		/* Mark object as being an invalid index of system catalogs */
		if (!indisvalid)
			invalid_system_index = true;
	}

	/* In the case of an invalid index, it is fine to bypass this check */
	if (!invalid_system_index && !allowSystemTableMods && IsSystemClass(relOid, classform))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						rel->relname)));

	ReleaseSysCache(tuple);

	/*
	 * In DROP INDEX, attempt to acquire lock on the parent table before
	 * locking the index.  index_drop() will need this anyway, and since
	 * regular queries lock tables before their indexes, we risk deadlock if
	 * we do it the other way around.  No error if we don't find a pg_index
	 * entry, though --- the relation may have been dropped.  Note that this
	 * code will execute for either plain or partitioned indexes.
	 */
	if (expected_relkind == RELKIND_INDEX &&
		relOid != oldRelOid)
	{
		state->heapOid = IndexGetRelation(relOid, true);
		if (OidIsValid(state->heapOid))
			LockRelationOid(state->heapOid, heap_lockmode);
	}

	/*
	 * Similarly, if the relation is a partition, we must acquire lock on its
	 * parent before locking the partition.  That's because queries lock the
	 * parent before its partitions, so we risk deadlock if we do it the other
	 * way around.
	 */
	if (is_partition && relOid != oldRelOid)
	{
		state->partParentOid = get_partition_parent(relOid, true);
		if (OidIsValid(state->partParentOid))
			LockRelationOid(state->partParentOid, AccessExclusiveLock);
	}
}

/*
 * ExecuteTruncate
 *		Executes a TRUNCATE command.
 *
 * This is a multi-relation truncate.  We first open and grab exclusive
 * lock on all relations involved, checking permissions and otherwise
 * verifying that the relation is OK for truncation.  Note that if relations
 * are foreign tables, at this stage, we have not yet checked that their
 * foreign data in external data sources are OK for truncation.  These are
 * checked when foreign data are actually truncated later.  In CASCADE mode,
 * relations having FK references to the targeted relations are automatically
 * added to the group; in RESTRICT mode, we check that all FK references are
 * internal to the group that's being truncated.  Finally all the relations
 * are truncated and reindexed.
 */
void
ExecuteTruncate(TruncateStmt *stmt)
{
	List	   *rels = NIL;
	List	   *relids = NIL;
	List	   *relids_logged = NIL;
	ListCell   *cell;

	/*
	 * Open, exclusive-lock, and check all the explicitly-specified relations
	 */
	foreach(cell, stmt->relations)
	{
		RangeVar   *rv = lfirst(cell);
		Relation	rel;
		bool		recurse = rv->inh;
		Oid			myrelid;
		LOCKMODE	lockmode = AccessExclusiveLock;

		myrelid = RangeVarGetRelidExtended(rv, lockmode,
										   0, RangeVarCallbackForTruncate,
										   NULL);

		/* don't throw error for "TRUNCATE foo, foo" */
		if (list_member_oid(relids, myrelid))
			continue;

		/* open the relation, we already hold a lock on it */
		rel = table_open(myrelid, NoLock);

		/*
		 * RangeVarGetRelidExtended() has done most checks with its callback,
		 * but other checks with the now-opened Relation remain.
		 */
		truncate_check_activity(rel);

		rels = lappend(rels, rel);
		relids = lappend_oid(relids, myrelid);

		/* Log this relation only if needed for logical decoding */
		if (RelationIsLogicallyLogged(rel))
			relids_logged = lappend_oid(relids_logged, myrelid);

		if (recurse)
		{
			ListCell   *child;
			List	   *children;

			children = find_all_inheritors(myrelid, lockmode, NULL);

			foreach(child, children)
			{
				Oid			childrelid = lfirst_oid(child);

				if (list_member_oid(relids, childrelid))
					continue;

				/* find_all_inheritors already got lock */
				rel = table_open(childrelid, NoLock);

				/*
				 * It is possible that the parent table has children that are
				 * temp tables of other backends.  We cannot safely access
				 * such tables (because of buffering issues), and the best
				 * thing to do is to silently ignore them.  Note that this
				 * check is the same as one of the checks done in
				 * truncate_check_activity() called below, still it is kept
				 * here for simplicity.
				 */
				if (RELATION_IS_OTHER_TEMP(rel))
				{
					table_close(rel, lockmode);
					continue;
				}

				/*
				 * Inherited TRUNCATE commands perform access permission
				 * checks on the parent table only. So we skip checking the
				 * children's permissions and don't call
				 * truncate_check_perms() here.
				 */
				truncate_check_rel(RelationGetRelid(rel), rel->rd_rel);
				truncate_check_activity(rel);

				rels = lappend(rels, rel);
				relids = lappend_oid(relids, childrelid);

				/* Log this relation only if needed for logical decoding */
				if (RelationIsLogicallyLogged(rel))
					relids_logged = lappend_oid(relids_logged, childrelid);
			}
		}
		else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot truncate only a partitioned table"),
					 errhint("Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions directly.")));
	}

	ExecuteTruncateGuts(rels, relids, relids_logged,
						stmt->behavior, stmt->restart_seqs, stmt);

	/* And close the rels */
	foreach(cell, rels)
	{
		Relation	rel = (Relation) lfirst(cell);

		table_close(rel, NoLock);
	}
}

/*
 * ExecuteTruncateGuts
 *
 * Internal implementation of TRUNCATE.  This is called by the actual TRUNCATE
 * command (see above) as well as replication subscribers that execute a
 * replicated TRUNCATE action.
 *
 * explicit_rels is the list of Relations to truncate that the command
 * specified.  relids is the list of Oids corresponding to explicit_rels.
 * relids_logged is the list of Oids (a subset of relids) that require
 * WAL-logging.  This is all a bit redundant, but the existing callers have
 * this information handy in this form.
 */
void
ExecuteTruncateGuts(List *explicit_rels,
					List *relids,
					List *relids_logged,
					DropBehavior behavior,
					bool restart_seqs,
					TruncateStmt *stmt)
{
	List	   *rels;
	List	   *seq_relids = NIL;
	HTAB	   *ft_htab = NULL;
	EState	   *estate;
	ResultRelInfo *resultRelInfos;
	ResultRelInfo *resultRelInfo;
	SubTransactionId mySubid;
	ListCell   *cell;
	Oid		   *logrelids;

	/*
	 * Check the explicitly-specified relations.
	 *
	 * In CASCADE mode, suck in all referencing relations as well.  This
	 * requires multiple iterations to find indirectly-dependent relations. At
	 * each phase, we need to exclusive-lock new rels before looking for their
	 * dependencies, else we might miss something.  Also, we check each rel as
	 * soon as we open it, to avoid a faux pas such as holding lock for a long
	 * time on a rel we have no permissions for.
	 */
	rels = list_copy(explicit_rels);
	if (behavior == DROP_CASCADE)
	{
		for (;;)
		{
			List	   *newrelids;

			newrelids = heap_truncate_find_FKs(relids);
			if (newrelids == NIL)
				break;			/* nothing else to add */

			foreach(cell, newrelids)
			{
				Oid			relid = lfirst_oid(cell);
				Relation	rel;

				rel = table_open(relid, AccessExclusiveLock);
				ereport(NOTICE,
						(errmsg("truncate cascades to table \"%s\"",
								RelationGetRelationName(rel))));
				truncate_check_rel(relid, rel->rd_rel);
				truncate_check_perms(relid, rel->rd_rel);
				truncate_check_activity(rel);
				rels = lappend(rels, rel);
				relids = lappend_oid(relids, relid);

				/* Log this relation only if needed for logical decoding */
				if (RelationIsLogicallyLogged(rel))
					relids_logged = lappend_oid(relids_logged, relid);
			}
		}
	}

	/* GPDB does not support all FK feature but keeps FK grammar recognition,
	 * which reduces migration manual workload from other databases.
	 * We do not want to reject relation truncate if the relation contains FK
	 * satisfied tuple, so skip heap_truncate_check_FKs function call.
	 */
#if 0
	/*
	 * Check foreign key references.  In CASCADE mode, this should be
	 * unnecessary since we just pulled in all the references; but as a
	 * cross-check, do it anyway if in an Assert-enabled build.
	 */
#ifdef USE_ASSERT_CHECKING
	heap_truncate_check_FKs(rels, false);
#else
	if (behavior == DROP_RESTRICT)
		heap_truncate_check_FKs(rels, false);
#endif
#endif

	/*
	 * If we are asked to restart sequences, find all the sequences, lock them
	 * (we need AccessExclusiveLock for ResetSequence), and check permissions.
	 * We want to do this early since it's pointless to do all the truncation
	 * work only to fail on sequence permissions.
	 */
	if (restart_seqs)
	{
		foreach(cell, rels)
		{
			Relation	rel = (Relation) lfirst(cell);
			List	   *seqlist = getOwnedSequences(RelationGetRelid(rel));
			ListCell   *seqcell;

			foreach(seqcell, seqlist)
			{
				Oid			seq_relid = lfirst_oid(seqcell);
				Relation	seq_rel;

				seq_rel = relation_open(seq_relid, AccessExclusiveLock);

				/* This check must match AlterSequence! */
				if (!pg_class_ownercheck(seq_relid, GetUserId()))
					aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SEQUENCE,
								   RelationGetRelationName(seq_rel));

				seq_relids = lappend_oid(seq_relids, seq_relid);

				relation_close(seq_rel, NoLock);
			}
		}
	}

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * To fire triggers, we'll need an EState as well as a ResultRelInfo for
	 * each relation.  We don't need to call ExecOpenIndices, though.
	 *
	 * We put the ResultRelInfos in the es_opened_result_relations list, even
	 * though we don't have a range table and don't populate the
	 * es_result_relations array.  That's a bit bogus, but it's enough to make
	 * ExecGetTriggerResultRel() find them.
	 */
	estate = CreateExecutorState();
	resultRelInfos = (ResultRelInfo *)
		palloc(list_length(rels) * sizeof(ResultRelInfo));
	resultRelInfo = resultRelInfos;
	foreach(cell, rels)
	{
		Relation	rel = (Relation) lfirst(cell);

		InitResultRelInfo(resultRelInfo,
						  rel,
						  0,	/* dummy rangetable index */
						  NULL,
						  0);
		estate->es_opened_result_relations =
			lappend(estate->es_opened_result_relations, resultRelInfo);
		resultRelInfo++;
	}

	/*
	 * Process all BEFORE STATEMENT TRUNCATE triggers before we begin
	 * truncating (this is because one of them might throw an error). Also, if
	 * we were to allow them to prevent statement execution, that would need
	 * to be handled here.
	 */
	resultRelInfo = resultRelInfos;
	foreach(cell, rels)
	{
		ExecBSTruncateTriggers(estate, resultRelInfo);
		resultRelInfo++;
	}

	/*
	 * OK, truncate each table.
	 */
	mySubid = GetCurrentSubTransactionId();

	foreach(cell, rels)
	{
		Relation	rel = (Relation) lfirst(cell);

		/* Skip partitioned tables as there is nothing to do */
		if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			continue;

		/*
		 * Build the lists of foreign tables belonging to each foreign server
		 * and pass each list to the foreign data wrapper's callback function,
		 * so that each server can truncate its all foreign tables in bulk.
		 * Each list is saved as a single entry in a hash table that uses the
		 * server OID as lookup key.
		 */
		if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		{
			Oid			serverid = GetForeignServerIdByRelId(RelationGetRelid(rel));
			bool		found;
			ForeignTruncateInfo *ft_info;

			/* First time through, initialize hashtable for foreign tables */
			if (!ft_htab)
			{
				HASHCTL		hctl;

				memset(&hctl, 0, sizeof(HASHCTL));
				hctl.keysize = sizeof(Oid);
				hctl.entrysize = sizeof(ForeignTruncateInfo);
				hctl.hcxt = CurrentMemoryContext;

				ft_htab = hash_create("TRUNCATE for Foreign Tables",
									  32,	/* start small and extend */
									  &hctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
			}

			/* Find or create cached entry for the foreign table */
			ft_info = hash_search(ft_htab, &serverid, HASH_ENTER, &found);
			if (!found)
			{
				ft_info->serverid = serverid;
				ft_info->rels = NIL;
			}

			/*
			 * Save the foreign table in the entry of the server that the
			 * foreign table belongs to.
			 */
			ft_info->rels = lappend(ft_info->rels, rel);
			continue;
		}

		/*
		 * Normally, we need a transaction-safe truncation here.  However, if
		 * the table was either created in the current (sub)transaction or has
		 * a new relfilenode in the current (sub)transaction, then we can just
		 * truncate it in-place, because a rollback would cause the whole
		 * table or the current physical file to be thrown away anyway.
		 */
		if (rel->rd_createSubid == mySubid ||
			rel->rd_newRelfilenodeSubid == mySubid)
		{
			/* Immediate, non-rollbackable truncation is OK */
			heap_truncate_one_rel(rel);
		}
		else
		{
			Oid			heap_relid;
			Oid			toast_relid;
			ReindexParams reindex_params = {0};

			/*
			 * This effectively deletes all rows in the table, and may be done
			 * in a serializable transaction.  In that case we must record a
			 * rw-conflict in to this transaction from each transaction
			 * holding a predicate lock on the table.
			 */
			CheckTableForSerializableConflictIn(rel);

			/*
			 * Need the full transaction-safe pushups.
			 *
			 * Create a new empty storage file for the relation, and assign it
			 * as the relfilenode value. The old storage file is scheduled for
			 * deletion at commit.
			 */
			RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);

			/* update view info */
			if (IS_QD_OR_SINGLENODE())
				SetRelativeMatviewAuxStatus(RelationGetRelid(rel), MV_DATA_STATUS_EXPIRED);

			heap_relid = RelationGetRelid(rel);

			/*
			 * The same for the toast table, if any.
			 */
			toast_relid = rel->rd_rel->reltoastrelid;
			if (OidIsValid(toast_relid))
			{
				Relation	toastrel = relation_open(toast_relid,
													 AccessExclusiveLock);

				RelationSetNewRelfilenode(toastrel,
										  toastrel->rd_rel->relpersistence);
				table_close(toastrel, NoLock);
			}

			/*
			 * The same for the ao auxiliary tables, if any.
			 */
			ao_aux_tables_safe_truncate(rel);

			/*
			 * Reconstruct the indexes to match, and we're done.
			 */
			reindex_relation(heap_relid, REINDEX_REL_PROCESS_TOAST,
							 &reindex_params);
		}

		pgstat_count_truncate(rel);
	}

	/* Now go through the hash table, and truncate foreign tables */
	if (ft_htab)
	{
		ForeignTruncateInfo *ft_info;
		HASH_SEQ_STATUS seq;

		hash_seq_init(&seq, ft_htab);

		PG_TRY();
		{
			while ((ft_info = hash_seq_search(&seq)) != NULL)
			{
				FdwRoutine *routine = GetFdwRoutineByServerId(ft_info->serverid);

				/* truncate_check_rel() has checked that already */
				Assert(routine->ExecForeignTruncate != NULL);

				routine->ExecForeignTruncate(ft_info->rels,
											 behavior,
											 restart_seqs);
			}
		}
		PG_FINALLY();
		{
			hash_destroy(ft_htab);
		}
		PG_END_TRY();
	}

	if (Gp_role == GP_ROLE_DISPATCH && stmt && ENABLE_DISPATCH())
	{
		ListCell	*lc;

		Assert(GetAssignedOidsForDispatch() == NIL);
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR |
									DF_WITH_SNAPSHOT |
									DF_NEED_TWO_PHASE,
									NIL,
									NULL);

		/* MPP-6929: metadata tracking */
		foreach(lc, rels)
		{
			Relation	rel = lfirst(lc);

			MetaTrackUpdObject(RelationRelationId,
							   RelationGetRelid(rel),
							   GetUserId(),
							   "VACUUM", "TRUNCATE");

			MetaTrackUpdObject(RelationRelationId,
							   RelationGetRelid(rel),
							   GetUserId(),
							   "TRUNCATE", "");
		}
	}

	/*
	 * Restart owned sequences if we were asked to.
	 */
	foreach(cell, seq_relids)
	{
		Oid			seq_relid = lfirst_oid(cell);

		ResetSequence(seq_relid);
	}

	/*
	 * Write a WAL record to allow this set of actions to be logically
	 * decoded.
	 *
	 * Assemble an array of relids so we can write a single WAL record for the
	 * whole action.
	 */
	if (list_length(relids_logged) > 0)
	{
		xl_heap_truncate xlrec;
		int			i = 0;

		/* should only get here if wal_level >= logical */
		Assert(XLogLogicalInfoActive());

		logrelids = palloc(list_length(relids_logged) * sizeof(Oid));
		foreach(cell, relids_logged)
			logrelids[i++] = lfirst_oid(cell);

		xlrec.dbId = MyDatabaseId;
		xlrec.nrelids = list_length(relids_logged);
		xlrec.flags = 0;
		if (behavior == DROP_CASCADE)
			xlrec.flags |= XLH_TRUNCATE_CASCADE;
		if (restart_seqs)
			xlrec.flags |= XLH_TRUNCATE_RESTART_SEQS;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapTruncate);
		XLogRegisterData((char *) logrelids, list_length(relids_logged) * sizeof(Oid));

		XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

		(void) XLogInsert(RM_HEAP_ID, XLOG_HEAP_TRUNCATE);
	}

	/*
	 * Process all AFTER STATEMENT TRUNCATE triggers.
	 */
	resultRelInfo = resultRelInfos;
	foreach(cell, rels)
	{
		ExecASTruncateTriggers(estate, resultRelInfo);
		resultRelInfo++;
	}

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	/* We can clean up the EState now */
	FreeExecutorState(estate);

	/*
	 * Close any rels opened by CASCADE (can't do this while EState still
	 * holds refs)
	 */
	rels = list_difference_ptr(rels, explicit_rels);
	foreach(cell, rels)
	{
		Relation	rel = (Relation) lfirst(cell);

		table_close(rel, NoLock);
	}
}

/*
 * Check that a given relation is safe to truncate.  Subroutine for
 * ExecuteTruncate() and RangeVarCallbackForTruncate().
 */
static void
truncate_check_rel(Oid relid, Form_pg_class reltuple)
{
	char	   *relname = NameStr(reltuple->relname);

	/*
	 * Only allow truncate on regular tables, foreign tables using foreign
	 * data wrappers supporting TRUNCATE and partitioned tables (although, the
	 * latter are only being included here for the following checks; no
	 * physical truncation will occur in their case.).
	 */

	/* GPDB_14_MERGE_FIXME: follow upstream logic, check foreign table first */

	if (reltuple->relkind == RELKIND_FOREIGN_TABLE)
	{
		Oid			serverid = GetForeignServerIdByRelId(relid);
		FdwRoutine *fdwroutine = GetFdwRoutineByServerId(serverid);

		if (!fdwroutine->ExecForeignTruncate)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot truncate foreign table \"%s\"",
							relname)));
	}
	else if (reltuple->relkind == RELKIND_DIRECTORY_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot truncate directory table \"%s\"",
						relname)));
	}
	else if (reltuple->relkind != RELKIND_RELATION &&
		reltuple->relkind != RELKIND_PARTITIONED_TABLE &&
		(!IsBinaryUpgrade || (
			reltuple->relkind != RELKIND_AOSEGMENTS &&
			reltuple->relkind != RELKIND_AOBLOCKDIR &&
			reltuple->relkind != RELKIND_AOVISIMAP)))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table", relname)));

	if (!allowSystemTableMods && IsSystemClass(relid, reltuple))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						relname)));

	InvokeObjectTruncateHook(relid);
}

/*
 * Check that current user has the permission to truncate given relation.
 */
static void
truncate_check_perms(Oid relid, Form_pg_class reltuple)
{
	char	   *relname = NameStr(reltuple->relname);
	AclResult	aclresult;

	/* Permissions checks */
	aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_TRUNCATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, get_relkind_objtype(reltuple->relkind),
					   relname);
}

/*
 * Set of extra sanity checks to check if a given relation is safe to
 * truncate.  This is split with truncate_check_rel() as
 * RangeVarCallbackForTruncate() cannot open a Relation yet.
 */
static void
truncate_check_activity(Relation rel)
{
	/*
	 * Don't allow truncate on temp tables of other backends ... their local
	 * buffer manager is not going to cope.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot truncate temporary tables of other sessions")));

	/*
	 * Also check for active uses of the relation in the current transaction,
	 * including open scans and pending AFTER trigger events.
	 */
	CheckTableNotInUse(rel, "TRUNCATE");
}

/*
 * storage_name
 *	  returns the name corresponding to a typstorage/attstorage enum value
 */
static const char *
storage_name(char c)
{
	switch (c)
	{
		case TYPSTORAGE_PLAIN:
			return "PLAIN";
		case TYPSTORAGE_EXTERNAL:
			return "EXTERNAL";
		case TYPSTORAGE_EXTENDED:
			return "EXTENDED";
		case TYPSTORAGE_MAIN:
			return "MAIN";
		default:
			return "???";
	}
}

/*----------
 * MergeAttributes
 *		Returns new schema given initial schema and superclasses.
 *
 * Input arguments:
 * 'schema' is the column/attribute definition for the table. (It's a list
 *		of ColumnDef's.) It is destructively changed.
 * 'supers' is a list of OIDs of parent relations, already locked by caller.
 * 'relpersistence' is the persistence type of the table.
 * 'is_partition' tells if the table is a partition.
 *
 * Output arguments:
 * 'supconstr' receives a list of constraints belonging to the parents,
 *		updated as necessary to be valid for the child.
 *
 * Return value:
 * Completed schema list.
 *
 * Notes:
 *	  The order in which the attributes are inherited is very important.
 *	  Intuitively, the inherited attributes should come first. If a table
 *	  inherits from multiple parents, the order of those attributes are
 *	  according to the order of the parents specified in CREATE TABLE.
 *
 *	  Here's an example:
 *
 *		create table person (name text, age int4, location point);
 *		create table emp (salary int4, manager text) inherits(person);
 *		create table student (gpa float8) inherits (person);
 *		create table stud_emp (percent int4) inherits (emp, student);
 *
 *	  The order of the attributes of stud_emp is:
 *
 *							person {1:name, 2:age, 3:location}
 *							/	 \
 *			   {6:gpa}	student   emp {4:salary, 5:manager}
 *							\	 /
 *						   stud_emp {7:percent}
 *
 *	   If the same attribute name appears multiple times, then it appears
 *	   in the result table in the proper location for its first appearance.
 *
 *	   Constraints (including NOT NULL constraints) for the child table
 *	   are the union of all relevant constraints, from both the child schema
 *	   and parent tables.
 *
 *	   The default value for a child column is defined as:
 *		(1) If the child schema specifies a default, that value is used.
 *		(2) If neither the child nor any parent specifies a default, then
 *			the column will not have a default.
 *		(3) If conflicting defaults are inherited from different parents
 *			(and not overridden by the child), an error is raised.
 *		(4) Otherwise the inherited default is used.
 *		Rule (3) is new in Postgres 7.1; in earlier releases you got a
 *		rather arbitrary choice of which parent default to use.
 *----------
 */
List *
MergeAttributes(List *schema, List *supers, char relpersistence,
				bool is_partition, List **supconstr)
{
	List	   *inhSchema = NIL;
	List	   *constraints = NIL;
	bool		have_bogus_defaults = false;
	int			child_attno;
	static Node bogus_marker = {0}; /* marks conflicting defaults */
	List	   *saved_schema = NIL;
	ListCell   *entry;

	/*
	 * Check for and reject tables with too many columns. We perform this
	 * check relatively early for two reasons: (a) we don't run the risk of
	 * overflowing an AttrNumber in subsequent code (b) an O(n^2) algorithm is
	 * okay if we're processing <= 1600 columns, but could take minutes to
	 * execute if the user attempts to create a table with hundreds of
	 * thousands of columns.
	 *
	 * Note that we also need to check that we do not exceed this figure after
	 * including columns from inherited relations.
	 */
	if (list_length(schema) > MaxHeapAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("tables can have at most %d columns",
						MaxHeapAttributeNumber)));

	/*
	 * Check for duplicate names in the explicit list of attributes.
	 *
	 * Although we might consider merging such entries in the same way that we
	 * handle name conflicts for inherited attributes, it seems to make more
	 * sense to assume such conflicts are errors.
	 *
	 * We don't use foreach() here because we have two nested loops over the
	 * schema list, with possible element deletions in the inner one.  If we
	 * used foreach_delete_current() it could only fix up the state of one of
	 * the loops, so it seems cleaner to use looping over list indexes for
	 * both loops.  Note that any deletion will happen beyond where the outer
	 * loop is, so its index never needs adjustment.
	 */
	for (int coldefpos = 0; coldefpos < list_length(schema); coldefpos++)
	{
		ColumnDef  *coldef = list_nth_node(ColumnDef, schema, coldefpos);

		if (!is_partition && coldef->typeName == NULL)
		{
			/*
			 * Typed table column option that does not belong to a column from
			 * the type.  This works because the columns from the type come
			 * first in the list.  (We omit this check for partition column
			 * lists; those are processed separately below.)
			 */
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist",
							coldef->colname)));
		}

		/* restpos scans all entries beyond coldef; incr is in loop body */
		for (int restpos = coldefpos + 1; restpos < list_length(schema);)
		{
			ColumnDef  *restdef = list_nth_node(ColumnDef, schema, restpos);

			if (strcmp(coldef->colname, restdef->colname) == 0)
			{
				if (coldef->is_from_type)
				{
					/*
					 * merge the column options into the column from the type
					 */
					coldef->is_not_null = restdef->is_not_null;
					coldef->raw_default = restdef->raw_default;
					coldef->cooked_default = restdef->cooked_default;
					coldef->constraints = restdef->constraints;
					coldef->is_from_type = false;
					schema = list_delete_nth_cell(schema, restpos);
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
							 errmsg("column \"%s\" specified more than once",
									coldef->colname)));
			}
			else
				restpos++;
		}
	}

	/*
	 * In case of a partition, there are no new column definitions, only dummy
	 * ColumnDefs created for column constraints.  Set them aside for now and
	 * process them at the end.
	 */
	if (is_partition)
	{
		saved_schema = schema;
		schema = NIL;
	}

	/*
	 * Scan the parents left-to-right, and merge their attributes to form a
	 * list of inherited attributes (inhSchema).  Also check to see if we need
	 * to inherit an OID column.
	 */
	child_attno = 0;
	foreach(entry, supers)
	{
		Oid			parent = lfirst_oid(entry);
		Relation	relation;
		TupleDesc	tupleDesc;
		TupleConstr *constr;
		AttrMap    *newattmap;
		List	   *inherited_defaults;
		List	   *cols_with_defaults;
		AttrNumber	parent_attno;
		ListCell   *lc1;
		ListCell   *lc2;

		/* caller already got lock */
		relation = table_open(parent, NoLock);

		/*
		 * Check for active uses of the parent partitioned table in the
		 * current transaction, such as being used in some manner by an
		 * enclosing command.
		 */
		/* SINGLENODE_FIXME: check and enable partition operations */
		if (is_partition && (Gp_role != GP_ROLE_DISPATCH && !IS_SINGLENODE()))
			CheckTableNotInUse(relation, "CREATE TABLE .. PARTITION OF or ALTER TABLE ADD PARTITION"); 

		/*
		 * We do not allow partitioned tables and partitions to participate in
		 * regular inheritance.
		 */
		if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
			!is_partition)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot inherit from partitioned table \"%s\"",
							RelationGetRelationName(relation))));
		if (relation->rd_rel->relispartition && !is_partition)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot inherit from partition \"%s\"",
							RelationGetRelationName(relation))));

		if (relation->rd_rel->relkind != RELKIND_RELATION &&
			relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
			relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("inherited relation \"%s\" is not a table or foreign table",
							RelationGetRelationName(relation))));

		/*
		 * If the parent is permanent, so must be all of its partitions.  Note
		 * that inheritance allows that case.
		 */
		if (is_partition &&
			relation->rd_rel->relpersistence != RELPERSISTENCE_TEMP &&
			relpersistence == RELPERSISTENCE_TEMP)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot create a temporary relation as partition of permanent relation \"%s\"",
							RelationGetRelationName(relation))));

		/* Permanent rels cannot inherit from temporary ones */
		if (relpersistence != RELPERSISTENCE_TEMP &&
			relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg(!is_partition
							? "cannot inherit from temporary relation \"%s\""
							: "cannot create a permanent relation as partition of temporary relation \"%s\"",
							RelationGetRelationName(relation))));

		/* If existing rel is temp, it must belong to this session */
		if (relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
			!relation->rd_islocaltemp)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg(!is_partition
							? "cannot inherit from temporary relation of another session"
							: "cannot create as partition of temporary relation of another session")));

		/*
		 * We should have an UNDER permission flag for this, but for now,
		 * demand that creator of a child table own the parent.
		 */
		if (!pg_class_ownercheck(RelationGetRelid(relation), GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(relation->rd_rel->relkind),
						   RelationGetRelationName(relation));

		tupleDesc = RelationGetDescr(relation);
		constr = tupleDesc->constr;

		/*
		 * newattmap->attnums[] will contain the child-table attribute numbers
		 * for the attributes of this parent table.  (They are not the same
		 * for parents after the first one, nor if we have dropped columns.)
		 */
		newattmap = make_attrmap(tupleDesc->natts);

		/* We can't process inherited defaults until newattmap is complete. */
		inherited_defaults = cols_with_defaults = NIL;

		for (parent_attno = 1; parent_attno <= tupleDesc->natts;
			 parent_attno++)
		{
			Form_pg_attribute attribute = TupleDescAttr(tupleDesc,
														parent_attno - 1);
			char	   *attributeName = NameStr(attribute->attname);
			int			exist_attno;
			ColumnDef  *def;

			/*
			 * Ignore dropped columns in the parent.
			 */
			if (attribute->attisdropped)
				continue;		/* leave newattmap->attnums entry as zero */

			/*
			 * Does it conflict with some previously inherited column?
			 */
			exist_attno = findAttrByName(attributeName, inhSchema);
			if (exist_attno > 0)
			{
				Oid			defTypeId;
				int32		deftypmod;
				Oid			defCollId;

				/*
				 * Yes, try to merge the two column definitions. They must
				 * have the same type, typmod, and collation.
				 */
				ereport(Gp_role == GP_ROLE_EXECUTE ? DEBUG1 : NOTICE,
						(errmsg("merging multiple inherited definitions of column \"%s\"",
								attributeName)));
				def = (ColumnDef *) list_nth(inhSchema, exist_attno - 1);
				typenameTypeIdAndMod(NULL, def->typeName, &defTypeId, &deftypmod);
				if (defTypeId != attribute->atttypid ||
					deftypmod != attribute->atttypmod)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("inherited column \"%s\" has a type conflict",
									attributeName),
							 errdetail("%s versus %s",
									   format_type_with_typemod(defTypeId,
																deftypmod),
									   format_type_with_typemod(attribute->atttypid,
																attribute->atttypmod))));
				defCollId = GetColumnDefCollation(NULL, def, defTypeId);
				if (defCollId != attribute->attcollation)
					ereport(ERROR,
							(errcode(ERRCODE_COLLATION_MISMATCH),
							 errmsg("inherited column \"%s\" has a collation conflict",
									attributeName),
							 errdetail("\"%s\" versus \"%s\"",
									   get_collation_name(defCollId),
									   get_collation_name(attribute->attcollation))));

				/* Copy/check storage parameter */
				if (def->storage == 0)
					def->storage = attribute->attstorage;
				else if (def->storage != attribute->attstorage)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("inherited column \"%s\" has a storage parameter conflict",
									attributeName),
							 errdetail("%s versus %s",
									   storage_name(def->storage),
									   storage_name(attribute->attstorage))));

				/* Copy/check compression parameter */
				if (CompressionMethodIsValid(attribute->attcompression))
				{
					const char *compression =
					GetCompressionMethodName(attribute->attcompression);

					if (def->compression == NULL)
						def->compression = pstrdup(compression);
					else if (strcmp(def->compression, compression) != 0)
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("column \"%s\" has a compression method conflict",
										attributeName),
								 errdetail("%s versus %s", def->compression, compression)));
				}

				def->inhcount++;
				/* Merge of NOT NULL constraints = OR 'em together */
				def->is_not_null |= attribute->attnotnull;
				/* Default and other constraints are handled below */
				newattmap->attnums[parent_attno - 1] = exist_attno;

				/* Check for GENERATED conflicts */
				if (def->generated != attribute->attgenerated)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("inherited column \"%s\" has a generation conflict",
									attributeName)));
			}
			else
			{
				/*
				 * No, create a new inherited column
				 */
				def = makeNode(ColumnDef);
				def->colname = pstrdup(attributeName);
				def->typeName = makeTypeNameFromOid(attribute->atttypid,
													attribute->atttypmod);
				def->inhcount = 1;
				def->is_local = false;
				def->is_not_null = attribute->attnotnull;
				def->is_from_type = false;
				def->storage = attribute->attstorage;
				def->raw_default = NULL;
				def->cooked_default = NULL;
				def->generated = attribute->attgenerated;
				def->collClause = NULL;
				def->collOid = attribute->attcollation;
				def->constraints = NIL;
				def->location = -1;
				if (CompressionMethodIsValid(attribute->attcompression))
					def->compression =
						pstrdup(GetCompressionMethodName(attribute->attcompression));
				else
					def->compression = NULL;
				inhSchema = lappend(inhSchema, def);
				newattmap->attnums[parent_attno - 1] = ++child_attno;
			}

			/*
			 * Locate default if any
			 */
			if (attribute->atthasdef)
			{
				Node	   *this_default = NULL;

				/* Find default in constraint structure */
				if (constr != NULL)
				{
					AttrDefault *attrdef = constr->defval;

					for (int i = 0; i < constr->num_defval; i++)
					{
						if (attrdef[i].adnum == parent_attno)
						{
							this_default = stringToNode(attrdef[i].adbin);
							break;
						}
					}
				}
				if (this_default == NULL)
					elog(ERROR, "default expression not found for attribute %d of relation \"%s\"",
						 parent_attno, RelationGetRelationName(relation));

				/*
				 * If it's a GENERATED default, it might contain Vars that
				 * need to be mapped to the inherited column(s)' new numbers.
				 * We can't do that till newattmap is ready, so just remember
				 * all the inherited default expressions for the moment.
				 */
				inherited_defaults = lappend(inherited_defaults, this_default);
				cols_with_defaults = lappend(cols_with_defaults, def);
			}
		}

		/*
		 * Now process any inherited default expressions, adjusting attnos
		 * using the completed newattmap map.
		 */
		forboth(lc1, inherited_defaults, lc2, cols_with_defaults)
		{
			Node	   *this_default = (Node *) lfirst(lc1);
			ColumnDef  *def = (ColumnDef *) lfirst(lc2);
			bool		found_whole_row;

			/* Adjust Vars to match new table's column numbering */
			this_default = map_variable_attnos(this_default,
											   1, 0,
											   newattmap,
											   InvalidOid, &found_whole_row);

			/*
			 * For the moment we have to reject whole-row variables.  We could
			 * convert them, if we knew the new table's rowtype OID, but that
			 * hasn't been assigned yet.  (A variable could only appear in a
			 * generation expression, so the error message is correct.)
			 */
			if (found_whole_row)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert whole-row table reference"),
						 errdetail("Generation expression for column \"%s\" contains a whole-row reference to table \"%s\".",
								   def->colname,
								   RelationGetRelationName(relation))));

			/*
			 * If we already had a default from some prior parent, check to
			 * see if they are the same.  If so, no problem; if not, mark the
			 * column as having a bogus default.  Below, we will complain if
			 * the bogus default isn't overridden by the child schema.
			 */
			Assert(def->raw_default == NULL);
			if (def->cooked_default == NULL)
				def->cooked_default = this_default;
			else if (!equal(def->cooked_default, this_default))
			{
				def->cooked_default = &bogus_marker;
				have_bogus_defaults = true;
			}
		}

		/*
		 * Now copy the CHECK constraints of this parent, adjusting attnos
		 * using the completed newattmap map.  Identically named constraints
		 * are merged if possible, else we throw error.
		 */
		if (constr && constr->num_check > 0)
		{
			ConstrCheck *check = constr->check;
			int			i;

			for (i = 0; i < constr->num_check; i++)
			{
				char	   *name = check[i].ccname;
				Node	   *expr;
				bool		found_whole_row;

				/* ignore if the constraint is non-inheritable */
				if (check[i].ccnoinherit)
					continue;

				/* Adjust Vars to match new table's column numbering */
				expr = map_variable_attnos(stringToNode(check[i].ccbin),
										   1, 0,
										   newattmap,
										   InvalidOid, &found_whole_row);

				/*
				 * For the moment we have to reject whole-row variables. We
				 * could convert them, if we knew the new table's rowtype OID,
				 * but that hasn't been assigned yet.
				 */
				if (found_whole_row)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot convert whole-row table reference"),
							 errdetail("Constraint \"%s\" contains a whole-row reference to table \"%s\".",
									   name,
									   RelationGetRelationName(relation))));

				/* check for duplicate */
				if (!MergeCheckConstraint(constraints, name, expr))
				{
					/* nope, this is a new one */
					CookedConstraint *cooked;

					cooked = (CookedConstraint *) palloc(sizeof(CookedConstraint));
					cooked->contype = CONSTR_CHECK;
					cooked->conoid = InvalidOid;	/* until created */
					cooked->name = pstrdup(name);
					cooked->attnum = 0; /* not used for constraints */
					cooked->expr = expr;
					cooked->skip_validation = false;
					cooked->is_local = false;
					cooked->inhcount = 1;
					cooked->is_no_inherit = false;
					constraints = lappend(constraints, cooked);
				}
			}
		}

		free_attrmap(newattmap);

		/*
		 * Close the parent rel, but keep our lock on it until xact commit.
		 * That will prevent someone else from deleting or ALTERing the parent
		 * before the child is committed.
		 */
		table_close(relation, NoLock);
	}

	/*
	 * If we had no inherited attributes, the result schema is just the
	 * explicitly declared columns.  Otherwise, we need to merge the declared
	 * columns into the inherited schema list.  Although, we never have any
	 * explicitly declared columns if the table is a partition.
	 */
	if (inhSchema != NIL)
	{
		int			schema_attno = 0;

		foreach(entry, schema)
		{
			ColumnDef  *newdef = lfirst(entry);
			char	   *attributeName = newdef->colname;
			int			exist_attno;

			schema_attno++;

			/*
			 * Does it conflict with some previously inherited column?
			 */
			exist_attno = findAttrByName(attributeName, inhSchema);
			if (exist_attno > 0)
			{
				ColumnDef  *def;
				Oid			defTypeId,
							newTypeId;
				int32		deftypmod,
							newtypmod;
				Oid			defcollid,
							newcollid;

				/*
				 * Partitions have only one parent and have no column
				 * definitions of their own, so conflict should never occur.
				 */
				Assert(!is_partition);

				/*
				 * Yes, try to merge the two column definitions. They must
				 * have the same type, typmod, and collation.
				 */
				if (exist_attno == schema_attno)
					ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
							(errmsg("merging column \"%s\" with inherited definition",
									attributeName)));
				else
					ereport(NOTICE,
							(errmsg("moving and merging column \"%s\" with inherited definition", attributeName),
							 errdetail("User-specified column moved to the position of the inherited column.")));
				def = (ColumnDef *) list_nth(inhSchema, exist_attno - 1);
				typenameTypeIdAndMod(NULL, def->typeName, &defTypeId, &deftypmod);
				typenameTypeIdAndMod(NULL, newdef->typeName, &newTypeId, &newtypmod);
				if (defTypeId != newTypeId || deftypmod != newtypmod)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("column \"%s\" has a type conflict",
									attributeName),
							 errdetail("%s versus %s",
									   format_type_with_typemod(defTypeId,
																deftypmod),
									   format_type_with_typemod(newTypeId,
																newtypmod))));
				defcollid = GetColumnDefCollation(NULL, def, defTypeId);
				newcollid = GetColumnDefCollation(NULL, newdef, newTypeId);
				if (defcollid != newcollid)
					ereport(ERROR,
							(errcode(ERRCODE_COLLATION_MISMATCH),
							 errmsg("column \"%s\" has a collation conflict",
									attributeName),
							 errdetail("\"%s\" versus \"%s\"",
									   get_collation_name(defcollid),
									   get_collation_name(newcollid))));

				/*
				 * Identity is never inherited.  The new column can have an
				 * identity definition, so we always just take that one.
				 */
				def->identity = newdef->identity;

				/* Copy storage parameter */
				if (def->storage == 0)
					def->storage = newdef->storage;
				else if (newdef->storage != 0 && def->storage != newdef->storage)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("column \"%s\" has a storage parameter conflict",
									attributeName),
							 errdetail("%s versus %s",
									   storage_name(def->storage),
									   storage_name(newdef->storage))));

				/* Copy compression parameter */
				if (def->compression == NULL)
					def->compression = newdef->compression;
				else if (newdef->compression != NULL)
				{
					if (strcmp(def->compression, newdef->compression) != 0)
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("column \"%s\" has a compression method conflict",
										attributeName),
								 errdetail("%s versus %s", def->compression, newdef->compression)));
				}

				/* Mark the column as locally defined */
				def->is_local = true;
				/* Merge of NOT NULL constraints = OR 'em together */
				def->is_not_null |= newdef->is_not_null;

				/*
				 * Check for conflicts related to generated columns.
				 *
				 * If the parent column is generated, the child column must be
				 * unadorned and will be made a generated column.  (We could
				 * in theory allow the child column definition specifying the
				 * exact same generation expression, but that's a bit
				 * complicated to implement and doesn't seem very useful.)  We
				 * also check that the child column doesn't specify a default
				 * value or identity, which matches the rules for a single
				 * column in parse_util.c.
				 */
				if (def->generated)
				{
					if (newdef->generated)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
								 errmsg("child column \"%s\" specifies generation expression",
										def->colname),
								 errhint("Omit the generation expression in the definition of the child table column to inherit the generation expression from the parent table.")));
					if (newdef->raw_default && !newdef->generated)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
								 errmsg("column \"%s\" inherits from generated column but specifies default",
										def->colname)));
					if (newdef->identity)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
								 errmsg("column \"%s\" inherits from generated column but specifies identity",
										def->colname)));
				}

				/*
				 * If the parent column is not generated, then take whatever
				 * the child column definition says.
				 */
				else
				{
					if (newdef->generated)
						def->generated = newdef->generated;
				}

				/* If new def has a default, override previous default */
				if (newdef->raw_default != NULL)
				{
					def->raw_default = newdef->raw_default;
					def->cooked_default = newdef->cooked_default;
				}
			}
			else
			{
				/*
				 * No, attach new column to result schema
				 */
				inhSchema = lappend(inhSchema, newdef);
			}
		}

		schema = inhSchema;

		/*
		 * Check that we haven't exceeded the legal # of columns after merging
		 * in inherited columns.
		 */
		if (list_length(schema) > MaxHeapAttributeNumber)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("tables can have at most %d columns",
							MaxHeapAttributeNumber)));
	}

	/*
	 * Now that we have the column definition list for a partition, we can
	 * check whether the columns referenced in the column constraint specs
	 * actually exist.  Also, we merge NOT NULL and defaults into each
	 * corresponding column definition.
	 */
	if (is_partition)
	{
		foreach(entry, saved_schema)
		{
			ColumnDef  *restdef = lfirst(entry);
			bool		found = false;
			ListCell   *l;

			foreach(l, schema)
			{
				ColumnDef  *coldef = lfirst(l);

				if (strcmp(coldef->colname, restdef->colname) == 0)
				{
					found = true;
					coldef->is_not_null |= restdef->is_not_null;

					/*
					 * Override the parent's default value for this column
					 * (coldef->cooked_default) with the partition's local
					 * definition (restdef->raw_default), if there's one. It
					 * should be physically impossible to get a cooked default
					 * in the local definition or a raw default in the
					 * inherited definition, but make sure they're nulls, for
					 * future-proofing.
					 */
					Assert(restdef->cooked_default == NULL);
					Assert(coldef->raw_default == NULL);
					if (restdef->raw_default)
					{
						coldef->raw_default = restdef->raw_default;
						coldef->cooked_default = NULL;
					}
				}
			}

			/* complain for constraints on columns not in parent */
			if (!found)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" does not exist",
								restdef->colname)));
		}
	}

	/*
	 * If we found any conflicting parent default values, check to make sure
	 * they were overridden by the child.
	 */
	if (have_bogus_defaults)
	{
		foreach(entry, schema)
		{
			ColumnDef  *def = lfirst(entry);

			if (def->cooked_default == &bogus_marker)
			{
				if (def->generated)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
							 errmsg("column \"%s\" inherits conflicting generation expressions",
									def->colname)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
							 errmsg("column \"%s\" inherits conflicting default values",
									def->colname),
							 errhint("To resolve the conflict, specify a default explicitly.")));
			}
		}
	}

	*supconstr = constraints;
	return schema;
}


/*
 * MergeCheckConstraint
 *		Try to merge an inherited CHECK constraint with previous ones
 *
 * If we inherit identically-named constraints from multiple parents, we must
 * merge them, or throw an error if they don't have identical definitions.
 *
 * constraints is a list of CookedConstraint structs for previous constraints.
 *
 * Returns true if merged (constraint is a duplicate), or false if it's
 * got a so-far-unique name, or throws error if conflict.
 */
static bool
MergeCheckConstraint(List *constraints, char *name, Node *expr)
{
	ListCell   *lc;

	foreach(lc, constraints)
	{
		CookedConstraint *ccon = (CookedConstraint *) lfirst(lc);

		Assert(ccon->contype == CONSTR_CHECK);

		/* Non-matching names never conflict */
		if (strcmp(ccon->name, name) != 0)
			continue;

		if (equal(expr, ccon->expr))
		{
			/* OK to merge */
			ccon->inhcount++;
			return true;
		}

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("check constraint name \"%s\" appears multiple times but with different expressions",
						name)));
	}

	return false;
}


/*
 * StoreCatalogInheritance
 *		Updates the system catalogs with proper inheritance information.
 *
 * supers is a list of the OIDs of the new relation's direct ancestors.
 */
static void
StoreCatalogInheritance(Oid relationId, List *supers,
						bool child_is_partition)
{
	Relation	relation;
	int32		seqNumber;
	ListCell   *entry;

	/*
	 * sanity checks
	 */
	AssertArg(OidIsValid(relationId));

	if (supers == NIL)
		return;

	/*
	 * Store INHERITS information in pg_inherits using direct ancestors only.
	 * Also enter dependencies on the direct ancestors, and make sure they are
	 * marked with relhassubclass = true.
	 *
	 * (Once upon a time, both direct and indirect ancestors were found here
	 * and then entered into pg_ipl.  Since that catalog doesn't exist
	 * anymore, there's no need to look for indirect ancestors.)
	 */
	relation = table_open(InheritsRelationId, RowExclusiveLock);

	seqNumber = 1;
	foreach(entry, supers)
	{
		Oid			parentOid = lfirst_oid(entry);

		StoreCatalogInheritance1(relationId, parentOid, seqNumber, relation,
								 child_is_partition);
		seqNumber++;
	}

	table_close(relation, RowExclusiveLock);
}

/*
 * Make catalog entries showing relationId as being an inheritance child
 * of parentOid.  inhRelation is the already-opened pg_inherits catalog.
 */
static void
StoreCatalogInheritance1(Oid relationId, Oid parentOid,
						 int32 seqNumber, Relation inhRelation,
						 bool child_is_partition)
{
	ObjectAddress childobject,
				parentobject;

	/* store the pg_inherits row */
	StoreSingleInheritance(relationId, parentOid, seqNumber);

	/*
	 * Store a dependency too
	 */
	parentobject.classId = RelationRelationId;
	parentobject.objectId = parentOid;
	parentobject.objectSubId = 0;
	childobject.classId = RelationRelationId;
	childobject.objectId = relationId;
	childobject.objectSubId = 0;

	recordDependencyOn(&childobject, &parentobject,
					   child_dependency_type(child_is_partition));

	/*
	 * Post creation hook of this inheritance. Since object_access_hook
	 * doesn't take multiple object identifiers, we relay oid of parent
	 * relation using auxiliary_id argument.
	 */
	InvokeObjectPostAlterHookArg(InheritsRelationId,
								 relationId, 0,
								 parentOid, false);

	/*
	 * Mark the parent as having subclasses.
	 */
	SetRelationHasSubclass(parentOid, true);
}

/*
 * Look for an existing schema entry with the given name.
 *
 * Returns the index (starting with 1) if attribute already exists in schema,
 * 0 if it doesn't.
 */
static int
findAttrByName(const char *attributeName, List *schema)
{
	ListCell   *s;
	int			i = 1;

	foreach(s, schema)
	{
		ColumnDef  *def = lfirst(s);

		if (strcmp(attributeName, def->colname) == 0)
			return i;

		i++;
	}
	return 0;
}


/*
 * SetRelationHasSubclass
 *		Set the value of the relation's relhassubclass field in pg_class.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 * ShareUpdateExclusiveLock is sufficient.
 *
 * NOTE: an important side-effect of this operation is that an SI invalidation
 * message is sent out to all backends --- including me --- causing plans
 * referencing the relation to be rebuilt with the new list of children.
 * This must happen even if we find that no change is needed in the pg_class
 * row.
 */
void
SetRelationHasSubclass(Oid relationId, bool relhassubclass)
{
	Relation	relationRelation;
	HeapTuple	tuple;
	Form_pg_class classtuple;

	/*
	 * Fetch a modifiable copy of the tuple, modify it, update pg_class.
	 */
	relationRelation = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relationId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relationId);
	classtuple = (Form_pg_class) GETSTRUCT(tuple);

	if (classtuple->relhassubclass != relhassubclass)
	{
		classtuple->relhassubclass = relhassubclass;
		CatalogTupleUpdate(relationRelation, &tuple->t_self, tuple);
	}
	else
	{
		/* no need to change tuple, but force relcache rebuild anyway */
		CacheInvalidateRelcacheByTuple(tuple);
	}

	heap_freetuple(tuple);
	table_close(relationRelation, RowExclusiveLock);
}

/*
 * CheckRelationTableSpaceMove
 *		Check if relation can be moved to new tablespace.
 *
 * NOTE: The caller must hold AccessExclusiveLock on the relation.
 *
 * Returns true if the relation can be moved to the new tablespace; raises
 * an error if it is not possible to do the move; returns false if the move
 * would have no effect.
 */
bool
CheckRelationTableSpaceMove(Relation rel, Oid newTableSpaceId)
{
	Oid			oldTableSpaceId;

	/*
	 * No work if no change in tablespace.  Note that MyDatabaseTableSpace is
	 * stored as 0.
	 */
	oldTableSpaceId = rel->rd_rel->reltablespace;
	if (newTableSpaceId == oldTableSpaceId ||
		(newTableSpaceId == MyDatabaseTableSpace && oldTableSpaceId == 0))
		return false;

	/*
	 * We cannot support moving mapped relations into different tablespaces.
	 * (In particular this eliminates all shared catalogs.)
	 */
	if (RelationIsMapped(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot move system relation \"%s\"",
						RelationGetRelationName(rel))));

	/* Cannot move a non-shared relation into pg_global */
	if (newTableSpaceId == GLOBALTABLESPACE_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only shared relations can be placed in pg_global tablespace")));

	/*
	 * Do not allow moving temp tables of other backends ... their local
	 * buffer manager is not going to cope.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot move temporary tables of other sessions")));

	return true;
}

/*
 * SetRelationTableSpace
 *		Set new reltablespace and relfilenode in pg_class entry.
 *
 * newTableSpaceId is the new tablespace for the relation, and
 * newRelFileNode its new filenode.  If newRelFileNode is InvalidOid,
 * this field is not updated.
 *
 * NOTE: The caller must hold AccessExclusiveLock on the relation.
 *
 * The caller of this routine had better check if a relation can be
 * moved to this new tablespace by calling CheckRelationTableSpaceMove()
 * first, and is responsible for making the change visible with
 * CommandCounterIncrement().
 */
void
SetRelationTableSpace(Relation rel,
					  Oid newTableSpaceId,
					  RelFileNodeId newRelFileNode)
{
	Relation	pg_class;
	HeapTuple	tuple;
	Form_pg_class rd_rel;
	Oid			reloid = RelationGetRelid(rel);

	Assert(CheckRelationTableSpaceMove(rel, newTableSpaceId));

	/* Get a modifiable copy of the relation's pg_class row. */
	pg_class = table_open(RelationRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(reloid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", reloid);
	rd_rel = (Form_pg_class) GETSTRUCT(tuple);

	/* Update the pg_class row. */
	rd_rel->reltablespace = (newTableSpaceId == MyDatabaseTableSpace) ?
		InvalidOid : newTableSpaceId;
	if (OidIsValid(newRelFileNode))
		rd_rel->relfilenode = newRelFileNode;
	CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);

	/*
	 * Record dependency on tablespace.  This is only required for relations
	 * that have no physical storage.
	 */
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		changeDependencyOnTablespace(RelationRelationId, reloid,
									 rd_rel->reltablespace);

	heap_freetuple(tuple);
	table_close(pg_class, RowExclusiveLock);
}

/*
 *		renameatt_check			- basic sanity checks before attribute rename
 */
static void
renameatt_check(Oid myrelid, Form_pg_class classform, bool recursing)
{
	char		relkind = classform->relkind;

	if (classform->reloftype && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot rename column of typed table")));

	/*
	 * Renaming the columns of sequences or toast tables doesn't actually
	 * break anything from the system's point of view, since internal
	 * references are by attnum.  But it doesn't seem right to allow users to
	 * change names that are hardcoded into the system, hence the following
	 * restriction.
	 */
	if (relkind != RELKIND_RELATION &&
		relkind != RELKIND_VIEW &&
		relkind != RELKIND_MATVIEW &&
		relkind != RELKIND_COMPOSITE_TYPE &&
		relkind != RELKIND_INDEX &&
		relkind != RELKIND_PARTITIONED_INDEX &&
		relkind != RELKIND_FOREIGN_TABLE &&
		relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table, view, materialized view, composite type, index, or foreign table",
						NameStr(classform->relname))));

	/*
	 * permissions checking.  only the owner of a class can change its schema.
	 */
	if (!pg_class_ownercheck(myrelid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(myrelid)),
					   NameStr(classform->relname));
	if (!allowSystemTableMods && IsSystemClass(myrelid, classform))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						NameStr(classform->relname))));
}

/*
 *		renameatt_internal		- workhorse for renameatt
 *
 * Return value is the attribute number in the 'myrelid' relation.
 */
static AttrNumber
renameatt_internal(Oid myrelid,
				   const char *oldattname,
				   const char *newattname,
				   bool recurse,
				   bool recursing,
				   int expected_parents,
				   DropBehavior behavior)
{
	Relation	targetrelation;
	Relation	attrelation;
	HeapTuple	atttup;
	Form_pg_attribute attform;
	AttrNumber	attnum;

	/*
	 * Grab an exclusive lock on the target table, which we will NOT release
	 * until end of transaction.
	 */
	targetrelation = relation_open(myrelid, AccessExclusiveLock);
	renameatt_check(myrelid, RelationGetForm(targetrelation), recursing);

	/*
	 * Don't rename IVM columns.
	 */
	if (RelationIsIVM(targetrelation) && isIvmName(oldattname))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("IVM column can not be renamed")));

	/*
	 * if the 'recurse' flag is set then we are supposed to rename this
	 * attribute in all classes that inherit from 'relname' (as well as in
	 * 'relname').
	 *
	 * any permissions or problems with duplicate attributes will cause the
	 * whole transaction to abort, which is what we want -- all or nothing.
	 */
	if (recurse)
	{
		List	   *child_oids,
				   *child_numparents;
		ListCell   *lo,
				   *li;

		/*
		 * we need the number of parents for each child so that the recursive
		 * calls to renameatt() can determine whether there are any parents
		 * outside the inheritance hierarchy being processed.
		 */
		child_oids = find_all_inheritors(myrelid, AccessExclusiveLock,
										 &child_numparents);

		/*
		 * find_all_inheritors does the recursive search of the inheritance
		 * hierarchy, so all we have to do is process all of the relids in the
		 * list that it returns.
		 */
		forboth(lo, child_oids, li, child_numparents)
		{
			Oid			childrelid = lfirst_oid(lo);
			int			numparents = lfirst_int(li);

			if (childrelid == myrelid)
				continue;
			/* note we need not recurse again */
			renameatt_internal(childrelid, oldattname, newattname, false, true, numparents, behavior);
		}
	}
	else
	{
		/*
		 * If we are told not to recurse, there had better not be any child
		 * tables; else the rename would put them out of step.
		 *
		 * expected_parents will only be 0 if we are not already recursing.
		 */
		if (expected_parents == 0 &&
			find_inheritance_children(myrelid, NoLock) != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("inherited column \"%s\" must be renamed in child tables too",
							oldattname)));
	}

	/* rename attributes in typed tables of composite type */
	if (targetrelation->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
	{
		List	   *child_oids;
		ListCell   *lo;

		child_oids = find_typed_table_dependencies(targetrelation->rd_rel->reltype,
												   RelationGetRelationName(targetrelation),
												   behavior);

		foreach(lo, child_oids)
			renameatt_internal(lfirst_oid(lo), oldattname, newattname, true, true, 0, behavior);
	}

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);

	atttup = SearchSysCacheCopyAttName(myrelid, oldattname);
	if (!HeapTupleIsValid(atttup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist",
						oldattname)));
	attform = (Form_pg_attribute) GETSTRUCT(atttup);

	attnum = attform->attnum;
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot rename system column \"%s\"",
						oldattname)));

	/*
	 * if the attribute is inherited, forbid the renaming.  if this is a
	 * top-level call to renameatt(), then expected_parents will be 0, so the
	 * effect of this code will be to prohibit the renaming if the attribute
	 * is inherited at all.  if this is a recursive call to renameatt(),
	 * expected_parents will be the number of parents the current relation has
	 * within the inheritance hierarchy being processed, so we'll prohibit the
	 * renaming only if there are additional parents from elsewhere.
	 */
	if (attform->attinhcount > expected_parents)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot rename inherited column \"%s\"",
						oldattname)));

	/* new name should not already exist */
	(void) check_for_column_name_collision(targetrelation, newattname, false);

	/* apply the update */
	namestrcpy(&(attform->attname), newattname);

	CatalogTupleUpdate(attrelation, &atttup->t_self, atttup);

	InvokeObjectPostAlterHook(RelationRelationId, myrelid, attnum);

	heap_freetuple(atttup);

	table_close(attrelation, RowExclusiveLock);

	/* MPP-6929, MPP-7600: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(targetrelation->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(targetrelation),
						   GetUserId(),
						   "ALTER", "RENAME COLUMN"
				);


	relation_close(targetrelation, NoLock); /* close rel but keep lock */

	return attnum;
}

/*
 * Perform permissions and integrity checks before acquiring a relation lock.
 */
static void
RangeVarCallbackForRenameAttribute(const RangeVar *rv, Oid relid, Oid oldrelid,
								   void *arg)
{
	HeapTuple	tuple;
	Form_pg_class form;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return;					/* concurrently dropped */
	form = (Form_pg_class) GETSTRUCT(tuple);
	renameatt_check(relid, form, false);
	ReleaseSysCache(tuple);
}

/*
 *		renameatt		- changes the name of an attribute in a relation
 *
 * The returned ObjectAddress is that of the renamed column.
 */
ObjectAddress
renameatt(RenameStmt *stmt)
{
	Oid			relid;
	AttrNumber	attnum;
	ObjectAddress address;

	/* lock level taken here should match renameatt_internal */
	relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
									 stmt->missing_ok ? RVR_MISSING_OK : 0,
									 RangeVarCallbackForRenameAttribute,
									 NULL);

	if (!OidIsValid(relid))
	{
		ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
				(errmsg("relation \"%s\" does not exist, skipping",
						stmt->relation->relname)));
		return InvalidObjectAddress;
	}

	attnum =
		renameatt_internal(relid,
						   stmt->subname,	/* old att name */
						   stmt->newname,	/* new att name */
						   stmt->relation->inh, /* recursive? */
						   false,	/* recursing? */
						   0,	/* expected inhcount */
						   stmt->behavior);

	ObjectAddressSubSet(address, RelationRelationId, relid, attnum);

	return address;
}

/*
 * same logic as renameatt_internal
 */
static ObjectAddress
rename_constraint_internal(Oid myrelid,
						   Oid mytypid,
						   const char *oldconname,
						   const char *newconname,
						   bool recurse,
						   bool recursing,
						   int expected_parents)
{
	Relation	targetrelation = NULL;
	Oid			constraintOid;
	HeapTuple	tuple;
	Form_pg_constraint con;
	ObjectAddress address;

	AssertArg(!myrelid || !mytypid);

	if (mytypid)
	{
		constraintOid = get_domain_constraint_oid(mytypid, oldconname, false);
	}
	else
	{
		targetrelation = relation_open(myrelid, AccessExclusiveLock);

		/*
		 * don't tell it whether we're recursing; we allow changing typed
		 * tables here
		 */
		renameatt_check(myrelid, RelationGetForm(targetrelation), false);

		constraintOid = get_relation_constraint_oid(myrelid, oldconname, false);
	}

	tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for constraint %u",
			 constraintOid);
	con = (Form_pg_constraint) GETSTRUCT(tuple);

	if (myrelid && con->contype == CONSTRAINT_CHECK && !con->connoinherit)
	{
		if (recurse)
		{
			List	   *child_oids,
					   *child_numparents;
			ListCell   *lo,
					   *li;

			child_oids = find_all_inheritors(myrelid, AccessExclusiveLock,
											 &child_numparents);

			forboth(lo, child_oids, li, child_numparents)
			{
				Oid			childrelid = lfirst_oid(lo);
				int			numparents = lfirst_int(li);

				if (childrelid == myrelid)
					continue;

				rename_constraint_internal(childrelid, InvalidOid, oldconname, newconname, false, true, numparents);
			}
		}
		else
		{
			if (expected_parents == 0 &&
				find_inheritance_children(myrelid, NoLock) != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("inherited constraint \"%s\" must be renamed in child tables too",
								oldconname)));
		}

		if (con->coninhcount > expected_parents)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot rename inherited constraint \"%s\"",
							oldconname)));
	}

	if (con->conindid
		&& (con->contype == CONSTRAINT_PRIMARY
			|| con->contype == CONSTRAINT_UNIQUE
			|| con->contype == CONSTRAINT_EXCLUSION))
		/* rename the index; this renames the constraint as well */
		RenameRelationInternal(con->conindid, newconname, false, true);
	else
		RenameConstraintById(constraintOid, newconname);

	ObjectAddressSet(address, ConstraintRelationId, constraintOid);

	ReleaseSysCache(tuple);

	if (targetrelation)
	{
		/*
		 * Invalidate relcache so as others can see the new constraint name.
		 */
		CacheInvalidateRelcache(targetrelation);

		relation_close(targetrelation, NoLock); /* close rel but keep lock */
	}

	return address;
}

ObjectAddress
RenameConstraint(RenameStmt *stmt)
{
	Oid			relid = InvalidOid;
	Oid			typid = InvalidOid;

	if (stmt->renameType == OBJECT_DOMCONSTRAINT)
	{
		Relation	rel;
		HeapTuple	tup;

		typid = typenameTypeId(NULL, makeTypeNameFromNameList(castNode(List, stmt->object)));
		rel = table_open(TypeRelationId, RowExclusiveLock);
		tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for type %u", typid);
		checkDomainOwner(tup);
		ReleaseSysCache(tup);
		table_close(rel, NoLock);
	}
	else
	{
		/* lock level taken here should match rename_constraint_internal */
		relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
										 stmt->missing_ok ? RVR_MISSING_OK : 0,
										 RangeVarCallbackForRenameAttribute,
										 NULL);
		if (!OidIsValid(relid))
		{
			ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
					(errmsg("relation \"%s\" does not exist, skipping",
							stmt->relation->relname)));
			return InvalidObjectAddress;
		}
	}

	return
		rename_constraint_internal(relid, typid,
								   stmt->subname,
								   stmt->newname,
								   (stmt->relation &&
									stmt->relation->inh),	/* recursive? */
								   false,	/* recursing? */
								   0 /* expected inhcount */ );

}

/*
 * Execute ALTER TABLE/INDEX/SEQUENCE/VIEW/MATERIALIZED VIEW/FOREIGN TABLE
 * RENAME
 */
ObjectAddress
RenameRelation(RenameStmt *stmt)
{
	bool		is_index_stmt = stmt->renameType == OBJECT_INDEX;
	Oid			relid;
	ObjectAddress address;
	Relation	targetrelation;
	char 	*oldrelname;

	/*
	 * Grab an exclusive lock on the target table, index, sequence, view,
	 * materialized view, or foreign table, which we will NOT release until
	 * end of transaction.
	 *
	 * Lock level used here should match RenameRelationInternal, to avoid lock
	 * escalation.  However, because ALTER INDEX can be used with any relation
	 * type, we mustn't believe without verification.
	 */
	for (;;)
	{
		LOCKMODE	lockmode;
		char		relkind;
		bool		obj_is_index;

		lockmode = is_index_stmt ? ShareUpdateExclusiveLock : AccessExclusiveLock;

		relid = RangeVarGetRelidExtended(stmt->relation, lockmode,
										 stmt->missing_ok ? RVR_MISSING_OK : 0,
										 RangeVarCallbackForAlterRelation,
										 (void *) stmt);

		if (!OidIsValid(relid))
		{
			ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
					(errmsg("relation \"%s\" does not exist, skipping",
							stmt->relation->relname)));
			return InvalidObjectAddress;
		}

		/*
		 * We allow mismatched statement and object types (e.g., ALTER INDEX
		 * to rename a table), but we might've used the wrong lock level.  If
		 * that happens, retry with the correct lock level.  We don't bother
		 * if we already acquired AccessExclusiveLock with an index, however.
		 */
		relkind = get_rel_relkind(relid);
		if (relkind == RELKIND_DIRECTORY_TABLE)
			ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("Rename directory table is not allowed.")));

		obj_is_index = (relkind == RELKIND_INDEX ||
						relkind == RELKIND_PARTITIONED_INDEX);
		if (obj_is_index || is_index_stmt == obj_is_index)
			break;

		UnlockRelationOid(relid, lockmode);
		is_index_stmt = obj_is_index;
	}

	targetrelation = relation_open(relid, is_index_stmt ? ShareUpdateExclusiveLock : AccessExclusiveLock);
	oldrelname = pstrdup(RelationGetRelationName(targetrelation));

	/* Do the work */
	RenameRelationInternal(relid, stmt->newname, false, is_index_stmt);

	/*
	 * if relation is a partitioned table, rename all children tables of it.
	 */
	if (targetrelation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		GpRenameChildPartitions(targetrelation, oldrelname, stmt->newname);
	relation_close(targetrelation, NoLock);

	ObjectAddressSet(address, RelationRelationId, relid);

	return address;
}

/*
 *		RenameRelationInternal - change the name of a relation
 */
void
RenameRelationInternal(Oid myrelid, const char *newrelname, bool is_internal, bool is_index)
{
	Relation	targetrelation;
	Relation	relrelation;	/* for RELATION relation */
	HeapTuple	reltup;
	Form_pg_class relform;
	Oid			namespaceId;

	/*
	 * In Postgres:
	 * Grab a lock on the target relation, which we will NOT release until end
	 * of transaction.  We need at least a self-exclusive lock so that
	 * concurrent DDL doesn't overwrite the rename if they start updating
	 * while still seeing the old version.  The lock also guards against
	 * triggering relcache reloads in concurrent sessions, which might not
	 * handle this information changing under them.  For indexes, we can use a
	 * reduced lock level because RelationReloadIndexInfo() handles indexes
	 * specially.
	 *
	 * In GPDB, added supportability feature under GUC to allow rename table
	 * without AccessExclusiveLock for scenarios like directly modifying system
	 * catalogs. This will change transaction isolation behaviors, however, this
	 * won't cause any data corruption.
	 */
	targetrelation = relation_open(myrelid, is_index ? ShareUpdateExclusiveLock : AccessExclusiveLock);
	namespaceId = RelationGetNamespace(targetrelation);

	/*
	 * Find relation's pg_class tuple, and make sure newrelname isn't in use.
	 */
	relrelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(myrelid));
	if (!HeapTupleIsValid(reltup))	/* shouldn't happen */
		elog(ERROR, "cache lookup failed for relation %u", myrelid);
	relform = (Form_pg_class) GETSTRUCT(reltup);

	if (get_relname_relid(newrelname, namespaceId) != InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists",
						newrelname)));

	/*
	 * RenameRelation is careful not to believe the caller's idea of the
	 * relation kind being handled.  We don't have to worry about this, but
	 * let's not be totally oblivious to it.  We can process an index as
	 * not-an-index, but not the other way around.
	 */
	Assert(!is_index ||
		   is_index == (targetrelation->rd_rel->relkind == RELKIND_INDEX ||
						targetrelation->rd_rel->relkind == RELKIND_PARTITIONED_INDEX));

	/*
	 * Update pg_class tuple with new relname.  (Scribbling on reltup is OK
	 * because it's a copy...)
	 */
	namestrcpy(&(relform->relname), newrelname);

	CatalogTupleUpdate(relrelation, &reltup->t_self, reltup);

	InvokeObjectPostAlterHookArg(RelationRelationId, myrelid, 0,
								 InvalidOid, is_internal);

	heap_freetuple(reltup);
	table_close(relrelation, RowExclusiveLock);

	/*
	 * Also rename the associated type, if any.
	 */
	if (OidIsValid(targetrelation->rd_rel->reltype))
		RenameTypeInternal(targetrelation->rd_rel->reltype,
						   newrelname, namespaceId);

	/*
	 * Also rename the associated constraint, if any.
	 */
	if (targetrelation->rd_rel->relkind == RELKIND_INDEX ||
		targetrelation->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
	{
		Oid			constraintId = get_index_constraint(myrelid);

		if (OidIsValid(constraintId))
			RenameConstraintById(constraintId, newrelname);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&&
		/* MPP-7773: don't track objects in system namespace
		 * if modifying system tables (eg during upgrade)
		 */
		( ! ( (PG_CATALOG_NAMESPACE == namespaceId) && (allowSystemTableMods)))
		&& (   MetaTrackValidRelkind(targetrelation->rd_rel->relkind)
			&& METATRACK_VALIDNAMESPACE(namespaceId)
			   && (!(isAnyTempNamespace(namespaceId)))
				))
		MetaTrackUpdObject(RelationRelationId,
						   myrelid,
						   GetUserId(),
						   "ALTER", "RENAME"
				);

	/*
	 * Close rel, but keep lock!
	 */
	relation_close(targetrelation, NoLock);
}

/*
 *		ResetRelRewrite - reset relrewrite
 */
void
ResetRelRewrite(Oid myrelid)
{
	Relation	relrelation;	/* for RELATION relation */
	HeapTuple	reltup;
	Form_pg_class relform;

	/*
	 * Find relation's pg_class tuple.
	 */
	relrelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(myrelid));
	if (!HeapTupleIsValid(reltup))	/* shouldn't happen */
		elog(ERROR, "cache lookup failed for relation %u", myrelid);
	relform = (Form_pg_class) GETSTRUCT(reltup);

	/*
	 * Update pg_class tuple.
	 */
	relform->relrewrite = InvalidOid;

	CatalogTupleUpdate(relrelation, &reltup->t_self, reltup);

	heap_freetuple(reltup);
	table_close(relrelation, RowExclusiveLock);
}

/*
 * Disallow ALTER TABLE (and similar commands) when the current backend has
 * any open reference to the target table besides the one just acquired by
 * the calling command; this implies there's an open cursor or active plan.
 * We need this check because our lock doesn't protect us against stomping
 * on our own foot, only other people's feet!
 *
 * For ALTER TABLE, the only case known to cause serious trouble is ALTER
 * COLUMN TYPE, and some changes are obviously pretty benign, so this could
 * possibly be relaxed to only error out for certain types of alterations.
 * But the use-case for allowing any of these things is not obvious, so we
 * won't work hard at it for now.
 *
 * We also reject these commands if there are any pending AFTER trigger events
 * for the rel.  This is certainly necessary for the rewriting variants of
 * ALTER TABLE, because they don't preserve tuple TIDs and so the pending
 * events would try to fetch the wrong tuples.  It might be overly cautious
 * in other cases, but again it seems better to err on the side of paranoia.
 *
 * REINDEX calls this with "rel" referencing the index to be rebuilt; here
 * we are worried about active indexscans on the index.  The trigger-event
 * check can be skipped, since we are doing no damage to the parent table.
 *
 * The statement name (eg, "ALTER TABLE") is passed for use in error messages.
 */
void
CheckTableNotInUse(Relation rel, const char *stmt)
{
	int			expected_refcnt;

	expected_refcnt = rel->rd_isnailed ? 2 : 1;

	/*
	 * XXX For a bitmap index, since vacuum (or vacuum full) is currently done through
	 * reindex_index, the reference count could be 2 (or 3). We set it
	 * here until vacuum is done properly.
	 */
	if (expected_refcnt == 1 &&
		RelationIsBitmapIndex(rel) &&
		(rel->rd_refcnt == 2 || rel->rd_refcnt == 3))
		expected_refcnt = rel->rd_refcnt;

	if (rel->rd_refcnt != expected_refcnt)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
		/* translator: first %s is a SQL command, eg ALTER TABLE */
				 errmsg("cannot %s \"%s\" because it is being used by active queries in this session",
						stmt, RelationGetRelationName(rel))));

	if (rel->rd_rel->relkind != RELKIND_INDEX &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX &&
		AfterTriggerPendingOnRel(RelationGetRelid(rel)))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
		/* translator: first %s is a SQL command, eg ALTER TABLE */
				 errmsg("cannot %s \"%s\" because it has pending trigger events",
						stmt, RelationGetRelationName(rel))));
}

/*
 * AlterTableLookupRelation
 *		Look up, and lock, the OID for the relation named by an alter table
 *		statement.
 */
Oid
AlterTableLookupRelation(AlterTableStmt *stmt, LOCKMODE lockmode)
{
	return RangeVarGetRelidExtended(stmt->relation, lockmode,
									stmt->missing_ok ? RVR_MISSING_OK : 0,
									RangeVarCallbackForAlterRelation,
									(void *) stmt);
}

/*
 * AlterTable
 *		Execute ALTER TABLE, which can be a list of subcommands
 *
 * ALTER TABLE is performed in three phases:
 *		1. Examine subcommands and perform pre-transformation checking.
 *		2. Validate and transform subcommands, and update system catalogs.
 *		3. Scan table(s) to check new constraints, and optionally recopy
 *		   the data into new table(s).
 * Phase 3 is not performed unless one or more of the subcommands requires
 * it.  The intention of this design is to allow multiple independent
 * updates of the table schema to be performed with only one pass over the
 * data.
 *
 * ATPrepCmd performs phase 1.  A "work queue" entry is created for
 * each table to be affected (there may be multiple affected tables if the
 * commands traverse a table inheritance hierarchy).  Also we do preliminary
 * validation of the subcommands.  Because earlier subcommands may change
 * the catalog state seen by later commands, there are limits to what can
 * be done in this phase.  Generally, this phase acquires table locks,
 * checks permissions and relkind, and recurses to find child tables.
 *
 * ATRewriteCatalogs performs phase 2 for each affected table.  (Note that
 * phases 2 and 3 normally do no explicit recursion, since phase 1 already
 * did it --- although some subcommands have to recurse in phase 2 instead.)
 * Certain subcommands need to be performed before others to avoid
 * unnecessary conflicts; for example, DROP COLUMN should come before
 * ADD COLUMN.  Therefore phase 1 divides the subcommands into multiple
 * lists, one for each logical "pass" of phase 2.
 *
 * ATRewriteTables performs phase 3 for those tables that need it.
 *
 * Thanks to the magic of MVCC, an error anywhere along the way rolls back
 * the whole operation; we don't have to do anything special to clean up.
 *
 * The caller must lock the relation, with an appropriate lock level
 * for the subcommands requested, using AlterTableGetLockLevel(stmt->cmds)
 * or higher. We pass the lock level down
 * so that we can apply it recursively to inherited tables. Note that the
 * lock level we want as we recurse might well be higher than required for
 * that specific subcommand. So we pass down the overall lock requirement,
 * rather than reassess it at lower levels.
 *
 * The caller also provides a "context" which is to be passed back to
 * utility.c when we need to execute a subcommand such as CREATE INDEX.
 * Some of the fields therein, such as the relid, are used here as well.
 */
void
AlterTable(AlterTableStmt *stmt, LOCKMODE lockmode,
		   AlterTableUtilityContext *context)
{
	Relation	rel;

	/* Caller is required to provide an adequate lock. */
	rel = relation_open(context->relid, NoLock);

	/*
	 * GPDB creates ALTER stmts and executes them internally as part of some
	 * partition related ALTER stmts, hence for such internal ALTER stmts
	 * can't meet this requirement.
	 */
	if (!stmt->is_internal)
		CheckTableNotInUse(rel, "ALTER TABLE");

	ATController(stmt, rel, stmt->cmds, stmt->relation->inh, lockmode, context);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/*
		 * If a transaction is in progress, kill any idle QE backends. They
		 * might be running with obsolete information in their relcaches. Any
		 * relcache invalidation events sent by the ALTER TABLE subcommands
		 * won't be sent to the other backend until the end of transaction, and
		 * we don't have any better way of invalidating them. The primary
		 * writer backends should be up-to-date, because we have used that to
		 * execute all the subcommands, so they should've created local
		 * invalidation events for themselves.
		 */
		if (IsTransactionBlock())
			DisconnectAndDestroyUnusedQEs();

		if (stmt->cmds && ENABLE_DISPATCH())
			CdbDispatchUtilityStatement((Node *) stmt,
										DF_CANCEL_ON_ERROR |
										DF_WITH_SNAPSHOT |
										DF_NEED_TWO_PHASE,
										GetAssignedOidsForDispatch(),
										NULL);
	}
}

/*
 * AlterTableInternal
 *
 * ALTER TABLE with target specified by OID
 *
 * We do not reject if the relation is already open, because it's quite
 * likely that one or more layers of caller have it open.  That means it
 * is unsafe to use this entry point for alterations that could break
 * existing query plans.  On the assumption it's not used for such, we
 * don't have to reject pending AFTER triggers, either.
 *
 * Also, since we don't have an AlterTableUtilityContext, this cannot be
 * used for any subcommand types that require parse transformation or
 * could generate subcommands that have to be passed to ProcessUtility.
 */
void
AlterTableInternal(Oid relid, List *cmds, bool recurse)
{
	Relation	rel;
	LOCKMODE	lockmode = AlterTableGetLockLevel(cmds);

	rel = relation_open(relid, lockmode);

	EventTriggerAlterTableRelid(relid);

	ATController(NULL, rel, cmds, recurse, lockmode, NULL);
}

/*
 * AlterTableGetLockLevel
 *
 * Sets the overall lock level required for the supplied list of subcommands.
 * Policy for doing this set according to needs of AlterTable(), see
 * comments there for overall explanation.
 *
 * Function is called before and after parsing, so it must give same
 * answer each time it is called. Some subcommands are transformed
 * into other subcommand types, so the transform must never be made to a
 * lower lock level than previously assigned. All transforms are noted below.
 *
 * Since this is called before we lock the table we cannot use table metadata
 * to influence the type of lock we acquire.
 *
 * There should be no lockmodes hardcoded into the subcommand functions. All
 * lockmode decisions for ALTER TABLE are made here only. The one exception is
 * ALTER TABLE RENAME which is treated as a different statement type T_RenameStmt
 * and does not travel through this section of code and cannot be combined with
 * any of the subcommands given here.
 *
 * Note that Hot Standby only knows about AccessExclusiveLocks on the primary
 * so any changes that might affect SELECTs running on standbys need to use
 * AccessExclusiveLocks even if you think a lesser lock would do, unless you
 * have a solution for that also.
 *
 * Also note that pg_dump uses only an AccessShareLock, meaning that anything
 * that takes a lock less than AccessExclusiveLock can change object definitions
 * while pg_dump is running. Be careful to check that the appropriate data is
 * derived by pg_dump using an MVCC snapshot, rather than syscache lookups,
 * otherwise we might end up with an inconsistent dump that can't restore.
 */
LOCKMODE
AlterTableGetLockLevel(List *cmds)
{
	/*
	 * This only works if we read catalog tables using MVCC snapshots.
	 */
	ListCell   *lcmd;
	LOCKMODE	lockmode = ShareUpdateExclusiveLock;

	foreach(lcmd, cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);
		LOCKMODE	cmd_lockmode = AccessExclusiveLock; /* default for compiler */

		switch (cmd->subtype)
		{
				/*
				 * These subcommands rewrite the heap, so require full locks.
				 */
			case AT_AddColumn:	/* may rewrite heap, in some cases and visible
								 * to SELECT */
			case AT_SetTableSpace:	/* must rewrite heap */
			case AT_AlterColumnType:	/* must rewrite heap */
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * These subcommands may require addition of toast tables. If
				 * we add a toast table to a table currently being scanned, we
				 * might miss data added to the new toast table by concurrent
				 * insert transactions.
				 */
			case AT_SetStorage: /* may add toast tables, see
								 * ATRewriteCatalogs() */
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * Removing constraints can affect SELECTs that have been
				 * optimized assuming the constraint holds true. See also
				 * CloneFkReferenced.
				 */
			case AT_DropConstraint: /* as DROP INDEX */
			case AT_DropNotNull:	/* may change some SQL plans */
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * Subcommands that may be visible to concurrent SELECTs
				 */
			case AT_DropColumn: /* change visible to SELECT */
			case AT_AddColumnToView:	/* CREATE VIEW */
			case AT_DropOids:	/* used to equiv to DropColumn */
			case AT_EnableAlwaysRule:	/* may change SELECT rules */
			case AT_EnableReplicaRule:	/* may change SELECT rules */
			case AT_EnableRule: /* may change SELECT rules */
			case AT_DisableRule:	/* may change SELECT rules */
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * Changing owner may remove implicit SELECT privileges
				 */
			case AT_ChangeOwner:	/* change visible to SELECT */
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * Changing foreign table options may affect optimization.
				 */
			case AT_GenericOptions:
			case AT_AlterColumnGenericOptions:
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * These subcommands affect write operations only.
				 */
			case AT_EnableTrig:
			case AT_EnableAlwaysTrig:
			case AT_EnableReplicaTrig:
			case AT_EnableTrigAll:
			case AT_EnableTrigUser:
			case AT_DisableTrig:
			case AT_DisableTrigAll:
			case AT_DisableTrigUser:
				cmd_lockmode = ShareRowExclusiveLock;
				break;

				/*
				 * These subcommands affect write operations only. XXX
				 * Theoretically, these could be ShareRowExclusiveLock.
				 */
			case AT_ColumnDefault:
			case AT_CookedColumnDefault:
			case AT_AlterConstraint:
			case AT_AddIndex:	/* from ADD CONSTRAINT */
			case AT_AddIndexConstraint:
			case AT_ReplicaIdentity:
			case AT_SetNotNull:
			case AT_EnableRowSecurity:
			case AT_DisableRowSecurity:
			case AT_ForceRowSecurity:
			case AT_NoForceRowSecurity:
			case AT_AddIdentity:
			case AT_DropIdentity:
			case AT_SetIdentity:
			case AT_DropExpression:
			case AT_SetCompression:
				cmd_lockmode = AccessExclusiveLock;
				break;

			case AT_AddConstraint:
			case AT_AddConstraintRecurse:	/* becomes AT_AddConstraint */
			case AT_ReAddConstraint:	/* becomes AT_AddConstraint */
			case AT_ReAddDomainConstraint:	/* becomes AT_AddConstraint */
				if (IsA(cmd->def, Constraint))
				{
					Constraint *con = (Constraint *) cmd->def;

					switch (con->contype)
					{
						case CONSTR_EXCLUSION:
						case CONSTR_PRIMARY:
						case CONSTR_UNIQUE:

							/*
							 * Cases essentially the same as CREATE INDEX. We
							 * could reduce the lock strength to ShareLock if
							 * we can work out how to allow concurrent catalog
							 * updates. XXX Might be set down to
							 * ShareRowExclusiveLock but requires further
							 * analysis.
							 */
							cmd_lockmode = AccessExclusiveLock;
							break;
						case CONSTR_FOREIGN:

							/*
							 * We add triggers to both tables when we add a
							 * Foreign Key, so the lock level must be at least
							 * as strong as CREATE TRIGGER.
							 */
							cmd_lockmode = ShareRowExclusiveLock;
							break;

						default:
							cmd_lockmode = AccessExclusiveLock;
					}
				}
				break;

				/*
				 * These subcommands affect inheritance behaviour. Queries
				 * started before us will continue to see the old inheritance
				 * behaviour, while queries started after we commit will see
				 * new behaviour. No need to prevent reads or writes to the
				 * subtable while we hook it up though. Changing the TupDesc
				 * may be a problem, so keep highest lock.
				 */
			case AT_AddInherit:
			case AT_DropInherit:
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * These subcommands affect implicit row type conversion. They
				 * have affects similar to CREATE/DROP CAST on queries. don't
				 * provide for invalidating parse trees as a result of such
				 * changes, so we keep these at AccessExclusiveLock.
				 */
			case AT_AddOf:
			case AT_DropOf:
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * Only used by CREATE OR REPLACE VIEW which must conflict
				 * with an SELECTs currently using the view.
				 */
			case AT_ReplaceRelOptions:
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * These subcommands affect general strategies for performance
				 * and maintenance, though don't change the semantic results
				 * from normal data reads and writes. Delaying an ALTER TABLE
				 * behind currently active writes only delays the point where
				 * the new strategy begins to take effect, so there is no
				 * benefit in waiting. In this case the minimum restriction
				 * applies: we don't currently allow concurrent catalog
				 * updates.
				 */
			case AT_SetStatistics:	/* Uses MVCC in getTableAttrs() */
			case AT_ClusterOn:	/* Uses MVCC in getIndexes() */
			case AT_DropCluster:	/* Uses MVCC in getIndexes() */
			case AT_SetOptions: /* Uses MVCC in getTableAttrs() */
			case AT_ResetOptions:	/* Uses MVCC in getTableAttrs() */
				cmd_lockmode = ShareUpdateExclusiveLock;
				break;

			case AT_SetLogged:
			case AT_SetUnLogged:
				cmd_lockmode = AccessExclusiveLock;
				break;

			case AT_ValidateConstraint: /* Uses MVCC in getConstraints() */
				cmd_lockmode = ShareUpdateExclusiveLock;
				break;

				/*
				 * Rel options are more complex than first appears. Options
				 * are set here for tables, views and indexes; for historical
				 * reasons these can all be used with ALTER TABLE, so we can't
				 * decide between them using the basic grammar.
				 */
			case AT_SetRelOptions:	/* Uses MVCC in getIndexes() and
									 * getTables() */
			case AT_ResetRelOptions:	/* Uses MVCC in getIndexes() and
										 * getTables() */
				cmd_lockmode = AlterTableGetRelOptionsLockLevel((List *) cmd->def);
				break;

			case AT_AttachPartition:
				cmd_lockmode = ShareUpdateExclusiveLock;
				break;

			case AT_DetachPartition:
				if (((PartitionCmd *) cmd->def)->concurrent)
					cmd_lockmode = ShareUpdateExclusiveLock;
				else
					cmd_lockmode = AccessExclusiveLock;
				break;

			case AT_DetachPartitionFinalize:
				cmd_lockmode = ShareUpdateExclusiveLock;
				break;

			case AT_CheckNotNull:

				/*
				 * This only examines the table's schema; but lock must be
				 * strong enough to prevent concurrent DROP NOT NULL.
				 */
				cmd_lockmode = AccessShareLock;
				break;

				/* GPDB additions */
			case AT_ExpandTable:
			case AT_ExpandPartitionTablePrepare:
			case AT_ShrinkTable:
			case AT_SetDistributedBy:
				cmd_lockmode = AccessExclusiveLock;
				break;

				/*
				 * GPDB: For these commands lookup root partition to construct
				 * the appropriate stmt. Hence, AccessShareLock should be
				 * good. Stronger lock is mostly not required.
				 */
			case AT_PartTruncate:
			case AT_PartAlter:
				cmd_lockmode = AccessShareLock;
				break;

			case AT_PartAdd:
			case AT_PartDrop:
			case AT_PartSplit:
			case AT_PartRename:
			case AT_PartExchange:
			case AT_PartSetTemplate:
				cmd_lockmode = AccessExclusiveLock;
				break;

			case AT_SetTags:
			case AT_UnsetTags:
				cmd_lockmode = ShareUpdateExclusiveLock;
				break;

			default:			/* oops */
				elog(ERROR, "unrecognized alter table type: %d",
					 (int) cmd->subtype);
				break;
		}

		/*
		 * Take the greatest lockmode from any subcommand
		 */
		if (cmd_lockmode > lockmode)
			lockmode = cmd_lockmode;
	}

	return lockmode;
}

/*
 * ATController provides top level control over the phases.
 *
 * parsetree is passed in to allow it to be passed to event triggers
 * when requested.
 */
static void
ATController(AlterTableStmt *parsetree,
			 Relation rel, List *cmds, bool recurse, LOCKMODE lockmode,
			 AlterTableUtilityContext *context)
{
	List	   *wqueue = NIL;
	ListCell   *lcmd;

	cdb_sync_oid_to_segments();

	/*
	 * If the parsetree is dispatched from QD, we use the wqueue generated in QD
	 * and ignore all subcmds that will be generated in QEs. Because the parsetree
	 * and its subcmds in `AlteredTableInfo` contain some info that must sync
	 * between QD and QEs, e.g. index name, sequence name. It's not allowed to
	 * choose the relation/index/sequence/etc. names on QEs
	 *
	 * The following phase 1 should also be executed in QEs, which is different to
	 * the previous version. When ALTER a column type with IDENTITY, the generated
	 * beforeStmts are also executed. In other words, the phase 1 does not only
	 * preparation step, but also execute some utilities, which can't be omitted.
	 * We only run the phase 1, but discard all generated subcmds by QE itself.
	 *
	 * GPDB_13_MERGE_FIXME:
	 * ATController() is called in two different situations:
	 * 1. By AlterTable(), parsetree and context are not NULL, and parsetree->wqueue
	 *     is dispatched from QD, we ignore all the subcmds generated in QE side.
	 * 2. By AlterTableInternal(), which is used in internal utility commands.
	 *    The parsetree and context are NULL, so the QE needs to build their own
	 *    subcmds normally.
	 *
	 * However, some internal functions doesn't know the difference. Currently,
	 * we assume that AlterTableInternal() always passes `context` as NULL,
	 * and the cmd->subtype isn't complicated, i.e. it doesn't call
	 * transformAlterTableStmt() internally, could not be AlterColumnType.
	 * If any of the assumptions is broken, we must refactor the logic to
	 * adapter GPDB.
	 */
	if (parsetree && parsetree->wqueue)
	{
		ListCell   *lc;

		Assert(Gp_role == GP_ROLE_EXECUTE);
		wqueue = parsetree->wqueue;

		foreach (lc, wqueue)
		{
			/*
			 * The old tuple descriptors are not dispatched, so fetch
			 * them here.
			 */
			AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(lc);
			Relation rel;

			rel = relation_open(tab->relid, lockmode);
			tab->oldDesc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
			relation_close(rel, NoLock);
		}
	}

	/* Phase 1: preliminary examination of commands, create work queue */
	foreach(lcmd, cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

		ATPrepCmd(&wqueue, rel, cmd, recurse, false, lockmode, context);
	}

	/* Close the relation, but keep lock until commit */
	relation_close(rel, NoLock);

	/* Phase 2: update system catalogs */
	ATRewriteCatalogs(&wqueue, lockmode, context);

	/* Phase 3: scan/rewrite tables as needed, and run afterStmts */
	ATRewriteTables(parsetree, &wqueue, lockmode, context);

#ifdef USE_ASSERT_CHECKING
	/* Check that all before/constraint list should be consumed */
	if (parsetree && parsetree->wqueue)
	{
		ListCell *lc;
		Assert(Gp_role == GP_ROLE_EXECUTE);
		foreach(lc, wqueue)
		{
			AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(lc);
			Assert(tab->beforeStmtLists == NULL);
			Assert(tab->constraintLists == NULL);
		}
	}
#endif
	/*
	 * In QD, include the work queue in the command for dispatching,
	 */
	if (Gp_role == GP_ROLE_DISPATCH && parsetree)
	{
		parsetree->lockmode = lockmode;
		parsetree->wqueue = wqueue;
	}
}

/*
 * ATPrepCmd
 *
 * Traffic cop for ALTER TABLE Phase 1 operations, including simple
 * recursion and permission checks.
 *
 * Caller must have acquired appropriate lock type on relation already.
 * This lock should be held until commit.
 */
static void
ATPrepCmd(List **wqueue, Relation rel, AlterTableCmd *cmd,
		  bool recurse, bool recursing, LOCKMODE lockmode,
		  AlterTableUtilityContext *context)
{
	AlteredTableInfo *tab;
	int			pass = AT_PASS_UNSET;

	/* Find or create work queue entry for this table */
	tab = ATGetQueueEntry(wqueue, rel);

	/*
	 * Disallow any ALTER TABLE other than ALTER TABLE DETACH FINALIZE on
	 * partitions that are pending detach.
	 */
	if (rel->rd_rel->relispartition &&
		cmd->subtype != AT_DetachPartitionFinalize &&
		PartitionHasPendingDetach(RelationGetRelid(rel)))
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("cannot alter partition \"%s\" with an incomplete detach",
					   RelationGetRelationName(rel)),
				errhint("Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending detach operation."));

	/*
	 * Copy the original subcommand for each table.  This avoids conflicts
	 * when different child tables need to make different parse
	 * transformations (for example, the same column may have different column
	 * numbers in different children).
	 */
	cmd = copyObject(cmd);

	/*
	 * Do permissions and relkind checking, recursion to child tables if
	 * needed, and any additional phase-1 processing needed.  (But beware of
	 * adding any processing that looks at table details that another
	 * subcommand could change.  In some cases we reject multiple subcommands
	 * that could try to change the same state in contrary ways.)
	 */
	switch (cmd->subtype)
	{
		case AT_AddColumn:		/* ADD COLUMN */
			ATSimplePermissions(rel,
								ATT_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
			ATPrepAddColumn(wqueue, rel, recurse, recursing, false, cmd,
							lockmode, context);
			/* Recursion occurs during execution phase */
			pass = AT_PASS_ADD_COL;
			break;
		case AT_AddColumnToView:	/* add column via CREATE OR REPLACE VIEW */
			ATSimplePermissions(rel, ATT_VIEW);
			ATPrepAddColumn(wqueue, rel, recurse, recursing, true, cmd,
							lockmode, context);
			/* Recursion occurs during execution phase */
			pass = AT_PASS_ADD_COL;
			break;
		case AT_ColumnDefault:	/* ALTER COLUMN DEFAULT */

			/*
			 * We allow defaults on views so that INSERT into a view can have
			 * default-ish behavior.  This works because the rewriter
			 * substitutes default values into INSERTs before it expands
			 * rules.
			 */
			ATSimplePermissions(rel, ATT_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE);
			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			/* No command-specific prep needed */
			pass = cmd->def ? AT_PASS_ADD_OTHERCONSTR : AT_PASS_DROP;
			break;
		case AT_CookedColumnDefault:	/* add a pre-cooked default */
			/* This is currently used only in CREATE TABLE */
			/* (so the permission check really isn't necessary) */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			/* This command never recurses */
			pass = AT_PASS_ADD_OTHERCONSTR;
			break;
		case AT_AddIdentity:
			ATSimplePermissions(rel, ATT_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE);
			/* This command never recurses */
			pass = AT_PASS_ADD_OTHERCONSTR;
			break;
		case AT_SetIdentity:
			ATSimplePermissions(rel, ATT_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE);
			/* This command never recurses */
			/* This should run after AddIdentity, so do it in MISC pass */
			pass = AT_PASS_MISC;
			break;
		case AT_DropIdentity:
			ATSimplePermissions(rel, ATT_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE);
			/* This command never recurses */
			pass = AT_PASS_DROP;
			break;
		case AT_DropNotNull:	/* ALTER COLUMN DROP NOT NULL */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			ATPrepDropNotNull(rel, recurse, recursing);
			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			pass = AT_PASS_DROP;
			break;
		case AT_SetNotNull:		/* ALTER COLUMN SET NOT NULL */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			/* Need command-specific recursion decision */
			ATPrepSetNotNull(wqueue, rel, cmd, recurse, recursing,
							 lockmode, context);
			pass = AT_PASS_COL_ATTRS;
			break;
		case AT_CheckNotNull:	/* check column is already marked NOT NULL */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			/* No command-specific prep needed */
			pass = AT_PASS_COL_ATTRS;
			break;
		case AT_DropExpression: /* ALTER COLUMN DROP EXPRESSION */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			ATPrepDropExpression(rel, cmd, recurse, recursing, lockmode);
			pass = AT_PASS_DROP;
			break;
		case AT_SetStatistics:	/* ALTER COLUMN SET STATISTICS */
			ATSimplePermissions(rel, ATT_TABLE | ATT_MATVIEW | ATT_INDEX | ATT_PARTITIONED_INDEX | ATT_FOREIGN_TABLE | ATT_DIRECTORY_TABLE);
			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_SetOptions:		/* ALTER COLUMN SET ( options ) */
		case AT_ResetOptions:	/* ALTER COLUMN RESET ( options ) */
			ATSimplePermissions(rel, ATT_TABLE | ATT_MATVIEW | ATT_FOREIGN_TABLE);
			/* This command never recurses */
			pass = AT_PASS_MISC;
			break;
		case AT_SetStorage:		/* ALTER COLUMN SET STORAGE */
			ATSimplePermissions(rel, ATT_TABLE | ATT_MATVIEW | ATT_FOREIGN_TABLE);
			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_SetCompression: /* ALTER COLUMN SET COMPRESSION */
			ATSimplePermissions(rel, ATT_TABLE | ATT_MATVIEW);
			/* This command never recurses */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_DropColumn:		/* DROP COLUMN */
		case AT_DropColumnRecurse:
			ATSimplePermissions(rel,
								ATT_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
			ATPrepDropColumn(wqueue, rel, recurse, recursing, cmd,
							 lockmode, context);
			/* Recursion occurs during execution phase */
			pass = AT_PASS_DROP;
			break;
		case AT_AddIndex:		/* ADD INDEX */
			ATSimplePermissions(rel, ATT_TABLE | ATT_DIRECTORY_TABLE);
			/* This command never recurses */
			/* No command-specific prep needed */
			pass = AT_PASS_ADD_INDEX;
			break;
		case AT_AddConstraint:	/* ADD CONSTRAINT */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			/* Recursion occurs during execution phase */
			/* No command-specific prep needed except saving recurse flag */
			if (recurse)
				cmd->subtype = AT_AddConstraintRecurse;
			pass = AT_PASS_ADD_CONSTR;
			break;
		case AT_AddConstraintRecurse: /* ADD check CONSTRAINT internal */
			/* Parent/Base CHECK constraints apply to child/part tables here.
			 * No need for ATPartitionCheck
			 */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			pass = AT_PASS_ADD_CONSTR;
			break;
		case AT_AddIndexConstraint: /* ADD CONSTRAINT USING INDEX */
			ATSimplePermissions(rel, ATT_TABLE);
			/* This command never recurses */
			/* No command-specific prep needed */
			pass = AT_PASS_ADD_INDEXCONSTR;
			break;
		case AT_DropConstraint: /* DROP CONSTRAINT */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			ATCheckPartitionsNotInUse(rel, lockmode);
			/* Other recursion occurs during execution phase */
			/* No command-specific prep needed except saving recurse flag */
			if (recurse)
				cmd->subtype = AT_DropConstraintRecurse;
			pass = AT_PASS_DROP;
			break;
		case AT_AlterColumnType:	/* ALTER COLUMN TYPE */
			ATSimplePermissions(rel,
								ATT_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
			/* See comments for ATPrepAlterColumnType */
			cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, recurse, lockmode,
									  AT_PASS_UNSET, context);
			Assert(cmd != NULL);
			/* Performs own recursion */
			ATPrepAlterColumnType(wqueue, tab, rel, recurse, recursing, cmd,
								  lockmode, context);
			pass = AT_PASS_ALTER_TYPE;
			break;
		case AT_AlterColumnGenericOptions:
			ATSimplePermissions(rel, ATT_FOREIGN_TABLE);
			/* This command never recurses */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_ChangeOwner:	/* ALTER OWNER */
			/* This command never recurses */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_ClusterOn:		/* CLUSTER ON */
		case AT_DropCluster:	/* SET WITHOUT CLUSTER */
			ATSimplePermissions(rel, ATT_TABLE | ATT_MATVIEW);
			/* These commands never recurse */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_SetLogged:		/* SET LOGGED */
			ATSimplePermissions(rel, ATT_TABLE);
			/* Commands are dispatched from QD to QE. Don't complain on QE. */
			if (tab->chgPersistence && Gp_role != GP_ROLE_EXECUTE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot change persistence setting twice")));
			tab->chgPersistence = ATPrepChangePersistence(rel, true);
			/* force rewrite if necessary; see comment in ATRewriteTables */
			if (tab->chgPersistence)
			{
				tab->rewrite |= AT_REWRITE_ALTER_PERSISTENCE;
				tab->newrelpersistence = RELPERSISTENCE_PERMANENT;
			}
			pass = AT_PASS_MISC;
			break;
		case AT_SetUnLogged:	/* SET UNLOGGED */
			ATSimplePermissions(rel, ATT_TABLE);
			/* Commands are dispatched from QD to QE. Don't complain on QE. */
			if (tab->chgPersistence && Gp_role != GP_ROLE_EXECUTE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot change persistence setting twice")));
			tab->chgPersistence = ATPrepChangePersistence(rel, false);
			/* force rewrite if necessary; see comment in ATRewriteTables */
			if (tab->chgPersistence)
			{
				tab->rewrite |= AT_REWRITE_ALTER_PERSISTENCE;
				tab->newrelpersistence = RELPERSISTENCE_UNLOGGED;
			}
			pass = AT_PASS_MISC;
			break;
		case AT_DropOids:		/* SET WITHOUT OIDS */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			pass = AT_PASS_DROP;
			break;
		case AT_SetTableSpace:	/* SET TABLESPACE */
			ATSimplePermissions(rel, ATT_TABLE | ATT_MATVIEW | ATT_INDEX |
								ATT_PARTITIONED_INDEX);
			/*
			 * GPDB: This command never recurses in upstream Postgres, however,
			 * it recurses in Cloudberry.
			 */
			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			ATPrepSetTableSpace(tab, rel, cmd->name, lockmode);
			pass = AT_PASS_MISC;	/* doesn't actually matter */
			break;
		case AT_SetRelOptions:	/* SET (...) */
		case AT_ResetRelOptions:	/* RESET (...) */
		case AT_ReplaceRelOptions:	/* reset them all, then set just these */
			ATSimplePermissions(rel, ATT_TABLE | ATT_VIEW | ATT_MATVIEW | ATT_INDEX);
			/* This command never recurses */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_SetDistributedBy:	/* SET DISTRIBUTED BY */
			/* Setting distributed by is meaningless in utility mode. */
			if (Gp_role == GP_ROLE_UTILITY)
			{
				pass = AT_PASS_MISC;
				break;
			}
			ATSimplePermissions(rel, ATT_TABLE | ATT_DIRECTORY_TABLE);

			if (!recursing) /* MPP-5772, MPP-5784 */
			{
				DistributedBy *ldistro;
				GpPolicy   *policy;

				// GPDB_12_MERGE_FIXME: is this still needed?
				//ATExternalPartitionCheck(cmd->subtype, rel, recursing);

				Assert(IsA(cmd->def, List));
				/* The distributeby clause is the second element of cmd->def */
				ldistro = (DistributedBy *) lsecond((List *)cmd->def);
				if (ldistro != NULL)
				{
					ldistro->numsegments = rel->rd_cdbpolicy->numsegments;

					policy = getPolicyForDistributedBy(ldistro, rel->rd_att);
					/* can't set the distribution policy of interior table */
					if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE && rel->rd_rel->relispartition)
					{
						ereport(ERROR,
								(errcode(ERRCODE_WRONG_OBJECT_TYPE),
								 errmsg("can't set the distribution policy of \"%s\"",
										RelationGetRelationName(rel)),
								 errhint("Distribution policy can not be set for an interior branch.")));
					}
					if (!GpPolicyEqual(policy, rel->rd_cdbpolicy))
					{
						/* Reject leaf of partitioned tables if new policy is different of parent table*/
						if (rel->rd_rel->relispartition)
						{
							/* We can only set policy of child table to the same with parent table */
							Oid parent_oid = get_partition_parent(RelationGetRelid(rel), false);
							/* Use AccessShareLock to allow set distributed in parallel */
							Relation parent_rel = relation_open(parent_oid, AccessShareLock);
							if (!GpPolicyEqualByName(RelationGetDescr(rel), policy,
													 RelationGetDescr(parent_rel), parent_rel->rd_cdbpolicy))
							{
								ereport(ERROR,
									(errcode(ERRCODE_WRONG_OBJECT_TYPE),
									 errmsg("can't set the distribution policy of \"%s\"",
											RelationGetRelationName(rel)),
									 errhint("Distribution policy of a partition can only be the same as its parent's.")));
							}
							relation_close(parent_rel, NoLock);
						}

						if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
							ldistro && ldistro->ptype == POLICYTYPE_REPLICATED)
							ereport(ERROR,
									(errcode(ERRCODE_WRONG_OBJECT_TYPE),
									 errmsg("can't set the distribution policy of a partition table to REPLICATED")));

						if (!recurse)
						{
							/* Don't allow ALTER TABLE ONLY on a partitioned table */
							if (RelationGetPartitionKey(rel))
							{
								ereport(ERROR,
										(errcode(ERRCODE_WRONG_OBJECT_TYPE),
										 errmsg("can't set the distribution policy of \"%s\" ONLY",
												RelationGetRelationName(rel)),
										 errhint("Distribution policy can be set for an entire partitioned table, not for one of its leaf parts or an interior branch.")));
							}
						}
					}
				}
			}
			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			pass = AT_PASS_MISC;
			break;
		case AT_ExpandTable:
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE | ATT_MATVIEW);

			/* ATTACH and DETACH will process in ATExecAttachPartition function */
			if (!recursing)
			{
				if (Gp_role == GP_ROLE_DISPATCH &&
					rel->rd_cdbpolicy->numsegments == getgpsegmentCount())
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot expand table \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("table has already been expanded")));

				if (rel->rd_rel->relispartition)
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot expand leaf or interior partition \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("Root/leaf/interior partitions need to have same numsegments"),
							 errhint("Call ALTER TABLE EXPAND TABLE on the root table instead")));
				}
			}

			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			pass = AT_PASS_MISC;
			break;

		case AT_ExpandPartitionTablePrepare:
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE | ATT_MATVIEW);

			/* ATTACH and DETACH will process in ATExecAttachPartition function */
			if (!recursing)
			{
				if (Gp_role == GP_ROLE_DISPATCH &&
					rel->rd_cdbpolicy->numsegments == getgpsegmentCount())
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot expand partition table prepare \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("table has already been expanded partiton prepare")));
				if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE || rel->rd_rel->relispartition)
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot expand partition table prepare \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("only root partition can be expanded partition prepare")));
				}

				if (!GpPolicyIsPartitioned(rel->rd_cdbpolicy))
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot expand partition table prepare \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("only hash/randomly table can be expanded partition prepare")));
				}
			}

			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			pass = AT_PASS_MISC;
			break;

		case AT_ShrinkTable:
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE | ATT_MATVIEW);

			/* ATTACH and DETACH will process in ATExecAttachPartition function */
			if (!recursing)
			{
				Assert(IsA(cmd->def, Integer));
				if (Gp_role == GP_ROLE_DISPATCH &&
					rel->rd_cdbpolicy->numsegments <= intVal(cmd->def))
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot shrink table \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("table numsegments \"%d\", shrink size \"%d\" " ,rel->rd_cdbpolicy->numsegments, intVal(cmd->def))));

				if (rel->rd_rel->relispartition)
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot shrink leaf or interior partition \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("Root/leaf/interior partitions need to have same numsegments"),
							 errhint("Call ALTER TABLE SHRINK TABLE on the root table instead")));
				}

			}

			ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			pass = AT_PASS_MISC;
			break;

		case AT_AddInherit:		/* INHERIT */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			/* This command never recurses */
			ATPrepAddInherit(rel);
			pass = AT_PASS_MISC;
			break;
		case AT_DropInherit:	/* NO INHERIT */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			/* This command never recurses */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_AlterConstraint:	/* ALTER CONSTRAINT */
			ATSimplePermissions(rel, ATT_TABLE);
			/* Recursion occurs during execution phase */
			pass = AT_PASS_MISC;
			break;
		case AT_ValidateConstraint: /* VALIDATE CONSTRAINT */
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);
			/* Recursion occurs during execution phase */
			/* No command-specific prep needed except saving recurse flag */
			if (recurse)
				cmd->subtype = AT_ValidateConstraintRecurse;
			pass = AT_PASS_MISC;
			break;
		case AT_ReplicaIdentity:	/* REPLICA IDENTITY ... */
			ATSimplePermissions(rel, ATT_TABLE | ATT_MATVIEW);
			pass = AT_PASS_MISC;
			/* This command never recurses */
			/* No command-specific prep needed */
			break;
		case AT_EnableTrig:		/* ENABLE TRIGGER variants */
		case AT_EnableAlwaysTrig:
		case AT_EnableReplicaTrig:
		case AT_EnableTrigAll:
		case AT_EnableTrigUser:
		case AT_DisableTrig:	/* DISABLE TRIGGER variants */
		case AT_DisableTrigAll:
		case AT_DisableTrigUser:
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE | ATT_DIRECTORY_TABLE);
			if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
				ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
			pass = AT_PASS_MISC;
			break;
		case AT_EnableRule:		/* ENABLE/DISABLE RULE variants */
		case AT_EnableAlwaysRule:
		case AT_EnableReplicaRule:
		case AT_DisableRule:
		case AT_AddOf:			/* OF */
		case AT_DropOf:			/* NOT OF */
		case AT_EnableRowSecurity:
		case AT_DisableRowSecurity:
		case AT_ForceRowSecurity:
		case AT_NoForceRowSecurity:
			ATSimplePermissions(rel, ATT_TABLE | ATT_DIRECTORY_TABLE);
			/* These commands never recurse */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_GenericOptions:
			ATSimplePermissions(rel, ATT_FOREIGN_TABLE);
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_AttachPartition:
			ATSimplePermissions(rel, ATT_TABLE | ATT_PARTITIONED_INDEX);
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_DetachPartition:
			ATSimplePermissions(rel, ATT_TABLE);
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_DetachPartitionFinalize:
			ATSimplePermissions(rel, ATT_TABLE);
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_PartAdd:
		case AT_PartDrop:
		case AT_PartAlter:
		case AT_PartSplit:
		case AT_PartRename:
		case AT_PartTruncate:
		case AT_PartExchange:
		case AT_PartSetTemplate:
			ATSimplePermissions(rel, ATT_TABLE);
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_SetTags:
		case AT_UnsetTags:
			ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE | ATT_INDEX | ATT_SEQUENCE | ATT_VIEW | ATT_MATVIEW);
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		default:				/* oops */
			elog(ERROR, "unrecognized alter table type: %d",
				 (int) cmd->subtype);
			pass = AT_PASS_UNSET;	/* keep compiler quiet */
			break;
	}
	Assert(pass > AT_PASS_UNSET);

	/* Add the subcommand to the appropriate list for phase 2 */
	/*
	 * The phase 1 still needs to run for QEs, because it may run
	 * utility command in `ATParseTransformCmd()`. If the QE doesn't
	 * run phase 1, its catalog may be different from the QD,
	 * an example is ALTER COLUMN TYPE for identity.
	 *
	 * GPDB_13_MERGE_FIXME:
	 * In normal ALTER TABLE, the tab->subcmds is dispatched from QD,
	 * the QE should ignore its own generated cmd, because these cmds
	 * may contain some relation/index/sequence names that must be
	 * the same on all nodes(coordinator + segments).
	 *
	 * The exception is that AlterTableInternal may call ATController()
	 * in some utility command internally.
	 * The parsetree and context are both NULL in this execution path,
	 * and the wqueue is not dispatched from QD, so no subcmds are filled.
	 * The QE needs to fill the tab->subcmds by itself.
	 *
	 * Currently, we assume that AlterTableInternal() always passes
	 * `context` as NULL, and the cmd->subtype isn't complicated, i.e.
	 * it doesn't calls transformAlterTableStmt() internally, could not
	 * be AlterColumnType.
	 * If any of the assumptions is broken, we must refactor the logic to
	 * adapter GPDB.
	 */
	if (Gp_role != GP_ROLE_EXECUTE || context == NULL)
		tab->subcmds[pass] = lappend(tab->subcmds[pass], cmd);
}

/*
 * ATRewriteCatalogs
 *
 * Traffic cop for ALTER TABLE Phase 2 operations.  Subcommands are
 * dispatched in a "safe" execution order (designed to avoid unnecessary
 * conflicts).
 */
static void
ATRewriteCatalogs(List **wqueue, LOCKMODE lockmode,
				  AlterTableUtilityContext *context)
{
	int			pass;
	ListCell   *ltab;

	/*
	 * We process all the tables "in parallel", one pass at a time.  This is
	 * needed because we may have to propagate work from one table to another
	 * (specifically, ALTER TYPE on a foreign key's PK has to dispatch the
	 * re-adding of the foreign key constraint to the other table).  Work can
	 * only be propagated into later passes, however.
	 */
	for (pass = 0; pass < AT_NUM_PASSES; pass++)
	{
		/* Go through each table that needs to be processed */
		foreach(ltab, *wqueue)
		{
			AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);
			List	   *subcmds = tab->subcmds[pass];
			ListCell   *lcmd;

			if (subcmds == NIL)
				continue;

			/*
			 * Open the relation and store it in tab.  This allows subroutines
			 * close and reopen, if necessary.  Appropriate lock was obtained
			 * by phase 1, needn't get it again.
			 */
			tab->rel = relation_open(tab->relid, NoLock);

			foreach(lcmd, subcmds)
				ATExecCmd(wqueue, tab,
						  castNode(AlterTableCmd, lfirst(lcmd)),
						  lockmode, pass, context);

			/*
			 * After the ALTER TYPE pass, do cleanup work (this is not done in
			 * ATExecAlterColumnType since it should be done only once if
			 * multiple columns of a table are altered).
			 */
			if (pass == AT_PASS_ALTER_TYPE)
				ATPostAlterTypeCleanup(wqueue, tab, lockmode, context);

			if (tab->rel)
			{
				relation_close(tab->rel, NoLock);
				tab->rel = NULL;
			}
		}
	}

	/* Check to see if a toast table must be added. */
	foreach(ltab, *wqueue)
	{
		AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);

		/*
		 * If the table is source table of ATTACH PARTITION command, we did
		 * not modify anything about it that will change its toasting
		 * requirement, so no need to check.
		 */
		if (((tab->relkind == RELKIND_RELATION ||
			  tab->relkind == RELKIND_PARTITIONED_TABLE) &&
			 tab->partition_constraint == NULL) ||
			tab->relkind == RELKIND_MATVIEW)
			AlterTableCreateToastTable(tab->relid, (Datum) 0, lockmode);
	}
}

/*
 * ATExecCmd: dispatch a subcommand to appropriate execution routine
 *
 * NOTE: we need to use a pointer to Relation here since the relation
 * address may be changed by ATPExecPartSplit(). This is different
 * behavior from Postgres upstream.
 */
static void
ATExecCmd(List **wqueue, AlteredTableInfo *tab,
		  AlterTableCmd *cmd, LOCKMODE lockmode, int cur_pass,
		  AlterTableUtilityContext *context)
{
	ObjectAddress address = InvalidObjectAddress;
	Relation	rel = tab->rel;

	switch (cmd->subtype)
	{
		case AT_AddColumn:		/* ADD COLUMN */
		case AT_AddColumnToView:	/* add column via CREATE OR REPLACE VIEW */
			address = ATExecAddColumn(wqueue, tab, rel, &cmd,
									  false, false,
									  lockmode, cur_pass, context);
			break;
		case AT_AddColumnRecurse:
			address = ATExecAddColumn(wqueue, tab, rel, &cmd,
									  true, false,
									  lockmode, cur_pass, context);
			break;
		case AT_ColumnDefault:	/* ALTER COLUMN DEFAULT */
			address = ATExecColumnDefault(rel, cmd->name, cmd->def, lockmode);
			break;
		case AT_CookedColumnDefault:	/* add a pre-cooked default */
			address = ATExecCookedColumnDefault(rel, cmd->num, cmd->def);
			break;
		case AT_AddIdentity:
			cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode,
									  cur_pass, context);
			Assert(cmd != NULL);
			address = ATExecAddIdentity(rel, cmd->name, cmd->def, lockmode);
			break;
		case AT_SetIdentity:
			cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode,
									  cur_pass, context);
			Assert(cmd != NULL);
			address = ATExecSetIdentity(rel, cmd->name, cmd->def, lockmode);
			break;
		case AT_DropIdentity:
			address = ATExecDropIdentity(rel, cmd->name, cmd->missing_ok, lockmode);
			break;
		case AT_DropNotNull:	/* ALTER COLUMN DROP NOT NULL */
			address = ATExecDropNotNull(rel, cmd->name, lockmode);
			break;
		case AT_SetNotNull:		/* ALTER COLUMN SET NOT NULL */
			address = ATExecSetNotNull(tab, rel, cmd->name, lockmode);
			break;
		case AT_CheckNotNull:	/* check column is already marked NOT NULL */
			ATExecCheckNotNull(tab, rel, cmd->name, lockmode);
			break;
		case AT_DropExpression:
			address = ATExecDropExpression(rel, cmd->name, cmd->missing_ok, lockmode);
			break;
		case AT_SetStatistics:	/* ALTER COLUMN SET STATISTICS */
			address = ATExecSetStatistics(rel, cmd->name, cmd->num, cmd->def, lockmode);
			break;
		case AT_SetOptions:		/* ALTER COLUMN SET ( options ) */
			address = ATExecSetOptions(rel, cmd->name, cmd->def, false, lockmode);
			break;
		case AT_ResetOptions:	/* ALTER COLUMN RESET ( options ) */
			address = ATExecSetOptions(rel, cmd->name, cmd->def, true, lockmode);
			break;
		case AT_SetStorage:		/* ALTER COLUMN SET STORAGE */
			address = ATExecSetStorage(rel, cmd->name, cmd->def, lockmode);
			break;
		case AT_SetCompression:
			address = ATExecSetCompression(tab, rel, cmd->name, cmd->def,
										   lockmode);
			break;
		case AT_DropColumn:		/* DROP COLUMN */
			address = ATExecDropColumn(wqueue, rel, cmd->name,
									   cmd->behavior, false, false,
									   cmd->missing_ok, lockmode,
									   NULL);
			break;
		case AT_DropColumnRecurse:	/* DROP COLUMN with recursion */
			address = ATExecDropColumn(wqueue, rel, cmd->name,
									   cmd->behavior, true, false,
									   cmd->missing_ok, lockmode,
									   NULL);
			break;
		case AT_AddIndex:		/* ADD INDEX */
			address = ATExecAddIndex(tab, rel, (IndexStmt *) cmd->def, false,
									 lockmode);
			break;
		case AT_ReAddIndex:		/* ADD INDEX */
			address = ATExecAddIndex(tab, rel, (IndexStmt *) cmd->def, true,
									 lockmode);
			break;
		case AT_ReAddStatistics:	/* ADD STATISTICS */
			address = ATExecAddStatistics(tab, rel, (CreateStatsStmt *) cmd->def,
										  true, lockmode);
			break;
		case AT_AddConstraint:	/* ADD CONSTRAINT */
			/* Transform the command only during initial examination */
			if (cur_pass == AT_PASS_ADD_CONSTR)
				cmd = ATParseTransformCmd(wqueue, tab, rel, cmd,
										  false, lockmode,
										  cur_pass, context);
			/* Depending on constraint type, might be no more work to do now */
			if (cmd != NULL)
				address =
					ATExecAddConstraint(wqueue, tab, rel,
										(Constraint *) cmd->def,
										false, false, lockmode);
			break;
		case AT_AddConstraintRecurse:	/* ADD CONSTRAINT with recursion */
			/* Transform the command only during initial examination */
			if (cur_pass == AT_PASS_ADD_CONSTR)
				cmd = ATParseTransformCmd(wqueue, tab, rel, cmd,
										  true, lockmode,
										  cur_pass, context);
			/* Depending on constraint type, might be no more work to do now */
			if (cmd != NULL)
				address =
					ATExecAddConstraint(wqueue, tab, rel,
										(Constraint *) cmd->def,
										true, false, lockmode);
			break;
		case AT_ReAddConstraint:	/* Re-add pre-existing check constraint */
			address =
				ATExecAddConstraint(wqueue, tab, rel, (Constraint *) cmd->def,
									true, true, lockmode);
			break;
		case AT_ReAddDomainConstraint:	/* Re-add pre-existing domain check
										 * constraint */
			address =
				AlterDomainAddConstraint(((AlterDomainStmt *) cmd->def)->typeName,
										 ((AlterDomainStmt *) cmd->def)->def,
										 NULL);
			break;
		case AT_ReAddComment:	/* Re-add existing comment */
			address = CommentObject((CommentStmt *) cmd->def);
			break;
		case AT_AddIndexConstraint: /* ADD CONSTRAINT USING INDEX */
			address = ATExecAddIndexConstraint(tab, rel, (IndexStmt *) cmd->def,
											   lockmode);
			break;
		case AT_AlterConstraint:	/* ALTER CONSTRAINT */
			address = ATExecAlterConstraint(rel, cmd, false, false, lockmode);
			break;
		case AT_ValidateConstraint: /* VALIDATE CONSTRAINT */
			address = ATExecValidateConstraint(wqueue, rel, cmd->name, false,
											   false, lockmode);
			break;
		case AT_ValidateConstraintRecurse:	/* VALIDATE CONSTRAINT with
											 * recursion */
			address = ATExecValidateConstraint(wqueue, rel, cmd->name, true,
											   false, lockmode);
			break;
		case AT_DropConstraint: /* DROP CONSTRAINT */
			ATExecDropConstraint(rel, cmd->name, cmd->behavior,
								 false, false,
								 cmd->missing_ok, lockmode);
			break;
		case AT_DropConstraintRecurse:	/* DROP CONSTRAINT with recursion */
			ATExecDropConstraint(rel, cmd->name, cmd->behavior,
								 true, false,
								 cmd->missing_ok, lockmode);
			break;
		case AT_AlterColumnType:	/* ALTER COLUMN TYPE */
			/* parse transformation was done earlier */
			address = ATExecAlterColumnType(tab, rel, cmd, lockmode);
			break;
		case AT_AlterColumnGenericOptions:	/* ALTER COLUMN OPTIONS */
			address =
				ATExecAlterColumnGenericOptions(rel, cmd->name,
												(List *) cmd->def, lockmode);
			break;
		case AT_ChangeOwner:	/* ALTER OWNER */
			ATExecChangeOwner(RelationGetRelid(rel),
							  get_rolespec_oid(cmd->newowner, false),
							  false, lockmode);
			break;
		case AT_ClusterOn:		/* CLUSTER ON */
			address = ATExecClusterOn(rel, cmd->name, lockmode);
			break;
		case AT_DropCluster:	/* SET WITHOUT CLUSTER */
			ATExecDropCluster(rel, lockmode);
			break;
		case AT_SetLogged:		/* SET LOGGED */
		case AT_SetUnLogged:	/* SET UNLOGGED */
			break;
		case AT_DropOids:		/* SET WITHOUT OIDS */
			/* nothing to do here, oid columns don't exist anymore */
			break;
		case AT_SetTableSpace:	/* SET TABLESPACE */

			/*
			 * Only do this for partitioned tables and indexes, for which this
			 * is just a catalog change.  Other relation types which have
			 * storage are handled by Phase 3.
			 */
			if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
				rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
				ATExecSetTableSpaceNoStorage(rel, tab->newTableSpace);

			break;
		case AT_SetRelOptions:	/* SET (...) */
		case AT_ResetRelOptions:	/* RESET (...) */
		case AT_ReplaceRelOptions:	/* replace entire option list */
			ATExecSetRelOptions(rel, (List *) cmd->def, cmd->subtype, lockmode);
			break;
		case AT_EnableTrig:		/* ENABLE TRIGGER name */
			ATExecEnableDisableTrigger(rel, cmd->name,
									   TRIGGER_FIRES_ON_ORIGIN, false, lockmode);
			break;
		case AT_EnableAlwaysTrig:	/* ENABLE ALWAYS TRIGGER name */
			ATExecEnableDisableTrigger(rel, cmd->name,
									   TRIGGER_FIRES_ALWAYS, false, lockmode);
			break;
		case AT_EnableReplicaTrig:	/* ENABLE REPLICA TRIGGER name */
			ATExecEnableDisableTrigger(rel, cmd->name,
									   TRIGGER_FIRES_ON_REPLICA, false, lockmode);
			break;
		case AT_DisableTrig:	/* DISABLE TRIGGER name */
			ATExecEnableDisableTrigger(rel, cmd->name,
									   TRIGGER_DISABLED, false, lockmode);
			break;
		case AT_EnableTrigAll:	/* ENABLE TRIGGER ALL */
			ATExecEnableDisableTrigger(rel, NULL,
									   TRIGGER_FIRES_ON_ORIGIN, false, lockmode);
			break;
		case AT_DisableTrigAll: /* DISABLE TRIGGER ALL */
			ATExecEnableDisableTrigger(rel, NULL,
									   TRIGGER_DISABLED, false, lockmode);
			break;
		case AT_EnableTrigUser: /* ENABLE TRIGGER USER */
			ATExecEnableDisableTrigger(rel, NULL,
									   TRIGGER_FIRES_ON_ORIGIN, true, lockmode);
			break;
		case AT_DisableTrigUser:	/* DISABLE TRIGGER USER */
			ATExecEnableDisableTrigger(rel, NULL,
									   TRIGGER_DISABLED, true, lockmode);
			break;

		case AT_EnableRule:		/* ENABLE RULE name */
			ATExecEnableDisableRule(rel, cmd->name,
									RULE_FIRES_ON_ORIGIN, lockmode);
			break;
		case AT_EnableAlwaysRule:	/* ENABLE ALWAYS RULE name */
			ATExecEnableDisableRule(rel, cmd->name,
									RULE_FIRES_ALWAYS, lockmode);
			break;
		case AT_EnableReplicaRule:	/* ENABLE REPLICA RULE name */
			ATExecEnableDisableRule(rel, cmd->name,
									RULE_FIRES_ON_REPLICA, lockmode);
			break;
		case AT_DisableRule:	/* DISABLE RULE name */
			ATExecEnableDisableRule(rel, cmd->name,
									RULE_DISABLED, lockmode);
			break;

		case AT_AddInherit:
			address = ATExecAddInherit(rel, (RangeVar *) cmd->def, lockmode);
			break;
		case AT_DropInherit:
			address = ATExecDropInherit(rel, (RangeVar *) cmd->def, lockmode);
			break;
		case AT_AddOf:
			address = ATExecAddOf(rel, (TypeName *) cmd->def, lockmode);
			break;
		case AT_DropOf:
			ATExecDropOf(rel, lockmode);
			break;
		case AT_ReplicaIdentity:
			ATExecReplicaIdentity(rel, (ReplicaIdentityStmt *) cmd->def, lockmode);
			break;
		case AT_EnableRowSecurity:
			ATExecSetRowSecurity(rel, true);
			break;
		case AT_DisableRowSecurity:
			ATExecSetRowSecurity(rel, false);
			break;
		case AT_ForceRowSecurity:
			ATExecForceNoForceRowSecurity(rel, true);
			break;
		case AT_NoForceRowSecurity:
			ATExecForceNoForceRowSecurity(rel, false);
			break;
		case AT_GenericOptions:
			ATExecGenericOptions(rel, (List *) cmd->def);
			break;
		case AT_SetDistributedBy:	/* SET DISTRIBUTED BY */
			ATExecSetDistributedBy(rel, (Node *) cmd->def, cmd);
			break;
		case AT_ExpandTable:	/* EXPAND TABLE */
			ATExecExpandTable(wqueue, rel, cmd, getgpsegmentCount());
			break;
		case AT_ExpandPartitionTablePrepare:	/* EXPAND PARTITION PREPARE */
			ATExecExpandPartitionTablePrepare(rel, getgpsegmentCount());
			break;
		case AT_ShrinkTable:	/* EXPAND TABLE */
			ATExecExpandTable(wqueue, rel, cmd, intVal(cmd->def));
			break;
		case AT_AttachPartition:
			cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode,
									  cur_pass, context);
			Assert(cmd != NULL);
			if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
				ATExecAttachPartition(wqueue, rel, (PartitionCmd *) cmd->def,
									  context);
			else
				ATExecAttachPartitionIdx(wqueue, rel,
										 ((PartitionCmd *) cmd->def)->name);
			break;
		case AT_DetachPartition:
			cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode,
									  cur_pass, context);
			Assert(cmd != NULL);
			/* ATPrepCmd ensures it must be a table */
			Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
			ATExecDetachPartition(wqueue, tab, rel,
								  ((PartitionCmd *) cmd->def)->name,
								  ((PartitionCmd *) cmd->def)->concurrent);
			break;
		case AT_DetachPartitionFinalize:
			ATExecDetachPartitionFinalize(rel, ((PartitionCmd *) cmd->def)->name);
			break;
		case AT_SetTags:
			ATSetTags(rel, cmd->tags, cmd->unsettag);
			break;
		case AT_UnsetTags:
			ATSetTags(rel, cmd->tags, cmd->unsettag);
			break;
		default:				/* oops */
			elog(ERROR, "unrecognized alter table type: %d",
				 (int) cmd->subtype);
			break;

		case AT_PartAdd:
		case AT_PartDrop:
		case AT_PartAlter:
		case AT_PartSplit:
		case AT_PartRename:
		case AT_PartTruncate:
		case AT_PartExchange:
		case AT_PartSetTemplate:
			/*
			 * GPDB_13_MERGE_FIXME: Just let QD drive execution process, which means
			 * run commands locally, then dispatch to QE.
			 */
			if (Gp_role != GP_ROLE_EXECUTE)
			{
				/*
				 * GPDB_13_MERGE_FIXME:
				 * It's just pass regression tests. We need to cleanup the code logic later.
				 * Create a new AlteredTableInfo without changing original one, which
				 * will be dispatched to QE later.
				 */
				AlteredTableInfo	*tab2 = copyObject(tab);

				cmd = ATParseTransformCmd(wqueue, tab2, rel, cmd, false, lockmode,
										  cur_pass, context);
				Assert(cmd != NULL);
				ATExecGPPartCmds(rel, cmd);
			}
			break;
	}

	/*
	 * Report the subcommand to interested event triggers.
	 */
	if (cmd)
		EventTriggerCollectAlterTableSubcmd((Node *) cmd, address);

	/*
	 * Bump the command counter to ensure the next subcommand in the sequence
	 * can see the changes so far
	 */
	CommandCounterIncrement();
}

/*
 * ATParseTransformCmd: perform parse transformation for one subcommand
 *
 * Returns the transformed subcommand tree, if there is one, else NULL.
 *
 * The parser may hand back additional AlterTableCmd(s) and/or other
 * utility statements, either before or after the original subcommand.
 * Other AlterTableCmds are scheduled into the appropriate slot of the
 * AlteredTableInfo (they had better be for later passes than the current one).
 * Utility statements that are supposed to happen before the AlterTableCmd
 * are executed immediately.  Those that are supposed to happen afterwards
 * are added to the tab->afterStmts list to be done at the very end.
 */
static AlterTableCmd *
ATParseTransformCmd(List **wqueue, AlteredTableInfo *tab, Relation rel,
					AlterTableCmd *cmd, bool recurse, LOCKMODE lockmode,
					int cur_pass, AlterTableUtilityContext *context)
{
	AlterTableCmd *newcmd = NULL;
	AlterTableStmt *atstmt = makeNode(AlterTableStmt);
	List	   *beforeStmts;
	List	   *afterStmts;
	ListCell   *lc;

	if (Gp_role != GP_ROLE_EXECUTE)
	{
		/* Gin up an AlterTableStmt with just this subcommand and this table */
		atstmt->relation =
			makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
						 pstrdup(RelationGetRelationName(rel)),
						 -1);
		atstmt->relation->inh = recurse;
		atstmt->cmds = list_make1(cmd);
		atstmt->objtype = OBJECT_TABLE; /* needn't be picky here */
		atstmt->missing_ok = false;

		/* Transform the AlterTableStmt */
		atstmt = transformAlterTableStmt(RelationGetRelid(rel),
										 atstmt,
										 context->queryString,
										 &beforeStmts,
										 &afterStmts);

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			tab->beforeStmtLists = lappend(tab->beforeStmtLists, beforeStmts);
			tab->constraintLists = lappend(tab->constraintLists, atstmt->cmds);
		}
	}
	else
	{
		if (tab->beforeStmtLists == NULL || tab->constraintLists == NULL)
			ereport(ERROR, (errmsg("BUG: before/constraint is NULL: %p/%p",
							tab->beforeStmtLists, tab->constraintLists)));
		beforeStmts = linitial(tab->beforeStmtLists);
		atstmt->cmds = linitial(tab->constraintLists);
		tab->beforeStmtLists = list_delete_first(tab->beforeStmtLists);
		tab->constraintLists = list_delete_first(tab->constraintLists);
		afterStmts = NIL;
	}

	/* Execute any statements that should happen before these subcommand(s) */
	foreach(lc, beforeStmts)
	{
		Node	   *stmt = (Node *) lfirst(lc);

		ProcessUtilityForAlterTable(stmt, context);
		CommandCounterIncrement();
	}

	/*
	 * cmd2 in atstmt->cmds on QD will be dispatched to QE,
	 * so it's not allowed to change in on QD, instead, copy the object
	 * if modification is required.
	 */
	/* Examine the transformed subcommands and schedule them appropriately */
	foreach(lc, atstmt->cmds)
	{
		AlterTableCmd *cmd2 = lfirst_node(AlterTableCmd, lc);
		int			pass;

		/*
		 * This switch need only cover the subcommand types that can be added
		 * by parse_utilcmd.c; otherwise, we'll use the default strategy of
		 * executing the subcommand immediately, as a substitute for the
		 * original subcommand.  (Note, however, that this does cause
		 * AT_AddConstraint subcommands to be rescheduled into later passes,
		 * which is important for index and foreign key constraints.)
		 *
		 * We assume we needn't do any phase-1 checks for added subcommands.
		 */
		switch (cmd2->subtype)
		{
			case AT_SetNotNull:
				/* Need command-specific recursion decision */
				ATPrepSetNotNull(wqueue, rel, cmd2,
								 recurse, false,
								 lockmode, context);
				pass = AT_PASS_COL_ATTRS;
				break;
			case AT_AddIndex:
				/* This command never recurses */
				/* No command-specific prep needed */
				pass = AT_PASS_ADD_INDEX;
				break;
			case AT_AddIndexConstraint:
				/* This command never recurses */
				/* No command-specific prep needed */
				pass = AT_PASS_ADD_INDEXCONSTR;
				break;
			case AT_AddConstraint:
				/* Recursion occurs during execution phase */
				if (recurse)
				{
					if (Gp_role == GP_ROLE_DISPATCH)
						cmd2 = copyObject(cmd2);
					cmd2->subtype = AT_AddConstraintRecurse;
				}
				switch (castNode(Constraint, cmd2->def)->contype)
				{
					case CONSTR_PRIMARY:
					case CONSTR_UNIQUE:
					case CONSTR_EXCLUSION:
						pass = AT_PASS_ADD_INDEXCONSTR;
						break;
					default:
						pass = AT_PASS_ADD_OTHERCONSTR;
						break;
				}
				break;
			case AT_AlterColumnGenericOptions:
				/* This command never recurses */
				/* No command-specific prep needed */
				pass = AT_PASS_MISC;
				break;
			default:
				pass = cur_pass;
				break;
		}

		if (pass < cur_pass)
		{
			/* Cannot schedule into a pass we already finished */
			elog(ERROR, "ALTER TABLE scheduling failure: too late for pass %d",
				 pass);
		}
		else if (pass > cur_pass)
		{
			/* OK, queue it up for later */
			/* It may run on QE in the phase 1, we must ignore it for QEs */
			if (Gp_role != GP_ROLE_EXECUTE)
				tab->subcmds[pass] = lappend(tab->subcmds[pass], cmd2);
		}
		else
		{
			/*
			 * We should see at most one subcommand for the current pass,
			 * which is the transformed version of the original subcommand.
			 */
			if (newcmd == NULL && cmd->subtype == cmd2->subtype)
			{
				/* Found the transformed version of our subcommand */
				newcmd = cmd2;
			}
			else
				elog(ERROR, "ALTER TABLE scheduling failure: bogus item for pass %d",
					 pass);
		}
	}

	/* Queue up any after-statements to happen at the end */
	tab->afterStmts = list_concat(tab->afterStmts, afterStmts);

	return newcmd;
}

/*
 * ATRewriteTables: ALTER TABLE phase 3
 */
static void
ATRewriteTables(AlterTableStmt *parsetree, List **wqueue, LOCKMODE lockmode,
				AlterTableUtilityContext *context)
{
	ListCell   *ltab;

	/* Go through each table that needs to be checked or rewritten */
	foreach(ltab, *wqueue)
	{
		AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);

		/* Relations without storage may be ignored here */
		if (!RELKIND_HAS_STORAGE(tab->relkind))
			continue;

		/*
		 * If we change column data types, the operation has to be propagated
		 * to tables that use this table's rowtype as a column type.
		 * tab->newvals will also be non-NULL in the case where we're adding a
		 * column with a default.  We choose to forbid that case as well,
		 * since composite types might eventually support defaults.
		 *
		 * (Eventually we'll probably need to check for composite type
		 * dependencies even when we're just scanning the table without a
		 * rewrite, but at the moment a composite type does not enforce any
		 * constraints, so it's not necessary/appropriate to enforce them just
		 * during ALTER.)
		 */
		if (tab->newvals != NIL || tab->rewrite > 0)
		{
			Relation	rel;

			rel = table_open(tab->relid, NoLock);
			find_composite_type_dependencies(rel->rd_rel->reltype, rel, NULL);
			table_close(rel, NoLock);
		}

		/*
		 * 'OldHeap' can be an AO or external table, but kept the upstream variable name
		 * to minimize the diff.
		 */
		Relation	OldHeap;
		bool		hasIndexes;
		Oid 		oldTableSpace;
		char		oldRelPersistence;
		Oid			oldAm;

		/* We will lock the table iff we decide to actually rewrite it */
		OldHeap = relation_open(tab->relid, NoLock);
		oldTableSpace = OldHeap->rd_rel->reltablespace;
		oldRelPersistence = OldHeap->rd_rel->relpersistence;
		oldAm = OldHeap->rd_rel->relam;

		{
			List	   *indexIds;

			indexIds = RelationGetIndexList(OldHeap);
			hasIndexes = (indexIds != NIL);
			list_free(indexIds);
		}

		/*
		 * There are two cases where we will rewrite the table, for these cases
		 * run the necessary sanity checks.
		 */
		if (tab->newvals != NIL || tab->newTableSpace)
		{
			/*
			 * We don't support rewriting of system catalogs; there are too
			 * many corner cases and too little benefit.  In particular this
			 * is certainly not going to work for mapped catalogs.
			 */
			if (IsSystemRelation(OldHeap))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rewrite system relation \"%s\"",
								RelationGetRelationName(OldHeap))));

			if (RelationIsUsedAsCatalogTable(OldHeap))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot rewrite table \"%s\" used as a catalog table",
					   RelationGetRelationName(OldHeap))));

			/*
			 * Don't allow rewrite on temp tables of other backends ... their
			 * local buffer manager is not going to cope.
			 */
			if (RELATION_IS_OTHER_TEMP(OldHeap))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot rewrite temporary tables of other sessions")));
		}
		heap_close(OldHeap, NoLock);

		/*
		 * GPDB_12_MERGE_FIXME: This is a AM specific optimization, currently
		 * exposted out of the AM handler.  The add-column optimization should
		 * ideally be implemented within the table AM.
		 *
		 * A counterargument can be made that this optimization is very
		 * specific to column-oriented table AM.  Should the table AM API be
		 * generalised to fit it?
		 *
		 * If table AM API needs to be changed, we can imagine a few options
		 * to implement the add-column optimization.
		 *
		 * (1) definfe a new interface on the lines of
		 * table_relation_copy_for_cluster.  It would require traslating the
		 * state currently maintained in AlteredTableInfo for per-row
		 * expression and constraint evaluation and passed as arguments to the
		 * new interface.
		 *
		 * (2) Define a new interface to scan the underlying table one block
		 * at a time, where block is a append-optimized varblock.  And another
		 * interface to scan tuples within the block.  After evaluating the
		 * expressions and constraints on this tuple, new slot is constructed,
		 * as is currently done.  A new interface is needed to insert this
		 * slot into specific block and finish the block being inserted into,
		 * when there are no more tuples in the scanned block.  Let's
		 * illustrate this with pseudocode:
		 *
		 * TableScanDesc sdesc = table_begin_block_scan();
		 * Block block;
		 * 
		 * // table AM API doesn't provide an insert descriptor
		 * TableInsertDesc idesc = table_begin_block_insert();
		 *
		 * while (block = table_getnext_block(sdesc))
		 * {
		 *     table_insert_begin_block(idesc, block);
		 *     while (slot = table_getnextslot_in_block(block))
		 *     {
		 *         // evaluate expressions and constraints for tab->newvals
		 *         newslot = ExecEvalExpr();
		 *         tuple_insert_in_block(idesc, block, newslot);
		 *     }
		 *     table_insert_end_block(idesc);
		 * }
		 *
		 * table_end_block_insert(idesc);
		 *
		 * table_end_blcok_scan(sdesc);
		 *
		 *
		 * Ideally, ALTER TABLE ADD COLUMN should not be exposed to any code
		 * specific to table AM.  Descide the best option to achieve this
		 * goal.
		 */
		if (tab->rewrite & AT_REWRITE_NEW_COLUMNS_ONLY_AOCS)
		{
			ATAocsWriteNewColumns(tab);
			continue;
		}
		/*
		 * We only need to rewrite the table if at least one column needs to
		 * be recomputed, or we are changing its persistence.
		 *
		 * There are two reasons for requiring a rewrite when changing
		 * persistence: on one hand, we need to ensure that the buffers
		 * belonging to each of the two relations are marked with or without
		 * BM_PERMANENT properly.  On the other hand, since rewriting creates
		 * and assigns a new relfilenode, we automatically create or drop an
		 * init fork for the relation as appropriate.
		 */
		if (tab->rewrite > 0)
		{
			/* Build a temporary relation and copy data */
			Oid			OIDNewHeap;
			Oid			NewTableSpace;
			char		persistence;

			OldHeap = table_open(tab->relid, NoLock);

			/*
			 * We don't support rewriting of system catalogs; there are too
			 * many corner cases and too little benefit.  In particular this
			 * is certainly not going to work for mapped catalogs.
			 */
			if (IsSystemRelation(OldHeap))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rewrite system relation \"%s\"",
								RelationGetRelationName(OldHeap))));

			if (RelationIsUsedAsCatalogTable(OldHeap))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rewrite table \"%s\" used as a catalog table",
								RelationGetRelationName(OldHeap))));

			/*
			 * Don't allow rewrite on temp tables of other backends ... their
			 * local buffer manager is not going to cope.
			 */
			if (RELATION_IS_OTHER_TEMP(OldHeap))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rewrite temporary tables of other sessions")));

			/*
			 * Select destination tablespace (same as original unless user
			 * requested a change)
			 */
			if (tab->newTableSpace)
				NewTableSpace = tab->newTableSpace;
			else
				NewTableSpace = oldTableSpace;

			/*
			 * Select persistence of transient table (same as original unless
			 * user requested a change)
			 */
			persistence = tab->chgPersistence ?
				tab->newrelpersistence : OldHeap->rd_rel->relpersistence;

			table_close(OldHeap, NoLock);

			/*
			 * Fire off an Event Trigger now, before actually rewriting the
			 * table.
			 *
			 * We don't support Event Trigger for nested commands anywhere,
			 * here included, and parsetree is given NULL when coming from
			 * AlterTableInternal.
			 *
			 * And fire it only once.
			 */
			if (parsetree)
				EventTriggerTableRewrite((Node *) parsetree,
										 tab->relid,
										 tab->rewrite);

			/*
			 * Create transient table that will receive the modified data.
			 *
			 * Ensure it is marked correctly as logged or unlogged.  We have
			 * to do this here so that buffers for the new relfilenode will
			 * have the right persistence set, and at the same time ensure
			 * that the original filenode's buffers will get read in with the
			 * correct setting (i.e. the original one).  Otherwise a rollback
			 * after the rewrite would possibly result with buffers for the
			 * original filenode having the wrong persistence setting.
			 *
			 * NB: This relies on swap_relation_files() also swapping the
			 * persistence. That wouldn't work for pg_class, but that can't be
			 * unlogged anyway.
			 */
			OIDNewHeap = make_new_heap(tab->relid, NewTableSpace, persistence,
									   lockmode, hasIndexes, false);

			/*
			 * Copy the heap data into the new table with the desired
			 * modifications, and test the current data within the table
			 * against new constraints generated by ALTER TABLE commands.
			 */
			ATRewriteTable(tab, OIDNewHeap, lockmode);

			/*
			 * Swap the physical files of the old and new heaps, then rebuild
			 * indexes and discard the old heap.  We can use RecentXmin for
			 * the table's new relfrozenxid because we rewrote all the tuples
			 * in ATRewriteTable, so no older Xid remains in the table.  Also,
			 * we never try to swap toast tables by content, since we have no
			 * interest in letting this code work on system catalogs.
			 *
			 * MPP-17516 - The 'swap_stats' argument dictates whether the
			 * relpages and reltuples of the fake relfile should be copied
			 * over to our original pg_class tuple. We do not want to do this
			 * in the case of ALTER TABLE rewrites as the temp relfile will
			 * not have correct stats.
			 */
			finish_heap_swap(tab->relid, OIDNewHeap,
							 false, false,
							 false /* swap_stats */,
							 true,
							 !OidIsValid(tab->newTableSpace),
							 RecentXmin,
							 ReadNextMultiXactId(),
							 persistence);
		}
		else
		{
			/*
			 * If required, test the current data within the table against new
			 * constraints generated by ALTER TABLE commands, but don't
			 * rebuild data.
			 */
			if (tab->constraints != NIL || tab->verify_new_notnull ||
				tab->partition_constraint != NULL)
				ATRewriteTable(tab, InvalidOid, lockmode);

			/*
			 * If we had SET TABLESPACE but no reason to reconstruct tuples,
			 * just do a block-by-block copy.
			 */
			if (tab->newTableSpace)
				ATExecSetTableSpace(tab->relid, tab->newTableSpace, lockmode);
		}
	}

	/*
	 * Foreign key constraints are checked in a final pass, since (a) it's
	 * generally best to examine each one separately, and (b) it's at least
	 * theoretically possible that we have changed both relations of the
	 * foreign key, and we'd better have finished both rewrites before we try
	 * to read the tables.
	 */
	foreach(ltab, *wqueue)
	{
		AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);
		Relation	rel = NULL;
		ListCell   *lcon;

		/* Relations without storage may be ignored here too */
		if (!RELKIND_HAS_STORAGE(tab->relkind))
			continue;

		foreach(lcon, tab->constraints)
		{
			NewConstraint *con = lfirst(lcon);

			if (con->contype == CONSTR_FOREIGN)
			{
				Constraint *fkconstraint = (Constraint *) con->qual;
				Relation	refrel;

				if (rel == NULL)
				{
					/* Long since locked, no need for another */
					rel = table_open(tab->relid, NoLock);
				}

				refrel = table_open(con->refrelid, RowShareLock);

				validateForeignKeyConstraint(fkconstraint->conname, rel, refrel,
											 con->refindid,
											 con->conid);

				/*
				 * No need to mark the constraint row as validated, we did
				 * that when we inserted the row earlier.
				 */

				table_close(refrel, NoLock);
			}
		}

		if (rel)
			table_close(rel, NoLock);
	}

	/* Finally, run any afterStmts that were queued up */
	foreach(ltab, *wqueue)
	{
		AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);
		ListCell   *lc;

		foreach(lc, tab->afterStmts)
		{
			Node	   *stmt = (Node *) lfirst(lc);

			ProcessUtilityForAlterTable(stmt, context);
			CommandCounterIncrement();
		}
	}
}

/*
 * A helper for ATAocsWriteNewColumns(). It scans an existing column for
 * varblock headers. Write one new segfile each for new columns.
 */
static void
ATAocsWriteSegFileNewColumns(
		AOCSAddColumnDesc idesc, AOCSHeaderScanDesc sdesc,
		AlteredTableInfo *tab, ExprContext *econtext, TupleTableSlot *slot, const char *relname)
{
	NewColumnValue *newval;
	TupleDesc tupdesc = RelationGetDescr(idesc->rel);
	Form_pg_attribute attr;
	Datum *values = slot->tts_values;
	bool *isnull = slot->tts_isnull;
	int64 expectedFRN = -1; /* expected firstRowNum of the next varblock */
	ListCell *l;
	int i;

	/* Start index in values and isnull array for newly added columns. */
	AttrNumber newcol = tupdesc->natts - idesc->num_newcols;

	/* Loop over each varblock in an appendonly segno. */
	while (aocs_get_nextheader(sdesc))
	{
		if (sdesc->ao_read.current.hasFirstRowNum)
		{
			if (expectedFRN == -1)
			{
				/*
				 * Initialize expected firstRowNum for each appendonly
				 * segment.  Initializing it to 1 may not always be
				 * good.  E.g. if the first insert into an appendonly
				 * segment is aborted.  A subsequent successful insert
				 * creates the first varblock having firstRowNum
				 * greater than 1.
				 */
				expectedFRN = sdesc->ao_read.current.firstRowNum;
				aocs_addcol_setfirstrownum(idesc, expectedFRN);
			}
			else
			{
				Assert(expectedFRN <= sdesc->ao_read.current.firstRowNum);
				if (expectedFRN < sdesc->ao_read.current.firstRowNum)
				{
					elogif(Debug_appendonly_print_storage_headers, LOG,
						   "hole in %s: exp FRN: " INT64_FORMAT ", actual FRN: "
						   INT64_FORMAT, sdesc->ao_read.segmentFileName,
						   expectedFRN, sdesc->ao_read.current.firstRowNum);
					/*
					 * We encountered a break in sequence of row
					 * numbers (hole), replicate it in the new
					 * segfiles.
					 */
					aocs_addcol_endblock(
							idesc, sdesc->ao_read.current.firstRowNum);
				}
			}
			for (i = 0; i < sdesc->ao_read.current.rowCount; ++i)
			{
				foreach (l, tab->newvals)
				{
					newval = lfirst(l);
					values[newval->attnum-1] =
							ExecEvalExprSwitchContext(newval->exprstate,
													  econtext,
													  &isnull[newval->attnum-1]);
					/*
					 * Ensure that NOT NULL constraint for the newly
					 * added columns is not being violated.  This
					 * covers the case when explicit "CHECK()"
					 * constraint is not specified but only "NOT NULL"
					 * is specified in the new column's definition.
					 */
					attr = TupleDescAttr(tupdesc, newval->attnum - 1);
					if (attr->attnotnull &&	isnull[newval->attnum-1])
					{
						ereport(ERROR,
								(errcode(ERRCODE_NOT_NULL_VIOLATION),
								 errmsg("column \"%s\" of relation \"%s\" contains null values",
										NameStr(attr->attname),
										relname)));
					}
				}
				foreach (l, tab->constraints)
				{
					NewConstraint *con = lfirst(l);
					switch(con->contype)
					{
						case CONSTR_CHECK:
							if(!ExecCheck(con->qualstate, econtext))
								ereport(ERROR,
										(errcode(ERRCODE_CHECK_VIOLATION),
										 errmsg("check constraint \"%s\" is violated by some row",
											con->name)));
							break;
						case CONSTR_FOREIGN:
							/* Nothing to do */
							break;
						default:
							elog(ERROR, "Unrecognized constraint type: %d",
								 (int) con->contype);
					}
				}
				aocs_addcol_insert_datum(idesc, values+newcol, isnull+newcol);
				ResetExprContext(econtext);
				CHECK_FOR_INTERRUPTS();
			}
			expectedFRN = sdesc->ao_read.current.firstRowNum +
					sdesc->ao_read.current.rowCount;
		}
	}
}

/*
 * Choose the column that has the smallest segfile size so as to
 * minimize disk I/O in subsequent varblock header scan. The natts arg
 * includes only existing columns and not the ones being added. Once
 * we find a segfile with nonzero tuplecount and find the column with
 * the smallest eof to return, we continue the loop but skip over all
 * segfiles except for those in AOSEG_STATE_AWAITING_DROP state which
 * we need to append to our drop list.
 */
static int
column_to_scan(AOCSFileSegInfo **segInfos, int nseg, int natts, Relation aocsrel)
{
	int scancol = -1;
	int segi;
	int i;
	AOCSVPInfoEntry *vpe;
	int64 min_eof = 0;

	for (segi = 0; segi < nseg; ++segi)
	{
		/*
		 * Don't use a AOSEG_STATE_AWAITING_DROP segfile. That seems
		 * like a bad idea in general, but there's one particular problem:
		 * the 'vpinfo' of a dropped segfile might be missing information
		 * for columns that were added later.
		 */
		if (segInfos[segi]->state == AOSEG_STATE_AWAITING_DROP)
			continue;

		/*
		 * Skip over appendonly segments with no tuples (caused by VACUUM)
		 */
		if (segInfos[segi]->total_tupcount > 0 && scancol == -1)
		{
			for (i = 0; i < natts; ++i)
			{
				vpe = getAOCSVPEntry(segInfos[segi], i);
				if (vpe->eof > 0 && (!min_eof || vpe->eof < min_eof))
				{
					min_eof = vpe->eof;
					scancol = i;
				}
			}
		}
	}

	return scancol;
}

static void
ATAocsWriteNewColumns(AlteredTableInfo *tab)
{
	AOCSFileSegInfo **segInfos;
	AOCSHeaderScanDesc sdesc;
	AOCSAddColumnDesc idesc;
	NewColumnValue *newval;
	NewConstraint *con;
	TupleTableSlot *slot;
	EState *estate;
	ExprContext *econtext;
	Relation rel; /* Relation being altered */
	int32 nseg;
	int32 segi;
	char *basepath;
	int32 scancol; /* chosen column number to scan from */
	ListCell *l;
	Snapshot snapshot;
	int addcols;

	snapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));

	estate = CreateExecutorState();
	foreach(l, tab->constraints)
	{
		con = lfirst(l);
		switch (con->contype)
		{
			case CONSTR_CHECK:
				con->qualstate = ExecPrepareExpr((Expr *) con->qual, estate);
				break;
			case CONSTR_FOREIGN:
				/* Nothing to do here */
				break;
			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 (int) con->contype);
		}
	}
	Assert(tab->newvals);
	foreach(l, tab->newvals)
	{
		newval = lfirst(l);
		newval->exprstate = ExecPrepareExpr((Expr *) newval->expr, estate);
	}

	rel = heap_open(tab->relid, NoLock);
	Assert(RelationIsAoCols(rel));

	/*
     * There might be AWAITING_DROP segments occupying spaces for failing
     * to drop at VACUUM in the case of cleaning up happened concurrently
     * with earlier readers which was accessing the dead segment files.
     *
     * We used to call AppendOptimizedRecycleDeadSegments() (current name is
     * ao_vacuum_rel_recycle_dead_segments) to recycle those segfiles to save
     * spaces in this scenario. But it didn't do corresponding index tuples
     * cleanup for unknown reason.
     *
     * After optimizing VACUUM AO strategy, we did refactor for
     * AppendOptimizedRecycleDeadSegments() a little bit and combine
     * dead segfiles cleanup with corresponding indexes cleanup together.
     * While it seems to be impossible to pass index vacuuming parameter in
     * this scenario, so we removed AppendOptimizedRecycleDeadSegments() out
     * of this function and dedicated it to be called only in VACUUM scenario.
     *
     * We are supposed to be fine without recycling spaces here, or find
     * another way to fix it if that does become a real problem.
     */

	segInfos = GetAllAOCSFileSegInfo(rel, snapshot, &nseg, NULL);
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);
	if (nseg > 0)
	{
		aocs_addcol_emptyvpe(rel, segInfos, nseg,
							 list_length(tab->newvals));
	}

	scancol = column_to_scan(segInfos, nseg, tab->oldDesc->natts, rel);
	elogif(Debug_appendonly_print_storage_headers, LOG,
		   "using column %d of relation %s for alter table scan",
		   scancol, RelationGetRelationName(rel));

	/*
	 * Continue only if a non-empty existing segfile was found above.
	 */
	if (Gp_role != GP_ROLE_DISPATCH && scancol != -1)
	{
		slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsVirtual);

		/*
		 * Initialize expression context for evaluating values and
		 * constraints of the newly added columns.
		 */
		econtext = GetPerTupleExprContext(estate);
		/*
		 * The slot's data will be populated for each newly added
		 * column by ExecEvalExpr().
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * Mark all attributes including newly added columns as valid.
		 * Used for per tuple constraint evaluation.
		 */
		ExecStoreAllNullTuple(slot);

		sdesc = aocs_begin_headerscan(rel, scancol);
		addcols = RelationGetDescr(rel)->natts - tab->oldDesc->natts;
		/*
		 * Protect against potential negative number here.
		 * Note that natts is not decremented to reflect dropped columns,
		 * so this should be safe
		 */
		Assert(addcols > 0);
		idesc = aocs_addcol_init(rel, addcols);

		/* Loop over all appendonly segments */
		for (segi = 0; segi < nseg; ++segi)
		{
			if (segInfos[segi]->total_tupcount <= 0 ||
				segInfos[segi]->state == AOSEG_STATE_AWAITING_DROP)
			{
				/*
				 * VACUUM may cause appendonly segments with eof=0.
				 * We only need to add new rows in pg_aocsseg_* in
				 * this case for each newly added column.  This is
				 * accomplished by aocs_addcol_emptyvpe() above.
				 *
				 * Compaction leaves redundant segments in
				 * AOSEG_STATE_AWAITING_DROP.  We skip over them too.
				 */
				elogif(Debug_appendonly_print_storage_headers, LOG,
					   "Skipping over empty segno %d relation %s",
					   segInfos[segi]->segno, RelationGetRelationName(rel));
				continue;
			}
			/*
			 * Open aocs segfile for chosen column for current
			 * appendonly segment.
			 */
			aocs_headerscan_opensegfile(sdesc, segInfos[segi], basepath);

			/*
			 * Create new segfiles for new columns for current
			 * appendonly segment.
			 */
			RelFileNodeBackend rnode;

			rnode.node = rel->rd_node;
			rnode.backend = rel->rd_backend;

			aocs_addcol_newsegfile(idesc, segInfos[segi],
								   basepath, rnode);

			ATAocsWriteSegFileNewColumns(idesc, sdesc, tab, econtext, slot, RelationGetRelationName(rel));
		}
		aocs_end_headerscan(sdesc);
		aocs_addcol_finish(idesc);
		ExecDropSingleTupleTableSlot(slot);
	}


	FreeExecutorState(estate);
	heap_close(rel, NoLock);
	UnregisterSnapshot(snapshot);
}

/*
 * ATRewriteTable: scan or rewrite one table
 *
 * OIDNewHeap is InvalidOid if we don't need to rewrite
 */
static void
ATRewriteTable(AlteredTableInfo *tab, Oid OIDNewHeap, LOCKMODE lockmode)
{
	Relation	oldrel;
	Relation	newrel;
	TupleDesc	oldTupDesc;
	TupleDesc	newTupDesc;
	bool		needscan = false;
	List	   *notnull_attrs;
	int			i;
	ListCell   *l;
	EState	   *estate;
	CommandId	mycid;
	BulkInsertState bistate;
	int			ti_options;
	ExprState  *partqualstate = NULL;

	/*
	 * Open the relation(s).  We have surely already locked the existing
	 * table.
	 */
	oldrel = table_open(tab->relid, NoLock);
	oldTupDesc = tab->oldDesc;
	newTupDesc = RelationGetDescr(oldrel);	/* includes all mods */

	if (OidIsValid(OIDNewHeap))
		newrel = table_open(OIDNewHeap, lockmode);
	else
		newrel = NULL;

	/*
	 * Prepare a BulkInsertState and options for table_tuple_insert. Because
	 * we're building a new heap, we can skip WAL-logging and fsync it to disk
	 * at the end instead (unless WAL-logging is required for archiving or
	 * streaming replication). The FSM is empty too, so don't bother using it.
	 */
	if (newrel)
	{
		mycid = GetCurrentCommandId(true);
		bistate = GetBulkInsertState();
		ti_options = TABLE_INSERT_SKIP_FSM;
	}
	else
	{
		/* keep compiler quiet about using these uninitialized */
		mycid = 0;
		bistate = NULL;
		ti_options = 0;
	}

	/*
	 * Generate the constraint and default execution states
	 */

	estate = CreateExecutorState();

	/* Build the needed expression execution states */
	foreach(l, tab->constraints)
	{
		NewConstraint *con = lfirst(l);

		switch (con->contype)
		{
			case CONSTR_CHECK:
				needscan = true;
				con->qualstate = ExecPrepareExpr((Expr *) con->qual, estate);
				break;
			case CONSTR_FOREIGN:
				/* Nothing to do here */
				break;
			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 (int) con->contype);
		}
	}

	/* Build expression execution states for partition check quals */
	if (tab->partition_constraint)
	{
		needscan = true;
		partqualstate = ExecPrepareExpr(tab->partition_constraint, estate);
	}

	foreach(l, tab->newvals)
	{
		NewColumnValue *ex = lfirst(l);

		/* expr already planned */
		ex->exprstate = ExecInitExpr((Expr *) ex->expr, NULL);
	}

	notnull_attrs = NIL;
	if (newrel || tab->verify_new_notnull)
	{
		/*
		 * If we are rebuilding the tuples OR if we added any new but not
		 * verified NOT NULL constraints, check all not-null constraints. This
		 * is a bit of overkill but it minimizes risk of bugs, and
		 * heap_attisnull is a pretty cheap test anyway.
		 */
		for (i = 0; i < newTupDesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(newTupDesc, i);

			if (attr->attnotnull && !attr->attisdropped)
				notnull_attrs = lappend_int(notnull_attrs, i);
		}
		if (notnull_attrs)
			needscan = true;
	}

	if (newrel || needscan)
	{
		ExprContext *econtext;
		TupleTableSlot *oldslot;
		TupleTableSlot *newslot;
		TableScanDesc scan;
		MemoryContext oldCxt;
		List	   *dropped_attrs = NIL;
		ListCell   *lc;
		Snapshot	snapshot;

		if (newrel)
			ereport(DEBUG1,
					(errmsg_internal("rewriting table \"%s\"",
									 RelationGetRelationName(oldrel))));
		else
			ereport(DEBUG1,
					(errmsg_internal("verifying table \"%s\"",
									 RelationGetRelationName(oldrel))));

		if (newrel)
		{
			/*
			 * All predicate locks on the tuples or pages are about to be made
			 * invalid, because we move tuples around.  Promote them to
			 * relation locks.
			 */
			TransferPredicateLocksToHeapRelation(oldrel);
		}

		econtext = GetPerTupleExprContext(estate);

		/*
		 * Create necessary tuple slots. When rewriting, two slots are needed,
		 * otherwise one suffices. In the case where one slot suffices, we
		 * need to use the new tuple descriptor, otherwise some constraints
		 * can't be evaluated.  Note that even when the tuple layout is the
		 * same and no rewrite is required, the tupDescs might not be
		 * (consider ADD COLUMN without a default).
		 */
		if (tab->rewrite)
		{
			Assert(newrel != NULL);
			oldslot = MakeSingleTupleTableSlot(oldTupDesc,
											   table_slot_callbacks(oldrel));
			newslot = MakeSingleTupleTableSlot(newTupDesc,
											   table_slot_callbacks(newrel));

			/*
			 * Set all columns in the new slot to NULL initially, to ensure
			 * columns added as part of the rewrite are initialized to NULL.
			 * That is necessary as tab->newvals will not contain an
			 * expression for columns with a NULL default, e.g. when adding a
			 * column without a default together with a column with a default
			 * requiring an actual rewrite.
			 */
			ExecStoreAllNullTuple(newslot);
		}
		else
		{
			oldslot = MakeSingleTupleTableSlot(newTupDesc,
											   table_slot_callbacks(oldrel));
			newslot = NULL;
		}

		/*
		 * Any attributes that are dropped according to the new tuple
		 * descriptor can be set to NULL. We precompute the list of dropped
		 * attributes to avoid needing to do so in the per-tuple loop.
		 */
		for (i = 0; i < newTupDesc->natts; i++)
		{
			if (TupleDescAttr(newTupDesc, i)->attisdropped)
				dropped_attrs = lappend_int(dropped_attrs, i);
		}

		/*
		 * Scan through the rows, generating a new row if needed and then
		 * checking all the constraints.
		 */
		snapshot = RegisterSnapshot(GetLatestSnapshot());
		scan = table_beginscan(oldrel, snapshot, 0, NULL);

		if (newrel && RelationIsAoRows(newrel))
		{
			/* Table access method is not permitted to change */
			Assert(RelationIsAoRows(oldrel));
			appendonly_dml_init(newrel, CMD_INSERT);
		}
		else if (newrel && RelationIsAoCols(newrel))
		{
			Assert(RelationIsAoCols(oldrel));
			aoco_dml_init(newrel, CMD_INSERT);
		}
		else if (newrel && ext_dml_init_hook)
			ext_dml_init_hook(newrel, CMD_INSERT);

		/*
		 * Switch to per-tuple memory context and reset it for each tuple
		 * produced, so we don't leak memory.
		 */
		oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		while (table_scan_getnextslot(scan, ForwardScanDirection, oldslot))
		{
			TupleTableSlot *insertslot;

			if (tab->rewrite > 0)
			{
				/* Extract data from old tuple */
				slot_getallattrs(oldslot);
				ExecClearTuple(newslot);

				/* copy attributes */
				memcpy(newslot->tts_values, oldslot->tts_values,
					   sizeof(Datum) * oldslot->tts_nvalid);
				memcpy(newslot->tts_isnull, oldslot->tts_isnull,
					   sizeof(bool) * oldslot->tts_nvalid);

				/* Set dropped attributes to null in new tuple */
				foreach(lc, dropped_attrs)
					newslot->tts_isnull[lfirst_int(lc)] = true;

				/*
				 * Constraints and GENERATED expressions might reference the
				 * tableoid column, so fill tts_tableOid with the desired
				 * value.  (We must do this each time, because it gets
				 * overwritten with newrel's OID during storing.)
				 */
				newslot->tts_tableOid = RelationGetRelid(oldrel);

				/*
				 * Process supplied expressions to replace selected columns.
				 *
				 * First, evaluate expressions whose inputs come from the old
				 * tuple.
				 */
				econtext->ecxt_scantuple = oldslot;

				foreach(l, tab->newvals)
				{
					NewColumnValue *ex = lfirst(l);

					if (ex->is_generated)
						continue;

					newslot->tts_values[ex->attnum - 1]
						= ExecEvalExpr(ex->exprstate,
									   econtext,
									   &newslot->tts_isnull[ex->attnum - 1]);
				}

				ExecStoreVirtualTuple(newslot);

				/*
				 * Now, evaluate any expressions whose inputs come from the
				 * new tuple.  We assume these columns won't reference each
				 * other, so that there's no ordering dependency.
				 */
				econtext->ecxt_scantuple = newslot;

				foreach(l, tab->newvals)
				{
					NewColumnValue *ex = lfirst(l);

					if (!ex->is_generated)
						continue;

					newslot->tts_values[ex->attnum - 1]
						= ExecEvalExpr(ex->exprstate,
									   econtext,
									   &newslot->tts_isnull[ex->attnum - 1]);
				}

				insertslot = newslot;
			}
			else
			{
				/*
				 * If there's no rewrite, old and new table are guaranteed to
				 * have the same AM, so we can just use the old slot to verify
				 * new constraints etc.
				 */
				insertslot = oldslot;
			}

			/* Now check any constraints on the possibly-changed tuple */
			econtext->ecxt_scantuple = insertslot;

			foreach(l, notnull_attrs)
			{
				int			attn = lfirst_int(l);

				if (slot_attisnull(insertslot, attn + 1))
				{
					Form_pg_attribute attr = TupleDescAttr(newTupDesc, attn);

					ereport(ERROR,
							(errcode(ERRCODE_NOT_NULL_VIOLATION),
							 errmsg("column \"%s\" of relation \"%s\" contains null values",
									NameStr(attr->attname),
									RelationGetRelationName(oldrel)),
							 errtablecol(oldrel, attn + 1)));
				}
			}

			foreach(l, tab->constraints)
			{
				NewConstraint *con = lfirst(l);

				switch (con->contype)
				{
					case CONSTR_CHECK:
						if (!ExecCheck(con->qualstate, econtext))
							ereport(ERROR,
									(errcode(ERRCODE_CHECK_VIOLATION),
									 errmsg("check constraint \"%s\" of relation \"%s\" is violated by some row",
											con->name,
											RelationGetRelationName(oldrel)),
									 errtableconstraint(oldrel, con->name)));
						break;
					case CONSTR_FOREIGN:
						/* Nothing to do here */
						break;
					default:
						elog(ERROR, "unrecognized constraint type: %d",
							 (int) con->contype);
				}
			}

			if (partqualstate && !ExecCheck(partqualstate, econtext))
			{
				if (tab->validate_default)
					ereport(ERROR,
							(errcode(ERRCODE_CHECK_VIOLATION),
							 errmsg("updated partition constraint for default partition \"%s\" would be violated by some row",
									RelationGetRelationName(oldrel)),
							 errtable(oldrel)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_CHECK_VIOLATION),
							 errmsg("partition constraint of relation \"%s\" is violated by some row",
									RelationGetRelationName(oldrel)),
							 errtable(oldrel)));
			}

			/* Write the tuple out to the new relation */
			if (newrel)
				table_tuple_insert(newrel, insertslot, mycid,
								   ti_options, bistate);

			ResetExprContext(econtext);

			CHECK_FOR_INTERRUPTS();
		}

		MemoryContextSwitchTo(oldCxt);
		table_endscan(scan);
		UnregisterSnapshot(snapshot);

		ExecDropSingleTupleTableSlot(oldslot);
		if (newslot)
			ExecDropSingleTupleTableSlot(newslot);
	}

	FreeExecutorState(estate);

	table_close(oldrel, NoLock);
	if (newrel)
	{
		FreeBulkInsertState(bistate);

		table_finish_bulk_insert(newrel, ti_options);

		table_close(newrel, NoLock);
	}
}

/*
 * ATGetQueueEntry: find or create an entry in the ALTER TABLE work queue
 */
static AlteredTableInfo *
ATGetQueueEntry(List **wqueue, Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	AlteredTableInfo *tab;
	ListCell   *ltab;

	foreach(ltab, *wqueue)
	{
		tab = (AlteredTableInfo *) lfirst(ltab);
		if (tab->relid == relid)
			return tab;
	}

	/*
	 * Not there, so add it.  Note that we make a copy of the relation's
	 * existing descriptor before anything interesting can happen to it.
	 */
	tab = makeNode(AlteredTableInfo);
	tab->relid = relid;
	tab->rel = NULL;			/* set later */
	tab->relkind = rel->rd_rel->relkind;
	tab->oldDesc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
	tab->newrelpersistence = RELPERSISTENCE_PERMANENT;
	tab->chgPersistence = false;

	*wqueue = lappend(*wqueue, tab);

	return tab;
}

/*
 * ATSimplePermissions
 *
 * - Ensure that it is a relation (or possibly a view)
 * - Ensure this user is the owner
 * - Ensure that it is not a system table
 */
static void
ATSimplePermissions(Relation rel, int allowed_targets)
{
	int			actual_target;

	switch (rel->rd_rel->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_PARTITIONED_TABLE:
			actual_target = ATT_TABLE;
			break;
		case RELKIND_VIEW:
			actual_target = ATT_VIEW;
			break;
		case RELKIND_MATVIEW:
			actual_target = ATT_MATVIEW;
			break;
		case RELKIND_INDEX:
			actual_target = ATT_INDEX;
			break;
		case RELKIND_PARTITIONED_INDEX:
			actual_target = ATT_PARTITIONED_INDEX;
			break;
		case RELKIND_COMPOSITE_TYPE:
			actual_target = ATT_COMPOSITE_TYPE;
			break;
		case RELKIND_FOREIGN_TABLE:
			actual_target = ATT_FOREIGN_TABLE;
			break;
		case RELKIND_DIRECTORY_TABLE:
			actual_target = ATT_DIRECTORY_TABLE;
			break;
		case RELKIND_SEQUENCE:
			actual_target = ATT_SEQUENCE;
			break;

		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
		case RELKIND_AOVISIMAP:
			/*
			 * Allow ALTER TABLE operations in standard alone mode on
			 * AO segment tables.
			 */
			if (IsUnderPostmaster)
				actual_target = ATT_TABLE;
			else
				actual_target = 0;
			break;

		default:
			actual_target = 0;
			break;
	}

	/* Wrong target type? */
	if ((actual_target & allowed_targets) == 0)
		ATWrongRelkindError(rel, allowed_targets);

	/* Permissions checks */
	if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(rel->rd_rel->relkind),
					   RelationGetRelationName(rel));

	if (!allowSystemTableMods && IsSystemRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						RelationGetRelationName(rel))));
}

/*
 * ATWrongRelkindError
 *
 * Throw an error when a relation has been determined to be of the wrong
 * type.
 */
static void
ATWrongRelkindError(Relation rel, int allowed_targets)
{
	char	   *msg;

	switch (allowed_targets)
	{
		case ATT_TABLE:
			msg = _("\"%s\" is not a table");
			break;
		case ATT_TABLE | ATT_VIEW:
			msg = _("\"%s\" is not a table or view");
			break;
		case ATT_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE:
			msg = _("\"%s\" is not a table, view, or foreign table");
			break;
		case ATT_TABLE | ATT_VIEW | ATT_MATVIEW | ATT_INDEX:
			msg = _("\"%s\" is not a table, view, materialized view, or index");
			break;
		case ATT_TABLE | ATT_MATVIEW:
			msg = _("\"%s\" is not a table or materialized view");
			break;
		case ATT_TABLE | ATT_MATVIEW | ATT_INDEX:
			msg = _("\"%s\" is not a table, materialized view, or index");
			break;
		case ATT_TABLE | ATT_MATVIEW | ATT_INDEX | ATT_PARTITIONED_INDEX:
			msg = _("\"%s\" is not a table, materialized view, index, or partitioned index");
			break;
		case ATT_TABLE | ATT_MATVIEW | ATT_INDEX | ATT_PARTITIONED_INDEX | ATT_FOREIGN_TABLE | ATT_DIRECTORY_TABLE:
			msg = _("\"%s\" is not a table, materialized view, index, partitioned index, or foreign table");
			break;
		case ATT_TABLE | ATT_MATVIEW | ATT_FOREIGN_TABLE:
			msg = _("\"%s\" is not a table, materialized view, or foreign table");
			break;
		case ATT_TABLE | ATT_FOREIGN_TABLE:
			msg = _("\"%s\" is not a table or foreign table");
			break;
		case ATT_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE:
			msg = _("\"%s\" is not a table, composite type, or foreign table");
			break;
		case ATT_TABLE | ATT_MATVIEW | ATT_INDEX | ATT_FOREIGN_TABLE:
			msg = _("\"%s\" is not a table, materialized view, index, or foreign table");
			break;
		case ATT_TABLE | ATT_PARTITIONED_INDEX:
			msg = _("\"%s\" is not a table or partitioned index");
			break;
		case ATT_VIEW:
			msg = _("\"%s\" is not a view");
			break;
		case ATT_FOREIGN_TABLE:
			msg = _("\"%s\" is not a foreign table");
			break;
		case ATT_TABLE | ATT_DIRECTORY_TABLE:
			msg = _("\"%s\" is not a table or directory table");
			break;
		case ATT_TABLE | ATT_FOREIGN_TABLE | ATT_DIRECTORY_TABLE:
			msg = _("\"%s\" is not a table, foreign table, or directory table");
			break;
		case ATT_DIRECTORY_TABLE:
			msg = _("\"%s\" is not a directory table");
			break;
		case ATT_TABLE | ATT_FOREIGN_TABLE | ATT_INDEX | ATT_SEQUENCE | ATT_VIEW | ATT_MATVIEW:
			msg = _("\"%s\" is not a table, foreign table, index, sequence, view or materialized view");
			break;
		default:
			/* shouldn't get here, add all necessary cases above */
			msg = _("\"%s\" is of the wrong type");
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_WRONG_OBJECT_TYPE),
			 errmsg(msg, RelationGetRelationName(rel))));
}

/*
 * ATSimpleRecursion
 *
 * Simple table recursion sufficient for most ALTER TABLE operations.
 * All direct and indirect children are processed in an unspecified order.
 * Note that if a child inherits from the original table via multiple
 * inheritance paths, it will be visited just once.
 */
static void
ATSimpleRecursion(List **wqueue, Relation rel,
				  AlterTableCmd *cmd, bool recurse, LOCKMODE lockmode,
				  AlterTableUtilityContext *context)
{
	/*
	 * Propagate to children, if desired and if there are (or might be) any
	 * children.
	 */
	if (recurse && rel->rd_rel->relhassubclass)
	{
		Oid			relid = RelationGetRelid(rel);
		ListCell   *child;
		List	   *children;

		children = find_all_inheritors(relid, lockmode, NULL);

		/*
		 * find_all_inheritors does the recursive search of the inheritance
		 * hierarchy, so all we have to do is process all of the relids in the
		 * list that it returns.
		 */
		foreach(child, children)
		{
			Oid			childrelid = lfirst_oid(child);
			Relation	childrel;

			if (childrelid == relid)
				continue;
			/* find_all_inheritors already got lock */
			childrel = relation_open(childrelid, NoLock);
			CheckTableNotInUse(childrel, "ALTER TABLE");
			ATPrepCmd(wqueue, childrel, cmd, false, true, lockmode, context);
			relation_close(childrel, NoLock);
		}
	}
}

/*
 * Obtain list of partitions of the given table, locking them all at the given
 * lockmode and ensuring that they all pass CheckTableNotInUse.
 *
 * This function is a no-op if the given relation is not a partitioned table;
 * in particular, nothing is done if it's a legacy inheritance parent.
 */
static void
ATCheckPartitionsNotInUse(Relation rel, LOCKMODE lockmode)
{
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		List	   *inh;
		ListCell   *cell;

		inh = find_all_inheritors(RelationGetRelid(rel), lockmode, NULL);
		/* first element is the parent rel; must ignore it */
		for_each_from(cell, inh, 1)
		{
			Relation	childrel;

			/* find_all_inheritors already got lock */
			childrel = table_open(lfirst_oid(cell), NoLock);
			CheckTableNotInUse(childrel, "ALTER TABLE");
			table_close(childrel, NoLock);
		}
		list_free(inh);
	}
}

/*
 * ATTypedTableRecursion
 *
 * Propagate ALTER TYPE operations to the typed tables of that type.
 * Also check the RESTRICT/CASCADE behavior.  Given CASCADE, also permit
 * recursion to inheritance children of the typed tables.
 */
static void
ATTypedTableRecursion(List **wqueue, Relation rel, AlterTableCmd *cmd,
					  LOCKMODE lockmode, AlterTableUtilityContext *context)
{
	ListCell   *child;
	List	   *children;

	Assert(rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE);

	children = find_typed_table_dependencies(rel->rd_rel->reltype,
											 RelationGetRelationName(rel),
											 cmd->behavior);

	foreach(child, children)
	{
		Oid			childrelid = lfirst_oid(child);
		Relation	childrel;

		childrel = relation_open(childrelid, lockmode);
		CheckTableNotInUse(childrel, "ALTER TABLE");
		ATPrepCmd(wqueue, childrel, cmd, true, true, lockmode, context);
		relation_close(childrel, NoLock);
	}
}


/*
 * find_composite_type_dependencies
 *
 * Check to see if the type "typeOid" is being used as a column in some table
 * (possibly nested several levels deep in composite types, arrays, etc!).
 * Eventually, we'd like to propagate the check or rewrite operation
 * into such tables, but for now, just error out if we find any.
 *
 * Caller should provide either the associated relation of a rowtype,
 * or a type name (not both) for use in the error message, if any.
 *
 * Note that "typeOid" is not necessarily a composite type; it could also be
 * another container type such as an array or range, or a domain over one of
 * these things.  The name of this function is therefore somewhat historical,
 * but it's not worth changing.
 *
 * We assume that functions and views depending on the type are not reasons
 * to reject the ALTER.  (How safe is this really?)
 */
void
find_composite_type_dependencies(Oid typeOid, Relation origRelation,
								 const char *origTypeName)
{
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc depScan;
	HeapTuple	depTup;

	/* since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/*
	 * We scan pg_depend to find those things that depend on the given type.
	 * (We assume we can ignore refobjsubid for a type.)
	 */
	depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(TypeRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(typeOid));

	depScan = systable_beginscan(depRel, DependReferenceIndexId, true,
								 NULL, 2, key);

	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		Relation	rel;
		Form_pg_attribute att;

		/* Check for directly dependent types */
		if (pg_depend->classid == TypeRelationId)
		{
			/*
			 * This must be an array, domain, or range containing the given
			 * type, so recursively check for uses of this type.  Note that
			 * any error message will mention the original type not the
			 * container; this is intentional.
			 */
			find_composite_type_dependencies(pg_depend->objid,
											 origRelation, origTypeName);
			continue;
		}

		/* Else, ignore dependees that aren't user columns of relations */
		/* (we assume system columns are never of interesting types) */
		if (pg_depend->classid != RelationRelationId ||
			pg_depend->objsubid <= 0)
			continue;

		rel = relation_open(pg_depend->objid, AccessShareLock);
		att = TupleDescAttr(rel->rd_att, pg_depend->objsubid - 1);

		if (rel->rd_rel->relkind == RELKIND_RELATION ||
			rel->rd_rel->relkind == RELKIND_MATVIEW ||
			rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			if (origTypeName)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter type \"%s\" because column \"%s.%s\" uses it",
								origTypeName,
								RelationGetRelationName(rel),
								NameStr(att->attname))));
			else if (origRelation->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter type \"%s\" because column \"%s.%s\" uses it",
								RelationGetRelationName(origRelation),
								RelationGetRelationName(rel),
								NameStr(att->attname))));
			else if (origRelation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter foreign table \"%s\" because column \"%s.%s\" uses its row type",
								RelationGetRelationName(origRelation),
								RelationGetRelationName(rel),
								NameStr(att->attname))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter table \"%s\" because column \"%s.%s\" uses its row type",
								RelationGetRelationName(origRelation),
								RelationGetRelationName(rel),
								NameStr(att->attname))));
		}
		else if (OidIsValid(rel->rd_rel->reltype))
		{
			/*
			 * A view or composite type itself isn't a problem, but we must
			 * recursively check for indirect dependencies via its rowtype.
			 */
			find_composite_type_dependencies(rel->rd_rel->reltype,
											 origRelation, origTypeName);
		}

		relation_close(rel, AccessShareLock);
	}

	systable_endscan(depScan);

	relation_close(depRel, AccessShareLock);
}


/*
 * find_typed_table_dependencies
 *
 * Check to see if a composite type is being used as the type of a
 * typed table.  Abort if any are found and behavior is RESTRICT.
 * Else return the list of tables.
 */
static List *
find_typed_table_dependencies(Oid typeOid, const char *typeName, DropBehavior behavior)
{
	Relation	classRel;
	ScanKeyData key[1];
	TableScanDesc scan;
	HeapTuple	tuple;
	List	   *result = NIL;

	classRel = table_open(RelationRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_class_reloftype,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(typeOid));

	scan = table_beginscan_catalog(classRel, 1, key);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);

		if (behavior == DROP_RESTRICT)
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("cannot alter type \"%s\" because it is the type of a typed table",
							typeName),
					 errhint("Use ALTER ... CASCADE to alter the typed tables too.")));
		else
			result = lappend_oid(result, classform->oid);
	}

	table_endscan(scan);
	table_close(classRel, AccessShareLock);

	return result;
}


/*
 * check_of_type
 *
 * Check whether a type is suitable for CREATE TABLE OF/ALTER TABLE OF.  If it
 * isn't suitable, throw an error.  Currently, we require that the type
 * originated with CREATE TYPE AS.  We could support any row type, but doing so
 * would require handling a number of extra corner cases in the DDL commands.
 * (Also, allowing domain-over-composite would open up a can of worms about
 * whether and how the domain's constraints should apply to derived tables.)
 */
void
check_of_type(HeapTuple typetuple)
{
	Form_pg_type typ = (Form_pg_type) GETSTRUCT(typetuple);
	bool		typeOk = false;

	if (typ->typtype == TYPTYPE_COMPOSITE)
	{
		Relation	typeRelation;

		Assert(OidIsValid(typ->typrelid));
		typeRelation = relation_open(typ->typrelid, AccessShareLock);
		typeOk = (typeRelation->rd_rel->relkind == RELKIND_COMPOSITE_TYPE);

		/*
		 * Close the parent rel, but keep our AccessShareLock on it until xact
		 * commit.  That will prevent someone else from deleting or ALTERing
		 * the type before the typed table creation/conversion commits.
		 */
		relation_close(typeRelation, NoLock);
	}
	if (!typeOk)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("type %s is not a composite type",
						format_type_be(typ->oid))));
}


/*
 * ALTER TABLE ADD COLUMN
 *
 * Adds an additional attribute to a relation making the assumption that
 * CHECK, NOT NULL, and FOREIGN KEY constraints will be removed from the
 * AT_AddColumn AlterTableCmd by parse_utilcmd.c and added as independent
 * AlterTableCmd's.
 *
 * ADD COLUMN cannot use the normal ALTER TABLE recursion mechanism, because we
 * have to decide at runtime whether to recurse or not depending on whether we
 * actually add a column or merely merge with an existing column.  (We can't
 * check this in a static pre-pass because it won't handle multiple inheritance
 * situations correctly.)
 */
static void
ATPrepAddColumn(List **wqueue, Relation rel, bool recurse, bool recursing,
				bool is_view, AlterTableCmd *cmd, LOCKMODE lockmode,
				AlterTableUtilityContext *context)
{
	if (rel->rd_rel->reloftype && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot add column to typed table")));

	if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);

	if (recurse && !is_view)
		cmd->subtype = AT_AddColumnRecurse;
}

/*
 * Add a column to a table.  The return value is the address of the
 * new column in the parent relation.
 *
 * cmd is pass-by-ref so that we can replace it with the parse-transformed
 * copy (but that happens only after we check for IF NOT EXISTS).
 */
static ObjectAddress
ATExecAddColumn(List **wqueue, AlteredTableInfo *tab, Relation rel,
				AlterTableCmd **cmd,
				bool recurse, bool recursing,
				LOCKMODE lockmode, int cur_pass,
				AlterTableUtilityContext *context)
{
	Oid			myrelid = RelationGetRelid(rel);
	ColumnDef  *colDef = castNode(ColumnDef, (*cmd)->def);
	bool		if_not_exists = (*cmd)->missing_ok;
	Relation	pgclass,
				attrdesc;
	HeapTuple	reltup;
	FormData_pg_attribute attribute;
	int			newattnum;
	char		relkind;
	HeapTuple	typeTuple;
	Oid			typeOid;
	int32		typmod;
	Oid			collOid;
	Form_pg_type tform;
	Expr	   *defval;
	List	   *children;
	ListCell   *child;
	AlterTableCmd *childcmd;
	AclResult	aclresult;
	ObjectAddress address;
	TupleDesc	tupdesc;
	FormData_pg_attribute *aattr[] = {&attribute};

	/* At top level, permission check was done in ATPrepCmd, else do it */
	if (recursing)
		ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);

	if (rel->rd_rel->relispartition && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot add column to a partition")));

	attrdesc = table_open(AttributeRelationId, RowExclusiveLock);

	/*
	 * Are we adding the column to a recursion child?  If so, check whether to
	 * merge with an existing definition for the column.  If we do merge, we
	 * must not recurse.  Children will already have the column, and recursing
	 * into them would mess up attinhcount.
	 */
	if (colDef->inhcount > 0)
	{
		HeapTuple	tuple;

		/* Does child already have a column by this name? */
		tuple = SearchSysCacheCopyAttName(myrelid, colDef->colname);
		if (HeapTupleIsValid(tuple))
		{
			Form_pg_attribute childatt = (Form_pg_attribute) GETSTRUCT(tuple);
			Oid			ctypeId;
			int32		ctypmod;
			Oid			ccollid;

			/* Child column must match on type, typmod, and collation */
			typenameTypeIdAndMod(NULL, colDef->typeName, &ctypeId, &ctypmod);
			if (ctypeId != childatt->atttypid ||
				ctypmod != childatt->atttypmod)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("child table \"%s\" has different type for column \"%s\"",
								RelationGetRelationName(rel), colDef->colname)));
			ccollid = GetColumnDefCollation(NULL, colDef, ctypeId);
			if (ccollid != childatt->attcollation)
				ereport(ERROR,
						(errcode(ERRCODE_COLLATION_MISMATCH),
						 errmsg("child table \"%s\" has different collation for column \"%s\"",
								RelationGetRelationName(rel), colDef->colname),
						 errdetail("\"%s\" versus \"%s\"",
								   get_collation_name(ccollid),
								   get_collation_name(childatt->attcollation))));

			/* Bump the existing child att's inhcount */
			childatt->attinhcount++;
			CatalogTupleUpdate(attrdesc, &tuple->t_self, tuple);

			heap_freetuple(tuple);

			/* Inform the user about the merge */
			ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
					(errmsg("merging definition of column \"%s\" for child \"%s\"",
							colDef->colname, RelationGetRelationName(rel))));

			table_close(attrdesc, RowExclusiveLock);
			return InvalidObjectAddress;
		}
	}

	/* skip if the name already exists and if_not_exists is true */
	if (!check_for_column_name_collision(rel, colDef->colname, if_not_exists))
	{
		table_close(attrdesc, RowExclusiveLock);
		return InvalidObjectAddress;
	}

	/*
	 * Okay, we need to add the column, so go ahead and do parse
	 * transformation.  This can result in queueing up, or even immediately
	 * executing, subsidiary operations (such as creation of unique indexes);
	 * so we mustn't do it until we have made the if_not_exists check.
	 *
	 * When recursing, the command was already transformed and we needn't do
	 * so again.  Also, if context isn't given we can't transform.  (That
	 * currently happens only for AT_AddColumnToView; we expect that view.c
	 * passed us a ColumnDef that doesn't need work.)
	 */
	if (context != NULL && !recursing)
	{
		*cmd = ATParseTransformCmd(wqueue, tab, rel, *cmd, recurse, lockmode,
								   cur_pass, context);
		Assert(*cmd != NULL);
		colDef = castNode(ColumnDef, (*cmd)->def);
	}

	/*
	 * Cannot add identity column if table has children, because identity does
	 * not inherit.  (Adding column and identity separately will work.)
	 */
	if (colDef->identity &&
		recurse &&
		find_inheritance_children(myrelid, NoLock) != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot recursively add identity column to table that has child tables")));

	pgclass = table_open(RelationRelationId, RowExclusiveLock);

	reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(myrelid));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", myrelid);
	relkind = ((Form_pg_class) GETSTRUCT(reltup))->relkind;

	/* Determine the new attribute's number */
	newattnum = ((Form_pg_class) GETSTRUCT(reltup))->relnatts + 1;
	if (newattnum > MaxHeapAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("tables can have at most %d columns",
						MaxHeapAttributeNumber)));

	typeTuple = typenameType(NULL, colDef->typeName, &typmod);
	tform = (Form_pg_type) GETSTRUCT(typeTuple);
	typeOid = tform->oid;

	aclresult = pg_type_aclcheck(typeOid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, typeOid);

	collOid = GetColumnDefCollation(NULL, colDef, typeOid);

	/* make sure datatype is legal for a column */
	CheckAttributeType(colDef->colname, typeOid, collOid,
					   list_make1_oid(rel->rd_rel->reltype),
					   0);

	/* construct new attribute's pg_attribute entry */
	attribute.attrelid = myrelid;
	namestrcpy(&(attribute.attname), colDef->colname);
	attribute.atttypid = typeOid;
	attribute.attstattarget = (newattnum > 0) ? -1 : 0;
	attribute.attlen = tform->typlen;
	attribute.attnum = newattnum;
	attribute.attndims = list_length(colDef->typeName->arrayBounds);
	attribute.atttypmod = typmod;
	attribute.attbyval = tform->typbyval;
	attribute.attalign = tform->typalign;
	attribute.attstorage = tform->typstorage;
	attribute.attcompression = GetAttributeCompression(typeOid,
													   colDef->compression);
	attribute.attnotnull = colDef->is_not_null;
	attribute.atthasdef = false;
	attribute.atthasmissing = false;
	attribute.attidentity = colDef->identity;
	attribute.attgenerated = colDef->generated;
	attribute.attisdropped = false;
	attribute.attislocal = colDef->is_local;
	attribute.attinhcount = colDef->inhcount;
	attribute.attcollation = collOid;

	/* attribute.attacl is handled by InsertPgAttributeTuples() */

	ReleaseSysCache(typeTuple);

	tupdesc = CreateTupleDesc(lengthof(aattr), (FormData_pg_attribute **) &aattr);

	InsertPgAttributeTuples(attrdesc, tupdesc, myrelid, NULL, NULL);

	table_close(attrdesc, RowExclusiveLock);

	/*
	 * Update pg_class tuple as appropriate
	 */
	((Form_pg_class) GETSTRUCT(reltup))->relnatts = newattnum;

	CatalogTupleUpdate(pgclass, &reltup->t_self, reltup);

	heap_freetuple(reltup);

	/* Post creation hook for new attribute */
	InvokeObjectPostCreateHook(RelationRelationId, myrelid, newattnum);

	table_close(pgclass, RowExclusiveLock);

	/* Make the attribute's catalog entry visible */
	CommandCounterIncrement();

	/*
	 * Store the DEFAULT, if any, in the catalogs
	 */
	if (colDef->raw_default)
	{
		RawColumnDefault *rawEnt;

		rawEnt = (RawColumnDefault *) palloc(sizeof(RawColumnDefault));
		rawEnt->attnum = attribute.attnum;
		rawEnt->raw_default = copyObject(colDef->raw_default);

		rawEnt->hasCookedMissingVal = colDef->hasCookedMissingVal;
		rawEnt->missingVal = colDef->missingVal;
		rawEnt->missingIsNull = colDef->missingIsNull;

		/*
		 * Attempt to skip a complete table rewrite by storing the specified
		 * DEFAULT value outside of the heap.  This may be disabled inside
		 * AddRelationNewConstraints if the optimization cannot be applied.
		 *
		 * In GPDB, AddRelationNewConstraints will also set rawEnt->missingVal/IsNull
		 */
		rawEnt->missingMode = (!colDef->generated);

		rawEnt->generated = colDef->generated;

		/*
		 * This function is intended for CREATE TABLE, so it processes a
		 * _list_ of defaults, but we just do one.
		 */
		AddRelationNewConstraints(rel, list_make1(rawEnt), NIL,
								  false, true, false, NULL);

		/* copy back the cooked attmissingval for dispatch */
		colDef->hasCookedMissingVal = rawEnt->hasCookedMissingVal;
		colDef->missingVal = rawEnt->missingVal;
		colDef->missingIsNull = rawEnt->missingIsNull;

		/* Make the additional catalog changes visible */
		CommandCounterIncrement();

		/*
		 * Did the request for a missing value work? If not we'll have to do a
		 * rewrite
		 */
		/*
		 * GPDB_12_MERGE_FIXME: This optimization to avoid rewriting a table
		 * is based on the assumption that at the time of reading tuples from
		 * this table, it is possible to determine if the tuple does not
		 * contain the value for the new column being added.  In that case,
		 * the missing value would be replaced with the default value from
		 * pg_attrdef catalog table.
		 *
		 * The optimization cannot be applied to appendoptimized row-oriented
		 * tables because the number of attributes is not recorded on disk.
		 * MemTuples only record the tuple length followed by the tuple data.
		 * This information is not sufficient to determine if the tuple
		 * contains a missing column.
		 *
		 * A possible solution involves recoding the number of attributes for
		 * each tuple or for each varblock, so that this optimization can be
		 * applied on similar lines as heap_getattr.
		 */
		if (!rawEnt->missingMode || RelationIsAoRows(rel))
			tab->rewrite |= AT_REWRITE_DEFAULT_VAL;
	}

	/*
	 * Tell Phase 3 to fill in the default expression, if there is one.
	 *
	 * If there is no default, Phase 3 doesn't have to do anything, because
	 * that effectively means that the default is NULL.  The heap tuple access
	 * routines always check for attnum > # of attributes in tuple, and return
	 * NULL if so, so without any modification of the tuple data we will get
	 * the effect of NULL values in the new column.
	 *
	 * An exception occurs when the new column is of a domain type: the domain
	 * might have a NOT NULL constraint, or a check constraint that indirectly
	 * rejects nulls.  If there are any domain constraints then we construct
	 * an explicit NULL default value that will be passed through
	 * CoerceToDomain processing.  (This is a tad inefficient, since it causes
	 * rewriting the table which we really don't have to do, but the present
	 * design of domain processing doesn't offer any simple way of checking
	 * the constraints more directly.)
	 *
	 * Note: we use build_column_default, and not just the cooked default
	 * returned by AddRelationNewConstraints, so that the right thing happens
	 * when a datatype's default applies.
	 *
	 * Note: it might seem that this should happen at the end of Phase 2, so
	 * that the effects of subsequent subcommands can be taken into account.
	 * It's intentional that we do it now, though.  The new column should be
	 * filled according to what is said in the ADD COLUMN subcommand, so that
	 * the effects are the same as if this subcommand had been run by itself
	 * and the later subcommands had been issued in new ALTER TABLE commands.
	 *
	 * We can skip this entirely for relations without storage, since Phase 3
	 * is certainly not going to touch them.  System attributes don't have
	 * interesting defaults, either.
	 */
	if (RELKIND_HAS_STORAGE(relkind) && attribute.attnum > 0)
	{
		/*
		 * For an identity column, we can't use build_column_default(),
		 * because the sequence ownership isn't set yet.  So do it manually.
		 */
		if (colDef->identity)
		{
			NextValueExpr *nve = makeNode(NextValueExpr);

			nve->seqid = RangeVarGetRelid(colDef->identitySequence, NoLock, false);
			nve->typeId = typeOid;

			defval = (Expr *) nve;

			/* must do a rewrite for identity columns */
			tab->rewrite |= AT_REWRITE_DEFAULT_VAL;
		}
		else
			defval = (Expr *) build_column_default(rel, attribute.attnum);

		if (!defval && DomainHasConstraints(typeOid))
		{
			Oid			baseTypeId;
			int32		baseTypeMod;
			Oid			baseTypeColl;

			baseTypeMod = typmod;
			baseTypeId = getBaseTypeAndTypmod(typeOid, &baseTypeMod);
			baseTypeColl = get_typcollation(baseTypeId);
			defval = (Expr *) makeNullConst(baseTypeId, baseTypeMod, baseTypeColl);
			defval = (Expr *) coerce_to_target_type(NULL,
													(Node *) defval,
													baseTypeId,
													typeOid,
													typmod,
													COERCION_ASSIGNMENT,
													COERCE_IMPLICIT_CAST,
													-1);
			if (defval == NULL) /* should not happen */
				elog(ERROR, "failed to coerce base type to domain");
		}

		/*
		 * Handling of default NULL for AO/CO tables.
		 *
		 * Currently memtuples cannot deal with the scenario where the number of
		 * attributes in the tuple data don't match the attnum. We will generate an
		 * explicit NULL default value and force a rewrite of the table below.
		 *
		 * At one point there were plans to restructure memtuples so that this
		 * rewrite did not have to occur. An optimization was added to
		 * column-oriented tables to avoid the rewrite, but it does not apply to
		 * row-oriented tables. Eventually it would be nice to remove this
		 * workaround; see GitHub issue
		 *     https://github.com/greenplum-db/gpdb/issues/3756
		 *
		 * GPDB_12_MERGE_FIXME: we used to do this only if no default was given,
		 * but starting with PostgreSQL v11, a table doesn't need to be rewritten
		 * even if a non-NULL default is used. That caused an assertion failure in
		 * the 'uao_ddl/alter_ao_table_constraint_column' test. To make that go
		 * away, always force full rewrite on AO_ROW and AO_COLUMN tables. We
		 * should be smarter..
		 */

		if (RelationIsAppendOptimized(rel))
		{
			if (!defval)
				defval = (Expr *) makeNullConst(typeOid, -1, collOid);
			tab->rewrite |= AT_REWRITE_DEFAULT_VAL;
		}

		if (defval)
		{
			NewColumnValue *newval;

			/* If QE, AlteredTableInfo streamed from QD already contains newvals */
			if (Gp_role != GP_ROLE_EXECUTE)
			{
				newval = (NewColumnValue *) palloc0(sizeof(NewColumnValue));
				newval->attnum = attribute.attnum;
				newval->expr = expression_planner(defval);
				newval->is_generated = (colDef->generated != '\0');

				/*
				 * tab is null if this is called by "create or replace view" which
				 * can't have any default value.
				 */
				Assert(tab);
				tab->newvals = lappend(tab->newvals, newval);
			}
			else
			{
				Assert(tab->newvals != NULL);
			}
		}

		if (DomainHasConstraints(typeOid))
			tab->rewrite |= AT_REWRITE_DEFAULT_VAL;

		if (!TupleDescAttr(rel->rd_att, attribute.attnum - 1)->atthasmissing)
		{
			/*
			 * If the new column is NOT NULL, and there is no missing value,
			 * tell Phase 3 it needs to check for NULLs.
			 */
			tab->verify_new_notnull |= colDef->is_not_null;
		}
	}

	/*
	 * Add needed dependency entries for the new column.
	 */
	add_column_datatype_dependency(myrelid, newattnum, attribute.atttypid);
	add_column_collation_dependency(myrelid, newattnum, attribute.attcollation);

	if (OidIsValid(rel->rd_rel->relam))
	{
		List *enc;
		const TableAmRoutine *tam = GetTableAmRoutineByAmId(rel->rd_rel->relam);

		/*
		 * Process the encoding clauses.
		 *
		 * For AO/CO tables, always store an encoding clause. If no encoding
		 * clause was provided, store the default encoding clause.
		 * If there's an encoding clause for non AO/CO tables, we'll throw an error
		 * in the function (indicated by errorOnEncodingClause == true).
		 *
		 * AOCO table creates default encoding clause, others will not.
		 */
		enc = transformColumnEncoding(tam /* TableAmRoutine */, rel, list_make1(colDef),
						NULL /* COLUMN ENCODING clauses is only for CREATE TABLE */,
						NULL /* withOptions */,
						RelationIsAoCols(rel) /* createDefaultOne */);
		/*
		 * Store the encoding clause for AO/CO tables.
		 */
		if (AMHandlerSupportEncodingClause(tam))
			AddRelationAttributeEncodings(rel, enc);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH) && MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ADD COLUMN");

	/*
	 * Propagate to children as appropriate.  Unlike most other ALTER
	 * routines, we have to do this one level of recursion at a time; we can't
	 * use find_all_inheritors to do it in one pass.
	 */
	children =
		find_inheritance_children(RelationGetRelid(rel), lockmode);

	/*
	 * If we are told not to recurse, there had better not be any child
	 * tables; else the addition would put them out of step.
	 */
	if (children && !recurse)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("column must be added to child tables too")));

	/* Children should see column as singly inherited */
	if (!recursing)
	{
		childcmd = copyObject(*cmd);
		colDef = castNode(ColumnDef, childcmd->def);
		colDef->inhcount = 1;
		colDef->is_local = false;
	}
	else
		childcmd = *cmd;		/* no need to copy again */

	/*
	 * Leave a flag on tables in the partition hierarchy that can benefit from the
	 * optimization for columnar tables.
	 * We have to do it while processing the root partition because that's the
	 * only level where the `ADD COLUMN` subcommands are populated.
	 *
	 * GPDB_12_MERGE_FIXME: Given now wqueue gets dispatched from QD to QE, no
	 * need to perform this step on QE. Only need to execute this block of
	 * code on QD and QE will get the information to perform optimized rewrite
	 * for CO or not. Leaving fixme here as CO code is not working currently,
	 * hence hard to validate if works correctly or not.
	 */
	if (!recursing && (tab->relkind == RELKIND_PARTITIONED_TABLE || tab->relkind == RELKIND_RELATION))
	{
		bool	aocs_write_new_columns_only;
		/*
		 * ADD COLUMN for CO can be optimized only if it is the
		 * only subcommand being performed.
		 */
		aocs_write_new_columns_only = true;
		for (int i = 0; i < AT_NUM_PASSES; ++i)
		{
			if (i != AT_PASS_ADD_COL && tab->subcmds[i])
			{
				aocs_write_new_columns_only = false;
				break;
			}
		}

		if (aocs_write_new_columns_only)
		{
			/*
			 * We have acquired lockmode on the root and first-level partitions
			 * already. This leaves the deeper subpartitions unlocked, but no
			 * operations can drop (or alter) those relations without locking
			 * through the root. Note that find_all_inheritors() also includes
			 * the root partition in the returned list.
			 *
			 * GPDB_12_MERGE_FIXME: we used to have NoLock here, but that caused
			 * assertion failures in the regression tests:
			 *
			 * FATAL:  Unexpected internal error (relation.c:74)
			 * DETAIL:  FailedAssertion("!(lockmode != 0 || (Mode == BootstrapProcessing) || CheckRelationLockedByMe(r, 1, 1))", File: "relation.c", Line: 74)
			 *
			 * so use AccessShareLock instead. Was it important that we used
			 * NoLock here?
			 */
			List *all_inheritors = find_all_inheritors(tab->relid, AccessShareLock, NULL);
			ListCell *lc;
			foreach (lc, all_inheritors)
			{
				Oid r = lfirst_oid(lc);
				Relation rel = heap_open(r, NoLock);
				AlteredTableInfo *childtab;
				childtab = ATGetQueueEntry(wqueue, rel);

				if (RelationIsAoCols(rel))
					childtab->rewrite |= AT_REWRITE_NEW_COLUMNS_ONLY_AOCS;
				heap_close(rel, NoLock);
			}
		}
	}

	foreach(child, children)
	{
		Oid			childrelid = lfirst_oid(child);
		Relation	childrel;
		AlteredTableInfo *childtab;

		/* find_inheritance_children already got lock */
		childrel = table_open(childrelid, NoLock);
		CheckTableNotInUse(childrel, "ALTER TABLE");

		/* Find or create work queue entry for this table */
		childtab = ATGetQueueEntry(wqueue, childrel);

		/* Recurse to child; return value is ignored */
		ATExecAddColumn(wqueue, childtab, childrel,
						&childcmd, recurse, true,
						lockmode, cur_pass, context);

		table_close(childrel, NoLock);
	}

	ObjectAddressSubSet(address, RelationRelationId, myrelid, newattnum);
	return address;
}

/*
 * If a new or renamed column will collide with the name of an existing
 * column and if_not_exists is false then error out, else do nothing.
 */
static bool
check_for_column_name_collision(Relation rel, const char *colname,
								bool if_not_exists)
{
	HeapTuple	attTuple;
	int			attnum;

	/*
	 * this test is deliberately not attisdropped-aware, since if one tries to
	 * add a column matching a dropped column name, it's gonna fail anyway.
	 */
	attTuple = SearchSysCache2(ATTNAME,
							   ObjectIdGetDatum(RelationGetRelid(rel)),
							   PointerGetDatum(colname));
	if (!HeapTupleIsValid(attTuple))
		return true;

	attnum = ((Form_pg_attribute) GETSTRUCT(attTuple))->attnum;
	ReleaseSysCache(attTuple);

	/*
	 * We throw a different error message for conflicts with system column
	 * names, since they are normally not shown and the user might otherwise
	 * be confused about the reason for the conflict.
	 */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_COLUMN),
				 errmsg("column name \"%s\" conflicts with a system column name",
						colname)));
	else
	{
		if (if_not_exists)
		{
			ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" already exists, skipping",
							colname, RelationGetRelationName(rel))));
			return false;
		}

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" already exists",
						colname, RelationGetRelationName(rel))));
	}

	return true;
}

/*
 * Install a column's dependency on its datatype.
 */
static void
add_column_datatype_dependency(Oid relid, int32 attnum, Oid typid)
{
	ObjectAddress myself,
				referenced;

	myself.classId = RelationRelationId;
	myself.objectId = relid;
	myself.objectSubId = attnum;
	referenced.classId = TypeRelationId;
	referenced.objectId = typid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
}

/*
 * Install a column's dependency on its collation.
 */
static void
add_column_collation_dependency(Oid relid, int32 attnum, Oid collid)
{
	ObjectAddress myself,
				referenced;

	/* We know the default collation is pinned, so don't bother recording it */
	if (OidIsValid(collid) && collid != DEFAULT_COLLATION_OID)
	{
		myself.classId = RelationRelationId;
		myself.objectId = relid;
		myself.objectSubId = attnum;
		referenced.classId = CollationRelationId;
		referenced.objectId = collid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
}

/*
 * ALTER TABLE ALTER COLUMN DROP NOT NULL
 */

static void
ATPrepDropNotNull(Relation rel, bool recurse, bool recursing)
{
	/*
	 * If the parent is a partitioned table, like check constraints, we do not
	 * support removing the NOT NULL while partitions exist.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		PartitionDesc partdesc = RelationGetPartitionDesc(rel, true);

		Assert(partdesc != NULL);
		if (partdesc->nparts > 0 && !recurse && !recursing)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot remove constraint from only the partitioned table when partitions exist"),
					 errhint("Do not specify the ONLY keyword.")));
	}
}

/*
 * Return the address of the modified column.  If the column was already
 * nullable, InvalidObjectAddress is returned.
 */
static ObjectAddress
ATExecDropNotNull(Relation rel, const char *colName, LOCKMODE lockmode)
{
	HeapTuple	tuple;
	Form_pg_attribute attTup;
	AttrNumber	attnum;
	Relation	attr_rel;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	ObjectAddress address;

	/*
	 * lookup the attribute
	 */
	attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));
	attTup = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = attTup->attnum;

	/* Prevent them from altering a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	if (attTup->attidentity)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("column \"%s\" of relation \"%s\" is an identity column",
						colName, RelationGetRelationName(rel))));

	/*
	 * Check that the attribute is not in a primary key or in an index used as
	 * a replica identity.
	 *
	 * Note: we'll throw error even if the pkey index is not valid.
	 */

	/* Loop over all indexes on the relation */
	indexoidlist = RelationGetIndexList(rel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		HeapTuple	indexTuple;
		Form_pg_index indexStruct;
		int			i;

		indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

		/*
		 * If the index is not a primary key or an index used as replica
		 * identity, skip the check.
		 */
		if (indexStruct->indisprimary || indexStruct->indisreplident)
		{
			/*
			 * Loop over each attribute in the primary key or the index used
			 * as replica identity and see if it matches the to-be-altered
			 * attribute.
			 */
			for (i = 0; i < indexStruct->indnkeyatts; i++)
			{
				if (indexStruct->indkey.values[i] == attnum)
				{
					if (indexStruct->indisprimary)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("column \"%s\" is in a primary key",
										colName)));
					else
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("column \"%s\" is in index used as replica identity",
										colName)));
				}
			}
		}

		ReleaseSysCache(indexTuple);
	}

	list_free(indexoidlist);

	/* If rel is partition, shouldn't drop NOT NULL if parent has the same */
	if (rel->rd_rel->relispartition)
	{
		Oid			parentId = get_partition_parent(RelationGetRelid(rel), false);
		Relation	parent = table_open(parentId, AccessShareLock);
		TupleDesc	tupDesc = RelationGetDescr(parent);
		AttrNumber	parent_attnum;

		parent_attnum = get_attnum(parentId, colName);
		if (TupleDescAttr(tupDesc, parent_attnum - 1)->attnotnull)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("column \"%s\" is marked NOT NULL in parent table",
							colName)));
		table_close(parent, AccessShareLock);
	}

	/*
	 * Okay, actually perform the catalog change ... if needed
	 */
	if (attTup->attnotnull)
	{
		attTup->attnotnull = false;

		CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);

		ObjectAddressSubSet(address, RelationRelationId,
							RelationGetRelid(rel), attnum);
	}
	else
		address = InvalidObjectAddress;

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel), attnum);

	table_close(attr_rel, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN DROP NOT NULL"
				);

	return address;
}

/*
 * ALTER TABLE ALTER COLUMN SET NOT NULL
 */

static void
ATPrepSetNotNull(List **wqueue, Relation rel,
				 AlterTableCmd *cmd, bool recurse, bool recursing,
				 LOCKMODE lockmode, AlterTableUtilityContext *context)
{
	/*
	 * If we're already recursing, there's nothing to do; the topmost
	 * invocation of ATSimpleRecursion already visited all children.
	 */
	if (recursing)
		return;

	/*
	 * If the target column is already marked NOT NULL, we can skip recursing
	 * to children, because their columns should already be marked NOT NULL as
	 * well.  But there's no point in checking here unless the relation has
	 * some children; else we can just wait till execution to check.  (If it
	 * does have children, however, this can save taking per-child locks
	 * unnecessarily.  This greatly improves concurrency in some parallel
	 * restore scenarios.)
	 *
	 * Unfortunately, we can only apply this optimization to partitioned
	 * tables, because traditional inheritance doesn't enforce that child
	 * columns be NOT NULL when their parent is.  (That's a bug that should
	 * get fixed someday.)
	 */
	if (rel->rd_rel->relhassubclass &&
		rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		HeapTuple	tuple;
		bool		attnotnull;

		tuple = SearchSysCacheAttName(RelationGetRelid(rel), cmd->name);

		/* Might as well throw the error now, if name is bad */
		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							cmd->name, RelationGetRelationName(rel))));

		attnotnull = ((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull;
		ReleaseSysCache(tuple);
		if (attnotnull)
			return;
	}

	/*
	 * If we have ALTER TABLE ONLY ... SET NOT NULL on a partitioned table,
	 * apply ALTER TABLE ... CHECK NOT NULL to every child.  Otherwise, use
	 * normal recursion logic.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
		!recurse)
	{
		AlterTableCmd *newcmd = makeNode(AlterTableCmd);

		newcmd->subtype = AT_CheckNotNull;
		newcmd->name = pstrdup(cmd->name);
		ATSimpleRecursion(wqueue, rel, newcmd, true, lockmode, context);
	}
	else
		ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
}

/*
 * Return the address of the modified column.  If the column was already NOT
 * NULL, InvalidObjectAddress is returned.
 */
static ObjectAddress
ATExecSetNotNull(AlteredTableInfo *tab, Relation rel,
				 const char *colName, LOCKMODE lockmode)
{
	HeapTuple	tuple;
	AttrNumber	attnum;
	Relation	attr_rel;
	ObjectAddress address;

	/*
	 * lookup the attribute
	 */
	attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));

	attnum = ((Form_pg_attribute) GETSTRUCT(tuple))->attnum;

	/* Prevent them from altering a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/*
	 * Okay, actually perform the catalog change ... if needed
	 */
	if (!((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull)
	{
		((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull = true;

		CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);

		/*
		 * Ordinarily phase 3 must ensure that no NULLs exist in columns that
		 * are set NOT NULL; however, if we can find a constraint which proves
		 * this then we can skip that.  We needn't bother looking if we've
		 * already found that we must verify some other NOT NULL constraint.
		 */
		if (!tab->verify_new_notnull &&
			!NotNullImpliedByRelConstraints(rel, (Form_pg_attribute) GETSTRUCT(tuple)))
		{
			/* Tell Phase 3 it needs to test the constraint */
			tab->verify_new_notnull = true;
		}

		ObjectAddressSubSet(address, RelationRelationId,
							RelationGetRelid(rel), attnum);
	}
	else
		address = InvalidObjectAddress;

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel), attnum);

	table_close(attr_rel, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN SET NOT NULL"
				);

	return address;
}

/*
 * ALTER TABLE ALTER COLUMN CHECK NOT NULL
 *
 * This doesn't exist in the grammar, but we generate AT_CheckNotNull
 * commands against the partitions of a partitioned table if the user
 * writes ALTER TABLE ONLY ... SET NOT NULL on the partitioned table,
 * or tries to create a primary key on it (which internally creates
 * AT_SetNotNull on the partitioned table).   Such a command doesn't
 * allow us to actually modify any partition, but we want to let it
 * go through if the partitions are already properly marked.
 *
 * In future, this might need to adjust the child table's state, likely
 * by incrementing an inheritance count for the attnotnull constraint.
 * For now we need only check for the presence of the flag.
 */
static void
ATExecCheckNotNull(AlteredTableInfo *tab, Relation rel,
				   const char *colName, LOCKMODE lockmode)
{
	HeapTuple	tuple;

	tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));

	if (!((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("constraint must be added to child tables too"),
				 errdetail("Column \"%s\" of relation \"%s\" is not already NOT NULL.",
						   colName, RelationGetRelationName(rel)),
				 errhint("Do not specify the ONLY keyword.")));

	ReleaseSysCache(tuple);
}

/*
 * NotNullImpliedByRelConstraints
 *		Does rel's existing constraints imply NOT NULL for the given attribute?
 */
static bool
NotNullImpliedByRelConstraints(Relation rel, Form_pg_attribute attr)
{
	NullTest   *nnulltest = makeNode(NullTest);

	nnulltest->arg = (Expr *) makeVar(1,
									  attr->attnum,
									  attr->atttypid,
									  attr->atttypmod,
									  attr->attcollation,
									  0);
	nnulltest->nulltesttype = IS_NOT_NULL;

	/*
	 * argisrow = false is correct even for a composite column, because
	 * attnotnull does not represent a SQL-spec IS NOT NULL test in such a
	 * case, just IS DISTINCT FROM NULL.
	 */
	nnulltest->argisrow = false;
	nnulltest->location = -1;

	if (ConstraintImpliedByRelConstraint(rel, list_make1(nnulltest), NIL))
	{
		ereport(DEBUG1,
				(errmsg_internal("existing constraints on column \"%s.%s\" are sufficient to prove that it does not contain nulls",
								 RelationGetRelationName(rel), NameStr(attr->attname))));
		return true;
	}

	return false;
}

/*
 * ALTER TABLE ALTER COLUMN SET/DROP DEFAULT
 *
 * Return the address of the affected column.
 */
static ObjectAddress
ATExecColumnDefault(Relation rel, const char *colName,
					Node *newDefault, LOCKMODE lockmode)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	AttrNumber	attnum;
	ObjectAddress address;

	/*
	 * get the number of the attribute
	 */
	attnum = get_attnum(RelationGetRelid(rel), colName);
	if (attnum == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));

	/* Prevent them from altering a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	if (TupleDescAttr(tupdesc, attnum - 1)->attidentity)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("column \"%s\" of relation \"%s\" is an identity column",
						colName, RelationGetRelationName(rel)),
				 newDefault ? 0 : errhint("Use ALTER TABLE ... ALTER COLUMN ... DROP IDENTITY instead.")));

	if (TupleDescAttr(tupdesc, attnum - 1)->attgenerated)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("column \"%s\" of relation \"%s\" is a generated column",
						colName, RelationGetRelationName(rel)),
				 newDefault || TupleDescAttr(tupdesc, attnum - 1)->attgenerated != ATTRIBUTE_GENERATED_STORED ? 0 :
				 errhint("Use ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION instead.")));

	/*
	 * Remove any old default for the column.  We use RESTRICT here for
	 * safety, but at present we do not expect anything to depend on the
	 * default.
	 *
	 * We treat removing the existing default as an internal operation when it
	 * is preparatory to adding a new default, but as a user-initiated
	 * operation when the user asked for a drop.
	 */
	RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false,
					  newDefault == NULL ? false : true);

	if (newDefault)
	{
		/* SET DEFAULT */
		RawColumnDefault *rawEnt;

		rawEnt = (RawColumnDefault *) palloc(sizeof(RawColumnDefault));
		rawEnt->attnum = attnum;
		rawEnt->raw_default = newDefault;
		rawEnt->missingMode = false;
		rawEnt->generated = '\0';

		/*
		 * This function is intended for CREATE TABLE, so it processes a
		 * _list_ of defaults, but we just do one.
		 */
		AddRelationNewConstraints(rel, list_make1(rawEnt), NIL,
								  false, true, false, NULL);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN DEFAULT"
				);

	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);
	return address;
}

/*
 * Add a pre-cooked default expression.
 *
 * Return the address of the affected column.
 */
static ObjectAddress
ATExecCookedColumnDefault(Relation rel, AttrNumber attnum,
						  Node *newDefault)
{
	ObjectAddress address;

	/* We assume no checking is required */

	/*
	 * Remove any old default for the column.  We use RESTRICT here for
	 * safety, but at present we do not expect anything to depend on the
	 * default.  (In ordinary cases, there could not be a default in place
	 * anyway, but it's possible when combining LIKE with inheritance.)
	 */
	RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false,
					  true);

	(void) StoreAttrDefault(rel, attnum, newDefault,
							NULL, NULL, NULL, /* missing val stuff */
							true, false);

	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);
	return address;
}

/*
 * ALTER TABLE ALTER COLUMN ADD IDENTITY
 *
 * Return the address of the affected column.
 */
static ObjectAddress
ATExecAddIdentity(Relation rel, const char *colName,
				  Node *def, LOCKMODE lockmode)
{
	Relation	attrelation;
	HeapTuple	tuple;
	Form_pg_attribute attTup;
	AttrNumber	attnum;
	ObjectAddress address;
	ColumnDef  *cdef = castNode(ColumnDef, def);

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));
	attTup = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = attTup->attnum;

	/* Can't alter a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/*
	 * Creating a column as identity implies NOT NULL, so adding the identity
	 * to an existing column that is not NOT NULL would create a state that
	 * cannot be reproduced without contortions.
	 */
	if (!attTup->attnotnull)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("column \"%s\" of relation \"%s\" must be declared NOT NULL before identity can be added",
						colName, RelationGetRelationName(rel))));

	if (attTup->attidentity)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("column \"%s\" of relation \"%s\" is already an identity column",
						colName, RelationGetRelationName(rel))));

	if (attTup->atthasdef)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("column \"%s\" of relation \"%s\" already has a default value",
						colName, RelationGetRelationName(rel))));

	attTup->attidentity = cdef->identity;
	CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel),
							  attTup->attnum);
	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);
	heap_freetuple(tuple);

	table_close(attrelation, RowExclusiveLock);

	return address;
}

/*
 * ALTER TABLE ALTER COLUMN SET { GENERATED or sequence options }
 *
 * Return the address of the affected column.
 */
static ObjectAddress
ATExecSetIdentity(Relation rel, const char *colName, Node *def, LOCKMODE lockmode)
{
	ListCell   *option;
	DefElem    *generatedEl = NULL;
	HeapTuple	tuple;
	Form_pg_attribute attTup;
	AttrNumber	attnum;
	Relation	attrelation;
	ObjectAddress address;

	foreach(option, castNode(List, def))
	{
		DefElem    *defel = lfirst_node(DefElem, option);

		if (strcmp(defel->defname, "generated") == 0)
		{
			if (generatedEl)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			generatedEl = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	/*
	 * Even if there is nothing to change here, we run all the checks.  There
	 * will be a subsequent ALTER SEQUENCE that relies on everything being
	 * there.
	 */

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));

	attTup = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = attTup->attnum;

	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	if (!attTup->attidentity)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("column \"%s\" of relation \"%s\" is not an identity column",
						colName, RelationGetRelationName(rel))));

	if (generatedEl)
	{
		attTup->attidentity = defGetInt32(generatedEl);
		CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

		InvokeObjectPostAlterHook(RelationRelationId,
								  RelationGetRelid(rel),
								  attTup->attnum);
		ObjectAddressSubSet(address, RelationRelationId,
							RelationGetRelid(rel), attnum);
	}
	else
		address = InvalidObjectAddress;

	heap_freetuple(tuple);
	table_close(attrelation, RowExclusiveLock);

	return address;
}

/*
 * ALTER TABLE ALTER COLUMN DROP IDENTITY
 *
 * Return the address of the affected column.
 */
static ObjectAddress
ATExecDropIdentity(Relation rel, const char *colName, bool missing_ok, LOCKMODE lockmode)
{
	HeapTuple	tuple;
	Form_pg_attribute attTup;
	AttrNumber	attnum;
	Relation	attrelation;
	ObjectAddress address;
	Oid			seqid;
	ObjectAddress seqaddress;

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));

	attTup = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = attTup->attnum;

	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	if (!attTup->attidentity)
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("column \"%s\" of relation \"%s\" is not an identity column",
							colName, RelationGetRelationName(rel))));
		else
		{
			ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
					(errmsg("column \"%s\" of relation \"%s\" is not an identity column, skipping",
							colName, RelationGetRelationName(rel))));
			heap_freetuple(tuple);
			table_close(attrelation, RowExclusiveLock);
			return InvalidObjectAddress;
		}
	}

	attTup->attidentity = '\0';
	CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel),
							  attTup->attnum);
	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);
	heap_freetuple(tuple);

	table_close(attrelation, RowExclusiveLock);

	/* drop the internal sequence */
	seqid = getIdentitySequence(RelationGetRelid(rel), attnum, false);
	deleteDependencyRecordsForClass(RelationRelationId, seqid,
									RelationRelationId, DEPENDENCY_INTERNAL);
	CommandCounterIncrement();
	seqaddress.classId = RelationRelationId;
	seqaddress.objectId = seqid;
	seqaddress.objectSubId = 0;
	performDeletion(&seqaddress, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	return address;
}

/*
 * ALTER TABLE ALTER COLUMN DROP EXPRESSION
 */
static void
ATPrepDropExpression(Relation rel, AlterTableCmd *cmd, bool recurse, bool recursing, LOCKMODE lockmode)
{
	/*
	 * Reject ONLY if there are child tables.  We could implement this, but it
	 * is a bit complicated.  GENERATED clauses must be attached to the column
	 * definition and cannot be added later like DEFAULT, so if a child table
	 * has a generation expression that the parent does not have, the child
	 * column will necessarily be an attlocal column.  So to implement ONLY
	 * here, we'd need extra code to update attislocal of the direct child
	 * tables, somewhat similar to how DROP COLUMN does it, so that the
	 * resulting state can be properly dumped and restored.
	 */
	if (!recurse &&
		find_inheritance_children(RelationGetRelid(rel), lockmode))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ALTER TABLE / DROP EXPRESSION must be applied to child tables too")));

	/*
	 * Cannot drop generation expression from inherited columns.
	 */
	if (!recursing)
	{
		HeapTuple	tuple;
		Form_pg_attribute attTup;

		tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), cmd->name);
		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							cmd->name, RelationGetRelationName(rel))));

		attTup = (Form_pg_attribute) GETSTRUCT(tuple);

		if (attTup->attinhcount > 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot drop generation expression from inherited column")));
	}
}

/*
 * Return the address of the affected column.
 */
static ObjectAddress
ATExecDropExpression(Relation rel, const char *colName, bool missing_ok, LOCKMODE lockmode)
{
	HeapTuple	tuple;
	Form_pg_attribute attTup;
	AttrNumber	attnum;
	Relation	attrelation;
	ObjectAddress address;

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));

	attTup = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = attTup->attnum;

	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	if (attTup->attgenerated != ATTRIBUTE_GENERATED_STORED)
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("column \"%s\" of relation \"%s\" is not a stored generated column",
							colName, RelationGetRelationName(rel))));
		else
		{
			ereport(NOTICE,
					(errmsg("column \"%s\" of relation \"%s\" is not a stored generated column, skipping",
							colName, RelationGetRelationName(rel))));
			heap_freetuple(tuple);
			table_close(attrelation, RowExclusiveLock);
			return InvalidObjectAddress;
		}
	}

	attTup->attgenerated = '\0';
	CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel),
							  attTup->attnum);
	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);
	heap_freetuple(tuple);

	table_close(attrelation, RowExclusiveLock);

	CommandCounterIncrement();

	RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, false);

	/*
	 * Remove all dependencies of this (formerly generated) column on other
	 * columns in the same table.  (See StoreAttrDefault() for which
	 * dependencies are created.)  We don't expect there to be dependencies
	 * between columns of the same table for other reasons, so it's okay to
	 * remove all of them.
	 */
	{
		Relation	depRel;
		ScanKeyData key[3];
		SysScanDesc scan;
		HeapTuple	tup;

		depRel = table_open(DependRelationId, RowExclusiveLock);

		ScanKeyInit(&key[0],
					Anum_pg_depend_classid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(RelationRelationId));
		ScanKeyInit(&key[1],
					Anum_pg_depend_objid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(RelationGetRelid(rel)));
		ScanKeyInit(&key[2],
					Anum_pg_depend_objsubid,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(attnum));

		scan = systable_beginscan(depRel, DependDependerIndexId, true,
								  NULL, 3, key);

		while (HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

			if (depform->refclassid == RelationRelationId &&
				depform->refobjid == RelationGetRelid(rel) &&
				depform->refobjsubid != 0 &&
				depform->deptype == DEPENDENCY_AUTO)
			{
				CatalogTupleDelete(depRel, &tup->t_self);
			}
		}

		systable_endscan(scan);

		table_close(depRel, RowExclusiveLock);
	}

	return address;
}

/*
 * ALTER TABLE ALTER COLUMN SET STATISTICS
 *
 * Return value is the address of the modified column
 */
static ObjectAddress
ATExecSetStatistics(Relation rel, const char *colName, int16 colNum, Node *newValue, LOCKMODE lockmode)
{
	int			newtarget;
	Relation	attrelation;
	HeapTuple	tuple;
	Form_pg_attribute attrtuple;
	AttrNumber	attnum;
	ObjectAddress address;

	/*
	 * We allow referencing columns by numbers only for indexes, since table
	 * column numbers could contain gaps if columns are later dropped.
	 */
	if (rel->rd_rel->relkind != RELKIND_INDEX &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX &&
		!colName)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot refer to non-index column by number")));

	Assert(IsA(newValue, Integer));
	newtarget = intVal(newValue);

	/*
	 * Limit target to a sane range
	 */
	if (newtarget < -1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("statistics target %d is too low",
						newtarget)));
	}
	else if (newtarget > 10000)
	{
		newtarget = 10000;
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("lowering statistics target to %d",
						newtarget)));
	}

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);

	if (colName)
	{
		tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							colName, RelationGetRelationName(rel))));
	}
	else
	{
		tuple = SearchSysCacheCopyAttNum(RelationGetRelid(rel), colNum);

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column number %d of relation \"%s\" does not exist",
							colNum, RelationGetRelationName(rel))));
	}

	attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

	attnum = attrtuple->attnum;
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	if (rel->rd_rel->relkind == RELKIND_INDEX ||
		rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
	{
		if (attnum > rel->rd_index->indnkeyatts)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot alter statistics on included column \"%s\" of index \"%s\"",
							NameStr(attrtuple->attname), RelationGetRelationName(rel))));
		else if (rel->rd_index->indkey.values[attnum - 1] != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot alter statistics on non-expression column \"%s\" of index \"%s\"",
							NameStr(attrtuple->attname), RelationGetRelationName(rel)),
					 errhint("Alter statistics on table column instead.")));
	}

	attrtuple->attstattarget = newtarget;

	CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel),
							  attrtuple->attnum);
	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);
	heap_freetuple(tuple);

	table_close(attrelation, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN SET STATISTICS"
				);
	return address;
}

/*
 * Return value is the address of the modified column
 */
static ObjectAddress
ATExecSetOptions(Relation rel, const char *colName, Node *options,
				 bool isReset, LOCKMODE lockmode)
{
	Relation	attrelation;
	HeapTuple	tuple,
				newtuple;
	Form_pg_attribute attrtuple;
	AttrNumber	attnum;
	Datum		datum,
				newOptions;
	bool		isnull;
	ObjectAddress address;
	Datum		repl_val[Natts_pg_attribute];
	bool		repl_null[Natts_pg_attribute];
	bool		repl_repl[Natts_pg_attribute];

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);

	tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));
	attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

	attnum = attrtuple->attnum;
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/* Generate new proposed attoptions (text array) */
	datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions,
							&isnull);
	newOptions = transformRelOptions(isnull ? (Datum) 0 : datum,
									 castNode(List, options), NULL, NULL,
									 false, isReset);
	/* Validate new options */
	(void) attribute_reloptions(newOptions, true);

	/* Build new tuple. */
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));
	if (newOptions != (Datum) 0)
		repl_val[Anum_pg_attribute_attoptions - 1] = newOptions;
	else
		repl_null[Anum_pg_attribute_attoptions - 1] = true;
	repl_repl[Anum_pg_attribute_attoptions - 1] = true;
	newtuple = heap_modify_tuple(tuple, RelationGetDescr(attrelation),
								 repl_val, repl_null, repl_repl);

	/* Update system catalog. */
	CatalogTupleUpdate(attrelation, &newtuple->t_self, newtuple);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel),
							  attrtuple->attnum);
	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);

	heap_freetuple(newtuple);

	ReleaseSysCache(tuple);

	table_close(attrelation, RowExclusiveLock);

	return address;
}

/*
 * Helper function for ATExecSetStorage and ATExecSetCompression
 *
 * Set the attstorage and/or attcompression fields for index columns
 * associated with the specified table column.
 */
static void
SetIndexStorageProperties(Relation rel, Relation attrelation,
						  AttrNumber attnum,
						  bool setstorage, char newstorage,
						  bool setcompression, char newcompression,
						  LOCKMODE lockmode)
{
	ListCell   *lc;

	foreach(lc, RelationGetIndexList(rel))
	{
		Oid			indexoid = lfirst_oid(lc);
		Relation	indrel;
		AttrNumber	indattnum = 0;
		HeapTuple	tuple;

		indrel = index_open(indexoid, lockmode);

		for (int i = 0; i < indrel->rd_index->indnatts; i++)
		{
			if (indrel->rd_index->indkey.values[i] == attnum)
			{
				indattnum = i + 1;
				break;
			}
		}

		if (indattnum == 0)
		{
			index_close(indrel, lockmode);
			continue;
		}

		tuple = SearchSysCacheCopyAttNum(RelationGetRelid(indrel), indattnum);

		if (HeapTupleIsValid(tuple))
		{
			Form_pg_attribute attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

			if (setstorage)
				attrtuple->attstorage = newstorage;

			if (setcompression)
				attrtuple->attcompression = newcompression;

			CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

			InvokeObjectPostAlterHook(RelationRelationId,
									  RelationGetRelid(rel),
									  attrtuple->attnum);

			heap_freetuple(tuple);
		}

		index_close(indrel, lockmode);
	}
}

/*
 * ALTER TABLE ALTER COLUMN SET STORAGE
 *
 * Return value is the address of the modified column
 */
static ObjectAddress
ATExecSetStorage(Relation rel, const char *colName, Node *newValue, LOCKMODE lockmode)
{
	char	   *storagemode;
	char		newstorage;
	Relation	attrelation;
	HeapTuple	tuple;
	Form_pg_attribute attrtuple;
	AttrNumber	attnum;
	ObjectAddress address;

	Assert(IsA(newValue, String));
	storagemode = strVal(newValue);

	if (pg_strcasecmp(storagemode, "plain") == 0)
		newstorage = TYPSTORAGE_PLAIN;
	else if (pg_strcasecmp(storagemode, "external") == 0)
		newstorage = TYPSTORAGE_EXTERNAL;
	else if (pg_strcasecmp(storagemode, "extended") == 0)
		newstorage = TYPSTORAGE_EXTENDED;
	else if (pg_strcasecmp(storagemode, "main") == 0)
		newstorage = TYPSTORAGE_MAIN;
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage type \"%s\"",
						storagemode)));
		newstorage = 0;			/* keep compiler quiet */
	}

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));
	attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

	attnum = attrtuple->attnum;
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/*
	 * safety check: do not allow toasted storage modes unless column datatype
	 * is TOAST-aware.
	 */
	if (newstorage == TYPSTORAGE_PLAIN || TypeIsToastable(attrtuple->atttypid))
		attrtuple->attstorage = newstorage;
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("column data type %s can only have storage PLAIN",
						format_type_be(attrtuple->atttypid))));

	CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel),
							  attrtuple->attnum);

	heap_freetuple(tuple);

	/*
	 * Apply the change to indexes as well (only for simple index columns,
	 * matching behavior of index.c ConstructTupleDescriptor()).
	 */
	SetIndexStorageProperties(rel, attrelation, attnum,
							  true, newstorage,
							  false, 0,
							  lockmode);

	table_close(attrelation, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN SET STORAGE"
				);

	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);
	return address;
}


/*
 * ALTER TABLE DROP COLUMN
 *
 * DROP COLUMN cannot use the normal ALTER TABLE recursion mechanism,
 * because we have to decide at runtime whether to recurse or not depending
 * on whether attinhcount goes to zero or not.  (We can't check this in a
 * static pre-pass because it won't handle multiple inheritance situations
 * correctly.)
 */
static void
ATPrepDropColumn(List **wqueue, Relation rel, bool recurse, bool recursing,
				 AlterTableCmd *cmd, LOCKMODE lockmode,
				 AlterTableUtilityContext *context)
{
	if (rel->rd_rel->reloftype && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot drop column from typed table")));

	if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);

	if (recurse)
		cmd->subtype = AT_DropColumnRecurse;
}

void
set_random_distribution_if_drop_distkey(Relation rel, AttrNumber attnum)
{
	int			ia = 0;
	if (attnum < 0 || !GpPolicyIsPartitioned(rel->rd_cdbpolicy))
		return;

	for (ia = 0; ia < rel->rd_cdbpolicy->nattrs; ia++)
	{
		if (attnum == rel->rd_cdbpolicy->attrs[ia])
		{
			MemoryContext oldcontext;
			GpPolicy *policy;

			/* force a random distribution */
			rel->rd_cdbpolicy->nattrs = 0;

			oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));
			policy = GpPolicyCopy(rel->rd_cdbpolicy);
			MemoryContextSwitchTo(oldcontext);

			/*
			 * replace policy first in catalog and then assign to
			 * rd_cdbpolicy to make sure we have intended policy in relcache
			 * even with relcache invalidation. Otherwise rd_cdbpolicy can
			 * become invalid soon after assignment.
			 */
			GpPolicyReplace(RelationGetRelid(rel), policy);
			rel->rd_cdbpolicy = policy;
			if (Gp_role != GP_ROLE_EXECUTE)
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("dropping a column that is part of the distribution policy forces a NULL distribution policy")));
			break;
		}
	}
}

/*
 * Drops column 'colName' from relation 'rel' and returns the address of the
 * dropped column.  The column is also dropped (or marked as no longer
 * inherited from relation) from the relation's inheritance children, if any.
 *
 * In the recursive invocations for inheritance child relations, instead of
 * dropping the column directly (if to be dropped at all), its object address
 * is added to 'addrs', which must be non-NULL in such invocations.  All
 * columns are dropped at the same time after all the children have been
 * checked recursively.
 */
static ObjectAddress
ATExecDropColumn(List **wqueue, Relation rel, const char *colName,
				 DropBehavior behavior,
				 bool recurse, bool recursing,
				 bool missing_ok, LOCKMODE lockmode,
				 ObjectAddresses *addrs)
{
	HeapTuple	tuple;
	Form_pg_attribute targetatt;
	AttrNumber	attnum;
	List	   *children;
	ObjectAddress object;
	bool		is_expr;

	/* At top level, permission check was done in ATPrepCmd, else do it */
	if (recursing)
		ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);

	/* Initialize addrs on the first invocation */
	Assert(!recursing || addrs != NULL);
	if (!recursing)
		addrs = new_object_addresses();

	/*
	 * get the number of the attribute
	 */
	tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(tuple))
	{
		if (!missing_ok)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							colName, RelationGetRelationName(rel))));
		}
		else
		{
			ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
					(errmsg("column \"%s\" of relation \"%s\" does not exist, skipping",
							colName, RelationGetRelationName(rel))));
			return InvalidObjectAddress;
		}
	}
	targetatt = (Form_pg_attribute) GETSTRUCT(tuple);

	attnum = targetatt->attnum;

	/* Can't drop a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop system column \"%s\"",
						colName)));

	/*
	 * Don't drop inherited columns, unless recursing (presumably from a drop
	 * of the parent column)
	 */
	if (targetatt->attinhcount > 0 && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot drop inherited column \"%s\"",
						colName)));

	/*
	 * Don't drop columns used in the partition key, either.  (If we let this
	 * go through, the key column's dependencies would cause a cascaded drop
	 * of the whole table, which is surely not what the user expected.)
	 */
	if (has_partition_attrs(rel,
							bms_make_singleton(attnum - FirstLowInvalidHeapAttributeNumber),
							&is_expr))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot drop column \"%s\" because it is part of the partition key of relation \"%s\"",
						colName, RelationGetRelationName(rel))));

	ReleaseSysCache(tuple);

	/*
	 * Propagate to children as appropriate.  Unlike most other ALTER
	 * routines, we have to do this one level of recursion at a time; we can't
	 * use find_all_inheritors to do it in one pass.
	 */
	children =
		find_inheritance_children(RelationGetRelid(rel), lockmode);

	if (children)
	{
		Relation	attr_rel;
		ListCell   *child;

		/*
		 * In case of a partitioned table, the column must be dropped from the
		 * partitions as well.
		 */
		if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE && !recurse)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot drop column from only the partitioned table when partitions exist"),
					 errhint("Do not specify the ONLY keyword.")));

		attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
		foreach(child, children)
		{
			Oid			childrelid = lfirst_oid(child);
			Relation	childrel;
			Form_pg_attribute childatt;

			/* find_inheritance_children already got lock */
			childrel = table_open(childrelid, NoLock);
			CheckTableNotInUse(childrel, "ALTER TABLE");

			tuple = SearchSysCacheCopyAttName(childrelid, colName);
			if (!HeapTupleIsValid(tuple))	/* shouldn't happen */
				elog(ERROR, "cache lookup failed for attribute \"%s\" of relation %u",
					 colName, childrelid);
			childatt = (Form_pg_attribute) GETSTRUCT(tuple);

			if (childatt->attinhcount <= 0) /* shouldn't happen */
				elog(ERROR, "relation %u has non-inherited attribute \"%s\"",
					 childrelid, colName);

			if (recurse)
			{
				/*
				 * If the child column has other definition sources, just
				 * decrement its inheritance count; if not or if this is part
				 * of a partition configuration, recurse to delete it.
				 */
				if (childatt->attinhcount == 1 && !childatt->attislocal)
				{
					/* Time to delete this child column, too */
					ATExecDropColumn(wqueue, childrel, colName,
									 behavior, true, true,
									 false, lockmode, addrs);
				}
				else
				{
					/* Child column must survive my deletion */
					childatt->attinhcount--;

					CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);

					/* Make update visible */
					CommandCounterIncrement();
				}
			}
			else
			{
				/*
				 * If we were told to drop ONLY in this table (no recursion),
				 * we need to mark the inheritors' attributes as locally
				 * defined rather than inherited.
				 */
				childatt->attinhcount--;
				childatt->attislocal = true;

				CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);

				/* Make update visible */
				CommandCounterIncrement();
			}

			heap_freetuple(tuple);

			table_close(childrel, NoLock);
		}
		table_close(attr_rel, RowExclusiveLock);
	}

	/* Add object to delete */
	object.classId = RelationRelationId;
	object.objectId = RelationGetRelid(rel);
	object.objectSubId = attnum;
	add_exact_object_address(&object, addrs);

	if (!recursing)
	{
		/* Recursion has ended, drop everything that was collected */
		performMultipleDeletions(addrs, behavior, 0);
		free_object_addresses(addrs);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH) && MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "DROP COLUMN");
	return object;
}

/*
 * ALTER TABLE ADD INDEX
 *
 * There is no such command in the grammar, but parse_utilcmd.c converts
 * UNIQUE and PRIMARY KEY constraints into AT_AddIndex subcommands.  This lets
 * us schedule creation of the index at the appropriate time during ALTER.
 *
 * Return value is the address of the new index.
 */
static ObjectAddress
ATExecAddIndex(AlteredTableInfo *tab, Relation rel,
			   IndexStmt *stmt, bool is_rebuild, LOCKMODE lockmode)
{
	bool		check_rights;
	bool		skip_build;
	bool		quiet;
	ObjectAddress address;

	Assert(IsA(stmt, IndexStmt));
	Assert(!stmt->concurrent);

	/* The IndexStmt has already been through transformIndexStmt */
	Assert(stmt->transformed);

	/* The index should already be built if we are a QE */
	/* GPDB_12_MERGE_FIXME: it doesn't seem to work that way anymore. */
#if 0
	if (Gp_role == GP_ROLE_EXECUTE)
		return InvalidObjectAddress;
#endif

	/* suppress schema rights check when rebuilding existing index */
	check_rights = !is_rebuild;
	/* skip index build if phase 3 will do it or we're reusing an old one */
	skip_build = tab->rewrite > 0 || OidIsValid(stmt->oldNode);
	/* suppress notices when rebuilding existing index */
	quiet = is_rebuild;

	/*
	 * Also don't dispatch this if it's part of an ALTER TABLE. We will dispatch
	 * the whole ALTER TABLE command later.
	 */
	HOLD_DISPATCH();
	address = DefineIndex(RelationGetRelid(rel),
						  stmt,
						  InvalidOid,	/* no predefined OID */
						  InvalidOid,	/* no parent index */
						  InvalidOid,	/* no parent constraint */
						  true, /* is_alter_table */
						  check_rights,
						  false,	/* check_not_in_use - we did it already */
						  skip_build,
						  quiet);
	RESUME_DISPATCH();

	/*
	 * If TryReuseIndex() stashed a relfilenode for us, we used it for the new
	 * index instead of building from scratch.  Restore associated fields.
	 * This may store InvalidSubTransactionId in both fields, in which case
	 * relcache.c will assume it can rebuild the relcache entry.  Hence, do
	 * this after the CCI that made catalog rows visible to any rebuild.  The
	 * DROP of the old edition of this index will have scheduled the storage
	 * for deletion at commit, so cancel that pending deletion.
	 */
	if (OidIsValid(stmt->oldNode))
	{
		Relation	irel = index_open(address.objectId, NoLock);

		irel->rd_createSubid = stmt->oldCreateSubid;
		irel->rd_firstRelfilenodeSubid = stmt->oldFirstRelfilenodeSubid;
		RelationPreserveStorage(irel->rd_node, true);
		index_close(irel, NoLock);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ADD INDEX");
	return address;
}

/*
 * ALTER TABLE ADD STATISTICS
 *
 * This is no such command in the grammar, but we use this internally to add
 * AT_ReAddStatistics subcommands to rebuild extended statistics after a table
 * column type change.
 */
static ObjectAddress
ATExecAddStatistics(AlteredTableInfo *tab, Relation rel,
					CreateStatsStmt *stmt, bool is_rebuild, LOCKMODE lockmode)
{
	ObjectAddress address;

	Assert(IsA(stmt, CreateStatsStmt));

	/* The CreateStatsStmt has already been through transformStatsStmt */
	Assert(stmt->transformed);

	HOLD_DISPATCH();
	address = CreateStatistics(stmt);
	RESUME_DISPATCH();

	return address;
}

/*
 * ALTER TABLE ADD CONSTRAINT USING INDEX
 *
 * Returns the address of the new constraint.
 */
static ObjectAddress
ATExecAddIndexConstraint(AlteredTableInfo *tab, Relation rel,
						 IndexStmt *stmt, LOCKMODE lockmode)
{
	Oid			index_oid = stmt->indexOid;
	Relation	indexRel;
	char	   *indexName;
	IndexInfo  *indexInfo;
	char	   *constraintName;
	char		constraintType;
	ObjectAddress address;
	bits16		flags;

	Assert(IsA(stmt, IndexStmt));
	Assert(OidIsValid(index_oid));
	Assert(stmt->isconstraint);

	/*
	 * Doing this on partitioned tables is not a simple feature to implement,
	 * so let's punt for now.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables")));

	indexRel = index_open(index_oid, AccessShareLock);

	indexName = pstrdup(RelationGetRelationName(indexRel));

	indexInfo = BuildIndexInfo(indexRel);

	/* this should have been checked at parse time */
	if (!indexInfo->ii_Unique)
		elog(ERROR, "index \"%s\" is not unique", indexName);

	/*
	 * Determine name to assign to constraint.  We require a constraint to
	 * have the same name as the underlying index; therefore, use the index's
	 * existing name as the default constraint name, and if the user
	 * explicitly gives some other name for the constraint, rename the index
	 * to match.
	 */
	constraintName = stmt->idxname;
	if (constraintName == NULL)
		constraintName = indexName;
	else if (strcmp(constraintName, indexName) != 0)
	{
		if (Gp_role != GP_ROLE_EXECUTE)
			ereport(NOTICE,
				(errmsg("ALTER TABLE / ADD CONSTRAINT USING INDEX will rename index \"%s\" to \"%s\"",
						indexName, constraintName)));
		RenameRelationInternal(index_oid, constraintName, false, true);
	}

	/* Extra checks needed if making primary key */
	if (stmt->primary)
		index_check_primary_key(rel, indexInfo, true, stmt);

	/* Note we currently don't support EXCLUSION constraints here */
	if (stmt->primary)
		constraintType = CONSTRAINT_PRIMARY;
	else
		constraintType = CONSTRAINT_UNIQUE;

	/* Create the catalog entries for the constraint */
	flags = INDEX_CONSTR_CREATE_UPDATE_INDEX |
		INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS |
		(stmt->initdeferred ? INDEX_CONSTR_CREATE_INIT_DEFERRED : 0) |
		(stmt->deferrable ? INDEX_CONSTR_CREATE_DEFERRABLE : 0) |
		(stmt->primary ? INDEX_CONSTR_CREATE_MARK_AS_PRIMARY : 0);

	address = index_constraint_create(rel,
									  index_oid,
									  InvalidOid,
									  indexInfo,
									  constraintName,
									  constraintType,
									  flags,
									  allowSystemTableMods,
									  false);	/* is_internal */

	index_close(indexRel, NoLock);

	return address;
}

/*
 * ALTER TABLE ADD CONSTRAINT
 *
 * Return value is the address of the new constraint; if no constraint was
 * added, InvalidObjectAddress is returned.
 */
static ObjectAddress
ATExecAddConstraint(List **wqueue, AlteredTableInfo *tab, Relation rel,
					Constraint *newConstraint, bool recurse, bool is_readd,
					LOCKMODE lockmode)
{
	ObjectAddress address = InvalidObjectAddress;

	Assert(IsA(newConstraint, Constraint));

	/*
	 * Currently, we only expect to see CONSTR_CHECK and CONSTR_FOREIGN nodes
	 * arriving here (see the preprocessing done in parse_utilcmd.c).  Use a
	 * switch anyway to make it easier to add more code later.
	 */
	switch (newConstraint->contype)
	{
		case CONSTR_CHECK:
			address =
				ATAddCheckConstraint(wqueue, tab, rel,
									 newConstraint, recurse, false, is_readd,
									 lockmode);
			break;

		case CONSTR_FOREIGN:

			/*
			 * Assign or validate constraint name
			 */
			if (newConstraint->conname)
			{
				if (ConstraintNameIsUsed(CONSTRAINT_RELATION,
										 RelationGetRelid(rel),
										 newConstraint->conname))
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("constraint \"%s\" for relation \"%s\" already exists",
									newConstraint->conname,
									RelationGetRelationName(rel))));
			}
			else
				newConstraint->conname =
					ChooseConstraintName(RelationGetRelationName(rel),
										 ChooseForeignKeyConstraintNameAddition(newConstraint->fk_attrs),
										 "fkey",
										 RelationGetNamespace(rel),
										 NIL);

			address = ATAddForeignKeyConstraint(wqueue, tab, rel,
												newConstraint,
												recurse, false,
												lockmode);
			break;

		default:
			elog(ERROR, "unrecognized constraint type: %d",
				 (int) newConstraint->contype);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ADD CONSTRAINT"
				);

	return address;
}

/*
 * Generate the column-name portion of the constraint name for a new foreign
 * key given the list of column names that reference the referenced
 * table.  This will be passed to ChooseConstraintName along with the parent
 * table name and the "fkey" suffix.
 *
 * We know that less than NAMEDATALEN characters will actually be used, so we
 * can truncate the result once we've generated that many.
 *
 * XXX see also ChooseExtendedStatisticNameAddition and
 * ChooseIndexNameAddition.
 */
static char *
ChooseForeignKeyConstraintNameAddition(List *colnames)
{
	char		buf[NAMEDATALEN * 2];
	int			buflen = 0;
	ListCell   *lc;

	buf[0] = '\0';
	foreach(lc, colnames)
	{
		const char *name = strVal(lfirst(lc));

		if (buflen > 0)
			buf[buflen++] = '_';	/* insert _ between names */

		/*
		 * At this point we have buflen <= NAMEDATALEN.  name should be less
		 * than NAMEDATALEN already, but use strlcpy for paranoia.
		 */
		strlcpy(buf + buflen, name, NAMEDATALEN);
		buflen += strlen(buf + buflen);
		if (buflen >= NAMEDATALEN)
			break;
	}
	return pstrdup(buf);
}

/*
 * Add a check constraint to a single table and its children.  Returns the
 * address of the constraint added to the parent relation, if one gets added,
 * or InvalidObjectAddress otherwise.
 *
 * Subroutine for ATExecAddConstraint.
 *
 * We must recurse to child tables during execution, rather than using
 * ALTER TABLE's normal prep-time recursion.  The reason is that all the
 * constraints *must* be given the same name, else they won't be seen as
 * related later.  If the user didn't explicitly specify a name, then
 * AddRelationNewConstraints would normally assign different names to the
 * child constraints.  To fix that, we must capture the name assigned at
 * the parent table and pass that down.
 */
static ObjectAddress
ATAddCheckConstraint(List **wqueue, AlteredTableInfo *tab, Relation rel,
					 Constraint *constr, bool recurse, bool recursing,
					 bool is_readd, LOCKMODE lockmode)
{
	List	   *newcons;
	ListCell   *lcon;
	List	   *children;
	ListCell   *child;
	ObjectAddress address = InvalidObjectAddress;

	/* At top level, permission check was done in ATPrepCmd, else do it */
	if (recursing)
		ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);

	/*
	 * Call AddRelationNewConstraints to do the work, making sure it works on
	 * a copy of the Constraint so transformExpr can't modify the original. It
	 * returns a list of cooked constraints.
	 *
	 * If the constraint ends up getting merged with a pre-existing one, it's
	 * omitted from the returned list, which is what we want: we do not need
	 * to do any validation work.  That can only happen at child tables,
	 * though, since we disallow merging at the top level.
	 */
	newcons = AddRelationNewConstraints(rel, NIL,
										list_make1(copyObject(constr)),
										recursing | is_readd,	/* allow_merge */
										!recursing, /* is_local */
										is_readd,	/* is_internal */
										NULL);	/* queryString not available
												 * here */

	/* we don't expect more than one constraint here */
	Assert(list_length(newcons) <= 1);

	/* Add each to-be-validated constraint to Phase 3's queue */
	foreach(lcon, newcons)
	{
		CookedConstraint *ccon = (CookedConstraint *) lfirst(lcon);

		if (!ccon->skip_validation && Gp_role != GP_ROLE_EXECUTE)
		{
			NewConstraint *newcon;

			newcon = (NewConstraint *) palloc0(sizeof(NewConstraint));
			newcon->name = ccon->name;
			newcon->contype = ccon->contype;
			newcon->qual = ccon->expr;

			tab->constraints = lappend(tab->constraints, newcon);
		}

		/* Save the actually assigned name if it was defaulted */
		if (constr->conname == NULL)
			constr->conname = ccon->name;

		ObjectAddressSet(address, ConstraintRelationId, ccon->conoid);
	}

	/* At this point we must have a locked-down name to use */
	Assert(constr->conname != NULL);

	/* Advance command counter in case same table is visited multiple times */
	CommandCounterIncrement();

	/*
	 * If the constraint got merged with an existing constraint, we're done.
	 * We mustn't recurse to child tables in this case, because they've
	 * already got the constraint, and visiting them again would lead to an
	 * incorrect value for coninhcount.
	 */
	if (newcons == NIL)
		return address;

	/*
	 * If adding a NO INHERIT constraint, no need to find our children.
	 */
	if (constr->is_no_inherit)
		return address;

	/*
	 * Propagate to children as appropriate.  Unlike most other ALTER
	 * routines, we have to do this one level of recursion at a time; we can't
	 * use find_all_inheritors to do it in one pass.
	 */
	children =
		find_inheritance_children(RelationGetRelid(rel), lockmode);

	/*
	 * If we are told not to recurse, there had better not be any child tables;
	 * else the addition would put them out of step.
	 * Check if ONLY was specified with ALTER TABLE.  If so, allow the
	 * constraint creation only if there are no children currently.  Error out
	 * otherwise.
	 */
	if ((Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY) && children && !recurse)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("constraint must be added to child tables too")));

	foreach(child, children)
	{
		Oid			childrelid = lfirst_oid(child);
		Relation	childrel;
		AlteredTableInfo *childtab;

		/* find_inheritance_children already got lock */
		childrel = table_open(childrelid, NoLock);
		CheckTableNotInUse(childrel, "ALTER TABLE");

		/* Find or create work queue entry for this table */
		childtab = ATGetQueueEntry(wqueue, childrel);

		/* Recurse to child */
		ATAddCheckConstraint(wqueue, childtab, childrel,
							 constr, recurse, true, is_readd, lockmode);

		table_close(childrel, NoLock);
	}

	return address;
}

/*
 * Add a foreign-key constraint to a single table; return the new constraint's
 * address.
 *
 * Subroutine for ATExecAddConstraint.  Must already hold exclusive
 * lock on the rel, and have done appropriate validity checks for it.
 * We do permissions checks here, however.
 *
 * When the referenced or referencing tables (or both) are partitioned,
 * multiple pg_constraint rows are required -- one for each partitioned table
 * and each partition on each side (fortunately, not one for every combination
 * thereof).  We also need action triggers on each leaf partition on the
 * referenced side, and check triggers on each leaf partition on the
 * referencing side.
 */
static ObjectAddress
ATAddForeignKeyConstraint(List **wqueue, AlteredTableInfo *tab, Relation rel,
						  Constraint *fkconstraint,
						  bool recurse, bool recursing, LOCKMODE lockmode)
{
	Relation	pkrel;
	int16		pkattnum[INDEX_MAX_KEYS];
	int16		fkattnum[INDEX_MAX_KEYS];
	Oid			pktypoid[INDEX_MAX_KEYS];
	Oid			fktypoid[INDEX_MAX_KEYS];
	Oid			opclasses[INDEX_MAX_KEYS];
	Oid			pfeqoperators[INDEX_MAX_KEYS];
	Oid			ppeqoperators[INDEX_MAX_KEYS];
	Oid			ffeqoperators[INDEX_MAX_KEYS];
	int			i;
	int			numfks,
				numpks;
	Oid			indexOid;
	bool		old_check_ok;
	ObjectAddress address;
	ListCell   *old_pfeqop_item = list_head(fkconstraint->old_conpfeqop);

	/*
	 * Grab ShareRowExclusiveLock on the pk table, so that someone doesn't
	 * delete rows out from under us.
	 */
	if (OidIsValid(fkconstraint->old_pktable_oid))
		pkrel = table_open(fkconstraint->old_pktable_oid, ShareRowExclusiveLock);
	else
		pkrel = table_openrv(fkconstraint->pktable, ShareRowExclusiveLock);

	/*
	 * GPDB: Schema-qualify the primary key table for the statement dispatch
	 * that will happen later. The QE nodes are not guaranteed to have the
	 * same search_path as the QD (e.g. CREATE SCHEMA command with schema
	 * elements creating relations).
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
		fkconstraint->pktable->schemaname =
			get_namespace_name(pkrel->rd_rel->relnamespace);

	/*
	 * Validity checks (permission checks wait till we have the column
	 * numbers)
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		if (!recurse)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot use ONLY for foreign key on partitioned table \"%s\" referencing relation \"%s\"",
							RelationGetRelationName(rel),
							RelationGetRelationName(pkrel))));
		if (fkconstraint->skip_validation && !fkconstraint->initially_valid)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot add NOT VALID foreign key on partitioned table \"%s\" referencing relation \"%s\"",
							RelationGetRelationName(rel),
							RelationGetRelationName(pkrel)),
					 errdetail("This feature is not yet supported on partitioned tables.")));
	}

	if (pkrel->rd_rel->relkind != RELKIND_RELATION &&
		pkrel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("referenced relation \"%s\" is not a table",
						RelationGetRelationName(pkrel))));

	if (!allowSystemTableMods && IsSystemRelation(pkrel))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						RelationGetRelationName(pkrel))));

	/*
	 * References from permanent or unlogged tables to temp tables, and from
	 * permanent tables to unlogged tables, are disallowed because the
	 * referenced data can vanish out from under us.  References from temp
	 * tables to any other table type are also disallowed, because other
	 * backends might need to run the RI triggers on the perm table, but they
	 * can't reliably see tuples in the local buffers of other backends.
	 */
	switch (rel->rd_rel->relpersistence)
	{
		case RELPERSISTENCE_PERMANENT:
			if (!RelationIsPermanent(pkrel))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("constraints on permanent tables may reference only permanent tables")));
			break;
		case RELPERSISTENCE_UNLOGGED:
			if (!RelationIsPermanent(pkrel)
				&& pkrel->rd_rel->relpersistence != RELPERSISTENCE_UNLOGGED)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("constraints on unlogged tables may reference only permanent or unlogged tables")));
			break;
		case RELPERSISTENCE_TEMP:
			if (pkrel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("constraints on temporary tables may reference only temporary tables")));
			if (!pkrel->rd_islocaltemp || !rel->rd_islocaltemp)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("constraints on temporary tables must involve temporary tables of this session")));
			break;
	}

	/*
	 * Look up the referencing attributes to make sure they exist, and record
	 * their attnums and type OIDs.
	 */
	MemSet(pkattnum, 0, sizeof(pkattnum));
	MemSet(fkattnum, 0, sizeof(fkattnum));
	MemSet(pktypoid, 0, sizeof(pktypoid));
	MemSet(fktypoid, 0, sizeof(fktypoid));
	MemSet(opclasses, 0, sizeof(opclasses));
	MemSet(pfeqoperators, 0, sizeof(pfeqoperators));
	MemSet(ppeqoperators, 0, sizeof(ppeqoperators));
	MemSet(ffeqoperators, 0, sizeof(ffeqoperators));

	numfks = transformColumnNameList(RelationGetRelid(rel),
									 fkconstraint->fk_attrs,
									 fkattnum, fktypoid);

	/*
	 * If the attribute list for the referenced table was omitted, lookup the
	 * definition of the primary key and use it.  Otherwise, validate the
	 * supplied attribute list.  In either case, discover the index OID and
	 * index opclasses, and the attnums and type OIDs of the attributes.
	 */
	if (fkconstraint->pk_attrs == NIL)
	{
		numpks = transformFkeyGetPrimaryKey(pkrel, &indexOid,
											&fkconstraint->pk_attrs,
											pkattnum, pktypoid,
											opclasses);
	}
	else
	{
		numpks = transformColumnNameList(RelationGetRelid(pkrel),
										 fkconstraint->pk_attrs,
										 pkattnum, pktypoid);
		/* Look for an index matching the column list */
		indexOid = transformFkeyCheckAttrs(pkrel, numpks, pkattnum,
										   opclasses);
	}

	/*
	 * Now we can check permissions.
	 */
	checkFkeyPermissions(pkrel, pkattnum, numpks);

	/*
	 * Check some things for generated columns.
	 */
	for (i = 0; i < numfks; i++)
	{
		char		attgenerated = TupleDescAttr(RelationGetDescr(rel), fkattnum[i] - 1)->attgenerated;

		if (attgenerated)
		{
			/*
			 * Check restrictions on UPDATE/DELETE actions, per SQL standard
			 */
			if (fkconstraint->fk_upd_action == FKCONSTR_ACTION_SETNULL ||
				fkconstraint->fk_upd_action == FKCONSTR_ACTION_SETDEFAULT ||
				fkconstraint->fk_upd_action == FKCONSTR_ACTION_CASCADE)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid %s action for foreign key constraint containing generated column",
								"ON UPDATE")));
			if (fkconstraint->fk_del_action == FKCONSTR_ACTION_SETNULL ||
				fkconstraint->fk_del_action == FKCONSTR_ACTION_SETDEFAULT)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid %s action for foreign key constraint containing generated column",
								"ON DELETE")));
		}
	}

	/*
	 * Look up the equality operators to use in the constraint.
	 *
	 * Note that we have to be careful about the difference between the actual
	 * PK column type and the opclass' declared input type, which might be
	 * only binary-compatible with it.  The declared opcintype is the right
	 * thing to probe pg_amop with.
	 */
	if (numfks != numpks)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("number of referencing and referenced columns for foreign key disagree")));

	/*
	 * On the strength of a previous constraint, we might avoid scanning
	 * tables to validate this one.  See below.
	 */
	old_check_ok = (fkconstraint->old_conpfeqop != NIL);
	Assert(!old_check_ok || numfks == list_length(fkconstraint->old_conpfeqop));

	for (i = 0; i < numpks; i++)
	{
		Oid			pktype = pktypoid[i];
		Oid			fktype = fktypoid[i];
		Oid			fktyped;
		HeapTuple	cla_ht;
		Form_pg_opclass cla_tup;
		Oid			amid;
		Oid			opfamily;
		Oid			opcintype;
		Oid			pfeqop;
		Oid			ppeqop;
		Oid			ffeqop;
		int16		eqstrategy;
		Oid			pfeqop_right;

		/* We need several fields out of the pg_opclass entry */
		cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclasses[i]));
		if (!HeapTupleIsValid(cla_ht))
			elog(ERROR, "cache lookup failed for opclass %u", opclasses[i]);
		cla_tup = (Form_pg_opclass) GETSTRUCT(cla_ht);
		amid = cla_tup->opcmethod;
		opfamily = cla_tup->opcfamily;
		opcintype = cla_tup->opcintype;
		ReleaseSysCache(cla_ht);

		/*
		 * Check it's a btree; currently this can never fail since no other
		 * index AMs support unique indexes.  If we ever did have other types
		 * of unique indexes, we'd need a way to determine which operator
		 * strategy number is equality.  (Is it reasonable to insist that
		 * every such index AM use btree's number for equality?)
		 */
		if (!IsIndexAccessMethod(amid, BTREE_AM_OID))
			elog(ERROR, "only b-tree indexes are supported for foreign keys");
		eqstrategy = BTEqualStrategyNumber;

		/*
		 * There had better be a primary equality operator for the index.
		 * We'll use it for PK = PK comparisons.
		 */
		ppeqop = get_opfamily_member(opfamily, opcintype, opcintype,
									 eqstrategy);

		if (!OidIsValid(ppeqop))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 eqstrategy, opcintype, opcintype, opfamily);

		/*
		 * Are there equality operators that take exactly the FK type? Assume
		 * we should look through any domain here.
		 */
		fktyped = getBaseType(fktype);

		pfeqop = get_opfamily_member(opfamily, opcintype, fktyped,
									 eqstrategy);
		if (OidIsValid(pfeqop))
		{
			pfeqop_right = fktyped;
			ffeqop = get_opfamily_member(opfamily, fktyped, fktyped,
										 eqstrategy);
		}
		else
		{
			/* keep compiler quiet */
			pfeqop_right = InvalidOid;
			ffeqop = InvalidOid;
		}

		if (!(OidIsValid(pfeqop) && OidIsValid(ffeqop)))
		{
			/*
			 * Otherwise, look for an implicit cast from the FK type to the
			 * opcintype, and if found, use the primary equality operator.
			 * This is a bit tricky because opcintype might be a polymorphic
			 * type such as ANYARRAY or ANYENUM; so what we have to test is
			 * whether the two actual column types can be concurrently cast to
			 * that type.  (Otherwise, we'd fail to reject combinations such
			 * as int[] and point[].)
			 */
			Oid			input_typeids[2];
			Oid			target_typeids[2];

			input_typeids[0] = pktype;
			input_typeids[1] = fktype;
			target_typeids[0] = opcintype;
			target_typeids[1] = opcintype;
			if (can_coerce_type(2, input_typeids, target_typeids,
								COERCION_IMPLICIT))
			{
				pfeqop = ffeqop = ppeqop;
				pfeqop_right = opcintype;
			}
		}

		if (!(OidIsValid(pfeqop) && OidIsValid(ffeqop)))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("foreign key constraint \"%s\" cannot be implemented",
							fkconstraint->conname),
					 errdetail("Key columns \"%s\" and \"%s\" "
							   "are of incompatible types: %s and %s.",
							   strVal(list_nth(fkconstraint->fk_attrs, i)),
							   strVal(list_nth(fkconstraint->pk_attrs, i)),
							   format_type_be(fktype),
							   format_type_be(pktype))));

		if (old_check_ok)
		{
			/*
			 * When a pfeqop changes, revalidate the constraint.  We could
			 * permit intra-opfamily changes, but that adds subtle complexity
			 * without any concrete benefit for core types.  We need not
			 * assess ppeqop or ffeqop, which RI_Initial_Check() does not use.
			 */
			old_check_ok = (pfeqop == lfirst_oid(old_pfeqop_item));
			old_pfeqop_item = lnext(fkconstraint->old_conpfeqop,
									old_pfeqop_item);
		}
		if (old_check_ok)
		{
			Oid			old_fktype;
			Oid			new_fktype;
			CoercionPathType old_pathtype;
			CoercionPathType new_pathtype;
			Oid			old_castfunc;
			Oid			new_castfunc;
			Form_pg_attribute attr = TupleDescAttr(tab->oldDesc,
												   fkattnum[i] - 1);

			/*
			 * Identify coercion pathways from each of the old and new FK-side
			 * column types to the right (foreign) operand type of the pfeqop.
			 * We may assume that pg_constraint.conkey is not changing.
			 */
			old_fktype = attr->atttypid;
			new_fktype = fktype;
			old_pathtype = findFkeyCast(pfeqop_right, old_fktype,
										&old_castfunc);
			new_pathtype = findFkeyCast(pfeqop_right, new_fktype,
										&new_castfunc);

			/*
			 * Upon a change to the cast from the FK column to its pfeqop
			 * operand, revalidate the constraint.  For this evaluation, a
			 * binary coercion cast is equivalent to no cast at all.  While
			 * type implementors should design implicit casts with an eye
			 * toward consistency of operations like equality, we cannot
			 * assume here that they have done so.
			 *
			 * A function with a polymorphic argument could change behavior
			 * arbitrarily in response to get_fn_expr_argtype().  Therefore,
			 * when the cast destination is polymorphic, we only avoid
			 * revalidation if the input type has not changed at all.  Given
			 * just the core data types and operator classes, this requirement
			 * prevents no would-be optimizations.
			 *
			 * If the cast converts from a base type to a domain thereon, then
			 * that domain type must be the opcintype of the unique index.
			 * Necessarily, the primary key column must then be of the domain
			 * type.  Since the constraint was previously valid, all values on
			 * the foreign side necessarily exist on the primary side and in
			 * turn conform to the domain.  Consequently, we need not treat
			 * domains specially here.
			 *
			 * Since we require that all collations share the same notion of
			 * equality (which they do, because texteq reduces to bitwise
			 * equality), we don't compare collation here.
			 *
			 * We need not directly consider the PK type.  It's necessarily
			 * binary coercible to the opcintype of the unique index column,
			 * and ri_triggers.c will only deal with PK datums in terms of
			 * that opcintype.  Changing the opcintype also changes pfeqop.
			 */
			old_check_ok = (new_pathtype == old_pathtype &&
							new_castfunc == old_castfunc &&
							(!IsPolymorphicType(pfeqop_right) ||
							 new_fktype == old_fktype));
		}

		pfeqoperators[i] = pfeqop;
		ppeqoperators[i] = ppeqop;
		ffeqoperators[i] = ffeqop;
	}

	/*
	 * Create all the constraint and trigger objects, recursing to partitions
	 * as necessary.  First handle the referenced side.
	 */
	address = addFkRecurseReferenced(wqueue, fkconstraint, rel, pkrel,
									 indexOid,
									 InvalidOid,	/* no parent constraint */
									 numfks,
									 pkattnum,
									 fkattnum,
									 pfeqoperators,
									 ppeqoperators,
									 ffeqoperators,
									 old_check_ok);

	/* Now handle the referencing side. */
	addFkRecurseReferencing(wqueue, fkconstraint, rel, pkrel,
							indexOid,
							address.objectId,
							numfks,
							pkattnum,
							fkattnum,
							pfeqoperators,
							ppeqoperators,
							ffeqoperators,
							old_check_ok,
							lockmode);

	/*
	 * Done.  Close pk table, but keep lock until we've committed.
	 */
	table_close(pkrel, NoLock);

	return address;
}

/*
 * addFkRecurseReferenced
 *		subroutine for ATAddForeignKeyConstraint; recurses on the referenced
 *		side of the constraint
 *
 * Create pg_constraint rows for the referenced side of the constraint,
 * referencing the parent of the referencing side; also create action triggers
 * on leaf partitions.  If the table is partitioned, recurse to handle each
 * partition.
 *
 * wqueue is the ALTER TABLE work queue; can be NULL when not running as part
 * of an ALTER TABLE sequence.
 * fkconstraint is the constraint being added.
 * rel is the root referencing relation.
 * pkrel is the referenced relation; might be a partition, if recursing.
 * indexOid is the OID of the index (on pkrel) implementing this constraint.
 * parentConstr is the OID of a parent constraint; InvalidOid if this is a
 * top-level constraint.
 * numfks is the number of columns in the foreign key
 * pkattnum is the attnum array of referenced attributes.
 * fkattnum is the attnum array of referencing attributes.
 * pf/pp/ffeqoperators are OID array of operators between columns.
 * old_check_ok signals that this constraint replaces an existing one that
 * was already validated (thus this one doesn't need validation).
 */
static ObjectAddress
addFkRecurseReferenced(List **wqueue, Constraint *fkconstraint, Relation rel,
					   Relation pkrel, Oid indexOid, Oid parentConstr,
					   int numfks,
					   int16 *pkattnum, int16 *fkattnum, Oid *pfeqoperators,
					   Oid *ppeqoperators, Oid *ffeqoperators, bool old_check_ok)
{
	ObjectAddress address;
	Oid			constrOid;
	char	   *conname;
	bool		conislocal;
	int			coninhcount;
	bool		connoinherit;

	/*
	 * Verify relkind for each referenced partition.  At the top level, this
	 * is redundant with a previous check, but we need it when recursing.
	 */
	if (pkrel->rd_rel->relkind != RELKIND_RELATION &&
		pkrel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("referenced relation \"%s\" is not a table",
						RelationGetRelationName(pkrel))));

	/*
	 * Caller supplies us with a constraint name; however, it may be used in
	 * this partition, so come up with a different one in that case.
	 */
	if (ConstraintNameIsUsed(CONSTRAINT_RELATION,
							 RelationGetRelid(rel),
							 fkconstraint->conname))
		conname = ChooseConstraintName(RelationGetRelationName(rel),
									   ChooseForeignKeyConstraintNameAddition(fkconstraint->fk_attrs),
									   "fkey",
									   RelationGetNamespace(rel), NIL);
	else
		conname = fkconstraint->conname;

	if (OidIsValid(parentConstr))
	{
		conislocal = false;
		coninhcount = 1;
		connoinherit = false;
	}
	else
	{
		conislocal = true;
		coninhcount = 0;

		/*
		 * always inherit for partitioned tables, never for legacy inheritance
		 */
		connoinherit = rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE;
	}

	/*
	 * Record the FK constraint in pg_constraint.
	 */
	constrOid = CreateConstraintEntry(conname,
									  RelationGetNamespace(rel),
									  CONSTRAINT_FOREIGN,
									  fkconstraint->deferrable,
									  fkconstraint->initdeferred,
									  fkconstraint->initially_valid,
									  parentConstr,
									  RelationGetRelid(rel),
									  fkattnum,
									  numfks,
									  numfks,
									  InvalidOid,	/* not a domain constraint */
									  indexOid,
									  RelationGetRelid(pkrel),
									  pkattnum,
									  pfeqoperators,
									  ppeqoperators,
									  ffeqoperators,
									  numfks,
									  fkconstraint->fk_upd_action,
									  fkconstraint->fk_del_action,
									  fkconstraint->fk_matchtype,
									  NULL, /* no exclusion constraint */
									  NULL, /* no check constraint */
									  NULL,
									  conislocal,	/* islocal */
									  coninhcount,	/* inhcount */
									  connoinherit, /* conNoInherit */
									  false);	/* is_internal */

	ObjectAddressSet(address, ConstraintRelationId, constrOid);

	/*
	 * Mark the child constraint as part of the parent constraint; it must not
	 * be dropped on its own.  (This constraint is deleted when the partition
	 * is detached, but a special check needs to occur that the partition
	 * contains no referenced values.)
	 */
	if (OidIsValid(parentConstr))
	{
		ObjectAddress referenced;

		ObjectAddressSet(referenced, ConstraintRelationId, parentConstr);
		recordDependencyOn(&address, &referenced, DEPENDENCY_INTERNAL);
	}

	/* make new constraint visible, in case we add more */
	CommandCounterIncrement();

	/*
	 * If the referenced table is a plain relation, create the action triggers
	 * that enforce the constraint.
	 */
	if (pkrel->rd_rel->relkind == RELKIND_RELATION)
	{
		createForeignKeyActionTriggers(rel, RelationGetRelid(pkrel),
									   fkconstraint,
									   constrOid, indexOid);
	}

	/*
	 * If the referenced table is partitioned, recurse on ourselves to handle
	 * each partition.  We need one pg_constraint row created for each
	 * partition in addition to the pg_constraint row for the parent table.
	 */
	if (pkrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		PartitionDesc pd = RelationGetPartitionDesc(pkrel, true);

		for (int i = 0; i < pd->nparts; i++)
		{
			Relation	partRel;
			AttrMap    *map;
			AttrNumber *mapped_pkattnum;
			Oid			partIndexId;

			partRel = table_open(pd->oids[i], ShareRowExclusiveLock);

			/*
			 * Map the attribute numbers in the referenced side of the FK
			 * definition to match the partition's column layout.
			 */
			map = build_attrmap_by_name_if_req(RelationGetDescr(partRel),
											   RelationGetDescr(pkrel));
			if (map)
			{
				mapped_pkattnum = palloc(sizeof(AttrNumber) * numfks);
				for (int j = 0; j < numfks; j++)
					mapped_pkattnum[j] = map->attnums[pkattnum[j] - 1];
			}
			else
				mapped_pkattnum = pkattnum;

			/* do the deed */
			partIndexId = index_get_partition(partRel, indexOid);
			if (!OidIsValid(partIndexId))
				elog(ERROR, "index for %u not found in partition %s",
					 indexOid, RelationGetRelationName(partRel));
			addFkRecurseReferenced(wqueue, fkconstraint, rel, partRel,
								   partIndexId, constrOid, numfks,
								   mapped_pkattnum, fkattnum,
								   pfeqoperators, ppeqoperators, ffeqoperators,
								   old_check_ok);

			/* Done -- clean up (but keep the lock) */
			table_close(partRel, NoLock);
			if (map)
			{
				pfree(mapped_pkattnum);
				free_attrmap(map);
			}
		}
	}

	return address;
}

/*
 * addFkRecurseReferencing
 *		subroutine for ATAddForeignKeyConstraint and CloneFkReferencing
 *
 * If the referencing relation is a plain relation, create the necessary check
 * triggers that implement the constraint, and set up for Phase 3 constraint
 * verification.  If the referencing relation is a partitioned table, then
 * we create a pg_constraint row for it and recurse on this routine for each
 * partition.
 *
 * We assume that the referenced relation is locked against concurrent
 * deletions.  If it's a partitioned relation, every partition must be so
 * locked.
 *
 * wqueue is the ALTER TABLE work queue; can be NULL when not running as part
 * of an ALTER TABLE sequence.
 * fkconstraint is the constraint being added.
 * rel is the referencing relation; might be a partition, if recursing.
 * pkrel is the root referenced relation.
 * indexOid is the OID of the index (on pkrel) implementing this constraint.
 * parentConstr is the OID of the parent constraint (there is always one).
 * numfks is the number of columns in the foreign key
 * pkattnum is the attnum array of referenced attributes.
 * fkattnum is the attnum array of referencing attributes.
 * pf/pp/ffeqoperators are OID array of operators between columns.
 * old_check_ok signals that this constraint replaces an existing one that
 *		was already validated (thus this one doesn't need validation).
 * lockmode is the lockmode to acquire on partitions when recursing.
 */
static void
addFkRecurseReferencing(List **wqueue, Constraint *fkconstraint, Relation rel,
						Relation pkrel, Oid indexOid, Oid parentConstr,
						int numfks, int16 *pkattnum, int16 *fkattnum,
						Oid *pfeqoperators, Oid *ppeqoperators, Oid *ffeqoperators,
						bool old_check_ok, LOCKMODE lockmode)
{
	AssertArg(OidIsValid(parentConstr));

	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("foreign key constraints are not supported on foreign tables")));

	/*
	 * If the referencing relation is a plain table, add the check triggers to
	 * it and, if necessary, schedule it to be checked in Phase 3.
	 *
	 * If the relation is partitioned, drill down to do it to its partitions.
	 */
	if (rel->rd_rel->relkind == RELKIND_RELATION)
	{
		createForeignKeyCheckTriggers(RelationGetRelid(rel),
									  RelationGetRelid(pkrel),
									  fkconstraint,
									  parentConstr,
									  indexOid);

		/*
		 * Tell Phase 3 to check that the constraint is satisfied by existing
		 * rows. We can skip this during table creation, when requested
		 * explicitly by specifying NOT VALID in an ADD FOREIGN KEY command,
		 * and when we're recreating a constraint following a SET DATA TYPE
		 * operation that did not impugn its validity.
		 */
		if (wqueue && !old_check_ok && !fkconstraint->skip_validation)
		{
			NewConstraint *newcon;
			AlteredTableInfo *tab;

			tab = ATGetQueueEntry(wqueue, rel);

			newcon = (NewConstraint *) palloc0(sizeof(NewConstraint));
			newcon->name = get_constraint_name(parentConstr);
			newcon->contype = CONSTR_FOREIGN;
			newcon->refrelid = RelationGetRelid(pkrel);
			newcon->refindid = indexOid;
			newcon->conid = parentConstr;
			newcon->qual = (Node *) fkconstraint;

			tab->constraints = lappend(tab->constraints, newcon);
		}
	}
	else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		PartitionDesc pd = RelationGetPartitionDesc(rel, true);

		/*
		 * Recurse to take appropriate action on each partition; either we
		 * find an existing constraint to reparent to ours, or we create a new
		 * one.
		 */
		for (int i = 0; i < pd->nparts; i++)
		{
			Oid			partitionId = pd->oids[i];
			Relation	partition = table_open(partitionId, lockmode);
			List	   *partFKs;
			AttrMap    *attmap;
			AttrNumber	mapped_fkattnum[INDEX_MAX_KEYS];
			bool		attached;
			char	   *conname;
			Oid			constrOid;
			ObjectAddress address,
						referenced;
			ListCell   *cell;

			CheckTableNotInUse(partition, "ALTER TABLE");

			attmap = build_attrmap_by_name(RelationGetDescr(partition),
										   RelationGetDescr(rel));
			for (int j = 0; j < numfks; j++)
				mapped_fkattnum[j] = attmap->attnums[fkattnum[j] - 1];

			/* Check whether an existing constraint can be repurposed */
			partFKs = copyObject(RelationGetFKeyList(partition));
			attached = false;
			foreach(cell, partFKs)
			{
				ForeignKeyCacheInfo *fk;

				fk = lfirst_node(ForeignKeyCacheInfo, cell);
				if (tryAttachPartitionForeignKey(fk,
												 partitionId,
												 parentConstr,
												 numfks,
												 mapped_fkattnum,
												 pkattnum,
												 pfeqoperators))
				{
					attached = true;
					break;
				}
			}
			if (attached)
			{
				table_close(partition, NoLock);
				continue;
			}

			/*
			 * No luck finding a good constraint to reuse; create our own.
			 */
			if (ConstraintNameIsUsed(CONSTRAINT_RELATION,
									 RelationGetRelid(partition),
									 fkconstraint->conname))
				conname = ChooseConstraintName(RelationGetRelationName(partition),
											   ChooseForeignKeyConstraintNameAddition(fkconstraint->fk_attrs),
											   "fkey",
											   RelationGetNamespace(partition), NIL);
			else
				conname = fkconstraint->conname;
			constrOid =
				CreateConstraintEntry(conname,
									  RelationGetNamespace(partition),
									  CONSTRAINT_FOREIGN,
									  fkconstraint->deferrable,
									  fkconstraint->initdeferred,
									  fkconstraint->initially_valid,
									  parentConstr,
									  partitionId,
									  mapped_fkattnum,
									  numfks,
									  numfks,
									  InvalidOid,
									  indexOid,
									  RelationGetRelid(pkrel),
									  pkattnum,
									  pfeqoperators,
									  ppeqoperators,
									  ffeqoperators,
									  numfks,
									  fkconstraint->fk_upd_action,
									  fkconstraint->fk_del_action,
									  fkconstraint->fk_matchtype,
									  NULL,
									  NULL,
									  NULL,
									  false,
									  1,
									  false,
									  false);

			/*
			 * Give this constraint partition-type dependencies on the parent
			 * constraint as well as the table.
			 */
			ObjectAddressSet(address, ConstraintRelationId, constrOid);
			ObjectAddressSet(referenced, ConstraintRelationId, parentConstr);
			recordDependencyOn(&address, &referenced, DEPENDENCY_PARTITION_PRI);
			ObjectAddressSet(referenced, RelationRelationId, partitionId);
			recordDependencyOn(&address, &referenced, DEPENDENCY_PARTITION_SEC);

			/* Make all this visible before recursing */
			CommandCounterIncrement();

			/* call ourselves to finalize the creation and we're done */
			addFkRecurseReferencing(wqueue, fkconstraint, partition, pkrel,
									indexOid,
									constrOid,
									numfks,
									pkattnum,
									mapped_fkattnum,
									pfeqoperators,
									ppeqoperators,
									ffeqoperators,
									old_check_ok,
									lockmode);

			table_close(partition, NoLock);
		}
	}
}

/*
 * CloneForeignKeyConstraints
 *		Clone foreign keys from a partitioned table to a newly acquired
 *		partition.
 *
 * partitionRel is a partition of parentRel, so we can be certain that it has
 * the same columns with the same datatypes.  The columns may be in different
 * order, though.
 *
 * wqueue must be passed to set up phase 3 constraint checking, unless the
 * referencing-side partition is known to be empty (such as in CREATE TABLE /
 * PARTITION OF).
 */
static void
CloneForeignKeyConstraints(List **wqueue, Relation parentRel,
						   Relation partitionRel)
{
	/* This only works for declarative partitioning */
	Assert(parentRel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

	/*
	 * Clone constraints for which the parent is on the referenced side.
	 */
	CloneFkReferenced(parentRel, partitionRel);

	/*
	 * Now clone constraints where the parent is on the referencing side.
	 */
	CloneFkReferencing(wqueue, parentRel, partitionRel);
}

/*
 * CloneFkReferenced
 *		Subroutine for CloneForeignKeyConstraints
 *
 * Find all the FKs that have the parent relation on the referenced side;
 * clone those constraints to the given partition.  This is to be called
 * when the partition is being created or attached.
 *
 * This recurses to partitions, if the relation being attached is partitioned.
 * Recursion is done by calling addFkRecurseReferenced.
 */
static void
CloneFkReferenced(Relation parentRel, Relation partitionRel)
{
	Relation	pg_constraint;
	AttrMap    *attmap;
	ListCell   *cell;
	SysScanDesc scan;
	ScanKeyData key[2];
	HeapTuple	tuple;
	List	   *clone = NIL;

	/*
	 * Search for any constraints where this partition's parent is in the
	 * referenced side.  However, we must not clone any constraint whose
	 * parent constraint is also going to be cloned, to avoid duplicates.  So
	 * do it in two steps: first construct the list of constraints to clone,
	 * then go over that list cloning those whose parents are not in the list.
	 * (We must not rely on the parent being seen first, since the catalog
	 * scan could return children first.)
	 */
	pg_constraint = table_open(ConstraintRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_constraint_confrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(parentRel)));
	ScanKeyInit(&key[1],
				Anum_pg_constraint_contype, BTEqualStrategyNumber,
				F_CHAREQ, CharGetDatum(CONSTRAINT_FOREIGN));
	/* This is a seqscan, as we don't have a usable index ... */
	scan = systable_beginscan(pg_constraint, InvalidOid, true,
							  NULL, 2, key);
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_constraint constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

		clone = lappend_oid(clone, constrForm->oid);
	}
	systable_endscan(scan);
	table_close(pg_constraint, RowShareLock);

	attmap = build_attrmap_by_name(RelationGetDescr(partitionRel),
								   RelationGetDescr(parentRel));
	foreach(cell, clone)
	{
		Oid			constrOid = lfirst_oid(cell);
		Form_pg_constraint constrForm;
		Relation	fkRel;
		Oid			indexOid;
		Oid			partIndexId;
		int			numfks;
		AttrNumber	conkey[INDEX_MAX_KEYS];
		AttrNumber	mapped_confkey[INDEX_MAX_KEYS];
		AttrNumber	confkey[INDEX_MAX_KEYS];
		Oid			conpfeqop[INDEX_MAX_KEYS];
		Oid			conppeqop[INDEX_MAX_KEYS];
		Oid			conffeqop[INDEX_MAX_KEYS];
		Constraint *fkconstraint;

		tuple = SearchSysCache1(CONSTROID, constrOid);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for constraint %u", constrOid);
		constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

		/*
		 * As explained above: don't try to clone a constraint for which we're
		 * going to clone the parent.
		 */
		if (list_member_oid(clone, constrForm->conparentid))
		{
			ReleaseSysCache(tuple);
			continue;
		}

		/*
		 * Because we're only expanding the key space at the referenced side,
		 * we don't need to prevent any operation in the referencing table, so
		 * AccessShareLock suffices (assumes that dropping the constraint
		 * acquires AEL).
		 */
		fkRel = table_open(constrForm->conrelid, AccessShareLock);

		indexOid = constrForm->conindid;
		DeconstructFkConstraintRow(tuple,
								   &numfks,
								   conkey,
								   confkey,
								   conpfeqop,
								   conppeqop,
								   conffeqop);

		for (int i = 0; i < numfks; i++)
			mapped_confkey[i] = attmap->attnums[confkey[i] - 1];

		fkconstraint = makeNode(Constraint);
		/* for now this is all we need */
		fkconstraint->conname = NameStr(constrForm->conname);
		fkconstraint->fk_upd_action = constrForm->confupdtype;
		fkconstraint->fk_del_action = constrForm->confdeltype;
		fkconstraint->deferrable = constrForm->condeferrable;
		fkconstraint->initdeferred = constrForm->condeferred;
		fkconstraint->initially_valid = true;
		fkconstraint->fk_matchtype = constrForm->confmatchtype;

		/* set up colnames that are used to generate the constraint name */
		for (int i = 0; i < numfks; i++)
		{
			Form_pg_attribute att;

			att = TupleDescAttr(RelationGetDescr(fkRel),
								conkey[i] - 1);
			fkconstraint->fk_attrs = lappend(fkconstraint->fk_attrs,
											 makeString(NameStr(att->attname)));
		}

		/*
		 * Add the new foreign key constraint pointing to the new partition.
		 * Because this new partition appears in the referenced side of the
		 * constraint, we don't need to set up for Phase 3 check.
		 */
		partIndexId = index_get_partition(partitionRel, indexOid);
		if (!OidIsValid(partIndexId))
			elog(ERROR, "index for %u not found in partition %s",
				 indexOid, RelationGetRelationName(partitionRel));
		addFkRecurseReferenced(NULL,
							   fkconstraint,
							   fkRel,
							   partitionRel,
							   partIndexId,
							   constrOid,
							   numfks,
							   mapped_confkey,
							   conkey,
							   conpfeqop,
							   conppeqop,
							   conffeqop,
							   true);

		table_close(fkRel, NoLock);
		ReleaseSysCache(tuple);
	}
}

/*
 * CloneFkReferencing
 *		Subroutine for CloneForeignKeyConstraints
 *
 * For each FK constraint of the parent relation in the given list, find an
 * equivalent constraint in its partition relation that can be reparented;
 * if one cannot be found, create a new constraint in the partition as its
 * child.
 *
 * If wqueue is given, it is used to set up phase-3 verification for each
 * cloned constraint; if omitted, we assume that such verification is not
 * needed (example: the partition is being created anew).
 */
static void
CloneFkReferencing(List **wqueue, Relation parentRel, Relation partRel)
{
	AttrMap    *attmap;
	List	   *partFKs;
	List	   *clone = NIL;
	ListCell   *cell;

	/* obtain a list of constraints that we need to clone */
	foreach(cell, RelationGetFKeyList(parentRel))
	{
		ForeignKeyCacheInfo *fk = lfirst(cell);

		clone = lappend_oid(clone, fk->conoid);
	}

	/*
	 * Silently do nothing if there's nothing to do.  In particular, this
	 * avoids throwing a spurious error for foreign tables.
	 */
	if (clone == NIL)
		return;

	if (partRel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("foreign key constraints are not supported on foreign tables")));

	/*
	 * The constraint key may differ, if the columns in the partition are
	 * different.  This map is used to convert them.
	 */
	attmap = build_attrmap_by_name(RelationGetDescr(partRel),
								   RelationGetDescr(parentRel));

	partFKs = copyObject(RelationGetFKeyList(partRel));

	foreach(cell, clone)
	{
		Oid			parentConstrOid = lfirst_oid(cell);
		Form_pg_constraint constrForm;
		Relation	pkrel;
		HeapTuple	tuple;
		int			numfks;
		AttrNumber	conkey[INDEX_MAX_KEYS];
		AttrNumber	mapped_conkey[INDEX_MAX_KEYS];
		AttrNumber	confkey[INDEX_MAX_KEYS];
		Oid			conpfeqop[INDEX_MAX_KEYS];
		Oid			conppeqop[INDEX_MAX_KEYS];
		Oid			conffeqop[INDEX_MAX_KEYS];
		Constraint *fkconstraint;
		bool		attached;
		Oid			indexOid;
		Oid			constrOid;
		ObjectAddress address,
					referenced;
		ListCell   *cell;

		tuple = SearchSysCache1(CONSTROID, parentConstrOid);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for constraint %u",
				 parentConstrOid);
		constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

		/* Don't clone constraints whose parents are being cloned */
		if (list_member_oid(clone, constrForm->conparentid))
		{
			ReleaseSysCache(tuple);
			continue;
		}

		/*
		 * Need to prevent concurrent deletions.  If pkrel is a partitioned
		 * relation, that means to lock all partitions.
		 */
		pkrel = table_open(constrForm->confrelid, ShareRowExclusiveLock);
		if (pkrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			(void) find_all_inheritors(RelationGetRelid(pkrel),
									   ShareRowExclusiveLock, NULL);

		DeconstructFkConstraintRow(tuple, &numfks, conkey, confkey,
								   conpfeqop, conppeqop, conffeqop);
		for (int i = 0; i < numfks; i++)
			mapped_conkey[i] = attmap->attnums[conkey[i] - 1];

		/*
		 * Before creating a new constraint, see whether any existing FKs are
		 * fit for the purpose.  If one is, attach the parent constraint to
		 * it, and don't clone anything.  This way we avoid the expensive
		 * verification step and don't end up with a duplicate FK, and we
		 * don't need to recurse to partitions for this constraint.
		 */
		attached = false;
		foreach(cell, partFKs)
		{
			ForeignKeyCacheInfo *fk = lfirst_node(ForeignKeyCacheInfo, cell);

			if (tryAttachPartitionForeignKey(fk,
											 RelationGetRelid(partRel),
											 parentConstrOid,
											 numfks,
											 mapped_conkey,
											 confkey,
											 conpfeqop))
			{
				attached = true;
				table_close(pkrel, NoLock);
				break;
			}
		}
		if (attached)
		{
			ReleaseSysCache(tuple);
			continue;
		}

		/* No dice.  Set up to create our own constraint */
		fkconstraint = makeNode(Constraint);
		if (ConstraintNameIsUsed(CONSTRAINT_RELATION,
								 RelationGetRelid(partRel),
								 NameStr(constrForm->conname)))
			fkconstraint->conname =
				ChooseConstraintName(RelationGetRelationName(partRel),
									 ChooseForeignKeyConstraintNameAddition(fkconstraint->fk_attrs),
									 "fkey",
									 RelationGetNamespace(partRel), NIL);
		else
			fkconstraint->conname = pstrdup(NameStr(constrForm->conname));
		fkconstraint->fk_upd_action = constrForm->confupdtype;
		fkconstraint->fk_del_action = constrForm->confdeltype;
		fkconstraint->deferrable = constrForm->condeferrable;
		fkconstraint->initdeferred = constrForm->condeferred;
		fkconstraint->fk_matchtype = constrForm->confmatchtype;
		for (int i = 0; i < numfks; i++)
		{
			Form_pg_attribute att;

			att = TupleDescAttr(RelationGetDescr(partRel),
								mapped_conkey[i] - 1);
			fkconstraint->fk_attrs = lappend(fkconstraint->fk_attrs,
											 makeString(NameStr(att->attname)));
		}

		indexOid = constrForm->conindid;
		constrOid =
			CreateConstraintEntry(fkconstraint->conname,
								  constrForm->connamespace,
								  CONSTRAINT_FOREIGN,
								  fkconstraint->deferrable,
								  fkconstraint->initdeferred,
								  constrForm->convalidated,
								  parentConstrOid,
								  RelationGetRelid(partRel),
								  mapped_conkey,
								  numfks,
								  numfks,
								  InvalidOid,	/* not a domain constraint */
								  indexOid,
								  constrForm->confrelid,	/* same foreign rel */
								  confkey,
								  conpfeqop,
								  conppeqop,
								  conffeqop,
								  numfks,
								  fkconstraint->fk_upd_action,
								  fkconstraint->fk_del_action,
								  fkconstraint->fk_matchtype,
								  NULL,
								  NULL,
								  NULL,
								  false,	/* islocal */
								  1,	/* inhcount */
								  false,	/* conNoInherit */
								  true);

		/* Set up partition dependencies for the new constraint */
		ObjectAddressSet(address, ConstraintRelationId, constrOid);
		ObjectAddressSet(referenced, ConstraintRelationId, parentConstrOid);
		recordDependencyOn(&address, &referenced, DEPENDENCY_PARTITION_PRI);
		ObjectAddressSet(referenced, RelationRelationId,
						 RelationGetRelid(partRel));
		recordDependencyOn(&address, &referenced, DEPENDENCY_PARTITION_SEC);

		/* Done with the cloned constraint's tuple */
		ReleaseSysCache(tuple);

		/* Make all this visible before recursing */
		CommandCounterIncrement();

		addFkRecurseReferencing(wqueue,
								fkconstraint,
								partRel,
								pkrel,
								indexOid,
								constrOid,
								numfks,
								confkey,
								mapped_conkey,
								conpfeqop,
								conppeqop,
								conffeqop,
								false,	/* no old check exists */
								AccessExclusiveLock);
		table_close(pkrel, NoLock);
	}
}

/*
 * When the parent of a partition receives [the referencing side of] a foreign
 * key, we must propagate that foreign key to the partition.  However, the
 * partition might already have an equivalent foreign key; this routine
 * compares the given ForeignKeyCacheInfo (in the partition) to the FK defined
 * by the other parameters.  If they are equivalent, create the link between
 * the two constraints and return true.
 *
 * If the given FK does not match the one defined by rest of the params,
 * return false.
 */
static bool
tryAttachPartitionForeignKey(ForeignKeyCacheInfo *fk,
							 Oid partRelid,
							 Oid parentConstrOid,
							 int numfks,
							 AttrNumber *mapped_conkey,
							 AttrNumber *confkey,
							 Oid *conpfeqop)
{
	HeapTuple	parentConstrTup;
	Form_pg_constraint parentConstr;
	HeapTuple	partcontup;
	Form_pg_constraint partConstr;
	Relation	trigrel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	trigtup;

	parentConstrTup = SearchSysCache1(CONSTROID,
									  ObjectIdGetDatum(parentConstrOid));
	if (!HeapTupleIsValid(parentConstrTup))
		elog(ERROR, "cache lookup failed for constraint %u", parentConstrOid);
	parentConstr = (Form_pg_constraint) GETSTRUCT(parentConstrTup);

	/*
	 * Do some quick & easy initial checks.  If any of these fail, we cannot
	 * use this constraint.
	 */
	if (fk->confrelid != parentConstr->confrelid || fk->nkeys != numfks)
	{
		ReleaseSysCache(parentConstrTup);
		return false;
	}
	for (int i = 0; i < numfks; i++)
	{
		if (fk->conkey[i] != mapped_conkey[i] ||
			fk->confkey[i] != confkey[i] ||
			fk->conpfeqop[i] != conpfeqop[i])
		{
			ReleaseSysCache(parentConstrTup);
			return false;
		}
	}

	/*
	 * Looks good so far; do some more extensive checks.  Presumably the check
	 * for 'convalidated' could be dropped, since we don't really care about
	 * that, but let's be careful for now.
	 */
	partcontup = SearchSysCache1(CONSTROID,
								 ObjectIdGetDatum(fk->conoid));
	if (!HeapTupleIsValid(partcontup))
		elog(ERROR, "cache lookup failed for constraint %u", fk->conoid);
	partConstr = (Form_pg_constraint) GETSTRUCT(partcontup);
	if (OidIsValid(partConstr->conparentid) ||
		!partConstr->convalidated ||
		partConstr->condeferrable != parentConstr->condeferrable ||
		partConstr->condeferred != parentConstr->condeferred ||
		partConstr->confupdtype != parentConstr->confupdtype ||
		partConstr->confdeltype != parentConstr->confdeltype ||
		partConstr->confmatchtype != parentConstr->confmatchtype)
	{
		ReleaseSysCache(parentConstrTup);
		ReleaseSysCache(partcontup);
		return false;
	}

	ReleaseSysCache(partcontup);
	ReleaseSysCache(parentConstrTup);

	/*
	 * Looks good!  Attach this constraint.  The action triggers in the new
	 * partition become redundant -- the parent table already has equivalent
	 * ones, and those will be able to reach the partition.  Remove the ones
	 * in the partition.  We identify them because they have our constraint
	 * OID, as well as being on the referenced rel.
	 */
	trigrel = table_open(TriggerRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				Anum_pg_trigger_tgconstraint,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(fk->conoid));

	scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true,
							  NULL, 1, &key);
	while ((trigtup = systable_getnext(scan)) != NULL)
	{
		Form_pg_trigger trgform = (Form_pg_trigger) GETSTRUCT(trigtup);
		ObjectAddress trigger;

		if (trgform->tgconstrrelid != fk->conrelid)
			continue;
		if (trgform->tgrelid != fk->confrelid)
			continue;

		/*
		 * The constraint is originally set up to contain this trigger as an
		 * implementation object, so there's a dependency record that links
		 * the two; however, since the trigger is no longer needed, we remove
		 * the dependency link in order to be able to drop the trigger while
		 * keeping the constraint intact.
		 */
		deleteDependencyRecordsFor(TriggerRelationId,
								   trgform->oid,
								   false);
		/* make dependency deletion visible to performDeletion */
		CommandCounterIncrement();
		ObjectAddressSet(trigger, TriggerRelationId,
						 trgform->oid);
		performDeletion(&trigger, DROP_RESTRICT, 0);
		/* make trigger drop visible, in case the loop iterates */
		CommandCounterIncrement();
	}

	systable_endscan(scan);
	table_close(trigrel, RowExclusiveLock);

	ConstraintSetParentConstraint(fk->conoid, parentConstrOid, partRelid);
	CommandCounterIncrement();
	return true;
}


/*
 * ALTER TABLE ALTER CONSTRAINT
 *
 * Update the attributes of a constraint.
 *
 * Currently only works for Foreign Key constraints.
 *
 * If the constraint is modified, returns its address; otherwise, return
 * InvalidObjectAddress.
 */
static ObjectAddress
ATExecAlterConstraint(Relation rel, AlterTableCmd *cmd, bool recurse,
					  bool recursing, LOCKMODE lockmode)
{
	Constraint *cmdcon;
	Relation	conrel;
	Relation	tgrel;
	SysScanDesc scan;
	ScanKeyData skey[3];
	HeapTuple	contuple;
	Form_pg_constraint currcon;
	ObjectAddress address;
	List	   *otherrelids = NIL;
	ListCell   *lc;

	cmdcon = castNode(Constraint, cmd->def);

	conrel = table_open(ConstraintRelationId, RowExclusiveLock);
	tgrel = table_open(TriggerRelationId, RowExclusiveLock);

	/*
	 * Find and check the target constraint
	 */
	ScanKeyInit(&skey[0],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	ScanKeyInit(&skey[1],
				Anum_pg_constraint_contypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(InvalidOid));
	ScanKeyInit(&skey[2],
				Anum_pg_constraint_conname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(cmdcon->conname));
	scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId,
							  true, NULL, 3, skey);

	/* There can be at most one matching row */
	if (!HeapTupleIsValid(contuple = systable_getnext(scan)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("constraint \"%s\" of relation \"%s\" does not exist",
						cmdcon->conname, RelationGetRelationName(rel))));

	currcon = (Form_pg_constraint) GETSTRUCT(contuple);
	if (currcon->contype != CONSTRAINT_FOREIGN)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("constraint \"%s\" of relation \"%s\" is not a foreign key constraint",
						cmdcon->conname, RelationGetRelationName(rel))));

	/*
	 * If it's not the topmost constraint, raise an error.
	 *
	 * Altering a non-topmost constraint leaves some triggers untouched, since
	 * they are not directly connected to this constraint; also, pg_dump would
	 * ignore the deferrability status of the individual constraint, since it
	 * only dumps topmost constraints.  Avoid these problems by refusing this
	 * operation and telling the user to alter the parent constraint instead.
	 */
	if (OidIsValid(currcon->conparentid))
	{
		HeapTuple	tp;
		Oid			parent = currcon->conparentid;
		char	   *ancestorname = NULL;
		char	   *ancestortable = NULL;

		/* Loop to find the topmost constraint */
		while (HeapTupleIsValid(tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent))))
		{
			Form_pg_constraint contup = (Form_pg_constraint) GETSTRUCT(tp);

			/* If no parent, this is the constraint we want */
			if (!OidIsValid(contup->conparentid))
			{
				ancestorname = pstrdup(NameStr(contup->conname));
				ancestortable = get_rel_name(contup->conrelid);
				ReleaseSysCache(tp);
				break;
			}

			parent = contup->conparentid;
			ReleaseSysCache(tp);
		}

		ereport(ERROR,
				(errmsg("cannot alter constraint \"%s\" on relation \"%s\"",
						cmdcon->conname, RelationGetRelationName(rel)),
				 ancestorname && ancestortable ?
				 errdetail("Constraint \"%s\" is derived from constraint \"%s\" of relation \"%s\".",
						   cmdcon->conname, ancestorname, ancestortable) : 0,
				 errhint("You may alter the constraint it derives from, instead.")));
	}

	/*
	 * Do the actual catalog work.  We can skip changing if already in the
	 * desired state, but not if a partitioned table: partitions need to be
	 * processed regardless, in case they had the constraint locally changed.
	 */
	address = InvalidObjectAddress;
	if (currcon->condeferrable != cmdcon->deferrable ||
		currcon->condeferred != cmdcon->initdeferred ||
		rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		if (ATExecAlterConstrRecurse(cmdcon, conrel, tgrel, rel, contuple,
									 &otherrelids, lockmode))
			ObjectAddressSet(address, ConstraintRelationId, currcon->oid);
	}

	/*
	 * ATExecConstrRecurse already invalidated relcache for the relations
	 * having the constraint itself; here we also invalidate for relations
	 * that have any triggers that are part of the constraint.
	 */
	foreach(lc, otherrelids)
		CacheInvalidateRelcacheByRelid(lfirst_oid(lc));

	systable_endscan(scan);

	table_close(tgrel, RowExclusiveLock);
	table_close(conrel, RowExclusiveLock);

	return address;
}

/*
 * Recursive subroutine of ATExecAlterConstraint.  Returns true if the
 * constraint is altered.
 *
 * *otherrelids is appended OIDs of relations containing affected triggers.
 *
 * Note that we must recurse even when the values are correct, in case
 * indirect descendants have had their constraints altered locally.
 * (This could be avoided if we forbade altering constraints in partitions
 * but existing releases don't do that.)
 */
static bool
ATExecAlterConstrRecurse(Constraint *cmdcon, Relation conrel, Relation tgrel,
						 Relation rel, HeapTuple contuple, List **otherrelids,
						 LOCKMODE lockmode)
{
	Form_pg_constraint currcon;
	Oid			conoid;
	Oid			refrelid;
	bool		changed = false;

	currcon = (Form_pg_constraint) GETSTRUCT(contuple);
	conoid = currcon->oid;
	refrelid = currcon->confrelid;

	/*
	 * Update pg_constraint with the flags from cmdcon.
	 *
	 * If called to modify a constraint that's already in the desired state,
	 * silently do nothing.
	 */
	if (currcon->condeferrable != cmdcon->deferrable ||
		currcon->condeferred != cmdcon->initdeferred)
	{
		HeapTuple	copyTuple;
		Form_pg_constraint copy_con;
		HeapTuple	tgtuple;
		ScanKeyData tgkey;
		SysScanDesc tgscan;

		copyTuple = heap_copytuple(contuple);
		copy_con = (Form_pg_constraint) GETSTRUCT(copyTuple);
		copy_con->condeferrable = cmdcon->deferrable;
		copy_con->condeferred = cmdcon->initdeferred;
		CatalogTupleUpdate(conrel, &copyTuple->t_self, copyTuple);

		InvokeObjectPostAlterHook(ConstraintRelationId,
								  conoid, 0);

		heap_freetuple(copyTuple);
		changed = true;

		/* Make new constraint flags visible to others */
		CacheInvalidateRelcache(rel);

		/*
		 * Now we need to update the multiple entries in pg_trigger that
		 * implement the constraint.
		 */
		ScanKeyInit(&tgkey,
					Anum_pg_trigger_tgconstraint,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(conoid));
		tgscan = systable_beginscan(tgrel, TriggerConstraintIndexId, true,
									NULL, 1, &tgkey);
		while (HeapTupleIsValid(tgtuple = systable_getnext(tgscan)))
		{
			Form_pg_trigger tgform = (Form_pg_trigger) GETSTRUCT(tgtuple);
			Form_pg_trigger copy_tg;
			HeapTuple	copyTuple;

			/*
			 * Remember OIDs of other relation(s) involved in FK constraint.
			 * (Note: it's likely that we could skip forcing a relcache inval
			 * for other rels that don't have a trigger whose properties
			 * change, but let's be conservative.)
			 */
			if (tgform->tgrelid != RelationGetRelid(rel))
				*otherrelids = list_append_unique_oid(*otherrelids,
													  tgform->tgrelid);

			/*
			 * Update deferrability of RI_FKey_noaction_del,
			 * RI_FKey_noaction_upd, RI_FKey_check_ins and RI_FKey_check_upd
			 * triggers, but not others; see createForeignKeyActionTriggers
			 * and CreateFKCheckTrigger.
			 */
			if (tgform->tgfoid != F_RI_FKEY_NOACTION_DEL &&
				tgform->tgfoid != F_RI_FKEY_NOACTION_UPD &&
				tgform->tgfoid != F_RI_FKEY_CHECK_INS &&
				tgform->tgfoid != F_RI_FKEY_CHECK_UPD)
				continue;

			copyTuple = heap_copytuple(tgtuple);
			copy_tg = (Form_pg_trigger) GETSTRUCT(copyTuple);

			copy_tg->tgdeferrable = cmdcon->deferrable;
			copy_tg->tginitdeferred = cmdcon->initdeferred;
			CatalogTupleUpdate(tgrel, &copyTuple->t_self, copyTuple);

			InvokeObjectPostAlterHook(TriggerRelationId, tgform->oid, 0);

			heap_freetuple(copyTuple);
		}

		systable_endscan(tgscan);
	}

	/*
	 * If the table at either end of the constraint is partitioned, we need to
	 * recurse and handle every constraint that is a child of this one.
	 *
	 * (This assumes that the recurse flag is forcibly set for partitioned
	 * tables, and not set for legacy inheritance, though we don't check for
	 * that here.)
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
		get_rel_relkind(refrelid) == RELKIND_PARTITIONED_TABLE)
	{
		ScanKeyData pkey;
		SysScanDesc pscan;
		HeapTuple	childtup;

		ScanKeyInit(&pkey,
					Anum_pg_constraint_conparentid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(conoid));

		pscan = systable_beginscan(conrel, ConstraintParentIndexId,
								   true, NULL, 1, &pkey);

		while (HeapTupleIsValid(childtup = systable_getnext(pscan)))
		{
			Form_pg_constraint childcon = (Form_pg_constraint) GETSTRUCT(childtup);
			Relation	childrel;

			childrel = table_open(childcon->conrelid, lockmode);
			ATExecAlterConstrRecurse(cmdcon, conrel, tgrel, childrel, childtup,
									 otherrelids, lockmode);
			table_close(childrel, NoLock);
		}

		systable_endscan(pscan);
	}

	return changed;
}

/*
 * ALTER TABLE VALIDATE CONSTRAINT
 *
 * XXX The reason we handle recursion here rather than at Phase 1 is because
 * there's no good way to skip recursing when handling foreign keys: there is
 * no need to lock children in that case, yet we wouldn't be able to avoid
 * doing so at that level.
 *
 * Return value is the address of the validated constraint.  If the constraint
 * was already validated, InvalidObjectAddress is returned.
 */
static ObjectAddress
ATExecValidateConstraint(List **wqueue, Relation rel, char *constrName,
						 bool recurse, bool recursing, LOCKMODE lockmode)
{
	Relation	conrel;
	SysScanDesc scan;
	ScanKeyData skey[3];
	HeapTuple	tuple;
	Form_pg_constraint con;
	ObjectAddress address;

	conrel = table_open(ConstraintRelationId, RowExclusiveLock);

	/*
	 * Find and check the target constraint
	 */
	ScanKeyInit(&skey[0],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	ScanKeyInit(&skey[1],
				Anum_pg_constraint_contypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(InvalidOid));
	ScanKeyInit(&skey[2],
				Anum_pg_constraint_conname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(constrName));
	scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId,
							  true, NULL, 3, skey);

	/* There can be at most one matching row */
	if (!HeapTupleIsValid(tuple = systable_getnext(scan)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("constraint \"%s\" of relation \"%s\" does not exist",
						constrName, RelationGetRelationName(rel))));

	con = (Form_pg_constraint) GETSTRUCT(tuple);
	if (con->contype != CONSTRAINT_FOREIGN &&
		con->contype != CONSTRAINT_CHECK)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("constraint \"%s\" of relation \"%s\" is not a foreign key or check constraint",
						constrName, RelationGetRelationName(rel))));

	if (!con->convalidated)
	{
		AlteredTableInfo *tab;
		HeapTuple	copyTuple;
		Form_pg_constraint copy_con;

		if (con->contype == CONSTRAINT_FOREIGN)
		{
			NewConstraint *newcon;
			Constraint *fkconstraint;

			/* Queue validation for phase 3 */
			fkconstraint = makeNode(Constraint);
			/* for now this is all we need */
			fkconstraint->conname = constrName;

			newcon = (NewConstraint *) palloc0(sizeof(NewConstraint));
			newcon->name = constrName;
			newcon->contype = CONSTR_FOREIGN;
			newcon->refrelid = con->confrelid;
			newcon->refindid = con->conindid;
			newcon->conid = con->oid;
			newcon->qual = (Node *) fkconstraint;

			/* Find or create work queue entry for this table */
			tab = ATGetQueueEntry(wqueue, rel);
			tab->constraints = lappend(tab->constraints, newcon);

			/*
			 * We disallow creating invalid foreign keys to or from
			 * partitioned tables, so ignoring the recursion bit is okay.
			 */
		}
		else if (con->contype == CONSTRAINT_CHECK)
		{
			List	   *children = NIL;
			ListCell   *child;
			NewConstraint *newcon;
			bool		isnull;
			Datum		val;
			char	   *conbin;

			/*
			 * If we're recursing, the parent has already done this, so skip
			 * it.  Also, if the constraint is a NO INHERIT constraint, we
			 * shouldn't try to look for it in the children.
			 */
			if (!recursing && !con->connoinherit)
				children = find_all_inheritors(RelationGetRelid(rel),
											   lockmode, NULL);

			/*
			 * For CHECK constraints, we must ensure that we only mark the
			 * constraint as validated on the parent if it's already validated
			 * on the children.
			 *
			 * We recurse before validating on the parent, to reduce risk of
			 * deadlocks.
			 */
			foreach(child, children)
			{
				Oid			childoid = lfirst_oid(child);
				Relation	childrel;

				if (childoid == RelationGetRelid(rel))
					continue;

				/*
				 * If we are told not to recurse, there had better not be any
				 * child tables, because we can't mark the constraint on the
				 * parent valid unless it is valid for all child tables.
				 */
				if (!recurse)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("constraint must be validated on child tables too")));

				/* find_all_inheritors already got lock */
				childrel = table_open(childoid, NoLock);

				ATExecValidateConstraint(wqueue, childrel, constrName, false,
										 true, lockmode);
				table_close(childrel, NoLock);
			}

			/* Queue validation for phase 3 */
			newcon = (NewConstraint *) palloc0(sizeof(NewConstraint));
			newcon->name = constrName;
			newcon->contype = CONSTR_CHECK;
			newcon->refrelid = InvalidOid;
			newcon->refindid = InvalidOid;
			newcon->conid = con->oid;

			val = SysCacheGetAttr(CONSTROID, tuple,
								  Anum_pg_constraint_conbin, &isnull);
			if (isnull)
				elog(ERROR, "null conbin for constraint %u", con->oid);

			conbin = TextDatumGetCString(val);
			newcon->qual = (Node *) stringToNode(conbin);

			/* Find or create work queue entry for this table */
			tab = ATGetQueueEntry(wqueue, rel);
			tab->constraints = lappend(tab->constraints, newcon);

			/*
			 * Invalidate relcache so that others see the new validated
			 * constraint.
			 */
			CacheInvalidateRelcache(rel);
		}

		/*
		 * Now update the catalog, while we have the door open.
		 */
		copyTuple = heap_copytuple(tuple);
		copy_con = (Form_pg_constraint) GETSTRUCT(copyTuple);
		copy_con->convalidated = true;
		CatalogTupleUpdate(conrel, &copyTuple->t_self, copyTuple);

		InvokeObjectPostAlterHook(ConstraintRelationId, con->oid, 0);

		heap_freetuple(copyTuple);

		ObjectAddressSet(address, ConstraintRelationId, con->oid);
	}
	else
		address = InvalidObjectAddress; /* already validated */

	systable_endscan(scan);

	table_close(conrel, RowExclusiveLock);

	return address;
}


/*
 * transformColumnNameList - transform list of column names
 *
 * Lookup each name and return its attnum and type OID
 */
static int
transformColumnNameList(Oid relId, List *colList,
						int16 *attnums, Oid *atttypids)
{
	ListCell   *l;
	int			attnum;

	attnum = 0;
	foreach(l, colList)
	{
		char	   *attname = strVal(lfirst(l));
		HeapTuple	atttuple;

		atttuple = SearchSysCacheAttName(relId, attname);
		if (!HeapTupleIsValid(atttuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" referenced in foreign key constraint does not exist",
							attname)));
		if (attnum >= INDEX_MAX_KEYS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("cannot have more than %d keys in a foreign key",
							INDEX_MAX_KEYS)));
		attnums[attnum] = ((Form_pg_attribute) GETSTRUCT(atttuple))->attnum;
		atttypids[attnum] = ((Form_pg_attribute) GETSTRUCT(atttuple))->atttypid;
		ReleaseSysCache(atttuple);
		attnum++;
	}

	return attnum;
}

/*
 * transformFkeyGetPrimaryKey -
 *
 *	Look up the names, attnums, and types of the primary key attributes
 *	for the pkrel.  Also return the index OID and index opclasses of the
 *	index supporting the primary key.
 *
 *	All parameters except pkrel are output parameters.  Also, the function
 *	return value is the number of attributes in the primary key.
 *
 *	Used when the column list in the REFERENCES specification is omitted.
 */
static int
transformFkeyGetPrimaryKey(Relation pkrel, Oid *indexOid,
						   List **attnamelist,
						   int16 *attnums, Oid *atttypids,
						   Oid *opclasses)
{
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	HeapTuple	indexTuple = NULL;
	Form_pg_index indexStruct = NULL;
	Datum		indclassDatum;
	bool		isnull;
	oidvector  *indclass;
	int			i;

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache until we find one marked primary key
	 * (hopefully there isn't more than one such).  Insist it's valid, too.
	 */
	*indexOid = InvalidOid;

	indexoidlist = RelationGetIndexList(pkrel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);

		indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);
		if (indexStruct->indisprimary && indexStruct->indisvalid)
		{
			/*
			 * Refuse to use a deferrable primary key.  This is per SQL spec,
			 * and there would be a lot of interesting semantic problems if we
			 * tried to allow it.
			 */
			if (!indexStruct->indimmediate)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("cannot use a deferrable primary key for referenced table \"%s\"",
								RelationGetRelationName(pkrel))));

			*indexOid = indexoid;
			break;
		}
		ReleaseSysCache(indexTuple);
	}

	list_free(indexoidlist);

	/*
	 * Check that we found it
	 */
	if (!OidIsValid(*indexOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("there is no primary key for referenced table \"%s\"",
						RelationGetRelationName(pkrel))));

	/* Must get indclass the hard way */
	indclassDatum = SysCacheGetAttr(INDEXRELID, indexTuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(indclassDatum);

	/*
	 * Now build the list of PK attributes from the indkey definition (we
	 * assume a primary key cannot have expressional elements)
	 */
	*attnamelist = NIL;
	for (i = 0; i < indexStruct->indnkeyatts; i++)
	{
		int			pkattno = indexStruct->indkey.values[i];

		attnums[i] = pkattno;
		atttypids[i] = attnumTypeId(pkrel, pkattno);
		opclasses[i] = indclass->values[i];
		*attnamelist = lappend(*attnamelist,
							   makeString(pstrdup(NameStr(*attnumAttName(pkrel, pkattno)))));
	}

	ReleaseSysCache(indexTuple);

	return i;
}

/*
 * transformFkeyCheckAttrs -
 *
 *	Make sure that the attributes of a referenced table belong to a unique
 *	(or primary key) constraint.  Return the OID of the index supporting
 *	the constraint, as well as the opclasses associated with the index
 *	columns.
 */
Oid
transformFkeyCheckAttrs(Relation pkrel,
						int numattrs, int16 *attnums,
						Oid *opclasses) /* output parameter */
{
	Oid			indexoid = InvalidOid;
	bool		found = false;
	bool		found_deferrable = false;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	int			i,
				j;

	/*
	 * Reject duplicate appearances of columns in the referenced-columns list.
	 * Such a case is forbidden by the SQL standard, and even if we thought it
	 * useful to allow it, there would be ambiguity about how to match the
	 * list to unique indexes (in particular, it'd be unclear which index
	 * opclass goes with which FK column).
	 */
	for (i = 0; i < numattrs; i++)
	{
		for (j = i + 1; j < numattrs; j++)
		{
			if (attnums[i] == attnums[j])
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FOREIGN_KEY),
						 errmsg("foreign key referenced-columns list must not contain duplicates")));
		}
	}

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache, and match unique indexes to the list
	 * of attnums we are given.
	 */
	indexoidlist = RelationGetIndexList(pkrel);

	foreach(indexoidscan, indexoidlist)
	{
		HeapTuple	indexTuple;
		Form_pg_index indexStruct;

		indexoid = lfirst_oid(indexoidscan);
		indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

		/*
		 * Must have the right number of columns; must be unique and not a
		 * partial index; forget it if there are any expressions, too. Invalid
		 * indexes are out as well.
		 */
		if (indexStruct->indnkeyatts == numattrs &&
			indexStruct->indisunique &&
			indexStruct->indisvalid &&
			heap_attisnull(indexTuple, Anum_pg_index_indpred, NULL) &&
			heap_attisnull(indexTuple, Anum_pg_index_indexprs, NULL))
		{
			Datum		indclassDatum;
			bool		isnull;
			oidvector  *indclass;

			/* Must get indclass the hard way */
			indclassDatum = SysCacheGetAttr(INDEXRELID, indexTuple,
											Anum_pg_index_indclass, &isnull);
			Assert(!isnull);
			indclass = (oidvector *) DatumGetPointer(indclassDatum);

			/*
			 * The given attnum list may match the index columns in any order.
			 * Check for a match, and extract the appropriate opclasses while
			 * we're at it.
			 *
			 * We know that attnums[] is duplicate-free per the test at the
			 * start of this function, and we checked above that the number of
			 * index columns agrees, so if we find a match for each attnums[]
			 * entry then we must have a one-to-one match in some order.
			 */
			for (i = 0; i < numattrs; i++)
			{
				found = false;
				for (j = 0; j < numattrs; j++)
				{
					if (attnums[i] == indexStruct->indkey.values[j])
					{
						opclasses[i] = indclass->values[j];
						found = true;
						break;
					}
				}
				if (!found)
					break;
			}

			/*
			 * Refuse to use a deferrable unique/primary key.  This is per SQL
			 * spec, and there would be a lot of interesting semantic problems
			 * if we tried to allow it.
			 */
			if (found && !indexStruct->indimmediate)
			{
				/*
				 * Remember that we found an otherwise matching index, so that
				 * we can generate a more appropriate error message.
				 */
				found_deferrable = true;
				found = false;
			}
		}
		ReleaseSysCache(indexTuple);
		if (found)
			break;
	}

	if (!found)
	{
		if (found_deferrable)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot use a deferrable unique constraint for referenced table \"%s\"",
							RelationGetRelationName(pkrel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FOREIGN_KEY),
					 errmsg("there is no unique constraint matching given keys for referenced table \"%s\"",
							RelationGetRelationName(pkrel))));
	}

	list_free(indexoidlist);

	return indexoid;
}

/*
 * findFkeyCast -
 *
 *	Wrapper around find_coercion_pathway() for ATAddForeignKeyConstraint().
 *	Caller has equal regard for binary coercibility and for an exact match.
*/
static CoercionPathType
findFkeyCast(Oid targetTypeId, Oid sourceTypeId, Oid *funcid)
{
	CoercionPathType ret;

	if (targetTypeId == sourceTypeId)
	{
		ret = COERCION_PATH_RELABELTYPE;
		*funcid = InvalidOid;
	}
	else
	{
		ret = find_coercion_pathway(targetTypeId, sourceTypeId,
									COERCION_IMPLICIT, funcid);
		if (ret == COERCION_PATH_NONE)
			/* A previously-relied-upon cast is now gone. */
			elog(ERROR, "could not find cast from %u to %u",
				 sourceTypeId, targetTypeId);
	}

	return ret;
}

/*
 * Permissions checks on the referenced table for ADD FOREIGN KEY
 *
 * Note: we have already checked that the user owns the referencing table,
 * else we'd have failed much earlier; no additional checks are needed for it.
 */
static void
checkFkeyPermissions(Relation rel, int16 *attnums, int natts)
{
	Oid			roleid = GetUserId();
	AclResult	aclresult;
	int			i;

	/* Okay if we have relation-level REFERENCES permission */
	aclresult = pg_class_aclcheck(RelationGetRelid(rel), roleid,
								  ACL_REFERENCES);
	if (aclresult == ACLCHECK_OK)
		return;
	/* Else we must have REFERENCES on each column */
	for (i = 0; i < natts; i++)
	{
		aclresult = pg_attribute_aclcheck(RelationGetRelid(rel), attnums[i],
										  roleid, ACL_REFERENCES);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, get_relkind_objtype(rel->rd_rel->relkind),
						   RelationGetRelationName(rel));
	}
}

/*
 * Scan the existing rows in a table to verify they meet a proposed FK
 * constraint.
 *
 * Caller must have opened and locked both relations appropriately.
 */
static void
validateForeignKeyConstraint(char *conname,
							 Relation rel,
							 Relation pkrel,
							 Oid pkindOid,
							 Oid constraintOid)
{
	TupleTableSlot *slot;
	TableScanDesc scan;
	Trigger		trig;
	Snapshot	snapshot;
	MemoryContext oldcxt;
	MemoryContext perTupCxt;

	ereport(DEBUG1,
			(errmsg_internal("validating foreign key constraint \"%s\"", conname)));

	/* Cloudberry Database: Ignore foreign keys for now, with a warning. */
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
		ereport(WARNING,
				(errcode(ERRCODE_GP_FEATURE_NOT_YET),
				 errmsg("referential integrity (FOREIGN KEY) constraints are not supported in Cloudberry Database, will not be enforced")));

	/*
	 * Build a trigger call structure; we'll need it either way.
	 */
	MemSet(&trig, 0, sizeof(trig));
	trig.tgoid = InvalidOid;
	trig.tgname = conname;
	trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
	trig.tgisinternal = true;
	trig.tgconstrrelid = RelationGetRelid(pkrel);
	trig.tgconstrindid = pkindOid;
	trig.tgconstraint = constraintOid;
	trig.tgdeferrable = false;
	trig.tginitdeferred = false;
	/* we needn't fill in remaining fields */

	/*
	 * See if we can do it with a single LEFT JOIN query.  A false result
	 * indicates we must proceed with the fire-the-trigger method.
	 */
	if (RI_Initial_Check(&trig, rel, pkrel))
		return;

	/*
	 * Scan through each tuple, calling RI_FKey_check_ins (insert trigger) as
	 * if that tuple had just been inserted.  If any of those fail, it should
	 * ereport(ERROR) and that's that.
	 */
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	slot = table_slot_create(rel, NULL);
	scan = table_beginscan(rel, snapshot, 0, NULL);

	perTupCxt = AllocSetContextCreate(CurrentMemoryContext,
									  "validateForeignKeyConstraint",
									  ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo(perTupCxt);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		LOCAL_FCINFO(fcinfo, 0);
		TriggerData trigdata = {0};

		CHECK_FOR_INTERRUPTS();

		/*
		 * Make a call to the trigger function
		 *
		 * No parameters are passed, but we do set a context
		 */
		MemSet(fcinfo, 0, SizeForFunctionCallInfo(0));

		/*
		 * We assume RI_FKey_check_ins won't look at flinfo...
		 */
		trigdata.type = T_TriggerData;
		trigdata.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW;
		trigdata.tg_relation = rel;
		trigdata.tg_trigtuple = ExecFetchSlotHeapTuple(slot, false, NULL);
		trigdata.tg_trigslot = slot;
		trigdata.tg_trigger = &trig;

		fcinfo->context = (Node *) &trigdata;

		RI_FKey_check_ins(fcinfo);

		MemoryContextReset(perTupCxt);
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(perTupCxt);
	table_endscan(scan);
	UnregisterSnapshot(snapshot);
	ExecDropSingleTupleTableSlot(slot);
}

static void
CreateFKCheckTrigger(Oid myRelOid, Oid refRelOid, Constraint *fkconstraint,
					 Oid constraintOid, Oid indexOid, bool on_insert)
{
	CreateTrigStmt *fk_trigger;

	/*
	 * Note: for a self-referential FK (referencing and referenced tables are
	 * the same), it is important that the ON UPDATE action fires before the
	 * CHECK action, since both triggers will fire on the same row during an
	 * UPDATE event; otherwise the CHECK trigger will be checking a non-final
	 * state of the row.  Triggers fire in name order, so we ensure this by
	 * using names like "RI_ConstraintTrigger_a_NNNN" for the action triggers
	 * and "RI_ConstraintTrigger_c_NNNN" for the check triggers.
	 */
	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->replace = false;
	fk_trigger->isconstraint = true;
	fk_trigger->trigname = "RI_ConstraintTrigger_c";
	fk_trigger->relation = NULL;

	/* Either ON INSERT or ON UPDATE */
	if (on_insert)
	{
		fk_trigger->funcname = SystemFuncName("RI_FKey_check_ins");
		fk_trigger->events = TRIGGER_TYPE_INSERT;
	}
	else
	{
		fk_trigger->funcname = SystemFuncName("RI_FKey_check_upd");
		fk_trigger->events = TRIGGER_TYPE_UPDATE;
	}

	fk_trigger->args = NIL;
	fk_trigger->row = true;
	fk_trigger->timing = TRIGGER_TYPE_AFTER;
	fk_trigger->columns = NIL;
	fk_trigger->whenClause = NULL;
	fk_trigger->transitionRels = NIL;
	fk_trigger->deferrable = fkconstraint->deferrable;
	fk_trigger->initdeferred = fkconstraint->initdeferred;
	fk_trigger->constrrel = NULL;

	(void) CreateTrigger(fk_trigger, NULL, myRelOid, refRelOid, constraintOid,
						 indexOid, InvalidOid, InvalidOid, NULL, true, false);

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

/*
 * createForeignKeyActionTriggers
 *		Create the referenced-side "action" triggers that implement a foreign
 *		key.
 */
static void
createForeignKeyActionTriggers(Relation rel, Oid refRelOid, Constraint *fkconstraint,
							   Oid constraintOid, Oid indexOid)
{
	CreateTrigStmt *fk_trigger;

	/*
	 * Special for Cloudberry Database: Ignore foreign keys for now, with warning
	 */
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("referential integrity (FOREIGN KEY) constraints are not supported in Cloudberry Database, will not be enforced")));
	}

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * DELETE action on the referenced table.
	 */
	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->replace = false;
	fk_trigger->isconstraint = true;
	fk_trigger->trigname = "RI_ConstraintTrigger_a";
	fk_trigger->relation = NULL;
	fk_trigger->args = NIL;
	fk_trigger->row = true;
	fk_trigger->timing = TRIGGER_TYPE_AFTER;
	fk_trigger->events = TRIGGER_TYPE_DELETE;
	fk_trigger->columns = NIL;
	fk_trigger->whenClause = NULL;
	fk_trigger->transitionRels = NIL;
	fk_trigger->constrrel = NULL;
	switch (fkconstraint->fk_del_action)
	{
		case FKCONSTR_ACTION_NOACTION:
			fk_trigger->deferrable = fkconstraint->deferrable;
			fk_trigger->initdeferred = fkconstraint->initdeferred;
			fk_trigger->funcname = SystemFuncName("RI_FKey_noaction_del");
			break;
		case FKCONSTR_ACTION_RESTRICT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_restrict_del");
			break;
		case FKCONSTR_ACTION_CASCADE:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_cascade_del");
			break;
		case FKCONSTR_ACTION_SETNULL:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setnull_del");
			break;
		case FKCONSTR_ACTION_SETDEFAULT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setdefault_del");
			break;
		default:
			elog(ERROR, "unrecognized FK action type: %d",
				 (int) fkconstraint->fk_del_action);
			break;
	}

	(void) CreateTrigger(fk_trigger, NULL, refRelOid, RelationGetRelid(rel),
						 constraintOid,
						 indexOid, InvalidOid, InvalidOid, NULL, true, false);

	/* Make changes-so-far visible */
	CommandCounterIncrement();

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * UPDATE action on the referenced table.
	 */
	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->replace = false;
	fk_trigger->isconstraint = true;
	fk_trigger->trigname = "RI_ConstraintTrigger_a";
	fk_trigger->relation = NULL;
	fk_trigger->args = NIL;
	fk_trigger->row = true;
	fk_trigger->timing = TRIGGER_TYPE_AFTER;
	fk_trigger->events = TRIGGER_TYPE_UPDATE;
	fk_trigger->columns = NIL;
	fk_trigger->whenClause = NULL;
	fk_trigger->transitionRels = NIL;
	fk_trigger->constrrel = NULL;
	switch (fkconstraint->fk_upd_action)
	{
		case FKCONSTR_ACTION_NOACTION:
			fk_trigger->deferrable = fkconstraint->deferrable;
			fk_trigger->initdeferred = fkconstraint->initdeferred;
			fk_trigger->funcname = SystemFuncName("RI_FKey_noaction_upd");
			break;
		case FKCONSTR_ACTION_RESTRICT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_restrict_upd");
			break;
		case FKCONSTR_ACTION_CASCADE:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_cascade_upd");
			break;
		case FKCONSTR_ACTION_SETNULL:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setnull_upd");
			break;
		case FKCONSTR_ACTION_SETDEFAULT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setdefault_upd");
			break;
		default:
			elog(ERROR, "unrecognized FK action type: %d",
				 (int) fkconstraint->fk_upd_action);
			break;
	}

	(void) CreateTrigger(fk_trigger, NULL, refRelOid, RelationGetRelid(rel),
						 constraintOid,
						 indexOid, InvalidOid, InvalidOid, NULL, true, false);
}

/*
 * createForeignKeyCheckTriggers
 *		Create the referencing-side "check" triggers that implement a foreign
 *		key.
 */
static void
createForeignKeyCheckTriggers(Oid myRelOid, Oid refRelOid,
							  Constraint *fkconstraint, Oid constraintOid,
							  Oid indexOid)
{
	CreateFKCheckTrigger(myRelOid, refRelOid, fkconstraint, constraintOid,
						 indexOid, true);
	CreateFKCheckTrigger(myRelOid, refRelOid, fkconstraint, constraintOid,
						 indexOid, false);
}

/*
 * ALTER TABLE DROP CONSTRAINT
 *
 * Like DROP COLUMN, we can't use the normal ALTER TABLE recursion mechanism.
 */
static void
ATExecDropConstraint(Relation rel, const char *constrName,
					 DropBehavior behavior,
					 bool recurse, bool recursing,
					 bool missing_ok, LOCKMODE lockmode)
{
	List	   *children;
	ListCell   *child;
	Relation	conrel;
	Form_pg_constraint con;
	SysScanDesc scan;
	ScanKeyData skey[3];
	HeapTuple	tuple;
	bool		found = false;
	bool		is_no_inherit_constraint = false;
	char		contype;

	/* At top level, permission check was done in ATPrepCmd, else do it */
	if (recursing)
		ATSimplePermissions(rel, ATT_TABLE | ATT_FOREIGN_TABLE);

	conrel = table_open(ConstraintRelationId, RowExclusiveLock);

	/*
	 * Find and drop the target constraint
	 */
	ScanKeyInit(&skey[0],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	ScanKeyInit(&skey[1],
				Anum_pg_constraint_contypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(InvalidOid));
	ScanKeyInit(&skey[2],
				Anum_pg_constraint_conname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(constrName));
	scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId,
							  true, NULL, 3, skey);

	/* There can be at most one matching row */
	if (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		ObjectAddress conobj;

		con = (Form_pg_constraint) GETSTRUCT(tuple);

		/* Don't drop inherited constraints */
		if (con->coninhcount > 0 && !recursing)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot drop inherited constraint \"%s\" of relation \"%s\"",
							constrName, RelationGetRelationName(rel))));

		is_no_inherit_constraint = con->connoinherit;
		contype = con->contype;

		/*
		 * If it's a foreign-key constraint, we'd better lock the referenced
		 * table and check that that's not in use, just as we've already done
		 * for the constrained table (else we might, eg, be dropping a trigger
		 * that has unfired events).  But we can/must skip that in the
		 * self-referential case.
		 */
		if (contype == CONSTRAINT_FOREIGN &&
			con->confrelid != RelationGetRelid(rel))
		{
			Relation	frel;

			/* Must match lock taken by RemoveTriggerById: */
			frel = table_open(con->confrelid, AccessExclusiveLock);
			CheckTableNotInUse(frel, "ALTER TABLE");
			table_close(frel, NoLock);
		}

		/*
		 * If it's a foreign-key constraint, we'd better lock the referenced
		 * table and check that that's not in use, just as we've already done
		 * for the constrained table (else we might, eg, be dropping a trigger
		 * that has unfired events).  But we can/must skip that in the
		 * self-referential case.
		 */
		if (con->contype == CONSTRAINT_FOREIGN &&
			con->confrelid != RelationGetRelid(rel))
		{
			Relation	frel;

			/* Must match lock taken by RemoveTriggerById: */
			frel = heap_open(con->confrelid, AccessExclusiveLock);
			CheckTableNotInUse(frel, "ALTER TABLE");
			heap_close(frel, NoLock);
		}

		/*
		 * Perform the actual constraint deletion
		 */
		conobj.classId = ConstraintRelationId;
		conobj.objectId = con->oid;
		conobj.objectSubId = 0;

		performDeletion(&conobj, behavior, 0);

		found = true;
	}

	systable_endscan(scan);

	if (!found)
	{
		if (!missing_ok)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("constraint \"%s\" of relation \"%s\" does not exist",
							constrName, RelationGetRelationName(rel))));
		}
		else
		{
			ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
					(errmsg("constraint \"%s\" of relation \"%s\" does not exist, skipping",
							constrName, RelationGetRelationName(rel))));
			table_close(conrel, RowExclusiveLock);
			return;
		}
	}

	/*
	 * For partitioned tables, non-CHECK inherited constraints are dropped via
	 * the dependency mechanism, so we're done here.
	 */
	if (contype != CONSTRAINT_CHECK &&
		rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		table_close(conrel, RowExclusiveLock);
		return;
	}

	/*
	 * Propagate to children as appropriate.  Unlike most other ALTER
	 * routines, we have to do this one level of recursion at a time; we can't
	 * use find_all_inheritors to do it in one pass.
	 */
	if (!is_no_inherit_constraint)
		children = find_inheritance_children(RelationGetRelid(rel), lockmode);
	else
		children = NIL;

	/*
	 * For a partitioned table, if partitions exist and we are told not to
	 * recurse, it's a user error.  It doesn't make sense to have a constraint
	 * be defined only on the parent, especially if it's a partitioned table.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
		children != NIL && !recurse)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot remove constraint from only the partitioned table when partitions exist"),
				 errhint("Do not specify the ONLY keyword.")));

	foreach(child, children)
	{
		Oid			childrelid = lfirst_oid(child);
		Relation	childrel;
		HeapTuple	copy_tuple;

		/* find_inheritance_children already got lock */
		childrel = table_open(childrelid, NoLock);
		CheckTableNotInUse(childrel, "ALTER TABLE");

		ScanKeyInit(&skey[0],
					Anum_pg_constraint_conrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(childrelid));
		ScanKeyInit(&skey[1],
					Anum_pg_constraint_contypid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(InvalidOid));
		ScanKeyInit(&skey[2],
					Anum_pg_constraint_conname,
					BTEqualStrategyNumber, F_NAMEEQ,
					CStringGetDatum(constrName));
		scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId,
								  true, NULL, 3, skey);

		/* There can be at most one matching row */
		if (!HeapTupleIsValid(tuple = systable_getnext(scan)))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("constraint \"%s\" of relation \"%s\" does not exist",
							constrName,
							RelationGetRelationName(childrel))));

		copy_tuple = heap_copytuple(tuple);

		systable_endscan(scan);

		con = (Form_pg_constraint) GETSTRUCT(copy_tuple);

		/* Right now only CHECK constraints can be inherited */
		if (con->contype != CONSTRAINT_CHECK)
			elog(ERROR, "inherited constraint is not a CHECK constraint");

		if (con->coninhcount <= 0)	/* shouldn't happen */
			elog(ERROR, "relation %u has non-inherited constraint \"%s\"",
				 childrelid, constrName);

		if (recurse)
		{
			/*
			 * If the child constraint has other definition sources, just
			 * decrement its inheritance count; if not, recurse to delete it.
			 */
			if (con->coninhcount == 1 && !con->conislocal)
			{
				/* Time to delete this child constraint, too */
				ATExecDropConstraint(childrel, constrName, behavior,
									 true, true,
									 false, lockmode);
			}
			else
			{
				/* Child constraint must survive my deletion */
				con->coninhcount--;
				CatalogTupleUpdate(conrel, &copy_tuple->t_self, copy_tuple);

				/* Make update visible */
				CommandCounterIncrement();
			}
		}
		else
		{
			/*
			 * If we were told to drop ONLY in this table (no recursion), we
			 * need to mark the inheritors' constraints as locally defined
			 * rather than inherited.
			 */
			con->coninhcount--;
			con->conislocal = true;

			CatalogTupleUpdate(conrel, &copy_tuple->t_self, copy_tuple);

			/* Make update visible */
			CommandCounterIncrement();
		}

		heap_freetuple(copy_tuple);

		table_close(childrel, NoLock);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH) && MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "DROP CONSTRAINT");

	table_close(conrel, RowExclusiveLock);
}

/*
 * ALTER COLUMN TYPE
 *
 * Unlike other subcommand types, we do parse transformation for ALTER COLUMN
 * TYPE during phase 1 --- the AlterTableCmd passed in here is already
 * transformed (and must be, because we rely on some transformed fields).
 *
 * The point of this is that the execution of all ALTER COLUMN TYPEs for a
 * table will be done "in parallel" during phase 3, so all the USING
 * expressions should be parsed assuming the original column types.  Also,
 * this allows a USING expression to refer to a field that will be dropped.
 *
 * To make this work safely, AT_PASS_DROP then AT_PASS_ALTER_TYPE must be
 * the first two execution steps in phase 2; they must not see the effects
 * of any other subcommand types, since the USING expressions are parsed
 * against the unmodified table's state.
 */
static void
ATPrepAlterColumnType(List **wqueue,
					  AlteredTableInfo *tab, Relation rel,
					  bool recurse, bool recursing,
					  AlterTableCmd *cmd, LOCKMODE lockmode,
					  AlterTableUtilityContext *context)
{
	char	   *colName = cmd->name;
	ColumnDef  *def = (ColumnDef *) cmd->def;
	TypeName   *typeName = def->typeName;
	Node	   *transform = def->cooked_default;
	HeapTuple	tuple;
	Form_pg_attribute attTup;
	AttrNumber	attnum;
	Oid			targettype;
	int32		targettypmod;
	Oid			targetcollid;
	NewColumnValue *newval;
	ParseState *pstate = make_parsestate(NULL);
	AclResult	aclresult;
	Oid			new_opclass = InvalidOid;
	Oid			old_opclass = InvalidOid;
	Oid			old_opfamily = InvalidOid;
	Oid			new_opfamily = InvalidOid;
	bool		is_expr;

	if (rel->rd_rel->reloftype && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot alter column type of typed table")));

	/* lookup the attribute so we can check inheritance status */
	tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));
	attTup = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = attTup->attnum;

	/* Can't alter a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/*
	 * Don't alter inherited columns.  At outer level, there had better not be
	 * any inherited definition; when recursing, we assume this was checked at
	 * the parent level (see below).
	 */
	if (attTup->attinhcount > 0 && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot alter inherited column \"%s\"",
						colName)));

	/* Don't alter columns used in the partition key */
	if (has_partition_attrs(rel,
							bms_make_singleton(attnum - FirstLowInvalidHeapAttributeNumber),
							&is_expr))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot alter column \"%s\" because it is part of the partition key of relation \"%s\"",
						colName, RelationGetRelationName(rel))));

	/* Look up the target type */
	typenameTypeIdAndMod(NULL, typeName, &targettype, &targettypmod);

	aclresult = pg_type_aclcheck(targettype, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, targettype);

	/* And the collation */
	targetcollid = GetColumnDefCollation(NULL, def, targettype);

	/* make sure datatype is legal for a column */
	CheckAttributeType(colName, targettype, targetcollid,
					   list_make1_oid(rel->rd_rel->reltype),
					   0);

	/*
	 * If the column is part of the distribution key, look up the new operator
	 * class
	 */
	if (rel->rd_cdbpolicy)
	{
		GpPolicy   *policy = rel->rd_cdbpolicy;
		int			i;

		for (i = 0; i < policy->nattrs; i++)
		{
			if (policy->attrs[i] == attnum)
			{
				/* found it! */
				old_opclass = policy->opclasses[i];
				break;
			}
		}

		if (old_opclass)
		{
			old_opfamily = get_opclass_family(old_opclass);

			new_opclass = GetDefaultOpClass(targettype, HASH_AM_OID);
			if (new_opclass)
			{
				new_opfamily = get_opclass_family(new_opclass);

				if (new_opclass != old_opclass)
					tab->new_opclass = new_opclass;

				if (new_opfamily != old_opfamily)
					tab->dist_opfamily_changed = true;
			}
			else
			{
				/*
				 * The new datatype doesn't have a default operator class.
				 * We'll have to turn the table randomly distributed.
				 */
				new_opfamily = InvalidOid;
				tab->new_opclass = InvalidOid;
				tab->dist_opfamily_changed = true;
			}
		}
	}

	/*
	 * Distribution key cannot be changed in a non-compatible way. Unless the
	 * table is completely empty.
	 */
	if (Gp_role == GP_ROLE_DISPATCH &&
		GpPolicyIsPartitioned(rel->rd_cdbpolicy) &&
		tab->dist_opfamily_changed)
	{
		bool		relContainsTuples;

		relContainsTuples = cdbRelMaxSegSize(rel) > 0;
		if (relContainsTuples)
		{
			for (int ia = 0; ia < rel->rd_cdbpolicy->nattrs; ia++)
			{
				if (attnum == rel->rd_cdbpolicy->attrs[ia])
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot alter type of a column used in "
									"a distribution policy")));
			}
		}
	}

	if (tab->relkind == RELKIND_RELATION ||
		tab->relkind == RELKIND_PARTITIONED_TABLE)
	{
		/*
		 * Set up an expression to transform the old data value to the new
		 * type. If a USING option was given, use the expression as
		 * transformed by transformAlterTableStmt, else just take the old
		 * value and try to coerce it.  We do this first so that type
		 * incompatibility can be detected before we waste effort, and because
		 * we need the expression to be parsed against the original table row
		 * type.
		 */
		if (!transform)
		{
			transform = (Node *) makeVar(1, attnum,
										 attTup->atttypid, attTup->atttypmod,
										 attTup->attcollation,
										 0);
		}

		transform = coerce_to_target_type(pstate,
										  transform, exprType(transform),
										  targettype, targettypmod,
										  COERCION_ASSIGNMENT,
										  COERCE_IMPLICIT_CAST,
										  -1);
		if (transform == NULL)
		{
			/* error text depends on whether USING was specified or not */
			if (def->cooked_default != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("result of USING clause for column \"%s\""
								" cannot be cast automatically to type %s",
								colName, format_type_be(targettype)),
						 errhint("You might need to add an explicit cast.")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("column \"%s\" cannot be cast automatically to type %s",
								colName, format_type_be(targettype)),
				/* translator: USING is SQL, don't translate it */
						 errhint("You might need to specify \"USING %s::%s\".",
								 quote_identifier(colName),
								 format_type_with_typemod(targettype,
														  targettypmod))));
		}

		/* Fix collations after all else */
		assign_expr_collations(pstate, transform);

		/* Plan the expr now so we can accurately assess the need to rewrite. */
		transform = (Node *) expression_planner((Expr *) transform);

		/*
		 * Add a work queue item to make ATRewriteTable update the column
		 * contents.
		 */
		newval = (NewColumnValue *) palloc0(sizeof(NewColumnValue));
		newval->attnum = attnum;
		newval->expr = (Expr *) transform;
		newval->is_generated = false;

		tab->newvals = lappend(tab->newvals, newval);
		if (ATColumnChangeRequiresRewrite(transform, attnum))
			tab->rewrite |= AT_REWRITE_COLUMN_REWRITE;

		/*
		 * If the column is part of the distribution key, and the new opclass is not
		 * in the same family as the old one, we'll need to rewrite the table because
		 * the distribution changes. (Unless the new datatype is not hashable, in
		 * which case we're going to drop it from the distribution key, and make
		 * the table randomly distributed.)
		 */
		if (old_opfamily != new_opfamily && new_opfamily != InvalidOid)
			tab->rewrite |= AT_REWRITE_COLUMN_REWRITE;
	}
	else if (transform &&
			 rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE &&
			 rel_is_external_table(RelationGetRelid(rel)))
	{
		/* Just to give a better error message than "foo is not a table" */
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot specify a USING expression when altering an external table")));
	}
	else if (transform)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table",
						RelationGetRelationName(rel))));

	if (!RELKIND_HAS_STORAGE(tab->relkind))
	{
		/*
		 * For relations without storage, do this check now.  Regular tables
		 * will check it later when the table is being rewritten.
		 */
		find_composite_type_dependencies(rel->rd_rel->reltype, rel, NULL);
	}

	ReleaseSysCache(tuple);

	/*
	 * Recurse manually by queueing a new command for each child, if
	 * necessary. We cannot apply ATSimpleRecursion here because we need to
	 * remap attribute numbers in the USING expression, if any.
	 *
	 * If we are told not to recurse, there had better not be any child
	 * tables; else the alter would put them out of step.
	 */
	if (recurse)
	{
		Oid			relid = RelationGetRelid(rel);
		List	   *child_oids,
				   *child_numparents;
		ListCell   *lo,
				   *li;

		child_oids = find_all_inheritors(relid, lockmode,
										 &child_numparents);

		/*
		 * find_all_inheritors does the recursive search of the inheritance
		 * hierarchy, so all we have to do is process all of the relids in the
		 * list that it returns.
		 */
		forboth(lo, child_oids, li, child_numparents)
		{
			Oid			childrelid = lfirst_oid(lo);
			int			numparents = lfirst_int(li);
			Relation	childrel;
			HeapTuple	childtuple;
			Form_pg_attribute childattTup;

			if (childrelid == relid)
				continue;

			/* find_all_inheritors already got lock */
			childrel = relation_open(childrelid, NoLock);
			CheckTableNotInUse(childrel, "ALTER TABLE");

			/*
			 * Verify that the child doesn't have any inherited definitions of
			 * this column that came from outside this inheritance hierarchy.
			 * (renameatt makes a similar test, though in a different way
			 * because of its different recursion mechanism.)
			 */
			childtuple = SearchSysCacheAttName(RelationGetRelid(childrel),
											   colName);
			if (!HeapTupleIsValid(childtuple))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" of relation \"%s\" does not exist",
								colName, RelationGetRelationName(childrel))));
			childattTup = (Form_pg_attribute) GETSTRUCT(childtuple);

			if (childattTup->attinhcount > numparents)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("cannot alter inherited column \"%s\" of relation \"%s\"",
								colName, RelationGetRelationName(childrel))));

			ReleaseSysCache(childtuple);

			/*
			 * Remap the attribute numbers.  If no USING expression was
			 * specified, there is no need for this step.
			 */
			if (def->cooked_default)
			{
				AttrMap    *attmap;
				bool		found_whole_row;

				/* create a copy to scribble on */
				cmd = copyObject(cmd);

				attmap = build_attrmap_by_name(RelationGetDescr(childrel),
											   RelationGetDescr(rel));
				((ColumnDef *) cmd->def)->cooked_default =
					map_variable_attnos(def->cooked_default,
										1, 0,
										attmap,
										InvalidOid, &found_whole_row);
				if (found_whole_row)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot convert whole-row table reference"),
							 errdetail("USING expression contains a whole-row table reference.")));
				pfree(attmap);
			}
			ATPrepCmd(wqueue, childrel, cmd, false, true, lockmode, context);
			relation_close(childrel, NoLock);
		}
	}
	else if (!recursing &&
			 find_inheritance_children(RelationGetRelid(rel), NoLock) != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("type of inherited column \"%s\" must be changed in child tables too",
						colName)));

	if (tab->relkind == RELKIND_COMPOSITE_TYPE)
		ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);
}

/*
 * When the data type of a column is changed, a rewrite might not be required
 * if the new type is sufficiently identical to the old one, and the USING
 * clause isn't trying to insert some other value.  It's safe to skip the
 * rewrite in these cases:
 *
 * - the old type is binary coercible to the new type
 * - the new type is an unconstrained domain over the old type
 * - {NEW,OLD} or {OLD,NEW} is {timestamptz,timestamp} and the timezone is UTC
 *
 * In the case of a constrained domain, we could get by with scanning the
 * table and checking the constraint rather than actually rewriting it, but we
 * don't currently try to do that.
 */
static bool
ATColumnChangeRequiresRewrite(Node *expr, AttrNumber varattno)
{
	Assert(expr != NULL);

	for (;;)
	{
		/* only one varno, so no need to check that */
		if (IsA(expr, Var) && ((Var *) expr)->varattno == varattno)
			return false;
		else if (IsA(expr, RelabelType))
			expr = (Node *) ((RelabelType *) expr)->arg;
		else if (IsA(expr, CoerceToDomain))
		{
			CoerceToDomain *d = (CoerceToDomain *) expr;

			if (DomainHasConstraints(d->resulttype))
				return true;
			expr = (Node *) d->arg;
		}
		else if (IsA(expr, FuncExpr))
		{
			FuncExpr   *f = (FuncExpr *) expr;

			switch (f->funcid)
			{
				case F_TIMESTAMPTZ_TIMESTAMP:
				case F_TIMESTAMP_TIMESTAMPTZ:
					if (TimestampTimestampTzRequiresRewrite())
						return true;
					else
						expr = linitial(f->args);
					break;
				default:
					return true;
			}
		}
		else
			return true;
	}
}

/*
 * ALTER COLUMN .. SET DATA TYPE
 *
 * Return the address of the modified column.
 */
static ObjectAddress
ATExecAlterColumnType(AlteredTableInfo *tab, Relation rel,
					  AlterTableCmd *cmd, LOCKMODE lockmode)
{
	char	   *colName = cmd->name;
	ColumnDef  *def = (ColumnDef *) cmd->def;
	TypeName   *typeName = def->typeName;
	HeapTuple	heapTup;
	Form_pg_attribute attTup,
				attOldTup;
	AttrNumber	attnum;
	HeapTuple	typeTuple;
	Form_pg_type tform;
	Oid			targettype;
	int32		targettypmod;
	Oid			targetcollid;
	Node	   *defaultexpr;
	Relation	attrelation;
	Relation	depRel;
	ScanKeyData key[3];
	SysScanDesc scan;
	HeapTuple	depTup;
	ObjectAddress address;

	/*
	 * Clear all the missing values if we're rewriting the table, since this
	 * renders them pointless.
	 */
	if (tab->rewrite)
	{
		Relation	newrel;

		newrel = table_open(RelationGetRelid(rel), NoLock);
		RelationClearMissing(newrel);
		relation_close(newrel, NoLock);
		/* make sure we don't conflict with later attribute modifications */
		CommandCounterIncrement();
	}

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);

	/* Look up the target column */
	heapTup = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(heapTup)) /* shouldn't happen */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));
	attTup = (Form_pg_attribute) GETSTRUCT(heapTup);
	attnum = attTup->attnum;
	attOldTup = TupleDescAttr(tab->oldDesc, attnum - 1);

	/* Check for multiple ALTER TYPE on same column --- can't cope */
	if (attTup->atttypid != attOldTup->atttypid ||
		attTup->atttypmod != attOldTup->atttypmod)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter type of column \"%s\" twice",
						colName)));

	/* Look up the target type (should not fail, since prep found it) */
	typeTuple = typenameType(NULL, typeName, &targettypmod);
	tform = (Form_pg_type) GETSTRUCT(typeTuple);
	targettype = tform->oid;
	/* And the collation */
	targetcollid = GetColumnDefCollation(NULL, def, targettype);

	/*
	 * If there is a default expression for the column, get it and ensure we
	 * can coerce it to the new datatype.  (We must do this before changing
	 * the column type, because build_column_default itself will try to
	 * coerce, and will not issue the error message we want if it fails.)
	 *
	 * We remove any implicit coercion steps at the top level of the old
	 * default expression; this has been agreed to satisfy the principle of
	 * least surprise.  (The conversion to the new column type should act like
	 * it started from what the user sees as the stored expression, and the
	 * implicit coercions aren't going to be shown.)
	 */
	if (attTup->atthasdef)
	{
		defaultexpr = build_column_default(rel, attnum);
		Assert(defaultexpr);
		defaultexpr = strip_implicit_coercions(defaultexpr);
		defaultexpr = coerce_to_target_type(NULL,	/* no UNKNOWN params */
											defaultexpr, exprType(defaultexpr),
											targettype, targettypmod,
											COERCION_ASSIGNMENT,
											COERCE_IMPLICIT_CAST,
											-1);
		if (defaultexpr == NULL)
		{
			if (attTup->attgenerated)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("generation expression for column \"%s\" cannot be cast automatically to type %s",
								colName, format_type_be(targettype))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("default for column \"%s\" cannot be cast automatically to type %s",
								colName, format_type_be(targettype))));
		}
	}
	else
		defaultexpr = NULL;

	/*
	 * Find everything that depends on the column (constraints, indexes, etc),
	 * and record enough information to let us recreate the objects.
	 *
	 * The actual recreation does not happen here, but only after we have
	 * performed all the individual ALTER TYPE operations.  We have to save
	 * the info before executing ALTER TYPE, though, else the deparser will
	 * get confused.
	 */
	depRel = table_open(DependRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	ScanKeyInit(&key[2],
				Anum_pg_depend_refobjsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum((int32) attnum));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(depTup = systable_getnext(scan)))
	{
		Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(depTup);
		ObjectAddress foundObject;

		/* We don't expect any PIN dependencies on columns */
		if (foundDep->deptype == DEPENDENCY_PIN)
			elog(ERROR, "cannot alter type of a pinned column");

		foundObject.classId = foundDep->classid;
		foundObject.objectId = foundDep->objid;
		foundObject.objectSubId = foundDep->objsubid;

		switch (getObjectClass(&foundObject))
		{
			case OCLASS_CLASS:
				{
					char		relKind = get_rel_relkind(foundObject.objectId);

					if (relKind == RELKIND_INDEX ||
						relKind == RELKIND_PARTITIONED_INDEX)
					{
						Assert(foundObject.objectSubId == 0);
						RememberIndexForRebuilding(foundObject.objectId, tab);
					}
					else if (relKind == RELKIND_SEQUENCE)
					{
						/*
						 * This must be a SERIAL column's sequence.  We need
						 * not do anything to it.
						 */
						Assert(foundObject.objectSubId == 0);
					}
					else if (relKind == RELKIND_RELATION &&
							 foundObject.objectSubId != 0 &&
							 get_attgenerated(foundObject.objectId, foundObject.objectSubId))
					{
						/*
						 * Changing the type of a column that is used by a
						 * generated column is not allowed by SQL standard. It
						 * might be doable with some thinking and effort.
						 */
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("cannot alter type of a column used by a generated column"),
								 errdetail("Column \"%s\" is used by generated column \"%s\".",
										   colName, get_attname(foundObject.objectId, foundObject.objectSubId, false))));
					}
					else
					{
						/* Not expecting any other direct dependencies... */
						elog(ERROR, "unexpected object depending on column: %s",
							 getObjectDescription(&foundObject, false));
					}
					break;
				}

			case OCLASS_CONSTRAINT:
				Assert(foundObject.objectSubId == 0);
				RememberConstraintForRebuilding(foundObject.objectId, tab);
				break;

			case OCLASS_REWRITE:
				/* XXX someday see if we can cope with revising views */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter type of a column used by a view or rule"),
						 errdetail("%s depends on column \"%s\"",
								   getObjectDescription(&foundObject, false),
								   colName)));
				break;

			case OCLASS_TRIGGER:

				/*
				 * A trigger can depend on a column because the column is
				 * specified as an update target, or because the column is
				 * used in the trigger's WHEN condition.  The first case would
				 * not require any extra work, but the second case would
				 * require updating the WHEN expression, which will take a
				 * significant amount of new code.  Since we can't easily tell
				 * which case applies, we punt for both.  FIXME someday.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter type of a column used in a trigger definition"),
						 errdetail("%s depends on column \"%s\"",
								   getObjectDescription(&foundObject, false),
								   colName)));
				break;

			case OCLASS_POLICY:

				/*
				 * A policy can depend on a column because the column is
				 * specified in the policy's USING or WITH CHECK qual
				 * expressions.  It might be possible to rewrite and recheck
				 * the policy expression, but punt for now.  It's certainly
				 * easy enough to remove and recreate the policy; still, FIXME
				 * someday.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter type of a column used in a policy definition"),
						 errdetail("%s depends on column \"%s\"",
								   getObjectDescription(&foundObject, false),
								   colName)));
				break;

			case OCLASS_DEFAULT:

				/*
				 * Ignore the column's default expression, since we will fix
				 * it below.
				 */
				Assert(defaultexpr);
				break;

			case OCLASS_STATISTIC_EXT:

				/*
				 * Give the extended-stats machinery a chance to fix anything
				 * that this column type change would break.
				 */
				RememberStatisticsForRebuilding(foundObject.objectId, tab);
				break;

			case OCLASS_PROC:
			case OCLASS_TYPE:
			case OCLASS_CAST:
			case OCLASS_COLLATION:
			case OCLASS_CONVERSION:
			case OCLASS_LANGUAGE:
			case OCLASS_LARGEOBJECT:
			case OCLASS_OPERATOR:
			case OCLASS_OPCLASS:
			case OCLASS_OPFAMILY:
			case OCLASS_AM:
			case OCLASS_AMOP:
			case OCLASS_AMPROC:
			case OCLASS_SCHEMA:
			case OCLASS_TSPARSER:
			case OCLASS_TSDICT:
			case OCLASS_TSTEMPLATE:
			case OCLASS_TSCONFIG:
			case OCLASS_ROLE:
			case OCLASS_DATABASE:
			case OCLASS_TBLSPACE:
			case OCLASS_FDW:
			case OCLASS_FOREIGN_SERVER:
			case OCLASS_USER_MAPPING:
			case OCLASS_DEFACL:
			case OCLASS_EXTENSION:
			case OCLASS_EVENT_TRIGGER:
			case OCLASS_PUBLICATION:
			case OCLASS_PUBLICATION_REL:
			case OCLASS_SUBSCRIPTION:
			case OCLASS_TRANSFORM:
			case OCLASS_EXTPROTOCOL:
			case OCLASS_TASK:
			case OCLASS_PROFILE:
			case OCLASS_PASSWORDHISTORY:
			case OCLASS_MATVIEW_AUX:
			case OCLASS_STORAGE_SERVER:
			case OCLASS_STORAGE_USER_MAPPING:
			case OCLASS_TAG:
			case OCLASS_TAG_DESCRIPTION:

				/*
				 * We don't expect any of these sorts of objects to depend on
				 * a column.
				 */
				elog(ERROR, "unexpected object depending on column: %s",
					 getObjectDescription(&foundObject, false));
				break;

			default:
			{
				struct CustomObjectClass *coc;
				coc = find_custom_object_class_by_classid(foundObject.classId, false);
				if (coc->alter_column_type)
					coc->alter_column_type(coc);
				/*
				 * There's intentionally no default: case here; we want the
				 * compiler to warn if a new OCLASS hasn't been handled above.
				 */
				break;
			}
		}
	}

	systable_endscan(scan);

	/*
	 * Now scan for dependencies of this column on other things.  The only
	 * thing we should find is the dependency on the column datatype, which we
	 * want to remove, possibly a collation dependency, and dependencies on
	 * other columns if it is a generated column.
	 */
	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	ScanKeyInit(&key[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum((int32) attnum));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(depTup = systable_getnext(scan)))
	{
		Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(depTup);
		ObjectAddress foundObject;

		foundObject.classId = foundDep->refclassid;
		foundObject.objectId = foundDep->refobjid;
		foundObject.objectSubId = foundDep->refobjsubid;

		if (foundDep->deptype != DEPENDENCY_NORMAL &&
			foundDep->deptype != DEPENDENCY_AUTO)
			elog(ERROR, "found unexpected dependency type '%c'",
				 foundDep->deptype);
		if (!(foundDep->refclassid == TypeRelationId &&
			  foundDep->refobjid == attTup->atttypid) &&
			!(foundDep->refclassid == CollationRelationId &&
			  foundDep->refobjid == attTup->attcollation) &&
			!(foundDep->refclassid == RelationRelationId &&
			  foundDep->refobjid == RelationGetRelid(rel) &&
			  foundDep->refobjsubid != 0)
			)
			elog(ERROR, "found unexpected dependency for column: %s",
				 getObjectDescription(&foundObject, false));

		CatalogTupleDelete(depRel, &depTup->t_self);
	}

	systable_endscan(scan);

	table_close(depRel, RowExclusiveLock);

	/*
	 * Here we go --- change the recorded column type and collation.  (Note
	 * heapTup is a copy of the syscache entry, so okay to scribble on.) First
	 * fix up the missing value if any.
	 */
	if (attTup->atthasmissing)
	{
		Datum		missingval;
		bool		missingNull;

		/* if rewrite is true the missing value should already be cleared */
		Assert(tab->rewrite == 0);

		/* Get the missing value datum */
		missingval = heap_getattr(heapTup,
								  Anum_pg_attribute_attmissingval,
								  attrelation->rd_att,
								  &missingNull);

		/* if it's a null array there is nothing to do */

		if (!missingNull)
		{
			/*
			 * Get the datum out of the array and repack it in a new array
			 * built with the new type data. We assume that since the table
			 * doesn't need rewriting, the actual Datum doesn't need to be
			 * changed, only the array metadata.
			 */

			int			one = 1;
			bool		isNull;
			Datum		valuesAtt[Natts_pg_attribute];
			bool		nullsAtt[Natts_pg_attribute];
			bool		replacesAtt[Natts_pg_attribute];
			HeapTuple	newTup;

			MemSet(valuesAtt, 0, sizeof(valuesAtt));
			MemSet(nullsAtt, false, sizeof(nullsAtt));
			MemSet(replacesAtt, false, sizeof(replacesAtt));

			missingval = array_get_element(missingval,
										   1,
										   &one,
										   0,
										   attTup->attlen,
										   attTup->attbyval,
										   attTup->attalign,
										   &isNull);
			missingval = PointerGetDatum(construct_array(&missingval,
														 1,
														 targettype,
														 tform->typlen,
														 tform->typbyval,
														 tform->typalign));

			valuesAtt[Anum_pg_attribute_attmissingval - 1] = missingval;
			replacesAtt[Anum_pg_attribute_attmissingval - 1] = true;
			nullsAtt[Anum_pg_attribute_attmissingval - 1] = false;

			newTup = heap_modify_tuple(heapTup, RelationGetDescr(attrelation),
									   valuesAtt, nullsAtt, replacesAtt);
			heap_freetuple(heapTup);
			heapTup = newTup;
			attTup = (Form_pg_attribute) GETSTRUCT(heapTup);
		}
	}

	attTup->atttypid = targettype;
	attTup->atttypmod = targettypmod;
	attTup->attcollation = targetcollid;
	attTup->attndims = list_length(typeName->arrayBounds);
	attTup->attlen = tform->typlen;
	attTup->attbyval = tform->typbyval;
	attTup->attalign = tform->typalign;
	attTup->attstorage = tform->typstorage;
	attTup->attcompression = InvalidCompressionMethod;

	ReleaseSysCache(typeTuple);

	CatalogTupleUpdate(attrelation, &heapTup->t_self, heapTup);

	table_close(attrelation, RowExclusiveLock);

	/* Update gp_distribution_policy */
	if (tab->dist_opfamily_changed || tab->new_opclass)
	{
		GpPolicy   *newpolicy;
		int			i;

		if (tab->dist_opfamily_changed && tab->new_opclass == InvalidOid)
		{
			/*
			 * The column was part of the distribution key, but the new datatype
			 * is not hashable. Make it randomly distributed.
			 *
			 * XXX: Perhaps a NOTICE would be in order? Or an ERROR?
			 */
			newpolicy = createRandomPartitionedPolicy(rel->rd_cdbpolicy->numsegments);
		}
		else
		{
			newpolicy = GpPolicyCopy(rel->rd_cdbpolicy);
			for (i = 0; i < newpolicy->nattrs; i++)
			{
				if (newpolicy->attrs[i] == attnum)
					newpolicy->opclasses[i] = tab->new_opclass;
			}
		}

		GpPolicyReplace(RelationGetRelid(rel), newpolicy);
	}

	/* Install dependencies on new datatype and collation */
	add_column_datatype_dependency(RelationGetRelid(rel), attnum, targettype);
	add_column_collation_dependency(RelationGetRelid(rel), attnum, targetcollid);

	/*
	 * Drop any pg_statistic entry for the column, since it's now wrong type
	 */
	RemoveStatistics(RelationGetRelid(rel), attnum);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel), attnum);

	/*
	 * Update the default, if present, by brute force --- remove and re-add
	 * the default.  Probably unsafe to take shortcuts, since the new version
	 * may well have additional dependencies.  (It's okay to do this now,
	 * rather than after other ALTER TYPE commands, since the default won't
	 * depend on other column types.)
	 */
	if (defaultexpr)
	{
		/* Must make new row visible since it will be updated again */
		CommandCounterIncrement();

		/*
		 * We use RESTRICT here for safety, but at present we do not expect
		 * anything to depend on the default.
		 */
		RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, true,
						  true);

		StoreAttrDefault(rel, attnum, defaultexpr,
						 NULL, NULL, NULL, /* missing val stuff */
						 true, false);
	}

	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);

	/* Cleanup */
	heap_freetuple(heapTup);

	/* metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH) && MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN TYPE");
	return address;
}

/*
 * Subroutine for ATExecAlterColumnType: remember that a replica identity
 * needs to be reset.
 */
static void
RememberReplicaIdentityForRebuilding(Oid indoid, AlteredTableInfo *tab)
{
	if (!get_index_isreplident(indoid))
		return;

	if (tab->replicaIdentityIndex)
		elog(ERROR, "relation %u has multiple indexes marked as replica identity", tab->relid);

	tab->replicaIdentityIndex = get_rel_name(indoid);
}

/*
 * Subroutine for ATExecAlterColumnType: remember any clustered index.
 */
static void
RememberClusterOnForRebuilding(Oid indoid, AlteredTableInfo *tab)
{
	if (!get_index_isclustered(indoid))
		return;

	if (tab->clusterOnIndex)
		elog(ERROR, "relation %u has multiple clustered indexes", tab->relid);

	tab->clusterOnIndex = get_rel_name(indoid);
}

/*
 * Subroutine for ATExecAlterColumnType: remember that a constraint needs
 * to be rebuilt (which we might already know).
 */
static void
RememberConstraintForRebuilding(Oid conoid, AlteredTableInfo *tab)
{
	/*
	 * This de-duplication check is critical for two independent reasons: we
	 * mustn't try to recreate the same constraint twice, and if a constraint
	 * depends on more than one column whose type is to be altered, we must
	 * capture its definition string before applying any of the column type
	 * changes.  ruleutils.c will get confused if we ask again later.
	 */
	if (!list_member_oid(tab->changedConstraintOids, conoid))
	{
		/* OK, capture the constraint's existing definition string */
		char	   *defstring = pg_get_constraintdef_command(conoid);
		Oid			indoid;

		tab->changedConstraintOids = lappend_oid(tab->changedConstraintOids,
												 conoid);
		tab->changedConstraintDefs = lappend(tab->changedConstraintDefs,
											 defstring);

		/*
		 * For the index of a constraint, if any, remember if it is used for
		 * the table's replica identity or if it is a clustered index, so that
		 * ATPostAlterTypeCleanup() can queue up commands necessary to restore
		 * those properties.
		 */
		indoid = get_constraint_index(conoid);
		if (OidIsValid(indoid))
		{
			RememberReplicaIdentityForRebuilding(indoid, tab);
			RememberClusterOnForRebuilding(indoid, tab);
		}
	}
}

/*
 * Subroutine for ATExecAlterColumnType: remember that an index needs
 * to be rebuilt (which we might already know).
 */
static void
RememberIndexForRebuilding(Oid indoid, AlteredTableInfo *tab)
{
	/*
	 * This de-duplication check is critical for two independent reasons: we
	 * mustn't try to recreate the same index twice, and if an index depends
	 * on more than one column whose type is to be altered, we must capture
	 * its definition string before applying any of the column type changes.
	 * ruleutils.c will get confused if we ask again later.
	 */
	if (!list_member_oid(tab->changedIndexOids, indoid))
	{
		/*
		 * Before adding it as an index-to-rebuild, we'd better see if it
		 * belongs to a constraint, and if so rebuild the constraint instead.
		 * Typically this check fails, because constraint indexes normally
		 * have only dependencies on their constraint.  But it's possible for
		 * such an index to also have direct dependencies on table columns,
		 * for example with a partial exclusion constraint.
		 */
		Oid			conoid = get_index_constraint(indoid);

		if (OidIsValid(conoid))
		{
			RememberConstraintForRebuilding(conoid, tab);
		}
		else
		{
			/* OK, capture the index's existing definition string */
			char	   *defstring = pg_get_indexdef_string(indoid);

			tab->changedIndexOids = lappend_oid(tab->changedIndexOids,
												indoid);
			tab->changedIndexDefs = lappend(tab->changedIndexDefs,
											defstring);

			/*
			 * Remember if this index is used for the table's replica identity
			 * or if it is a clustered index, so that ATPostAlterTypeCleanup()
			 * can queue up commands necessary to restore those properties.
			 */
			RememberReplicaIdentityForRebuilding(indoid, tab);
			RememberClusterOnForRebuilding(indoid, tab);
		}
	}
}

/*
 * Subroutine for ATExecAlterColumnType: remember that a statistics object
 * needs to be rebuilt (which we might already know).
 */
static void
RememberStatisticsForRebuilding(Oid stxoid, AlteredTableInfo *tab)
{
	/*
	 * This de-duplication check is critical for two independent reasons: we
	 * mustn't try to recreate the same statistics object twice, and if the
	 * statistics object depends on more than one column whose type is to be
	 * altered, we must capture its definition string before applying any of
	 * the type changes. ruleutils.c will get confused if we ask again later.
	 */
	if (!list_member_oid(tab->changedStatisticsOids, stxoid))
	{
		/* OK, capture the statistics object's existing definition string */
		char	   *defstring = pg_get_statisticsobjdef_string(stxoid);

		tab->changedStatisticsOids = lappend_oid(tab->changedStatisticsOids,
												 stxoid);
		tab->changedStatisticsDefs = lappend(tab->changedStatisticsDefs,
											 defstring);
	}
}

/*
 * Cleanup after we've finished all the ALTER TYPE operations for a
 * particular relation.  We have to drop and recreate all the indexes
 * and constraints that depend on the altered columns.  We do the
 * actual dropping here, but re-creation is managed by adding work
 * queue entries to do those steps later.
 */
static void
ATPostAlterTypeCleanup(List **wqueue, AlteredTableInfo *tab, LOCKMODE lockmode,
					   AlterTableUtilityContext *context)
{
	ObjectAddress obj;
	ObjectAddresses *objects;
	ListCell   *def_item;
	ListCell   *oid_item;

	/*
	 * Collect all the constraints and indexes to drop so we can process them
	 * in a single call.  That way we don't have to worry about dependencies
	 * among them.
	 */
	objects = new_object_addresses();

	/*
	 * Re-parse the index and constraint definitions, and attach them to the
	 * appropriate work queue entries.  We do this before dropping because in
	 * the case of a FOREIGN KEY constraint, we might not yet have exclusive
	 * lock on the table the constraint is attached to, and we need to get
	 * that before reparsing/dropping.
	 *
	 * We can't rely on the output of deparsing to tell us which relation to
	 * operate on, because concurrent activity might have made the name
	 * resolve differently.  Instead, we've got to use the OID of the
	 * constraint or index we're processing to figure out which relation to
	 * operate on.
	 */
	forboth(oid_item, tab->changedConstraintOids,
			def_item, tab->changedConstraintDefs)
	{
		Oid			oldId = lfirst_oid(oid_item);
		HeapTuple	tup;
		Form_pg_constraint con;
		Oid			relid;
		Oid			confrelid;
		char		contype;
		bool		conislocal;

		tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(oldId));
		if (!HeapTupleIsValid(tup)) /* should not happen */
			elog(ERROR, "cache lookup failed for constraint %u", oldId);
		con = (Form_pg_constraint) GETSTRUCT(tup);
		if (OidIsValid(con->conrelid))
			relid = con->conrelid;
		else
		{
			/* must be a domain constraint */
			relid = get_typ_typrelid(getBaseType(con->contypid));
			if (!OidIsValid(relid))
				elog(ERROR, "could not identify relation associated with constraint %u", oldId);
		}
		confrelid = con->confrelid;
		contype = con->contype;
		conislocal = con->conislocal;
		ReleaseSysCache(tup);

		if (contype == CONSTRAINT_PRIMARY || contype == CONSTRAINT_UNIQUE)
		{
			/*
			 * Currently, GPDB doesn't support alter type on primary key and unique
			 * constraint column. Because it requires drop - recreate logic.
			 * The drop currently only performs on master which lead error when
			 * recreating index (since recreate index will dispatch to segments and
			 * there still old constraint index exists)
			 * Related issue: https://github.com/greenplum-db/gpdb/issues/10561.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot alter column with primary key or unique constraint"),
					 errhint("DROP the constraint first, and recreate it after the ALTER")));
		}

		ObjectAddressSet(obj, ConstraintRelationId, oldId);
		add_exact_object_address(&obj, objects);

		/*
		 * If the constraint is inherited (only), we don't want to inject a
		 * new definition here; it'll get recreated when ATAddCheckConstraint
		 * recurses from adding the parent table's constraint.  But we had to
		 * carry the info this far so that we can drop the constraint below.
		 */
		if (!conislocal)
			continue;

		/*
		 * When rebuilding an FK constraint that references the table we're
		 * modifying, we might not yet have any lock on the FK's table, so get
		 * one now.  We'll need AccessExclusiveLock for the DROP CONSTRAINT
		 * step, so there's no value in asking for anything weaker.
		 */
		if (relid != tab->relid && contype == CONSTRAINT_FOREIGN)
			LockRelationOid(relid, AccessExclusiveLock);

		ATPostAlterTypeParse(oldId, relid, confrelid,
							 (char *) lfirst(def_item),
							 wqueue, lockmode, tab->rewrite, context);
	}
	forboth(oid_item, tab->changedIndexOids,
			def_item, tab->changedIndexDefs)
	{
		Oid			oldId = lfirst_oid(oid_item);
		Oid			relid;

		relid = IndexGetRelation(oldId, false);
		ATPostAlterTypeParse(oldId, relid, InvalidOid,
							 (char *) lfirst(def_item),
							 wqueue, lockmode, tab->rewrite, context);

		ObjectAddressSet(obj, RelationRelationId, oldId);
		add_exact_object_address(&obj, objects);
	}

	/* add dependencies for new statistics */
	forboth(oid_item, tab->changedStatisticsOids,
			def_item, tab->changedStatisticsDefs)
	{
		Oid			oldId = lfirst_oid(oid_item);
		Oid			relid;

		relid = StatisticsGetRelation(oldId, false);
		ATPostAlterTypeParse(oldId, relid, InvalidOid,
							 (char *) lfirst(def_item),
							 wqueue, lockmode, tab->rewrite, context);

		ObjectAddressSet(obj, StatisticExtRelationId, oldId);
		add_exact_object_address(&obj, objects);
	}

	/*
	 * Queue up command to restore replica identity index marking
	 */
	if (tab->replicaIdentityIndex && Gp_role != GP_ROLE_EXECUTE)
	{
		Assert(context != NULL);

		AlterTableCmd *cmd = makeNode(AlterTableCmd);
		ReplicaIdentityStmt *subcmd = makeNode(ReplicaIdentityStmt);

		subcmd->identity_type = REPLICA_IDENTITY_INDEX;
		subcmd->name = tab->replicaIdentityIndex;
		cmd->subtype = AT_ReplicaIdentity;
		cmd->def = (Node *) subcmd;

		/* do it after indexes and constraints */
		tab->subcmds[AT_PASS_OLD_CONSTR] =
			lappend(tab->subcmds[AT_PASS_OLD_CONSTR], cmd);
	}

	/*
	 * Queue up command to restore marking of index used for cluster.
	 */
	if (tab->clusterOnIndex && Gp_role != GP_ROLE_EXECUTE)
	{
		Assert(context != NULL);

		AlterTableCmd *cmd = makeNode(AlterTableCmd);

		cmd->subtype = AT_ClusterOn;
		cmd->name = tab->clusterOnIndex;

		/* do it after indexes and constraints */
		tab->subcmds[AT_PASS_OLD_CONSTR] =
			lappend(tab->subcmds[AT_PASS_OLD_CONSTR], cmd);
	}

	/*
	 * It should be okay to use DROP_RESTRICT here, since nothing else should
	 * be depending on these objects.
	 */
	performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	free_object_addresses(objects);

	/*
	 * The objects will get recreated during subsequent passes over the work
	 * queue.
	 */
}

/*
 * Parse the previously-saved definition string for a constraint, index or
 * statistics object against the newly-established column data type(s), and
 * queue up the resulting command parsetrees for execution.
 *
 * This might fail if, for example, you have a WHERE clause that uses an
 * operator that's not available for the new column type.
 */
static void
ATPostAlterTypeParse(Oid oldId, Oid oldRelId, Oid refRelId, char *cmd,
					 List **wqueue, LOCKMODE lockmode, bool rewrite,
					 AlterTableUtilityContext *context)
{
	List	   *raw_parsetree_list;
	List	   *querytree_list;
	ListCell   *list_item;
	Relation	rel;

	/*
	 * In the QE, don't add items to the work queues. They were already
	 * added in the QD, and we don't want to do them twice.
	 * If ATController() is called by internal utility command, not driven
	 * by QD, we still need to add items to the work queue.
	 * See details at ATController
	 *
	 * However, we need to check if we are to reuse the relfilenode of an
	 * existing index. This information is unique to QD and each QE. If so,
	 * we need to replace that with QE's own relfilenode. Note that this is
	 * not a problem for foreign key operator oids (see TryReuseForeignKey)
	 * since those are the same among QD/QEs.
	 */
	if (Gp_role == GP_ROLE_EXECUTE && context != NULL)
	{
		ListCell 		*lc;
		Relation 		rel;
		Relation 		irel = NULL;
		AlteredTableInfo 	*tab;

		/* Caller should already have acquired whatever lock we need. */
		rel = relation_open(oldRelId, NoLock);
		tab = ATGetQueueEntry(wqueue, rel);

		/* Check the commands I got from QD for any re-used index. */
		foreach (lc, tab->subcmds[AT_PASS_OLD_INDEX])
		{
			AlterTableCmd 	*cmd = castNode(AlterTableCmd, lfirst(lc));
			IndexStmt 	*stmt;

			Assert(cmd->subtype == AT_ReAddIndex);

			stmt = (IndexStmt *) cmd->def;

			/* if we are not reusing this index, continue */
			if (!OidIsValid(stmt->oldNode))
				continue;

			if (irel == NULL)
			{
				Oid 		indoid = InvalidOid;
				HeapTuple 	tup;

				tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(oldId));
				if (HeapTupleIsValid(tup))
					indoid = oldId;
				else
					/* not an index, them it must be a constraint */
					indoid = get_constraint_index(oldId);

				if (HeapTupleIsValid(tup))
					ReleaseSysCache(tup);

				Assert(OidIsValid(indoid));

				/* We might not open index on QE, so acquire a valid lock */
				irel = index_open(indoid, AccessShareLock);
			}

			/* replace it with my own */
			stmt->oldNode = irel->rd_node.relNode;
		}

		if (irel != NULL)
			index_close(irel, AccessShareLock);
		relation_close(rel, NoLock);
		return;
	}

	/*
	 * We expect that we will get only ALTER TABLE and CREATE INDEX
	 * statements. Hence, there is no need to pass them through
	 * parse_analyze() or the rewriter, but instead we need to pass them
	 * through parse_utilcmd.c to make them ready for execution.
	 */
	raw_parsetree_list = raw_parser(cmd, RAW_PARSE_DEFAULT);
	querytree_list = NIL;
	foreach(list_item, raw_parsetree_list)
	{
		RawStmt    *rs = lfirst_node(RawStmt, list_item);
		Node	   *stmt = rs->stmt;

		if (IsA(stmt, IndexStmt))
			querytree_list = lappend(querytree_list,
									 transformIndexStmt(oldRelId,
														(IndexStmt *) stmt,
														cmd));
		else if (IsA(stmt, AlterTableStmt))
		{
			List	   *beforeStmts;
			List	   *afterStmts;

			stmt = (Node *) transformAlterTableStmt(oldRelId,
													(AlterTableStmt *) stmt,
													cmd,
													&beforeStmts,
													&afterStmts);
			querytree_list = list_concat(querytree_list, beforeStmts);
			querytree_list = lappend(querytree_list, stmt);
			querytree_list = list_concat(querytree_list, afterStmts);
		}
		else if (IsA(stmt, CreateStatsStmt))
			querytree_list = lappend(querytree_list,
									 transformStatsStmt(oldRelId,
														(CreateStatsStmt *) stmt,
														cmd));
		else
			querytree_list = lappend(querytree_list, stmt);
	}

	/* Caller should already have acquired whatever lock we need. */
	rel = relation_open(oldRelId, NoLock);

	/*
	 * Attach each generated command to the proper place in the work queue.
	 * Note this could result in creation of entirely new work-queue entries.
	 *
	 * Also note that we have to tweak the command subtypes, because it turns
	 * out that re-creation of indexes and constraints has to act a bit
	 * differently from initial creation.
	 */
	foreach(list_item, querytree_list)
	{
		Node	   *stm = (Node *) lfirst(list_item);
		AlteredTableInfo *tab;

		tab = ATGetQueueEntry(wqueue, rel);

		if (IsA(stm, IndexStmt))
		{
			IndexStmt  *stmt = (IndexStmt *) stm;
			AlterTableCmd *newcmd;

			if (!rewrite)
				TryReuseIndex(oldId, stmt);
			stmt->reset_default_tblspc = true;
			/* keep the index's comment */
			stmt->idxcomment = GetComment(oldId, RelationRelationId, 0);

			newcmd = makeNode(AlterTableCmd);
			newcmd->subtype = AT_ReAddIndex;
			newcmd->def = (Node *) stmt;
			tab->subcmds[AT_PASS_OLD_INDEX] =
				lappend(tab->subcmds[AT_PASS_OLD_INDEX], newcmd);
		}
		else if (IsA(stm, AlterTableStmt))
		{
			AlterTableStmt *stmt = (AlterTableStmt *) stm;
			ListCell   *lcmd;

			foreach(lcmd, stmt->cmds)
			{
				AlterTableCmd *cmd = castNode(AlterTableCmd, lfirst(lcmd));

				if (cmd->subtype == AT_AddIndex)
				{
					IndexStmt  *indstmt;
					Oid			indoid;

					indstmt = castNode(IndexStmt, cmd->def);
					indoid = get_constraint_index(oldId);

					if (!rewrite)
						TryReuseIndex(indoid, indstmt);
					/* keep any comment on the index */
					indstmt->idxcomment = GetComment(indoid,
													 RelationRelationId, 0);
					indstmt->reset_default_tblspc = true;

					cmd->subtype = AT_ReAddIndex;
					tab->subcmds[AT_PASS_OLD_INDEX] =
						lappend(tab->subcmds[AT_PASS_OLD_INDEX], cmd);

					/* recreate any comment on the constraint */
					RebuildConstraintComment(tab,
											 AT_PASS_OLD_INDEX,
											 oldId,
											 rel,
											 NIL,
											 indstmt->idxname);
				}
				else if (cmd->subtype == AT_AddConstraint)
				{
					Constraint *con = castNode(Constraint, cmd->def);

					con->old_pktable_oid = refRelId;
					/* rewriting neither side of a FK */
					if (con->contype == CONSTR_FOREIGN &&
						!rewrite && tab->rewrite == 0)
						TryReuseForeignKey(oldId, con);
					con->reset_default_tblspc = true;
					cmd->subtype = AT_ReAddConstraint;
					tab->subcmds[AT_PASS_OLD_CONSTR] =
						lappend(tab->subcmds[AT_PASS_OLD_CONSTR], cmd);

					/* recreate any comment on the constraint */
					RebuildConstraintComment(tab,
											 AT_PASS_OLD_CONSTR,
											 oldId,
											 rel,
											 NIL,
											 con->conname);
				}
				else if (cmd->subtype == AT_SetNotNull)
				{
					/*
					 * The parser will create AT_SetNotNull subcommands for
					 * columns of PRIMARY KEY indexes/constraints, but we need
					 * not do anything with them here, because the columns'
					 * NOT NULL marks will already have been propagated into
					 * the new table definition.
					 */
				}
				else
					elog(ERROR, "unexpected statement subtype: %d",
						 (int) cmd->subtype);
			}
		}
		else if (IsA(stm, AlterDomainStmt))
		{
			AlterDomainStmt *stmt = (AlterDomainStmt *) stm;

			if (stmt->subtype == 'C')	/* ADD CONSTRAINT */
			{
				Constraint *con = castNode(Constraint, stmt->def);
				AlterTableCmd *cmd = makeNode(AlterTableCmd);

				cmd->subtype = AT_ReAddDomainConstraint;
				cmd->def = (Node *) stmt;
				tab->subcmds[AT_PASS_OLD_CONSTR] =
					lappend(tab->subcmds[AT_PASS_OLD_CONSTR], cmd);

				/* recreate any comment on the constraint */
				RebuildConstraintComment(tab,
										 AT_PASS_OLD_CONSTR,
										 oldId,
										 NULL,
										 stmt->typeName,
										 con->conname);
			}
			else
				elog(ERROR, "unexpected statement subtype: %d",
					 (int) stmt->subtype);
		}
		else if (IsA(stm, CreateStatsStmt))
		{
			CreateStatsStmt *stmt = (CreateStatsStmt *) stm;
			AlterTableCmd *newcmd;

			/* keep the statistics object's comment */
			stmt->stxcomment = GetComment(oldId, StatisticExtRelationId, 0);

			newcmd = makeNode(AlterTableCmd);
			newcmd->subtype = AT_ReAddStatistics;
			newcmd->def = (Node *) stmt;
			tab->subcmds[AT_PASS_MISC] =
				lappend(tab->subcmds[AT_PASS_MISC], newcmd);
		}
		else
			elog(ERROR, "unexpected statement type: %d",
				 (int) nodeTag(stm));
	}

	relation_close(rel, NoLock);
}

/*
 * Subroutine for ATPostAlterTypeParse() to recreate any existing comment
 * for a table or domain constraint that is being rebuilt.
 *
 * objid is the OID of the constraint.
 * Pass "rel" for a table constraint, or "domname" (domain's qualified name
 * as a string list) for a domain constraint.
 * (We could dig that info, as well as the conname, out of the pg_constraint
 * entry; but callers already have them so might as well pass them.)
 */
static void
RebuildConstraintComment(AlteredTableInfo *tab, int pass, Oid objid,
						 Relation rel, List *domname,
						 const char *conname)
{
	CommentStmt *cmd;
	char	   *comment_str;
	AlterTableCmd *newcmd;

	/* RebuildConstraintComment() isn't called in QE now. */
	/* Otherwise we must ignore the generated cmd. */
	Assert(Gp_role != GP_ROLE_EXECUTE);

	/* Look for comment for object wanted, and leave if none */
	comment_str = GetComment(objid, ConstraintRelationId, 0);
	if (comment_str == NULL)
		return;

	/* Build CommentStmt node, copying all input data for safety */
	cmd = makeNode(CommentStmt);
	if (rel)
	{
		cmd->objtype = OBJECT_TABCONSTRAINT;
		cmd->object = (Node *)
			list_make3(makeString(get_namespace_name(RelationGetNamespace(rel))),
					   makeString(pstrdup(RelationGetRelationName(rel))),
					   makeString(pstrdup(conname)));
	}
	else
	{
		cmd->objtype = OBJECT_DOMCONSTRAINT;
		cmd->object = (Node *)
			list_make2(makeTypeNameFromNameList(copyObject(domname)),
					   makeString(pstrdup(conname)));
	}
	cmd->comment = comment_str;

	/* Append it to list of commands */
	newcmd = makeNode(AlterTableCmd);
	newcmd->subtype = AT_ReAddComment;
	newcmd->def = (Node *) cmd;
	tab->subcmds[pass] = lappend(tab->subcmds[pass], newcmd);
}

/*
 * Subroutine for ATPostAlterTypeParse().  Calls out to CheckIndexCompatible()
 * for the real analysis, then mutates the IndexStmt based on that verdict.
 */
static void
TryReuseIndex(Oid oldId, IndexStmt *stmt)
{
	if (CheckIndexCompatible(oldId,
							 stmt->accessMethod,
							 stmt->indexParams,
							 stmt->excludeOpNames))
	{
		Relation	irel = index_open(oldId, NoLock);

		/* If it's a partitioned index, there is no storage to share. */
		if (irel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		{
			stmt->oldNode = irel->rd_node.relNode;
			stmt->oldCreateSubid = irel->rd_createSubid;
			stmt->oldFirstRelfilenodeSubid = irel->rd_firstRelfilenodeSubid;
		}
		index_close(irel, NoLock);
	}
}

/*
 * Subroutine for ATPostAlterTypeParse().
 *
 * Stash the old P-F equality operator into the Constraint node, for possible
 * use by ATAddForeignKeyConstraint() in determining whether revalidation of
 * this constraint can be skipped.
 */
static void
TryReuseForeignKey(Oid oldId, Constraint *con)
{
	HeapTuple	tup;
	Datum		adatum;
	bool		isNull;
	ArrayType  *arr;
	Oid		   *rawarr;
	int			numkeys;
	int			i;

	Assert(con->contype == CONSTR_FOREIGN);
	Assert(con->old_conpfeqop == NIL);	/* already prepared this node */

	tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(oldId));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for constraint %u", oldId);

	adatum = SysCacheGetAttr(CONSTROID, tup,
							 Anum_pg_constraint_conpfeqop, &isNull);
	if (isNull)
		elog(ERROR, "null conpfeqop for constraint %u", oldId);
	arr = DatumGetArrayTypeP(adatum);	/* ensure not toasted */
	numkeys = ARR_DIMS(arr)[0];
	/* test follows the one in ri_FetchConstraintInfo() */
	if (ARR_NDIM(arr) != 1 ||
		ARR_HASNULL(arr) ||
		ARR_ELEMTYPE(arr) != OIDOID)
		elog(ERROR, "conpfeqop is not a 1-D Oid array");
	rawarr = (Oid *) ARR_DATA_PTR(arr);

	/* stash a List of the operator Oids in our Constraint node */
	for (i = 0; i < numkeys; i++)
		con->old_conpfeqop = lappend_oid(con->old_conpfeqop, rawarr[i]);

	ReleaseSysCache(tup);
}

/*
 * ALTER COLUMN .. OPTIONS ( ... )
 *
 * Returns the address of the modified column
 */
static ObjectAddress
ATExecAlterColumnGenericOptions(Relation rel,
								const char *colName,
								List *options,
								LOCKMODE lockmode)
{
	Relation	ftrel;
	Relation	attrel;
	ForeignServer *server;
	ForeignDataWrapper *fdw;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	bool		isnull;
	Datum		repl_val[Natts_pg_attribute];
	bool		repl_null[Natts_pg_attribute];
	bool		repl_repl[Natts_pg_attribute];
	Datum		datum;
	Form_pg_foreign_table fttableform;
	Form_pg_attribute atttableform;
	AttrNumber	attnum;
	ObjectAddress address;

	if (options == NIL)
		return InvalidObjectAddress;

	/* First, determine FDW validator associated to the foreign table. */
	ftrel = table_open(ForeignTableRelationId, AccessShareLock);
	tuple = SearchSysCache1(FOREIGNTABLEREL, rel->rd_id);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("foreign table \"%s\" does not exist",
						RelationGetRelationName(rel))));
	fttableform = (Form_pg_foreign_table) GETSTRUCT(tuple);
	server = GetForeignServer(fttableform->ftserver);
	fdw = GetForeignDataWrapper(server->fdwid);

	table_close(ftrel, AccessShareLock);
	ReleaseSysCache(tuple);

	attrel = table_open(AttributeRelationId, RowExclusiveLock);
	tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));

	/* Prevent them from altering a system attribute */
	atttableform = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = atttableform->attnum;
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"", colName)));


	/* Initialize buffers for new tuple values */
	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	/* Extract the current options */
	datum = SysCacheGetAttr(ATTNAME,
							tuple,
							Anum_pg_attribute_attfdwoptions,
							&isnull);
	if (isnull)
		datum = PointerGetDatum(NULL);

	/* Transform the options */
	datum = transformGenericOptions(AttributeRelationId,
									datum,
									options,
									fdw->fdwvalidator);

	if (PointerIsValid(DatumGetPointer(datum)))
		repl_val[Anum_pg_attribute_attfdwoptions - 1] = datum;
	else
		repl_null[Anum_pg_attribute_attfdwoptions - 1] = true;

	repl_repl[Anum_pg_attribute_attfdwoptions - 1] = true;

	/* Everything looks good - update the tuple */

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(attrel),
								 repl_val, repl_null, repl_repl);

	CatalogTupleUpdate(attrel, &newtuple->t_self, newtuple);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel),
							  atttableform->attnum);
	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);

	ReleaseSysCache(tuple);

	table_close(attrel, RowExclusiveLock);

	heap_freetuple(newtuple);

	return address;
}

/*
 * ALTER TABLE OWNER
 *
 * recursing is true if we are recursing from a table to its indexes,
 * sequences, or toast table.  We don't allow the ownership of those things to
 * be changed separately from the parent table.  Also, we can skip permission
 * checks (this is necessary not just an optimization, else we'd fail to
 * handle toast tables properly).
 *
 * recursing is also true if ALTER TYPE OWNER is calling us to fix up a
 * free-standing composite type.
 */
void
ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing, LOCKMODE lockmode)
{
	Relation	target_rel;
	Relation	class_rel;
	HeapTuple	tuple;
	Form_pg_class tuple_class;

	/*
	 * Get exclusive lock till end of transaction on the target table. Use
	 * relation_open so that we can work on indexes and sequences.
	 */
	target_rel = relation_open(relationOid, lockmode);

	/* Get its pg_class tuple, too */
	class_rel = table_open(RelationRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationOid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relationOid);
	tuple_class = (Form_pg_class) GETSTRUCT(tuple);

	/* Can we change the ownership of this tuple? */
	switch (tuple_class->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_VIEW:
		case RELKIND_MATVIEW:
		case RELKIND_FOREIGN_TABLE:
		case RELKIND_PARTITIONED_TABLE:
		case RELKIND_DIRECTORY_TABLE:
			/* ok to change owner */
			break;
		case RELKIND_INDEX:
			if (!recursing)
			{
				/*
				 * Because ALTER INDEX OWNER used to be allowed, and in fact
				 * is generated by old versions of pg_dump, we give a warning
				 * and do nothing rather than erroring out.  Also, to avoid
				 * unnecessary chatter while restoring those old dumps, say
				 * nothing at all if the command would be a no-op anyway.
				 */
				if (tuple_class->relowner != newOwnerId)
					ereport(WARNING,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot change owner of index \"%s\"",
									NameStr(tuple_class->relname)),
							 errhint("Change the ownership of the index's table, instead.")));
				/* quick hack to exit via the no-op path */
				newOwnerId = tuple_class->relowner;
			}
			break;
		case RELKIND_PARTITIONED_INDEX:
			if (recursing)
				break;
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot change owner of index \"%s\"",
							NameStr(tuple_class->relname)),
					 errhint("Change the ownership of the index's table, instead.")));
			break;
		case RELKIND_SEQUENCE:
			if (!recursing &&
				tuple_class->relowner != newOwnerId)
			{
				/* if it's an owned sequence, disallow changing it by itself */
				Oid			tableId;
				int32		colId;

				if (sequenceIsOwned(relationOid, DEPENDENCY_AUTO, &tableId, &colId) ||
					sequenceIsOwned(relationOid, DEPENDENCY_INTERNAL, &tableId, &colId))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot change owner of sequence \"%s\"",
									NameStr(tuple_class->relname)),
							 errdetail("Sequence \"%s\" is linked to table \"%s\".",
									   NameStr(tuple_class->relname),
									   get_rel_name(tableId))));
			}
			break;
		case RELKIND_COMPOSITE_TYPE:
			if (recursing)
				break;
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a composite type",
							NameStr(tuple_class->relname)),
					 errhint("Use ALTER TYPE instead.")));
			break;
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
		case RELKIND_AOVISIMAP:
			if (recursing)
				break;
			/* FALL THRU */
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a table, view, sequence, or foreign table",
							NameStr(tuple_class->relname))));
	}

	/*
	 * If the new owner is the same as the existing owner, consider the
	 * command to have succeeded.  This is for dump restoration purposes.
	 */
	if (tuple_class->relowner != newOwnerId)
	{
		Datum		repl_val[Natts_pg_class];
		bool		repl_null[Natts_pg_class];
		bool		repl_repl[Natts_pg_class];
		Acl		   *newAcl;
		Datum		aclDatum;
		bool		isNull;
		HeapTuple	newtuple;

		/* skip permission checks when recursing to index or toast table */
		if (!recursing)
		{
			/* Superusers can always do it */
			if (!superuser())
			{
				Oid			namespaceOid = tuple_class->relnamespace;
				AclResult	aclresult;

				/* Otherwise, must be owner of the existing object */
				if (!pg_class_ownercheck(relationOid, GetUserId()))
					aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relationOid)),
								   RelationGetRelationName(target_rel));

				/* Must be able to become new owner */
				check_is_member_of_role(GetUserId(), newOwnerId);

				/* New owner must have CREATE privilege on namespace */
				aclresult = pg_namespace_aclcheck(namespaceOid, newOwnerId,
												  ACL_CREATE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, OBJECT_SCHEMA,
								   get_namespace_name(namespaceOid));
			}
		}

		memset(repl_null, false, sizeof(repl_null));
		memset(repl_repl, false, sizeof(repl_repl));

		repl_repl[Anum_pg_class_relowner - 1] = true;
		repl_val[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(newOwnerId);

		/*
		 * Determine the modified ACL for the new owner.  This is only
		 * necessary when the ACL is non-null.
		 */
		aclDatum = SysCacheGetAttr(RELOID, tuple,
								   Anum_pg_class_relacl,
								   &isNull);
		if (!isNull)
		{
			newAcl = aclnewowner(DatumGetAclP(aclDatum),
								 tuple_class->relowner, newOwnerId);
			repl_repl[Anum_pg_class_relacl - 1] = true;
			repl_val[Anum_pg_class_relacl - 1] = PointerGetDatum(newAcl);
		}

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(class_rel), repl_val, repl_null, repl_repl);

		CatalogTupleUpdate(class_rel, &newtuple->t_self, newtuple);

		heap_freetuple(newtuple);

		/*
		 * We must similarly update any per-column ACLs to reflect the new
		 * owner; for neatness reasons that's split out as a subroutine.
		 */
		change_owner_fix_column_acls(relationOid,
									 tuple_class->relowner,
									 newOwnerId);

		/*
		 * Update owner dependency reference, if any.  A composite type has
		 * none, because it's tracked for the pg_type entry instead of here;
		 * indexes and TOAST tables don't have their own entries either.
		 */
		if (tuple_class->relkind != RELKIND_COMPOSITE_TYPE &&
			tuple_class->relkind != RELKIND_INDEX &&
			tuple_class->relkind != RELKIND_PARTITIONED_INDEX &&
			tuple_class->relkind != RELKIND_TOASTVALUE)
			changeDependencyOnOwner(RelationRelationId, relationOid,
									newOwnerId);

		/*
		 * Also change the ownership of the table's row type, if it has one
		 */
		if (OidIsValid(tuple_class->reltype))
			AlterTypeOwnerInternal(tuple_class->reltype, newOwnerId);

		/*
		 * If we are operating on a table or materialized view, also change
		 * the ownership of any indexes and sequences that belong to the
		 * relation, as well as its toast table (if it has one).
		 */
		if (tuple_class->relkind == RELKIND_RELATION ||
			tuple_class->relkind == RELKIND_PARTITIONED_TABLE ||
			tuple_class->relkind == RELKIND_MATVIEW ||
			tuple_class->relkind == RELKIND_TOASTVALUE ||
			tuple_class->relkind == RELKIND_DIRECTORY_TABLE ||
			tuple_class->relnamespace == PG_EXTAUX_NAMESPACE ||
			IsAppendonlyMetadataRelkind(tuple_class->relkind))
		{
			List	   *index_oid_list;
			ListCell   *i;

			/* Find all the indexes belonging to this relation */
			index_oid_list = RelationGetIndexList(target_rel);

			/* For each index, recursively change its ownership */
			foreach(i, index_oid_list)
				ATExecChangeOwner(lfirst_oid(i), newOwnerId, true, lockmode);

			list_free(index_oid_list);
		}

		/* If it has a toast table, recurse to change its ownership */
		if (tuple_class->reltoastrelid != InvalidOid)
			ATExecChangeOwner(tuple_class->reltoastrelid, newOwnerId,
							  true, lockmode);

		if (RelationIsAppendOptimized(target_rel))
		{
			Oid segrelid, blkdirrelid;
			Oid visimap_relid;
			GetAppendOnlyEntryAuxOids(target_rel,
									  &segrelid,
									  &blkdirrelid, NULL,
									  &visimap_relid, NULL);

			/* If it has an AO segment table, recurse to change its
			 * ownership */
			if (segrelid != InvalidOid)
				ATExecChangeOwner(segrelid, newOwnerId, true, lockmode);

			/* If it has an AO block directory table, recurse to change its
			 * ownership */
			if (blkdirrelid != InvalidOid)
				ATExecChangeOwner(blkdirrelid, newOwnerId, true, lockmode);

			/* If it has an AO visimap table, recurse to change its
			 * ownership */
			if (visimap_relid != InvalidOid)
			{
				ATExecChangeOwner(visimap_relid, newOwnerId, true, lockmode);
			}
		}

		/* If it has dependent sequences, recurse to change them too */
		change_owner_recurse_to_sequences(relationOid, newOwnerId, lockmode);
	}

	InvokeObjectPostAlterHook(RelationRelationId, relationOid, 0);

	ReleaseSysCache(tuple);
	table_close(class_rel, RowExclusiveLock);
	relation_close(target_rel, NoLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(tuple_class)
			)
		MetaTrackUpdObject(RelationRelationId,
						   relationOid,
						   GetUserId(),
						   "ALTER", "OWNER"
				);
}

/*
 * change_owner_fix_column_acls
 *
 * Helper function for ATExecChangeOwner.  Scan the columns of the table
 * and fix any non-null column ACLs to reflect the new owner.
 */
static void
change_owner_fix_column_acls(Oid relationOid, Oid oldOwnerId, Oid newOwnerId)
{
	Relation	attRelation;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	attributeTuple;

	attRelation = table_open(AttributeRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationOid));
	scan = systable_beginscan(attRelation, AttributeRelidNumIndexId,
							  true, NULL, 1, key);
	while (HeapTupleIsValid(attributeTuple = systable_getnext(scan)))
	{
		Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);
		Datum		repl_val[Natts_pg_attribute];
		bool		repl_null[Natts_pg_attribute];
		bool		repl_repl[Natts_pg_attribute];
		Acl		   *newAcl;
		Datum		aclDatum;
		bool		isNull;
		HeapTuple	newtuple;

		/* Ignore dropped columns */
		if (att->attisdropped)
			continue;

		aclDatum = heap_getattr(attributeTuple,
								Anum_pg_attribute_attacl,
								RelationGetDescr(attRelation),
								&isNull);
		/* Null ACLs do not require changes */
		if (isNull)
			continue;

		memset(repl_null, false, sizeof(repl_null));
		memset(repl_repl, false, sizeof(repl_repl));

		newAcl = aclnewowner(DatumGetAclP(aclDatum),
							 oldOwnerId, newOwnerId);
		repl_repl[Anum_pg_attribute_attacl - 1] = true;
		repl_val[Anum_pg_attribute_attacl - 1] = PointerGetDatum(newAcl);

		newtuple = heap_modify_tuple(attributeTuple,
									 RelationGetDescr(attRelation),
									 repl_val, repl_null, repl_repl);

		CatalogTupleUpdate(attRelation, &newtuple->t_self, newtuple);

		heap_freetuple(newtuple);
	}
	systable_endscan(scan);
	table_close(attRelation, RowExclusiveLock);
}

/*
 * change_owner_recurse_to_sequences
 *
 * Helper function for ATExecChangeOwner.  Examines pg_depend searching
 * for sequences that are dependent on serial columns, and changes their
 * ownership.
 */
static void
change_owner_recurse_to_sequences(Oid relationOid, Oid newOwnerId, LOCKMODE lockmode)
{
	Relation	depRel;
	SysScanDesc scan;
	ScanKeyData key[2];
	HeapTuple	tup;

	/*
	 * SERIAL sequences are those having an auto dependency on one of the
	 * table's columns (we don't care *which* column, exactly).
	 */
	depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationOid));
	/* we leave refobjsubid unspecified */

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depForm = (Form_pg_depend) GETSTRUCT(tup);
		Relation	seqRel;

		/* skip dependencies other than auto dependencies on columns */
		if (depForm->refobjsubid == 0 ||
			depForm->classid != RelationRelationId ||
			depForm->objsubid != 0 ||
			!(depForm->deptype == DEPENDENCY_AUTO || depForm->deptype == DEPENDENCY_INTERNAL))
			continue;

		/* Use relation_open just in case it's an index */
		seqRel = relation_open(depForm->objid, lockmode);

		/* skip non-sequence relations */
		if (RelationGetForm(seqRel)->relkind != RELKIND_SEQUENCE)
		{
			/* No need to keep the lock */
			relation_close(seqRel, lockmode);
			continue;
		}

		/* We don't need to close the sequence while we alter it. */
		ATExecChangeOwner(depForm->objid, newOwnerId, true, lockmode);

		/* Now we can close it.  Keep the lock till end of transaction. */
		relation_close(seqRel, NoLock);
	}

	systable_endscan(scan);

	relation_close(depRel, AccessShareLock);
}

/*
 * ALTER TABLE CLUSTER ON
 *
 * The only thing we have to do is to change the indisclustered bits.
 *
 * Return the address of the new clustering index.
 */
static ObjectAddress
ATExecClusterOn(Relation rel, const char *indexName, LOCKMODE lockmode)
{
	Oid			indexOid;
	ObjectAddress address;

	indexOid = get_relname_relid(indexName, rel->rd_rel->relnamespace);

	if (!OidIsValid(indexOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("index \"%s\" for table \"%s\" does not exist",
						indexName, RelationGetRelationName(rel))));

	if (RelationIsAppendOptimized(rel))
	{
		bool isBtree = false;
		Relation oldIndex = index_open(indexOid, AccessExclusiveLock);
		isBtree = oldIndex->rd_rel->relam == BTREE_AM_OID;
		index_close(oldIndex, NoLock);

		if (!isBtree)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot cluster append-optimized table \"%s\"", RelationGetRelationName(rel)),
					errdetail("Append-optimized tables can only be clustered against a B-tree index")));
	}


	/* Check index is valid to cluster on */
	check_index_is_clusterable(rel, indexOid, false, lockmode);

	/* And do the work */
	mark_index_clustered(rel, indexOid, false);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "CLUSTER ON"
				);

	ObjectAddressSet(address,
					 RelationRelationId, indexOid);

	return address;
}

/*
 * ALTER TABLE SET WITHOUT CLUSTER
 *
 * We have to find any indexes on the table that have indisclustered bit
 * set and turn it off.
 */
static void
ATExecDropCluster(Relation rel, LOCKMODE lockmode)
{
	mark_index_clustered(rel, InvalidOid, false);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "SET WITHOUT CLUSTER"
				);
}

/*
 * ALTER TABLE SET TABLESPACE
 */
static void
ATPrepSetTableSpace(AlteredTableInfo *tab, Relation rel, const char *tablespacename, LOCKMODE lockmode)
{
	Oid			tablespaceId;

	/* Check that the tablespace exists */
	tablespaceId = get_tablespace_oid(tablespacename, false);

	/* Check permissions except when moving to database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TABLESPACE, tablespacename);
	}

	/* Save info for Phase 3 to do the real work */
	if (OidIsValid(tab->newTableSpace))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot have multiple SET TABLESPACE subcommands")));

	tab->newTableSpace = tablespaceId;
}

/*
 * Set, reset, or replace reloptions.
 */
static void
ATExecSetRelOptions(Relation rel, List *defList, AlterTableType operation,
					LOCKMODE lockmode)
{
	Oid			relid;
	Relation	pgclass;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		datum;
	bool		isnull;
	Datum		newOptions;
	Datum		repl_val[Natts_pg_class];
	bool		repl_null[Natts_pg_class];
	bool		repl_repl[Natts_pg_class];
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;

	if (defList == NIL && operation != AT_ReplaceRelOptions)
		return;					/* nothing to do */

	pgclass = table_open(RelationRelationId, RowExclusiveLock);

	/* Fetch heap tuple */
	relid = RelationGetRelid(rel);
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	if (operation == AT_ReplaceRelOptions)
	{
		/*
		 * If we're supposed to replace the reloptions list, we just pretend
		 * there were none before.
		 */
		datum = (Datum) 0;
		isnull = true;
	}
	else
	{
		/* Get the old reloptions */
		datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
								&isnull);
	}

	/*
	 * Disallow all AO options.  ALTER TABLE SET ... command does not rewrite a
	 * table.  Setting AO options (e.g. setting appendonly=true when it's
	 * currently unset) involves rewrite of the table and is handled using the
	 * SET WITH variant of ALTER TABLE.
	 *
	 * GPDP_91_MERGE_FIXME: Is it possible to use the new reloptions format to
	 * avoid replicating each AO reloption here?  How about introducing new
	 * RELOPT_KIND_AO and RELOPT_KIND_AOCO to relopt_kind?  As of PostgreSQL v12,
	 * RELOPT_KIND_APPENDOPTIMIZED is introduced and can be leveraged when
	 * addressing this fixme.
	 *
	 * Future work: could convert from SET to SET WITH codepath which
	 * can support additional reloption types
	 */
	if (defList != NIL)
	{
		ListCell   *cell;

		foreach(cell, defList)
		{
			DefElem    *def = lfirst(cell);
			int			kw_len = strlen(def->defname);

			if (pg_strncasecmp(SOPT_APPENDONLY, def->defname, kw_len) == 0 ||
				pg_strncasecmp(SOPT_BLOCKSIZE, def->defname, kw_len) == 0 ||
				pg_strncasecmp(SOPT_COMPTYPE, def->defname, kw_len) == 0 ||
				pg_strncasecmp(SOPT_COMPLEVEL, def->defname, kw_len) == 0 ||
				pg_strncasecmp(SOPT_CHECKSUM, def->defname, kw_len) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot SET reloption \"%s\"",
								def->defname)));
			/*
			 * Autovacuum on user tables is not enabled in Cloudberry.  Move on
			 * with a warning.  The decision to not error out is in favor of
			 * DDL compatibility with external BI tools.
			 */
			if ((Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY) &&
				pg_strncasecmp(def->defname, "autovacuum",
							   strlen("autovaccum")) == 0)
				ereport(WARNING,
						(errcode(ERRCODE_GP_FEATURE_NOT_YET),
						 errmsg("autovacuum is not supported in Cloudberry")));
		}
	}

	/* Generate new proposed reloptions (text array) */
	newOptions = transformRelOptions(isnull ? (Datum) 0 : datum,
									 defList, NULL, validnsps, false,
									 operation == AT_ResetRelOptions);

	/* Validate */
	switch (rel->rd_rel->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_TOASTVALUE:
		case RELKIND_MATVIEW:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
		case RELKIND_AOVISIMAP:
			(void) table_reloptions(rel->rd_tableam->amoptions, newOptions, rel->rd_rel->relkind, true);
			break;
		case RELKIND_PARTITIONED_TABLE:
			(void) partitioned_table_reloptions(newOptions, true);
			break;
		case RELKIND_VIEW:
			(void) view_reloptions(newOptions, true);
			break;
		case RELKIND_INDEX:
		case RELKIND_PARTITIONED_INDEX:
			(void) index_reloptions(rel->rd_indam->amoptions, newOptions, true);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a table, view, materialized view, index, or TOAST table",
							RelationGetRelationName(rel))));
			break;
	}

	/* Special-case validation of view options */
	if (rel->rd_rel->relkind == RELKIND_VIEW)
	{
		Query	   *view_query = get_view_query(rel);
		List	   *view_options = untransformRelOptions(newOptions);
		ListCell   *cell;
		bool		check_option = false;

		foreach(cell, view_options)
		{
			DefElem    *defel = (DefElem *) lfirst(cell);

			if (strcmp(defel->defname, "check_option") == 0)
				check_option = true;
		}

		/*
		 * If the check option is specified, look to see if the view is
		 * actually auto-updatable or not.
		 */
		if (check_option)
		{
			const char *view_updatable_error =
			view_query_is_auto_updatable(view_query, true);

			if (view_updatable_error)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("WITH CHECK OPTION is supported only on automatically updatable views"),
						 errhint("%s", _(view_updatable_error))));
		}
	}

	/*
	 * All we need do here is update the pg_class row; the new options will be
	 * propagated into relcaches during post-commit cache inval.
	 */
	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	if (newOptions != (Datum) 0)
		repl_val[Anum_pg_class_reloptions - 1] = newOptions;
	else
		repl_null[Anum_pg_class_reloptions - 1] = true;

	repl_repl[Anum_pg_class_reloptions - 1] = true;

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(pgclass),
								 repl_val, repl_null, repl_repl);

	CatalogTupleUpdate(pgclass, &newtuple->t_self, newtuple);

	InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

	heap_freetuple(newtuple);

	ReleaseSysCache(tuple);

	/* repeat the whole exercise for the toast table, if there's one */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
	{
		Relation	toastrel;
		Oid			toastid = rel->rd_rel->reltoastrelid;

		toastrel = table_open(toastid, lockmode);

		/* Fetch heap tuple */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", toastid);

		if (operation == AT_ReplaceRelOptions)
		{
			/*
			 * If we're supposed to replace the reloptions list, we just
			 * pretend there were none before.
			 */
			datum = (Datum) 0;
			isnull = true;
		}
		else
		{
			/* Get the old reloptions */
			datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
									&isnull);
		}

		newOptions = transformRelOptions(isnull ? (Datum) 0 : datum,
										 defList, "toast", validnsps, false,
										 operation == AT_ResetRelOptions);

		(void) table_reloptions(toastrel->rd_tableam->amoptions, newOptions, RELKIND_TOASTVALUE, true);

		memset(repl_val, 0, sizeof(repl_val));
		memset(repl_null, false, sizeof(repl_null));
		memset(repl_repl, false, sizeof(repl_repl));

		if (newOptions != (Datum) 0)
			repl_val[Anum_pg_class_reloptions - 1] = newOptions;
		else
			repl_null[Anum_pg_class_reloptions - 1] = true;

		repl_repl[Anum_pg_class_reloptions - 1] = true;

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(pgclass),
									 repl_val, repl_null, repl_repl);

		CatalogTupleUpdate(pgclass, &newtuple->t_self, newtuple);

		InvokeObjectPostAlterHookArg(RelationRelationId,
									 RelationGetRelid(toastrel), 0,
									 InvalidOid, true);

		heap_freetuple(newtuple);

		ReleaseSysCache(tuple);

		table_close(toastrel, NoLock);
	}

	table_close(pgclass, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER",
						   operation == AT_ResetRelOptions ? "RESET" : "SET"
				);
}

/*
 * Execute ALTER TABLE SET TABLESPACE for cases where there is no tuple
 * rewriting to be done, so we just want to copy the data as fast as possible.
 */
static void
ATExecSetTableSpace(Oid tableOid, Oid newTableSpace, LOCKMODE lockmode)
{
	Relation	rel;
	Oid			reltoastrelid;
	Oid			relaosegrelid = InvalidOid;
	Oid			relaoblkdirrelid = InvalidOid;
	Oid			relaoblkdiridxid = InvalidOid;
	Oid         relaovisimaprelid = InvalidOid;
	Oid         relaovisimapidxid = InvalidOid;
	Oid			relbmrelid = InvalidOid;
	Oid			relbmidxid = InvalidOid;
	RelFileNodeId newrelfilenode;
	RelFileNode newrnode;
	List	   *reltoastidxids = NIL;
	ListCell   *lc;

	/*
	 * Need lock here in case we are recursing to toast table or index
	 */
	rel = relation_open(tableOid, lockmode);

	/* Check first if relation can be moved to new tablespace */
	if (!CheckRelationTableSpaceMove(rel, newTableSpace))
	{
		InvokeObjectPostAlterHook(RelationRelationId,
								  RelationGetRelid(rel), 0);
		relation_close(rel, NoLock);
		return;
	}

	reltoastrelid = rel->rd_rel->reltoastrelid;
	/* Fetch the list of indexes on toast relation if necessary */
	if (OidIsValid(reltoastrelid))
	{
		Relation	toastRel = relation_open(reltoastrelid, lockmode);

		reltoastidxids = RelationGetIndexList(toastRel);
		relation_close(toastRel, lockmode);
	}

	/* Get the ao sub objects */
	if (RelationIsAppendOptimized(rel))
		GetAppendOnlyEntryAuxOids(rel,
								  &relaosegrelid,
								  &relaoblkdirrelid, &relaoblkdiridxid,
								  &relaovisimaprelid, &relaovisimapidxid);

	/* Get the bitmap sub objects */
	if (RelationIsBitmapIndex(rel))
		GetBitmapIndexAuxOids(rel, &relbmrelid, &relbmidxid);

	/*
	 * Relfilenodes are not unique in databases across tablespaces, so we need
	 * to allocate a new one in the new tablespace.
	 */
	newrelfilenode = GetNewRelFileNode(newTableSpace, NULL,
									   rel->rd_rel->relpersistence);

	/* Open old and new relation */
	newrnode = rel->rd_node;
	newrnode.relNode = newrelfilenode;
	newrnode.spcNode = newTableSpace;

	/* hand off to AM to actually create the new filenode and copy the data */
	if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		index_copy_data(rel, newrnode);
	}
	else
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE ||
			   rel->rd_rel->relkind == RELKIND_AOSEGMENTS ||
			   rel->rd_rel->relkind == RELKIND_AOBLOCKDIR ||
			   rel->rd_rel->relkind == RELKIND_AOVISIMAP);
		table_relation_copy_data(rel, &newrnode);
	}

	/*
	 * Update the pg_class row.
	 *
	 * NB: This wouldn't work if ATExecSetTableSpace() were allowed to be
	 * executed on pg_class or its indexes (the above copy wouldn't contain
	 * the updated pg_class entry), but that's forbidden with
	 * CheckRelationTableSpaceMove().
	 */
	SetRelationTableSpace(rel, newTableSpace, newrelfilenode);

	InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH) && MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "SET TABLESPACE");

	RelationAssumeNewRelfilenode(rel);

	relation_close(rel, NoLock);

	/* Make sure the reltablespace change is visible */
	CommandCounterIncrement();

	/* Move associated toast relation and/or indexes, too */
	if (OidIsValid(reltoastrelid))
		ATExecSetTableSpace(reltoastrelid, newTableSpace, lockmode);
	foreach(lc, reltoastidxids)
		ATExecSetTableSpace(lfirst_oid(lc), newTableSpace, lockmode);

	/* Move associated ao subobjects */
	if (OidIsValid(relaosegrelid))
		ATExecSetTableSpace(relaosegrelid, newTableSpace, lockmode);
	if (OidIsValid(relaoblkdirrelid))
		ATExecSetTableSpace(relaoblkdirrelid, newTableSpace, lockmode);
	if (OidIsValid(relaoblkdiridxid))
		ATExecSetTableSpace(relaoblkdiridxid, newTableSpace, lockmode);
	if (OidIsValid(relaovisimaprelid))
		ATExecSetTableSpace(relaovisimaprelid, newTableSpace, lockmode);
	if (OidIsValid(relaovisimapidxid))
		ATExecSetTableSpace(relaovisimapidxid, newTableSpace, lockmode);

	/* 
	 * MPP-7996 - bitmap index subobjects w/Alter Table Set tablespace
	 */
	if (OidIsValid(relbmrelid))
	{
		Assert(!relaosegrelid);
		ATExecSetTableSpace(relbmrelid, newTableSpace, lockmode);
	}
	if (OidIsValid(relbmidxid))
		ATExecSetTableSpace(relbmidxid, newTableSpace, lockmode);

	/* Clean up */
	list_free(reltoastidxids);
}

/*
 * Special handling of ALTER TABLE SET TABLESPACE for relations with no
 * storage that have an interest in preserving tablespace.
 *
 * Since these have no storage the tablespace can be updated with a simple
 * metadata only operation to update the tablespace.
 */
static void
ATExecSetTableSpaceNoStorage(Relation rel, Oid newTableSpace)
{
	/*
	 * Shouldn't be called on relations having storage; these are processed in
	 * phase 3.
	 */
	Assert(!RELKIND_HAS_STORAGE(rel->rd_rel->relkind));

	/* check if relation can be moved to its new tablespace */
	if (!CheckRelationTableSpaceMove(rel, newTableSpace))
	{
		InvokeObjectPostAlterHook(RelationRelationId,
								  RelationGetRelid(rel),
								  0);
		return;
	}

	/* Update can be done, so change reltablespace */
	SetRelationTableSpace(rel, newTableSpace, InvalidOid);

	InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

	/* Make sure the reltablespace change is visible */
	CommandCounterIncrement();
}

/*
 * Alter Table ALL ... SET TABLESPACE
 *
 * Allows a user to move all objects of some type in a given tablespace in the
 * current database to another tablespace.  Objects can be chosen based on the
 * owner of the object also, to allow users to move only their objects.
 * The user must have CREATE rights on the new tablespace, as usual.   The main
 * permissions handling is done by the lower-level table move function.
 *
 * All to-be-moved objects are locked first. If NOWAIT is specified and the
 * lock can't be acquired then we ereport(ERROR).
 */
Oid
AlterTableMoveAll(AlterTableMoveAllStmt *stmt)
{
	List	   *relations = NIL;
	ListCell   *l;
	ScanKeyData key[1];
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tuple;
	Oid			orig_tablespaceoid;
	Oid			new_tablespaceoid;
	List	   *role_oids = roleSpecsToIds(stmt->roles);

	/* Ensure we were not asked to move something we can't */
	if (stmt->objtype != OBJECT_TABLE && stmt->objtype != OBJECT_INDEX &&
		stmt->objtype != OBJECT_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only tables, indexes, and materialized views exist in tablespaces")));

	/* Get the orig and new tablespace OIDs */
	orig_tablespaceoid = get_tablespace_oid(stmt->orig_tablespacename, false);
	new_tablespaceoid = get_tablespace_oid(stmt->new_tablespacename, false);

	/* Can't move shared relations in to or out of pg_global */
	/* This is also checked by ATExecSetTableSpace, but nice to stop earlier */
	if (orig_tablespaceoid == GLOBALTABLESPACE_OID ||
		new_tablespaceoid == GLOBALTABLESPACE_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot move relations in to or out of pg_global tablespace")));

	/*
	 * Must have CREATE rights on the new tablespace, unless it is the
	 * database default tablespace (which all users implicitly have CREATE
	 * rights on).
	 */
	if (OidIsValid(new_tablespaceoid) && new_tablespaceoid != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(new_tablespaceoid, GetUserId(),
										   ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TABLESPACE,
						   get_tablespace_name(new_tablespaceoid));
	}

	/*
	 * Now that the checks are done, check if we should set either to
	 * InvalidOid because it is our database's default tablespace.
	 */
	if (orig_tablespaceoid == MyDatabaseTableSpace)
		orig_tablespaceoid = InvalidOid;

	if (new_tablespaceoid == MyDatabaseTableSpace)
		new_tablespaceoid = InvalidOid;

	/* no-op */
	if (orig_tablespaceoid == new_tablespaceoid)
		return new_tablespaceoid;

	/*
	 * Walk the list of objects in the tablespace and move them. This will
	 * only find objects in our database, of course.
	 */
	ScanKeyInit(&key[0],
				Anum_pg_class_reltablespace,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(orig_tablespaceoid));

	rel = table_open(RelationRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 1, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class relForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relOid = relForm->oid;

		/*
		 * Do not move objects in pg_catalog as part of this, if an admin
		 * really wishes to do so, they can issue the individual ALTER
		 * commands directly.
		 *
		 * Also, explicitly avoid any shared tables, temp tables, or TOAST
		 * (TOAST will be moved with the main table).
		 */
		if (IsCatalogNamespace(relForm->relnamespace) ||
			relForm->relisshared ||
			isAnyTempNamespace(relForm->relnamespace) ||
			IsToastNamespace(relForm->relnamespace))
			continue;

		/* Only move the object type requested */
		if ((stmt->objtype == OBJECT_TABLE &&
			 relForm->relkind != RELKIND_RELATION &&
			 relForm->relkind != RELKIND_PARTITIONED_TABLE) ||
			(stmt->objtype == OBJECT_INDEX &&
			 relForm->relkind != RELKIND_INDEX &&
			 relForm->relkind != RELKIND_PARTITIONED_INDEX) ||
			(stmt->objtype == OBJECT_MATVIEW &&
			 relForm->relkind != RELKIND_MATVIEW))
			continue;

		/* Check if we are only moving objects owned by certain roles */
		if (role_oids != NIL && !list_member_oid(role_oids, relForm->relowner))
			continue;

		/*
		 * Handle permissions-checking here since we are locking the tables
		 * and also to avoid doing a bunch of work only to fail part-way. Note
		 * that permissions will also be checked by AlterTableInternal().
		 *
		 * Caller must be considered an owner on the table to move it.
		 */
		if (!pg_class_ownercheck(relOid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relOid)),
						   NameStr(relForm->relname));

		if (stmt->nowait &&
			!ConditionalLockRelationOid(relOid, AccessExclusiveLock))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("aborting because lock on relation \"%s.%s\" is not available",
							get_namespace_name(relForm->relnamespace),
							NameStr(relForm->relname))));
		else
			LockRelationOid(relOid, AccessExclusiveLock);

		/* Add to our list of objects to move */
		relations = lappend_oid(relations, relOid);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	if (relations == NIL)
		ereport(NOTICE,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("no matching relations in tablespace \"%s\" found",
						orig_tablespaceoid == InvalidOid ? "(database default)" :
						get_tablespace_name(orig_tablespaceoid))));

	/* Everything is locked, loop through and move all of the relations. */
	foreach(l, relations)
	{
		List	   *cmds = NIL;
		AlterTableCmd *cmd = makeNode(AlterTableCmd);

		cmd->subtype = AT_SetTableSpace;
		cmd->name = stmt->new_tablespacename;

		cmds = lappend(cmds, cmd);

		EventTriggerAlterTableStart((Node *) stmt);
		/* OID is set by AlterTableInternal */
		AlterTableInternal(lfirst_oid(l), cmds, false);
		EventTriggerAlterTableEnd();
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									NIL,
									NULL);
	}

	return new_tablespaceoid;
}

static void
index_copy_data(Relation rel, RelFileNode newrnode)
{
	SMgrRelation dstrel;

	SMgrImpl smgr_which = RelationIsAppendOptimized(rel) ? SMGR_AO : SMGR_MD;

	dstrel = smgropen(newrnode, rel->rd_backend, smgr_which, rel);
					  
	RelationOpenSmgr(rel);

	/*
	 * Since we copy the file directly without looking at the shared buffers,
	 * we'd better first flush out any pages of the source relation that are
	 * in shared buffers.  We assume no new changes will be made while we are
	 * holding exclusive lock on the rel.
	 */
	FlushRelationBuffers(rel);

	/*
	 * Create and copy all forks of the relation, and schedule unlinking of
	 * old physical files.
	 *
	 * NOTE: any conflict in relfilenode value will be caught in
	 * RelationCreateStorage().
	 */
	RelationCreateStorage(newrnode, rel->rd_rel->relpersistence, smgr_which, rel);

	/* copy main fork */
	RelationCopyStorage(rel->rd_smgr, dstrel, MAIN_FORKNUM,
						rel->rd_rel->relpersistence);

	/* copy those extra forks that exist */
	for (ForkNumber forkNum = MAIN_FORKNUM + 1;
		 forkNum <= MAX_FORKNUM; forkNum++)
	{
		if (smgrexists(rel->rd_smgr, forkNum))
		{
			smgrcreate(dstrel, forkNum, false);

			/*
			 * WAL log creation if the relation is persistent, or this is the
			 * init fork of an unlogged relation.
			 */
			if (RelationIsPermanent(rel) ||
				(rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED &&
				 forkNum == INIT_FORKNUM))
				log_smgrcreate(&newrnode, forkNum, smgr_which);
			RelationCopyStorage(rel->rd_smgr, dstrel, forkNum,
								rel->rd_rel->relpersistence);
		}
	}

	/* drop old relation, and close new one */
	RelationDropStorage(rel);
	smgrclose(dstrel);
}

/*
 * ALTER TABLE ENABLE/DISABLE TRIGGER
 *
 * We just pass this off to trigger.c.
 */
static void
ATExecEnableDisableTrigger(Relation rel, const char *trigname,
						   char fires_when, bool skip_system, LOCKMODE lockmode)
{
	EnableDisableTrigger(rel, trigname, fires_when, skip_system, lockmode);

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH && MetaTrackValidKindNsp(rel->rd_rel))
	{
		char	   *subtype;
		switch (fires_when)
		{
			case TRIGGER_FIRES_ON_ORIGIN:
				subtype = "ENABLE TRIGGER";
				break;
			case TRIGGER_FIRES_ALWAYS:
				subtype = "ENABLE ALWAYS TRIGGER";
				break;
			case TRIGGER_FIRES_ON_REPLICA:
				subtype = "ENABLE REPLICA TRIGGER";
				break;
			case TRIGGER_DISABLED:
				subtype = "DISBLE TRIGGER";
				break;
			default:
				subtype = "unknown trigger mode";
				break;
		}
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", 
						   subtype);
	}
}

/*
 * ALTER TABLE ENABLE/DISABLE RULE
 *
 * We just pass this off to rewriteDefine.c.
 */
static void
ATExecEnableDisableRule(Relation rel, const char *rulename,
						char fires_when, LOCKMODE lockmode)
{
	EnableDisableRule(rel, rulename, fires_when);
}

/*
 * ALTER TABLE INHERIT
 *
 * Add a parent to the child's parents. This verifies that all the columns and
 * check constraints of the parent appear in the child and that they have the
 * same data types and expressions.
 */
static void
ATPrepAddInherit(Relation child_rel)
{
	if (child_rel->rd_rel->reloftype)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot change inheritance of typed table")));

	if (child_rel->rd_rel->relispartition)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot change inheritance of a partition")));

	if (child_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot change inheritance of partitioned table")));
}

/*
 * Return the address of the new parent relation.
 */
static ObjectAddress
ATExecAddInherit(Relation child_rel, RangeVar *parent, LOCKMODE lockmode)
{
	Relation	parent_rel;
	List	   *children;
	ObjectAddress address;
	const char *trigger_name;

	/* 1. Replicated table cannot inherit a parent */
	if (child_rel->rd_cdbpolicy &&
		child_rel->rd_cdbpolicy->ptype == POLICYTYPE_REPLICATED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Replicated table cannot inherit a parent")));

	/*
	 * A self-exclusive lock is needed here.  See the similar case in
	 * MergeAttributes() for a full explanation.
	 */
	parent_rel = table_openrv(parent, ShareUpdateExclusiveLock);

	/* 2. Replicated table cannot be inherited */
	if (parent_rel->rd_cdbpolicy &&
		parent_rel->rd_cdbpolicy->ptype == POLICYTYPE_REPLICATED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Replicated table cannot be inherited")));

	/*
	 * Must be owner of both parent and child -- child was checked by
	 * ATSimplePermissions call in ATPrepCmd
	 */
	ATSimplePermissions(parent_rel, ATT_TABLE | ATT_FOREIGN_TABLE);

	/* Permanent rels cannot inherit from temporary ones */
	if (parent_rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
		child_rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot inherit from temporary relation \"%s\"",
						RelationGetRelationName(parent_rel))));
	/* If parent rel is temp, it must belong to this session */
	if (parent_rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
		!parent_rel->rd_islocaltemp)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot inherit from temporary relation of another session")));

	/* Ditto for the child */
	if (child_rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
		!child_rel->rd_islocaltemp)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot inherit to temporary relation of another session")));

	/* Prevent partitioned tables from becoming inheritance parents */
	if (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot inherit from partitioned table \"%s\"",
						parent->relname)));

	/* Likewise for partitions */
	if (parent_rel->rd_rel->relispartition)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot inherit from a partition")));

	/*
	 * Prevent circularity by seeing if proposed parent inherits from child.
	 * (In particular, this disallows making a rel inherit from itself.)
	 *
	 * This is not completely bulletproof because of race conditions: in
	 * multi-level inheritance trees, someone else could concurrently be
	 * making another inheritance link that closes the loop but does not join
	 * either of the rels we have locked.  Preventing that seems to require
	 * exclusive locks on the entire inheritance tree, which is a cure worse
	 * than the disease.  find_all_inheritors() will cope with circularity
	 * anyway, so don't sweat it too much.
	 *
	 * We use weakest lock we can on child's children, namely AccessShareLock.
	 */
	children = find_all_inheritors(RelationGetRelid(child_rel),
								   AccessShareLock, NULL);

	if (list_member_oid(children, RelationGetRelid(parent_rel)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("circular inheritance not allowed"),
				 errdetail("\"%s\" is already a child of \"%s\".",
						   RelationGetRelationName(parent_rel),
						   RelationGetRelationName(child_rel))));

	/*
	 * If child_rel has row-level triggers with transition tables, we
	 * currently don't allow it to become an inheritance child.  See also
	 * prohibitions in ATExecAttachPartition() and CreateTrigger().
	 */
	trigger_name = FindTriggerIncompatibleWithInheritance(child_rel->trigdesc);
	if (trigger_name != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("trigger \"%s\" prevents table \"%s\" from becoming an inheritance child",
						trigger_name, RelationGetRelationName(child_rel)),
				 errdetail("ROW triggers with transition tables are not supported in inheritance hierarchies.")));

	/* OK to create inheritance */
	CreateInheritance(child_rel, parent_rel);

	ObjectAddressSet(address, RelationRelationId,
					 RelationGetRelid(parent_rel));

	/* keep our lock on the parent relation until commit */
	table_close(parent_rel, NoLock);

	return address;
}

/*
 * CreateInheritance
 *		Catalog manipulation portion of creating inheritance between a child
 *		table and a parent table.
 *
 * Common to ATExecAddInherit() and ATExecAttachPartition().
 */
static void
CreateInheritance(Relation child_rel, Relation parent_rel)
{
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	inheritsTuple;
	int32		inhseqno;

	/* Note: get RowExclusiveLock because we will write pg_inherits below. */
	catalogRelation = table_open(InheritsRelationId, RowExclusiveLock);

	/*
	 * Check for duplicates in the list of parents, and determine the highest
	 * inhseqno already present; we'll use the next one for the new parent.
	 * Also, if proposed child is a partition, it cannot already be
	 * inheriting.
	 *
	 * Note: we do not reject the case where the child already inherits from
	 * the parent indirectly; CREATE TABLE doesn't reject comparable cases.
	 */
	ScanKeyInit(&key,
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(child_rel)));
	scan = systable_beginscan(catalogRelation, InheritsRelidSeqnoIndexId,
							  true, NULL, 1, &key);

	/* inhseqno sequences start at 1 */
	inhseqno = 0;
	while (HeapTupleIsValid(inheritsTuple = systable_getnext(scan)))
	{
		Form_pg_inherits inh = (Form_pg_inherits) GETSTRUCT(inheritsTuple);

		if (inh->inhparent == RelationGetRelid(parent_rel))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" would be inherited from more than once",
							RelationGetRelationName(parent_rel))));

		if (inh->inhseqno > inhseqno)
			inhseqno = inh->inhseqno;
	}
	systable_endscan(scan);

	/* Match up the columns and bump attinhcount and attislocal */
	MergeAttributesIntoExisting(child_rel, parent_rel);

	/* Match up the constraints and bump coninhcount as needed */
	MergeConstraintsIntoExisting(child_rel, parent_rel);

	/*
	 * OK, it looks valid.  Make the catalog entries that show inheritance.
	 */
	StoreCatalogInheritance1(RelationGetRelid(child_rel),
							 RelationGetRelid(parent_rel),
							 inhseqno + 1,
							 catalogRelation,
							 parent_rel->rd_rel->relkind ==
							 RELKIND_PARTITIONED_TABLE);

	/* Now we're done with pg_inherits */
	table_close(catalogRelation, RowExclusiveLock);
}

/*
 * Obtain the source-text form of the constraint expression for a check
 * constraint, given its pg_constraint tuple
 */
static char *
decompile_conbin(HeapTuple contup, TupleDesc tupdesc)
{
	Form_pg_constraint con;
	bool		isnull;
	Datum		attr;
	Datum		expr;

	con = (Form_pg_constraint) GETSTRUCT(contup);
	attr = heap_getattr(contup, Anum_pg_constraint_conbin, tupdesc, &isnull);
	if (isnull)
		elog(ERROR, "null conbin for constraint %u", con->oid);

	expr = DirectFunctionCall2(pg_get_expr, attr,
							   ObjectIdGetDatum(con->conrelid));
	return TextDatumGetCString(expr);
}

/*
 * Determine whether two check constraints are functionally equivalent
 *
 * The test we apply is to see whether they reverse-compile to the same
 * source string.  This insulates us from issues like whether attributes
 * have the same physical column numbers in parent and child relations.
 */
static bool
constraints_equivalent(HeapTuple a, HeapTuple b, TupleDesc tupleDesc)
{
	Form_pg_constraint acon = (Form_pg_constraint) GETSTRUCT(a);
	Form_pg_constraint bcon = (Form_pg_constraint) GETSTRUCT(b);

	if (acon->condeferrable != bcon->condeferrable ||
		acon->condeferred != bcon->condeferred ||
		strcmp(decompile_conbin(a, tupleDesc),
			   decompile_conbin(b, tupleDesc)) != 0)
		return false;
	else
		return true;
}

/*
 * Check columns in child table match up with columns in parent, and increment
 * their attinhcount.
 *
 * Called by CreateInheritance
 *
 * Currently all parent columns must be found in child. Missing columns are an
 * error.  One day we might consider creating new columns like CREATE TABLE
 * does.  However, that is widely unpopular --- in the common use case of
 * partitioned tables it's a foot-gun.
 *
 * The data type must match exactly. If the parent column is NOT NULL then
 * the child must be as well. Defaults are not compared, however.
 */
static void
MergeAttributesIntoExisting(Relation child_rel, Relation parent_rel)
{
	Relation	attrrel;
	AttrNumber	parent_attno;
	int			parent_natts;
	TupleDesc	tupleDesc;
	HeapTuple	tuple;
	bool		child_is_partition = false;

	attrrel = table_open(AttributeRelationId, RowExclusiveLock);

	tupleDesc = RelationGetDescr(parent_rel);
	parent_natts = tupleDesc->natts;

	/* If parent_rel is a partitioned table, child_rel must be a partition */
	if (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		child_is_partition = true;

	for (parent_attno = 1; parent_attno <= parent_natts; parent_attno++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc,
													parent_attno - 1);
		char	   *attributeName = NameStr(attribute->attname);

		/* Ignore dropped columns in the parent. */
		if (attribute->attisdropped)
			continue;

		/* Find same column in child (matching on column name). */
		tuple = SearchSysCacheCopyAttName(RelationGetRelid(child_rel),
										  attributeName);
		if (HeapTupleIsValid(tuple))
		{
			/* Check they are same type, typmod, and collation */
			Form_pg_attribute childatt = (Form_pg_attribute) GETSTRUCT(tuple);

			if (attribute->atttypid != childatt->atttypid ||
				attribute->atttypmod != childatt->atttypmod)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("child table \"%s\" has different type for column \"%s\"",
								RelationGetRelationName(child_rel),
								attributeName)));

			if (attribute->attcollation != childatt->attcollation)
				ereport(ERROR,
						(errcode(ERRCODE_COLLATION_MISMATCH),
						 errmsg("child table \"%s\" has different collation for column \"%s\"",
								RelationGetRelationName(child_rel),
								attributeName)));

			/*
			 * Check child doesn't discard NOT NULL property.  (Other
			 * constraints are checked elsewhere.)
			 */
			if (attribute->attnotnull && !childatt->attnotnull)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("column \"%s\" in child table must be marked NOT NULL",
								attributeName)));

			/*
			 * If parent column is generated, child column must be, too.
			 */
			if (attribute->attgenerated && !childatt->attgenerated)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("column \"%s\" in child table must be a generated column",
								attributeName)));

			/*
			 * Check that both generation expressions match.
			 *
			 * The test we apply is to see whether they reverse-compile to the
			 * same source string.  This insulates us from issues like whether
			 * attributes have the same physical column numbers in parent and
			 * child relations.  (See also constraints_equivalent().)
			 */
			if (attribute->attgenerated && childatt->attgenerated)
			{
				TupleConstr *child_constr = child_rel->rd_att->constr;
				TupleConstr *parent_constr = parent_rel->rd_att->constr;
				char	   *child_expr = NULL;
				char	   *parent_expr = NULL;

				Assert(child_constr != NULL);
				Assert(parent_constr != NULL);

				for (int i = 0; i < child_constr->num_defval; i++)
				{
					if (child_constr->defval[i].adnum == childatt->attnum)
					{
						child_expr =
							TextDatumGetCString(DirectFunctionCall2(pg_get_expr,
																	CStringGetTextDatum(child_constr->defval[i].adbin),
																	ObjectIdGetDatum(child_rel->rd_id)));
						break;
					}
				}
				Assert(child_expr != NULL);

				for (int i = 0; i < parent_constr->num_defval; i++)
				{
					if (parent_constr->defval[i].adnum == attribute->attnum)
					{
						parent_expr =
							TextDatumGetCString(DirectFunctionCall2(pg_get_expr,
																	CStringGetTextDatum(parent_constr->defval[i].adbin),
																	ObjectIdGetDatum(parent_rel->rd_id)));
						break;
					}
				}
				Assert(parent_expr != NULL);

				if (strcmp(child_expr, parent_expr) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("column \"%s\" in child table has a conflicting generation expression",
									attributeName)));
			}

			/*
			 * OK, bump the child column's inheritance count.  (If we fail
			 * later on, this change will just roll back.)
			 */
			childatt->attinhcount++;

			/*
			 * In case of partitions, we must enforce that value of attislocal
			 * is same in all partitions. (Note: there are only inherited
			 * attributes in partitions)
			 */
			if (child_is_partition)
			{
				Assert(childatt->attinhcount == 1);
				childatt->attislocal = false;
			}

			CatalogTupleUpdate(attrrel, &tuple->t_self, tuple);
			heap_freetuple(tuple);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("child table is missing column \"%s\"",
							attributeName)));
		}
	}

	table_close(attrrel, RowExclusiveLock);
}

/*
 * Check constraints in child table match up with constraints in parent,
 * and increment their coninhcount.
 *
 * Constraints that are marked ONLY in the parent are ignored.
 *
 * Called by CreateInheritance
 *
 * Currently all constraints in parent must be present in the child. One day we
 * may consider adding new constraints like CREATE TABLE does.
 *
 * XXX This is O(N^2) which may be an issue with tables with hundreds of
 * constraints. As long as tables have more like 10 constraints it shouldn't be
 * a problem though. Even 100 constraints ought not be the end of the world.
 *
 * XXX See MergeWithExistingConstraint too if you change this code.
 */
static void
MergeConstraintsIntoExisting(Relation child_rel, Relation parent_rel)
{
	Relation	catalog_relation;
	TupleDesc	tuple_desc;
	SysScanDesc parent_scan;
	ScanKeyData parent_key;
	HeapTuple	parent_tuple;
	bool		child_is_partition = false;

	catalog_relation = table_open(ConstraintRelationId, RowExclusiveLock);
	tuple_desc = RelationGetDescr(catalog_relation);

	/* If parent_rel is a partitioned table, child_rel must be a partition */
	if (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		child_is_partition = true;

	/* Outer loop scans through the parent's constraint definitions */
	ScanKeyInit(&parent_key,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(parent_rel)));
	parent_scan = systable_beginscan(catalog_relation, ConstraintRelidTypidNameIndexId,
									 true, NULL, 1, &parent_key);

	while (HeapTupleIsValid(parent_tuple = systable_getnext(parent_scan)))
	{
		Form_pg_constraint parent_con = (Form_pg_constraint) GETSTRUCT(parent_tuple);
		SysScanDesc child_scan;
		ScanKeyData child_key;
		HeapTuple	child_tuple;
		bool		found = false;

		if (parent_con->contype != CONSTRAINT_CHECK)
			continue;

		/* if the parent's constraint is marked NO INHERIT, it's not inherited */
		if (parent_con->connoinherit)
			continue;

		/* Search for a child constraint matching this one */
		ScanKeyInit(&child_key,
					Anum_pg_constraint_conrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(RelationGetRelid(child_rel)));
		child_scan = systable_beginscan(catalog_relation, ConstraintRelidTypidNameIndexId,
										true, NULL, 1, &child_key);

		while (HeapTupleIsValid(child_tuple = systable_getnext(child_scan)))
		{
			Form_pg_constraint child_con = (Form_pg_constraint) GETSTRUCT(child_tuple);
			HeapTuple	child_copy;

			if (child_con->contype != CONSTRAINT_CHECK)
				continue;

			if (strcmp(NameStr(parent_con->conname),
					   NameStr(child_con->conname)) != 0)
				continue;

			if (!constraints_equivalent(parent_tuple, child_tuple, tuple_desc))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("child table \"%s\" has different definition for check constraint \"%s\"",
								RelationGetRelationName(child_rel),
								NameStr(parent_con->conname))));

			/* If the child constraint is "no inherit" then cannot merge */
			if (child_con->connoinherit)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("constraint \"%s\" conflicts with non-inherited constraint on child table \"%s\"",
								NameStr(child_con->conname),
								RelationGetRelationName(child_rel))));

			/*
			 * If the child constraint is "not valid" then cannot merge with a
			 * valid parent constraint
			 */
			if (parent_con->convalidated && !child_con->convalidated)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("constraint \"%s\" conflicts with NOT VALID constraint on child table \"%s\"",
								NameStr(child_con->conname),
								RelationGetRelationName(child_rel))));

			/*
			 * OK, bump the child constraint's inheritance count.  (If we fail
			 * later on, this change will just roll back.)
			 */
			child_copy = heap_copytuple(child_tuple);
			child_con = (Form_pg_constraint) GETSTRUCT(child_copy);
			child_con->coninhcount++;

			/*
			 * In case of partitions, an inherited constraint must be
			 * inherited only once since it cannot have multiple parents and
			 * it is never considered local.
			 */
			if (child_is_partition)
			{
				Assert(child_con->coninhcount == 1);
				child_con->conislocal = false;
			}

			CatalogTupleUpdate(catalog_relation, &child_copy->t_self, child_copy);
			heap_freetuple(child_copy);

			found = true;
			break;
		}

		systable_endscan(child_scan);

		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("child table is missing constraint \"%s\"",
							NameStr(parent_con->conname))));
	}

	systable_endscan(parent_scan);
	table_close(catalog_relation, RowExclusiveLock);
}

/*
 * ALTER TABLE NO INHERIT
 *
 * Return value is the address of the relation that is no longer parent.
 */
static ObjectAddress
ATExecDropInherit(Relation rel, RangeVar *parent, LOCKMODE lockmode)
{
	ObjectAddress address;
	Relation	parent_rel;

	if (rel->rd_rel->relispartition)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot change inheritance of a partition")));

	/*
	 * AccessShareLock on the parent is probably enough, seeing that DROP
	 * TABLE doesn't lock parent tables at all.  We need some lock since we'll
	 * be inspecting the parent's schema.
	 */
	parent_rel = table_openrv(parent, AccessShareLock);

	/*
	 * We don't bother to check ownership of the parent table --- ownership of
	 * the child is presumed enough rights.
	 */

	/* Off to RemoveInheritance() where most of the work happens */
	RemoveInheritance(rel, parent_rel, false);

	ObjectAddressSet(address, RelationRelationId,
					 RelationGetRelid(parent_rel));

	/* keep our lock on the parent relation until commit */
	table_close(parent_rel, NoLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "NO INHERIT"
		);

	return address;
}

/*
 * MarkInheritDetached
 *
 * Set inhdetachpending for a partition, for ATExecDetachPartition
 * in concurrent mode.  While at it, verify that no other partition is
 * already pending detach.
 */
static void
MarkInheritDetached(Relation child_rel, Relation parent_rel)
{
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	inheritsTuple;
	bool		found = false;

	Assert(parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

	/*
	 * Find pg_inherits entries by inhparent.  (We need to scan them all in
	 * order to verify that no other partition is pending detach.)
	 */
	catalogRelation = table_open(InheritsRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				Anum_pg_inherits_inhparent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(parent_rel)));
	scan = systable_beginscan(catalogRelation, InheritsParentIndexId,
							  true, NULL, 1, &key);

	while (HeapTupleIsValid(inheritsTuple = systable_getnext(scan)))
	{
		Form_pg_inherits inhForm;

		inhForm = (Form_pg_inherits) GETSTRUCT(inheritsTuple);
		if (inhForm->inhdetachpending)
			ereport(ERROR,
					errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("partition \"%s\" already pending detach in partitioned table \"%s.%s\"",
						   get_rel_name(inhForm->inhrelid),
						   get_namespace_name(parent_rel->rd_rel->relnamespace),
						   RelationGetRelationName(parent_rel)),
					errhint("Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending detach operation."));

		if (inhForm->inhrelid == RelationGetRelid(child_rel))
		{
			HeapTuple	newtup;

			newtup = heap_copytuple(inheritsTuple);
			((Form_pg_inherits) GETSTRUCT(newtup))->inhdetachpending = true;

			CatalogTupleUpdate(catalogRelation,
							   &inheritsTuple->t_self,
							   newtup);
			found = true;
			heap_freetuple(newtup);
			/* keep looking, to ensure we catch others pending detach */
		}
	}

	/* Done */
	systable_endscan(scan);
	table_close(catalogRelation, RowExclusiveLock);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"%s\" is not a partition of relation \"%s\"",
						RelationGetRelationName(child_rel),
						RelationGetRelationName(parent_rel))));
}

/*
 * RemoveInheritance
 *
 * Drop a parent from the child's parents. This just adjusts the attinhcount
 * and attislocal of the columns and removes the pg_inherit and pg_depend
 * entries.  expect_detached is passed down to DeleteInheritsTuple, q.v..
 *
 * If attinhcount goes to 0 then attislocal gets set to true. If it goes back
 * up attislocal stays true, which means if a child is ever removed from a
 * parent then its columns will never be automatically dropped which may
 * surprise. But at least we'll never surprise by dropping columns someone
 * isn't expecting to be dropped which would actually mean data loss.
 *
 * coninhcount and conislocal for inherited constraints are adjusted in
 * exactly the same way.
 *
 * Common to ATExecDropInherit() and ATExecDetachPartition().
 */
static void
RemoveInheritance(Relation child_rel, Relation parent_rel, bool expect_detached)
{
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key[3];
	HeapTuple	attributeTuple,
				constraintTuple;
	List	   *connames;
	bool		found;
	bool		child_is_partition = false;

	/* If parent_rel is a partitioned table, child_rel must be a partition */
	if (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		child_is_partition = true;

	found = DeleteInheritsTuple(RelationGetRelid(child_rel),
								RelationGetRelid(parent_rel),
								expect_detached,
								RelationGetRelationName(child_rel));
	if (!found)
	{
		if (child_is_partition)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s\" is not a partition of relation \"%s\"",
							RelationGetRelationName(child_rel),
							RelationGetRelationName(parent_rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s\" is not a parent of relation \"%s\"",
							RelationGetRelationName(parent_rel),
							RelationGetRelationName(child_rel))));
	}

	/*
	 * Search through child columns looking for ones matching parent rel
	 */
	catalogRelation = table_open(AttributeRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(child_rel)));
	scan = systable_beginscan(catalogRelation, AttributeRelidNumIndexId,
							  true, NULL, 1, key);
	while (HeapTupleIsValid(attributeTuple = systable_getnext(scan)))
	{
		Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);

		/* Ignore if dropped or not inherited */
		if (att->attisdropped)
			continue;
		if (att->attinhcount <= 0)
			continue;

		if (SearchSysCacheExistsAttName(RelationGetRelid(parent_rel),
										NameStr(att->attname)))
		{
			/* Decrement inhcount and possibly set islocal to true */
			HeapTuple	copyTuple = heap_copytuple(attributeTuple);
			Form_pg_attribute copy_att = (Form_pg_attribute) GETSTRUCT(copyTuple);

			copy_att->attinhcount--;
			if (copy_att->attinhcount == 0)
				copy_att->attislocal = true;

			CatalogTupleUpdate(catalogRelation, &copyTuple->t_self, copyTuple);
			heap_freetuple(copyTuple);
		}
	}
	systable_endscan(scan);
	table_close(catalogRelation, RowExclusiveLock);

	/*
	 * Likewise, find inherited check constraints and disinherit them. To do
	 * this, we first need a list of the names of the parent's check
	 * constraints.  (We cheat a bit by only checking for name matches,
	 * assuming that the expressions will match.)
	 */
	catalogRelation = table_open(ConstraintRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(parent_rel)));
	scan = systable_beginscan(catalogRelation, ConstraintRelidTypidNameIndexId,
							  true, NULL, 1, key);

	connames = NIL;

	while (HeapTupleIsValid(constraintTuple = systable_getnext(scan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(constraintTuple);

		if (con->contype == CONSTRAINT_CHECK)
			connames = lappend(connames, pstrdup(NameStr(con->conname)));
	}

	systable_endscan(scan);

	/* Now scan the child's constraints */
	ScanKeyInit(&key[0],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(child_rel)));
	scan = systable_beginscan(catalogRelation, ConstraintRelidTypidNameIndexId,
							  true, NULL, 1, key);

	while (HeapTupleIsValid(constraintTuple = systable_getnext(scan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(constraintTuple);
		bool		match;
		ListCell   *lc;

		if (con->contype != CONSTRAINT_CHECK)
			continue;

		match = false;
		foreach(lc, connames)
		{
			if (strcmp(NameStr(con->conname), (char *) lfirst(lc)) == 0)
			{
				match = true;
				break;
			}
		}

		if (match)
		{
			/* Decrement inhcount and possibly set islocal to true */
			HeapTuple	copyTuple = heap_copytuple(constraintTuple);
			Form_pg_constraint copy_con = (Form_pg_constraint) GETSTRUCT(copyTuple);

			if (copy_con->coninhcount <= 0) /* shouldn't happen */
				elog(ERROR, "relation %u has non-inherited constraint \"%s\"",
					 RelationGetRelid(child_rel), NameStr(copy_con->conname));

			copy_con->coninhcount--;
			if (copy_con->coninhcount == 0)
				copy_con->conislocal = true;

			CatalogTupleUpdate(catalogRelation, &copyTuple->t_self, copyTuple);
			heap_freetuple(copyTuple);
		}
	}

	systable_endscan(scan);
	table_close(catalogRelation, RowExclusiveLock);

	drop_parent_dependency(RelationGetRelid(child_rel),
						   RelationRelationId,
						   RelationGetRelid(parent_rel),
						   child_dependency_type(child_is_partition));

	/*
	 * Post alter hook of this inherits. Since object_access_hook doesn't take
	 * multiple object identifiers, we relay oid of parent relation using
	 * auxiliary_id argument.
	 */
	InvokeObjectPostAlterHookArg(InheritsRelationId,
								 RelationGetRelid(child_rel), 0,
								 RelationGetRelid(parent_rel), false);
}

/*
 * Drop the dependency created by StoreCatalogInheritance1 (CREATE TABLE
 * INHERITS/ALTER TABLE INHERIT -- refclassid will be RelationRelationId) or
 * heap_create_with_catalog (CREATE TABLE OF/ALTER TABLE OF -- refclassid will
 * be TypeRelationId).  There's no convenient way to do this, so go trawling
 * through pg_depend.
 */
static void
drop_parent_dependency(Oid relid, Oid refclassid, Oid refobjid,
					   DependencyType deptype)
{
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key[3];
	HeapTuple	depTuple;

	catalogRelation = table_open(DependRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&key[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(0));

	scan = systable_beginscan(catalogRelation, DependDependerIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(depTuple = systable_getnext(scan)))
	{
		Form_pg_depend dep = (Form_pg_depend) GETSTRUCT(depTuple);

		if (dep->refclassid == refclassid &&
			dep->refobjid == refobjid &&
			dep->refobjsubid == 0 &&
			dep->deptype == deptype)
			CatalogTupleDelete(catalogRelation, &depTuple->t_self);
	}

	systable_endscan(scan);
	table_close(catalogRelation, RowExclusiveLock);
}

/*
 * Update the pg_attribute entries of dropped columns in given relation,
 * as if they were of type int4.
 *
 * This is used by ALTER TABLE SET DISTRIBUTED BY, which swaps the
 * relation file with a newly constructed temp table. The temp table is
 * constructed with int4 columns standing in for the dropped columns,
 * and this function is used to update the original table's definition
 * to match that.
 */
static void
change_dropped_col_datatypes(Relation rel)
{
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;

	/*
	 * Loop through all dropped columns.
	 */
	catalogRelation = table_open(AttributeRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	scan = systable_beginscan(catalogRelation, AttributeRelidNumIndexId,
							  true, NULL, 1, &key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(tuple);
		HeapTuple copyTuple;

		if (att->attisdropped)
		{
			Assert(att->attnum > 0 && att->attnum <= RelationGetNumberOfAttributes(rel));

			copyTuple = heap_copytuple(tuple);
			att = (Form_pg_attribute) GETSTRUCT(copyTuple);

			att->attlen = sizeof(int32);
			att->attndims = 0;
			att->atttypmod = -1;
			att->attbyval = true;
			att->attstorage = 'p';
			att->attalign = 'i';

			CatalogTupleUpdate(catalogRelation, &tuple->t_self, copyTuple);
		}
	}
	systable_endscan(scan);

	table_close(catalogRelation, RowExclusiveLock);
}

/*
 * Build:
 *
 * CREATE TABLE pg_temp_<NNNN> AS SELECT * FROM rel
 *   DISTRIBUTED BY dist_clause
 */
static QueryDesc *
build_ctas_with_dist(Relation rel, DistributedBy *dist_clause,
					 List *storage_opts, RangeVar **tmprv,
					 bool useExistingColumnAttributes)
{
	Query *q;
	SelectStmt *s = makeNode(SelectStmt);
	Node *n;
	RangeVar *from_tbl;
	List *rewritten;
	PlannedStmt *stmt;
	DestReceiver *dest;
	QueryDesc *queryDesc;
	RangeVar *tmprel = make_temp_table_name(rel, MyBackendId);
	TupleDesc	tupdesc;
	int			attno;
	bool		pre_built;
	IntoClause	*into = NULL;

	tupdesc = RelationGetDescr(rel);

	for (attno = 0; attno < tupdesc->natts; attno++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attno);
		ResTarget  *t;

		t = makeNode(ResTarget);

		if (!att->attisdropped)
		{
			ColumnRef 		   *c;

			c = makeNode(ColumnRef);
			c->location = -1;
			c->fields = lappend(c->fields, makeString(pstrdup(get_namespace_name(RelationGetNamespace(rel)))));
			c->fields = lappend(c->fields, makeString(pstrdup(RelationGetRelationName(rel))));
			c->fields = lappend(c->fields, makeString(pstrdup(NameStr(att->attname))));
			t->val = (Node *) c;
		}
		else
		{
			/* Use a dummy NULL::int4 column to stand in for any dropped columns. */
			t->val = (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32), (Datum) 0, true, true);
		}
		t->location = -1;

		s->targetList = lappend(s->targetList, t);
	}

	from_tbl = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
							pstrdup(RelationGetRelationName(rel)), -1);
	from_tbl->inh = false;	   /* MPP-5300: turn off inheritance -
								* Otherwise, the data from the child
								* tables is added to the parent!
								*/
	s->fromClause = list_make1(from_tbl);

	pre_built = prebuild_temp_table(rel, tmprel, dist_clause,
									get_am_name(rel->rd_rel->relam),
									storage_opts,
									RelationIsAppendOptimized(rel),
									useExistingColumnAttributes);
	if (pre_built)
	{
		InsertStmt *i = makeNode(InsertStmt);

		i->relation = tmprel;
		i->selectStmt = (Node *)s;
		n = (Node *)i;
	}
	else
	{
		Oid tblspc = rel->rd_rel->reltablespace;
		List *q_list, *p_list;
		PlannedStmt *pstmt;
		CreateTableAsStmt *ctas;

		into = makeNode(IntoClause);
		into->rel = tmprel;
		into->accessMethod = get_am_name(rel->rd_rel->relam);
		into->options = storage_opts;
		into->tableSpaceName = get_tablespace_name(tblspc);
		into->distributedBy = (Node *)dist_clause;
		s->intoClause = into;

		RawStmt *rawstmt = makeNode(RawStmt);
		rawstmt->stmt = (Node *) s;
		rawstmt->stmt_location = -1;
		rawstmt->stmt_len = 0;

		q_list = pg_analyze_and_rewrite(rawstmt, synthetic_sql, NULL, 0, NULL);
		p_list = pg_plan_queries(q_list, synthetic_sql, 0, NULL);
		pstmt = linitial_node(PlannedStmt, p_list);
		ctas = castNode(CreateTableAsStmt, pstmt->utilityStmt);

		n = ctas->query;
	}
	*tmprv = tmprel;

	if (pre_built)
	{
		RawStmt *rawstmt = makeNode(RawStmt);
		rawstmt->stmt = (Node *) n;
		rawstmt->stmt_location = -1;
		rawstmt->stmt_len = 0;

		q = parse_analyze(rawstmt, synthetic_sql, NULL, 0, NULL);
	}
	else
		q = (Query *) n;

	AcquireRewriteLocks(q, true, false);

	/* Rewrite through rule system */
	rewritten = QueryRewrite(q);

	/* We don't expect more or less than one result query */
	Assert(list_length(rewritten) == 1);

	q = (Query *) linitial(rewritten);
	Assert(q->commandType == CMD_SELECT || q->commandType == CMD_INSERT);

	/* plan the query */
	stmt = planner(q, synthetic_sql, 0, NULL);
	stmt->intoClause = into;

	/*
	 * Update snapshot command ID to ensure this query sees results of any
	 * previously executed queries.
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create dest receiver for COPY OUT */
	dest = CreateIntoRelDestReceiver(into);

	/* Create a QueryDesc requesting no output */
	queryDesc = CreateQueryDesc(stmt, debug_query_string,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, NULL, GP_INSTRUMENT_OPTS);
	PopActiveSnapshot();

	return queryDesc;
}

/*
 * GPDB: Convenience function to get reloptions for a given relation.
 */
static Datum
get_rel_opts(Relation rel)
{
	Datum newOptions = PointerGetDatum(NULL);

	/* Get the old reloptions */
	bool isnull;
	Oid relid = RelationGetRelid(rel);
	HeapTuple optsTuple;

	optsTuple = SearchSysCache1(RELOID,
								ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(optsTuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	newOptions = SysCacheGetAttr(RELOID, optsTuple,
								 Anum_pg_class_reloptions, &isnull);

	/* take a copy since we're using it after ReleaseSysCache() */
	if (!isnull)
		newOptions = datumCopy(newOptions, false, -1);

	ReleaseSysCache(optsTuple);

	return newOptions;
}

static RangeVar *
make_temp_table_name(Relation rel, BackendId id)
{
	char	   *nspname;
	char		tmpname[NAMEDATALEN];
	RangeVar   *tmprel;

	/* temporary enough */
	snprintf(tmpname, NAMEDATALEN, "pg_temp_%u_%i", RelationGetRelid(rel), id);
	nspname = get_namespace_name(RelationGetNamespace(rel));

	tmprel = makeRangeVar(nspname, pstrdup(tmpname), -1);

	/*
	 * Ensure the temp relation has the same persistence setting with the
	 * original relation.
	 */
	tmprel->relpersistence = rel->rd_rel->relpersistence;

	return tmprel;
}

/*
 * If the table has dropped columns, we must create the table and
 * drop the columns before we can dispatch the select statement.
 * Return true if we do it, false if we do not. If we return false,
 * there are no dropped columns and we can do a SELECT INTO later.
 * If we need to do it, but fail, issue an error. (See make_type.)
 *
 * Specifically for build_ctas_with_dist.
 *
 * Note that the caller should guarantee that isTmpTableAo has
 * a value that matches 'opts'.
 */
static bool
prebuild_temp_table(Relation rel, RangeVar *tmpname, DistributedBy *distro,
					char *amname, List *opts,
					bool isTmpTableAo, bool useExistingColumnAttributes)
{
	bool need_rebuild = false;
	int attno = 0;
	TupleDesc tupdesc = RelationGetDescr(rel);

	/* 
	 * We cannot CTAS and do per column compression for AO_COLUMN tables so we need
	 * to CREATE and then INSERT.
	 */
	if (RelationIsAoCols(rel))
		need_rebuild = true;

	if (!need_rebuild)
	{
		for (attno = 0; attno < tupdesc->natts; attno++)
		{
			if (TupleDescAttr(tupdesc, attno)->attisdropped)
			{
				need_rebuild = true;
				break;
			}
		}
	}

	/*
	 * If the new table is an AO table with indexes, always use
	 * Create Table + Insert Into. During Create Table phase,
	 * we determine whether to create the block directory
	 * depending on whether the original table has indexes. It is
	 * important to create the block directory to support the reindex
	 * later. See MPP-9545 for more info.
	 */
	if (isTmpTableAo &&
		rel->rd_rel->relhasindex)
		need_rebuild = true;

	if (need_rebuild)
	{
		CreateStmt *cs = makeNode(CreateStmt);
		Query *q;
		DestReceiver *dest = None_Receiver;
		List **col_encs = NULL;

		cs->relKind = RELKIND_RELATION;
		cs->accessMethod = amname;
		cs->distributedBy = distro;
		cs->relation = tmpname;
		cs->ownerid = rel->rd_rel->relowner;
		cs->tablespacename = get_tablespace_name(rel->rd_rel->reltablespace);
		cs->buildAoBlkdir = false;

		if (isTmpTableAo &&
			rel->rd_rel->relhasindex)
			cs->buildAoBlkdir = true;

		/* 
		 * For AO/CO tables, need to remove table level compression settings 
		 * for the AO_COLUMN case since they're set at the column level.
		 */
		if (RelationIsAoCols(rel))
		{
			ListCell *lc;

			foreach(lc, opts)
			{
				DefElem *de = lfirst(lc);

				if (!useExistingColumnAttributes || 
						!de->defname || 
						!is_storage_encoding_directive(de->defname))
					cs->options = lappend(cs->options, de);
			}
			if (useExistingColumnAttributes)
				col_encs = RelationGetUntransformedAttributeOptions(rel);
		}
		else
			cs->options = opts;

		for (attno = 0; attno < tupdesc->natts; attno++)
		{
			ColumnDef *cd = makeNode(ColumnDef);
			TypeName *tname = NULL;
			Form_pg_attribute att = TupleDescAttr(tupdesc, attno);

			cd->is_local = true;

			if (att->attisdropped)
			{
				/*
				 * Use dummy int4 columns to stand in for dropped columns.
				 * We cannot easily reconstruct the original layout, because
				 * we don't know what the original datatype was, and it might
				 * not even exist anymore. This means that the temp table is
				 * not binary-compatible with the old table. We will fix that
				 * by updating the catalogs of the original table, to match
				 * the temp table we build here, before swapping the relation
				 * files.
				 */
				tname = makeTypeNameFromOid(INT4OID, -1);
				cd->colname = pstrdup(NameStr(att->attname));
			}
			else
			{
				Type typ = typeidType(att->atttypid);
				Oid typnamespace = ((Form_pg_type) GETSTRUCT(typ))->typnamespace;
				char *nspname = get_namespace_name(typnamespace);
				int arno;
				char *typstr;
				int32 ndims = att->attndims;

				tname = makeNode(TypeName);

				if (!PointerIsValid(nspname))
					elog(ERROR, "could not lookup namespace %d", typnamespace);
				cd->colname = pstrdup(NameStr(att->attname));
				typstr = typeTypeName(typ);
				tname->names = list_make2(makeString(nspname),
										  makeString(typstr));
				ReleaseSysCache(typ);
				tname->typemod = att->atttypmod;

				/*
				 * If this is a built in array type, like _int4, then reduce
				 * the array dimensions by 1. This is an annoying postgres
				 * hack which I wish would go away.
				 */
				if (typstr && typstr[0] == '_' && ndims > 0)
					ndims--;

				for (arno = 0; arno < ndims; arno++)
					/* bound of -1 are fine because this has no effect on data */
					tname->arrayBounds = lappend(tname->arrayBounds,
												 makeInteger(-1));
			}

			/* Per column encoding settings */
			if (col_encs)
				cd->encoding = col_encs[attno];

			tname->location = -1;
			cd->typeName = tname;
			cs->tableElts = lappend(cs->tableElts, cd);
		}

		RawStmt *rawstmt = makeNode(RawStmt);
		rawstmt->stmt = (Node *) cs;
		rawstmt->stmt_location = -1;
		rawstmt->stmt_len = 0;

		q = parse_analyze(rawstmt, synthetic_sql, NULL, 0, NULL);

		/* No planning needed, just make a wrapper PlannedStmt */
		PlannedStmt *pstmt = makeNode(PlannedStmt);
		pstmt->commandType = CMD_UTILITY;
		pstmt->canSetTag = false;
		pstmt->utilityStmt = (Node *) q->utilityStmt;
		pstmt->stmt_location = rawstmt->stmt_location;
		pstmt->stmt_len = rawstmt->stmt_len;

		ProcessUtility(pstmt,
					   synthetic_sql,
					   false,
					   PROCESS_UTILITY_SUBCOMMAND,
					   NULL,
					   NULL,
					   dest,
					   NULL);
		CommandCounterIncrement();
	}
	return need_rebuild;
}

/* Build a human readable tag for what we're doing */
static char *
make_distro_str(List *lwith, DistributedBy *ldistro)
{
	char       *distro_str = "SET WITH DISTRIBUTED BY";

	if (lwith && ldistro)
		distro_str = "SET WITH DISTRIBUTED BY";
	else
	{
		if (lwith)
			distro_str = "SET WITH";
		else if (ldistro)
			distro_str = "SET DISTRIBUTED BY";
	}
	return pstrdup(distro_str); /* don't return a stack address */
}

/*
 * Check if a new DISTRIBUTED BY clause is compatible with existing indexes.
 */
static void
checkPolicyCompatibleWithIndexes(Relation rel, GpPolicy *pol)
{
	List	   *indexoidlist = RelationGetIndexList(rel);
	ListCell   *indexoidscan;

	if (pol == NULL || pol->nattrs == 0)
		return;

	/* Loop over all indexes on the relation */
	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		HeapTuple	indexTuple;
		Form_pg_index indexStruct;

		indexTuple = SearchSysCache1(INDEXRELID,
									 ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

		if (indexStruct->indisprimary ||
			indexStruct->indisunique ||
			indexStruct->indisexclusion)
		{
			int2vector *indkey;
			oidvector  *indclass;
			Datum		datum;
			bool		isnull;
			Oid		   *exclops = NULL;

			indkey = &indexStruct->indkey;
			datum = SysCacheGetAttr(INDEXRELID, indexTuple,
									Anum_pg_index_indclass, &isnull);
			Assert(!isnull);
			indclass = (oidvector *) DatumGetPointer(datum);

			if (indexStruct->indisexclusion)
			{
				HeapTuple	ht_constr;
				Form_pg_constraint conrec;
				Oid			constraintId;
				Datum	   *elems;
				int			nElems;

				/*
				 * For an exclusion constraint, we need to extract the operator OIDs
				 * from pg_constraint
				 */
				constraintId = get_index_constraint(indexoid);
				if (!constraintId)
					elog(ERROR, "could not find pg_constraint entry for index %u", indexoid);

				ht_constr = SearchSysCache1(CONSTROID,
											ObjectIdGetDatum(constraintId));
				if (!HeapTupleIsValid(ht_constr))
					elog(ERROR, "cache lookup failed for constraint %u",
						 constraintId);
				conrec = (Form_pg_constraint) GETSTRUCT(ht_constr);
				datum = SysCacheGetAttr(CONSTROID, ht_constr,
										Anum_pg_constraint_conexclop,
										&isnull);
				if (isnull)
					elog(ERROR, "null conexclop for constraint %u",
						 constraintId);

				deconstruct_array(DatumGetArrayTypeP(datum),
								  OIDOID, sizeof(Oid), true, 'i',
								  &elems, NULL, &nElems);
				exclops = palloc(nElems * sizeof(Oid));
				for (int i = 0; i < nElems; i++)
					exclops[i] = DatumGetObjectId(elems[i]);
				ReleaseSysCache(ht_constr);
			}

			index_check_policy_compatible_context ctx;

			memset(&ctx, 0, sizeof(ctx));
			ctx.for_alter_dist_policy = true;
			ctx.is_constraint = indexStruct->indisprimary; /* unknown */
			ctx.is_unique = indexStruct->indisunique;
			ctx.is_primarykey = indexStruct->indisprimary;
			ctx.constraint_name = get_rel_name(indexoid);

			(void) index_check_policy_compatible(pol,
												 RelationGetDescr(rel),
												 indkey->values,
												 indclass->values,
												 exclops,
												 indexStruct->indnkeyatts,
												 true, /* report_error */
												 &ctx);
			if (exclops)
				pfree(exclops);
		}

		ReleaseSysCache(indexTuple);
	}

	list_free(indexoidlist);
}

/*
 * ALTER TABLE EXPAND TABLE
 *
 * Update a table's "numsegments" value to current cluster size, and move
 * data as needed to the new segments.
 *
 * There are currently only one way we can perform EXPAND TABLE:
 *
 * 1. Create a whole new relation file, with the new 'numsegments', copy all
 *    the data to the new reltion file, and swap it in place of the old one.
 *    This is called the "CTAS method", because it uses a CREATE TABLE AS
 *    command internally to create the new physical relation.
 * 
 * To support shrink, We add parameter numsegments to set table policy to 
 * arbitrary size. For expand, the numsegments is getgpsegmentCount. For shrink
 * the numsegments is input of user.
 */
static void
ATExecExpandTable(List **wqueue, Relation rel, AlterTableCmd *cmd, int numsegments)
{
	AlteredTableInfo	*tab;
	AlterTableCmd		*rootCmd;
	MemoryContext		oldContext;
	Oid					relid = RelationGetRelid(rel);
	GpPolicy			*newPolicy;
	GpPolicy			*policy = rel->rd_cdbpolicy;

	if (Gp_role == GP_ROLE_UTILITY)
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("EXPAND not supported in utility mode")));

	/* Permissions checks */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   RelationGetRelationName(rel));

	/* Can't ALTER TABLE SET system catalogs */
	if (IsSystemRelation(rel))
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			errmsg("permission denied: \"%s\" is a system catalog", RelationGetRelationName(rel))));

	oldContext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));
	newPolicy = GpPolicyCopy(policy);
	MemoryContextSwitchTo(oldContext);

	tab = linitial(*wqueue);
	rootCmd = (AlterTableCmd *)linitial(tab->subcmds[AT_PASS_MISC]);

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		/*
		 * Nothing to do on a partitioned table. But we better recurse to the
		 * child partitions.
		 */
	}
	else if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		if (rel_is_external_table(relid))
		{
			ExtTableEntry *ext = GetExtTableEntry(relid);

			if (!ext->iswritable)
			{
				/*
				 * Skip expanding readable external table, since data is not
				 * located inside gpdb
				 */
				return;
			}
		}
		else
		{
			/* Skip expanding foreign table, since data is not located inside gpdb */
			return;
		}
	}
	else
	{
		ATExecExpandTableCTAS(rootCmd, rel, cmd, numsegments);
	}

	/* Update numsegments to cluster size */
	newPolicy->numsegments = numsegments;
	GpPolicyReplace(relid, newPolicy);
}

/*
 * ALTER TABLE xxx EXPAND PARTITION PREPARE
 *
 * Update a partition table's "numsegments" value to current cluster size,
 * change policy type of leaf partitions to randomly,
 * the policy type of root and interior partitions are the same as before.
 *
 * For external(foreign) tables, only writable external tables have distribution
 * policy. So for writable external leaf partitions, expansion is finished during
 * prepare stage (the following functon) by simply updating numsegments field
 * in policy. For other external(foreign) tables, just ignore them.
 *
 * After we expand partition prepare from 2 segments to 3 segments, 
 * possible distribution policies of partition table:
 * a) original policy type is randomly:
 *    new policy type of all root/interior/leaf partitions are randomly on 3 segments
 * b) original policy type is hashed:
 *    new policy type of root/interior partitions are hashed on 3 segments
 *    and new policy type of leaf partitions are randomly on 3 segments
 *
 * @param rel the parent or leaf of partition table
 * 
 * Add parameter numsegments, see ATExecExpandTable for details.
 */
static void
ATExecExpandPartitionTablePrepare(Relation rel, int numsegments)
{
	Oid       relid = RelationGetRelid(rel);

	if (GpPolicyIsRandomPartitioned(rel->rd_cdbpolicy) || rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		GpPolicy	 *new_policy;
		MemoryContext oldcontext;

		/*
		 * we only change numsegments for root/interior/leaf partitions distributed randomly
		 * and root/interior partitions distributed by hash, and change the numsegments of policy to
		 * current cluster size
		 */
		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));
		new_policy = GpPolicyCopy(rel->rd_cdbpolicy);
		new_policy->numsegments = numsegments;
		MemoryContextSwitchTo(oldcontext);

		GpPolicyReplace(relid, new_policy);
		/* We should make the policy between on-disk catalog and on-memory relation cache consistently */
		rel->rd_cdbpolicy = new_policy;
	}
	else
	{
		if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		{
			/*
			 * For external|foreign leaves, only writable external
			 * table has policy entry and need to be handled.
			 */
			if (rel_is_external_table(relid))
			{
				ExtTableEntry *ext = GetExtTableEntry(relid);
				if (ext->iswritable)
				{
					GpPolicy	 *new_policy;
					MemoryContext oldcontext;

					/* Just modify the numsegments for external writable leaves */
					oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));
					new_policy = GpPolicyCopy(rel->rd_cdbpolicy);
					new_policy->numsegments = numsegments;
					MemoryContextSwitchTo(oldcontext);

					GpPolicyReplace(relid, new_policy);
					/* We should make the policy between on-disk catalog and on-memory relation cache consistently */
					rel->rd_cdbpolicy = new_policy;
				}
			}
		}
		else
		{
			GpPolicy	 *new_policy;
			MemoryContext oldcontext;

			/* we change policy type to randomly for regular leaf partitions distributed by hash */
			oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));
			new_policy = createRandomPartitionedPolicy(numsegments);
			MemoryContextSwitchTo(oldcontext);

			GpPolicyReplace(relid, new_policy);
			/* We should make the policy between on-disk catalog and on-memory relation cache consistently */
			rel->rd_cdbpolicy = new_policy;
		}
	}
}

static void
ATExecExpandTableCTAS(AlterTableCmd *rootCmd, Relation rel, AlterTableCmd *cmd, int numsegments)
{
	RangeVar			*tmprv;
	Oid					tmprelid;
	Oid					relid = RelationGetRelid(rel);
	ReindexParams		params = {0};
	/*--
	 * a) Ensure that the proposed policy is sensible
	 * b) Create a temporary table and reorganise data according to our desired
	 *    distribution policy. To do this, we build a Query node which express
	 *    the query:
	 *    CREATE TABLE tmp_tab_nam AS SELECT * FROM cur_table DISTRIBUTED BY (policy)
	 * c) Execute the query across all nodes
	 * d) Update our parse tree to include the details of the newly created
	 *    table
	 * e) Update the ownership of the temporary table
	 * f) Swap the relfilenodes of the existing table and the temporary table
	 * g) Update the policy on the QD to reflect the underlying data
	 * h) Drop the temporary table -- and with it, the old copy of the data
	 *--
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		AutoStatsCmdType	cmdType = AUTOSTATS_CMDTYPE_SENTINEL;
		DistributedBy		*distby;
		QueryDesc			*queryDesc;
		Oid					relationOid = InvalidOid;
		bool 				saveOptimizerGucValue;;

		/* Step (a) */
		/*
		 * Force the use of Postgres query optimizer, since Pivotal Optimizer (GPORCA) will not
		 * redistribute the tuples if the current and required distributions
		 * are both RANDOM even when reorganize is set to "true"
		 */
		saveOptimizerGucValue = optimizer;
		optimizer = false;

		/* Step (b) - build CTAS */
		distby = make_distributedby_for_rel(rel);
		distby->numsegments = numsegments;

		queryDesc = build_ctas_with_dist(rel, distby,
						untransformRelOptions(get_rel_opts(rel)),
						&tmprv,
						true);

		/*
		 * We need to update our snapshot here to make sure we see all
		 * committed work. We have an exclusive lock on the table so no one
		 * will be able to access the table now.
		 */
		PushActiveSnapshot(GetLatestSnapshot());

		/* Step (c) - run on all nodes */
		queryDesc->ddesc = makeNode(QueryDispatchDesc);
		queryDesc->ddesc->useChangedAOOpts = false;

		check_and_unassign_from_resgroup(queryDesc->plannedstmt);
		queryDesc->plannedstmt->query_mem =
				ResourceManagerGetQueryMemoryLimit(queryDesc->plannedstmt);

		ExecutorStart(queryDesc, 0);
		ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

		autostats_get_cmdtype(queryDesc, &cmdType, &relationOid);

		queryDesc->dest->rDestroy(queryDesc->dest);
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);

		
		auto_stats(cmdType, relationOid, queryDesc->es_processed,
				   already_under_executor_run() || utility_nested());

		FreeQueryDesc(queryDesc);

		/* Restore the old snapshot */
		PopActiveSnapshot();
		optimizer = saveOptimizerGucValue;

		CommandCounterIncrement(); /* see the effects of the command */

		/*
		 * Step (d) - tell the seg nodes about the temporary relation.
		 */
		/*
		 * Store the dispatch info in the command so that it gets sent to the QEs.
		 * We add one to it, so that '0' isn't a valid value. Makes it easier
		 * to sanity check that it's set in the QEs.
		 */
		cmd->backendId = MyBackendId + 1;
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		if (cmd->backendId <= 0)
			elog(ERROR, "did not receive backend ID info from QD for EXPAND TABLE");

		tmprv = make_temp_table_name(rel, cmd->backendId - 1);
	}
	else
		elog(ERROR, "cannot perform EXPAND TABLE in utility mode");

	/*
	 * Step (e) - Correct ownership on temporary table:
	 *   necessary so that the toast tables/indices have the correct
	 *   owner after we swap them.
	 *
	 * Note: ATExecChangeOwner does NOT dispatch, so this does not
	 * belong in the dispatch block above (MPP-9663).
	 */
	ATExecChangeOwner(RangeVarGetRelid(tmprv, NoLock, false),
					  rel->rd_rel->relowner, true, AccessExclusiveLock);
	CommandCounterIncrement(); /* see the effects of the command */

	/*
	 * Update pg_attribute for dropped columns. The temp table we built
	 * uses int4 to stand in for any dropped columns, so we need to update
	 * the original table's definition to match the new contents.
	 */
	change_dropped_col_datatypes(rel);

	/*
	 * Step (f) - swap relfilenodes and MORE !!!
	 */
	tmprelid = RangeVarGetRelid(tmprv, NoLock, false);
	swap_relation_files(relid, tmprelid,
						false, /* target_is_pg_class */
						false, /* swap_toast_by_content */
						false, /* swap_stats */
						true,
						RecentXmin,
						ReadNextMultiXactId(),
						NULL);

	/*
	 * Make changes from swapping relation files visible before updating
	 * options below or else we get an already updated tuple error.
	 */
	CommandCounterIncrement();

	/* now, reindex */
	reindex_relation(relid, 0, &params);

	/* Step (h) Drop the table */
	{
		ObjectAddress object;
		object.classId = RelationRelationId;
		object.objectId = tmprelid;
		object.objectSubId = 0;

		performDeletion(&object, DROP_RESTRICT, 0);
	}
}

/*
 * ALTER TABLE SET DISTRIBUTED BY
 *
 * set distribution policy for rel
 */
static void
ATExecSetDistributedBy(Relation rel, Node *node, AlterTableCmd *cmd)
{
	List 	   *lprime;
	List 	   *lwith;
	DistributedBy *ldistro;
	List	   *cols = NIL;
	ListCell   *lc;
	GpPolicy   *policy = NULL;
	QueryDesc  *queryDesc;
	RangeVar   *tmprv;
	Oid			tmprelid = InvalidOid;
	Oid			tarrelid = RelationGetRelid(rel);
	bool        rand_pol = false;
	bool        rep_pol = false;
	bool        force_reorg = false;
	bool		need_reorg;
	bool		change_policy = false;
	int			numsegments;
	bool 				save_optimizer_replicated_table_insert;
	Oid					relationOid = InvalidOid;
	AutoStatsCmdType 	cmdType = AUTOSTATS_CMDTYPE_SENTINEL;

	/* Can't ALTER TABLE SET system catalogs */
	if (IsSystemRelation(rel))
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			errmsg("permission denied: \"%s\" is a system catalog", RelationGetRelationName(rel))));

	Assert(PointerIsValid(node));
	Assert(IsA(node, List));

	lprime = (List *) node;

	/* 
	 * First element is the WITH clause, second element is the actual
	 * distribution clause.
	 */
	lwith   = (List *)linitial(lprime);
	ldistro = (DistributedBy *)lsecond(lprime);

	if (Gp_role == GP_ROLE_UTILITY)
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("SET DISTRIBUTED BY not supported in utility mode")));
	/*
	 * SET DISTRIBUTED BY only change the distribution policy, but should not
	 * change numsegments, keep the old value.
	 */
	numsegments = rel->rd_cdbpolicy->numsegments;

	if (Gp_role == GP_ROLE_DISPATCH && ldistro)
		ldistro->numsegments = numsegments;

	/* we only support partitioned/replicated tables */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (GpPolicyIsEntry(rel->rd_cdbpolicy))
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s not supported on non-distributed tables",
						ldistro ? "SET DISTRIBUTED BY" : "SET WITH")));

		if (rel_is_external_table(RelationGetRelid(rel)))
		{
			ExtTableEntry *e = GetExtTableEntry(RelationGetRelid(rel));
			if (!e->iswritable)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot set distribution policy of readable external table \"%s\"",
								RelationGetRelationName(rel))));
		}
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (lwith)
		{
			char		*reorg_str = "reorganize";
			List		*nlist = NIL;

			if (list_length(lwith) > 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot specify more than one option in WITH clause")));
			}

			DefElem *def = linitial(lwith);
			if (pg_strcasecmp(reorg_str, def->defname) == 0)
			{
				if (!def->arg)
					force_reorg = true;
				else if (IsA(def->arg, String) && pg_strcasecmp("TRUE", strVal(def->arg)) == 0)
					force_reorg = true;
				else if (IsA(def->arg, String) && pg_strcasecmp("FALSE", strVal(def->arg)) == 0)
					force_reorg = false;
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid REORGANIZE option"),
							 errhint("Valid REORGANIZE options are \"true\" or \"false\".")));
				}
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("option \"%s\" not supported",
								def->defname)));
			}

			lwith = nlist;
		}

		if (ldistro)
			change_policy = true;

		if (ldistro && ldistro->ptype == POLICYTYPE_PARTITIONED && ldistro->keyCols == NIL)
		{
			bool hasPrimaryKey = relationHasPrimaryKey(rel);
			bool hasUniqueIndex = relationHasUniqueIndex(rel);
			rand_pol = true;

			if (hasPrimaryKey || hasUniqueIndex)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("cannot set to DISTRIBUTED RANDOMLY because relation has %s", hasPrimaryKey ? "primary Key" : "unique index"),
						 errhint("Drop the %s first.", hasPrimaryKey ? "primary key" : "unique index")));
			}

			if (!force_reorg)
			{
				if (GpPolicyIsRandomPartitioned(rel->rd_cdbpolicy))
					ereport(WARNING,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("distribution policy of relation \"%s\" already set to DISTRIBUTED RANDOMLY",
									RelationGetRelationName(rel)),
							 errhint("Use ALTER TABLE \"%s\" SET WITH (REORGANIZE=TRUE) DISTRIBUTED RANDOMLY to force a random redistribution.",
									 RelationGetRelationName(rel))));
			}

			policy = createRandomPartitionedPolicy(ldistro->numsegments);

			/* always need to rebuild if changed from replicated policy */
			if (!GpPolicyIsReplicated(rel->rd_cdbpolicy))
			{
				MemoryContext oldcontext;

				GpPolicyReplace(RelationGetRelid(rel), policy);
				oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));
				rel->rd_cdbpolicy = GpPolicyCopy(policy);
				MemoryContextSwitchTo(oldcontext);

				cmd->policy = policy;

				/* no need to rebuild if REORGANIZE=false*/
				if (!force_reorg)
					goto l_distro_fini;
			}
		}

		if (ldistro && ldistro->ptype == POLICYTYPE_REPLICATED)
		{
			rep_pol = true;

			if (GpPolicyIsReplicated(rel->rd_cdbpolicy))
				ereport(WARNING,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("distribution policy of relation \"%s\" already set to DISTRIBUTED REPLICATED",
								RelationGetRelationName(rel)),
						 errhint("Use ALTER TABLE \"%s\" SET WITH (REORGANIZE=TRUE) DISTRIBUTED REPLICATED to force a replicated redistribution.",
								 RelationGetRelationName(rel))));

			policy = createReplicatedGpPolicy(ldistro->numsegments);

			/* rebuild only if policy changed */
			if (GpPolicyIsReplicated(rel->rd_cdbpolicy))
				goto l_distro_fini;

			/*
			 * system columns is not visiable to users for replicated table,
			 * so if table is convertint to replicated table, check if there
			 * are dependencies on the system columns.
			 */
			AttrNumber	attr;
			ObjectAddresses *checkObjects = new_object_addresses();

			for (attr = FirstLowInvalidHeapAttributeNumber + 1;
				 attr != InvalidAttrNumber; attr++)
			{
				ObjectAddress obj;
				obj.classId = RelationRelationId;
				obj.objectId = RelationGetRelid(rel);
				obj.objectSubId = attr;

				add_exact_object_address(&obj, checkObjects);
			}

			checkDependencies(checkObjects,
							  "cannot set distributed replicated because "
							  "other object depend on its system columns",
							  "system columns of replicated table will be exposed "
							  "to users after altering, resolve dependencies first");

			free_object_addresses(checkObjects);
		}
	}

	/*--
	 * Changing a table from random distribution to a specific distribution
	 * policy is the hard bit. For that, we must do the following:
	 *
	 * a) Ensure that the proposed policy is sensible
	 * b) Create a temporary table and reorganise data according to our desired
	 *    distribution policy. To do this, we build a Query node which express
	 *    the query:
	 *    CREATE TABLE tmp_tab_nam AS SELECT * FROM cur_table DISTRIBUTED BY (policy)
	 * c) Execute the query across all nodes
	 * d) Update our parse tree to include the details of the newly created
	 *    table
	 * e) Update the ownership of the temporary table
	 * f) Swap the relfilenodes of the existing table and the temporary table
	 * g) Update the policy on the QD to reflect the underlying data
	 * h) Drop the temporary table -- and with it, the old copy of the data
	 *--
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (change_policy)
		{
			List	*policykeys = NIL;
			List	*policyopclasses = NIL;

			/* Step (a) */
			if (!(rand_pol || rep_pol))
			{
				foreach(lc, ldistro->keyCols)
				{
					DistributionKeyElem *dkelem = (DistributionKeyElem *) lfirst(lc);
					char	   *colName = dkelem->name;
					HeapTuple	tuple;
					AttrNumber	attnum;
					Form_pg_attribute attform;
					Oid			opclass;

					tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);

					if (!HeapTupleIsValid(tuple))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("column \"%s\" of relation \"%s\" does not exist",
										colName,
										RelationGetRelationName(rel))));
					attform = (Form_pg_attribute) GETSTRUCT(tuple);
					attnum = attform->attnum;

					/* Prevent them from altering a system attribute */
					if (attnum <= 0)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot distribute by system column \"%s\"",
										colName)));

					/*
					 * Look up the opclass, like we do in for CREATE TABLE.
					 */
					opclass = cdb_get_opclass_for_column_def(dkelem->opclass, attform->atttypid);
					policykeys = lappend_int(policykeys, attnum);
					policyopclasses = lappend_oid(policyopclasses, opclass);

					ReleaseSysCache(tuple);
					cols = lappend(cols, lfirst(lc));
				} /* end foreach */

				Assert(policykeys != NIL);
				policy = createHashPartitionedPolicy(policykeys,
													 policyopclasses,
													 ldistro->numsegments);

				/*
				 * See if the old policy is the same as the new one.
				 */
				if (!force_reorg &&
					(policy->nattrs == rel->rd_cdbpolicy->nattrs))
				{
					int i;
					bool diff = false;

					for (i = 0; i < policy->nattrs; i++)
					{
						if (policy->attrs[i] != rel->rd_cdbpolicy->attrs[i])
						{
							diff = true;
							break;
						}
						if (policy->opclasses[i] != rel->rd_cdbpolicy->opclasses[i])
						{
							diff = true;
							break;
						}
					}
					if (!diff)
					{
						StringInfoData buf;

						initStringInfo(&buf);
						foreach(lc, ldistro->keyCols)
						{
							DistributionKeyElem *dkelem = (DistributionKeyElem *) lfirst(lc);

							if (buf.len > 0)
								appendStringInfo(&buf, ", ");
							appendStringInfoString(&buf, dkelem->name);
						}
						ereport(WARNING,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("distribution policy of relation \"%s\" "
								"already set to (%s)",
								RelationGetRelationName(rel),
								buf.data),
							 errhint("Use ALTER TABLE \"%s\" "
								"SET WITH (REORGANIZE=TRUE) "
								"DISTRIBUTED BY (%s) "
								"to force redistribution",
								RelationGetRelationName(rel),
								buf.data)));
						/* Tell QEs to do nothing */
						return;
						/* don't goto l_distro_fini -- didn't do anything! */
					}
				}
			}

			checkPolicyCompatibleWithIndexes(rel, policy);
		}

		if (!ldistro)
			ldistro = make_distributedby_for_rel(rel);

		if (rel->rd_rel->relkind == RELKIND_RELATION ||
			rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE ||
			rel->rd_rel->relkind == RELKIND_MATVIEW)
		{
			need_reorg = true;
		}
		else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			need_reorg = false;
		else
			elog(ERROR, "unexpected relkind '%c'", rel->rd_rel->relkind);

		if (need_reorg)
		{
			/*
			 * Make sure the redistribution happens for a randomly distributed table.
			 *
			 * Force the use of Postgres query optimizer, since Pivotal Optimizer (GPORCA) will not
			 * redistribute the tuples if the current and required distributions
			 * are both RANDOM even when reorganize is set to "true"
			 * Also set gp_force_random_redistribution to true.
			 */
			bool saveOptimizerGucValue = optimizer;
			bool saveRedistributeGucValue = gp_force_random_redistribution;
			optimizer = false;
			gp_force_random_redistribution = true;

			if (saveOptimizerGucValue)
				ereport(LOG,
						(errmsg("ALTER SET DISTRIBUTED BY: falling back to Postgres query optimizer to ensure re-distribution of tuples.")));

			GpPolicy *original_policy = NULL;

			/*
			 * Disable optimizer_replicated_table_insert so planner 
			 * can force a broadcast motion even both source and target
			 * are replicated table. This is important when altering
			 * distribution policy is called by gpexpand.
			 */
			save_optimizer_replicated_table_insert = optimizer_replicated_table_insert;
			optimizer_replicated_table_insert = false;

			if (force_reorg && !rand_pol && !GpPolicyIsReplicated(rel->rd_cdbpolicy))
			{
				/*
				 * since we force the reorg, we don't care about the original
				 * distribution policy of the source table hence, we can set the
				 * policy to random, which will force it to redistribute if the new
				 * distribution policy is partitioned, even the new partition policy
				 * is same as the original one, the query optimizer will generate
				 * redistribute plan.
				 */
				MemoryContext oldcontext;
				GpPolicy *random_policy = createRandomPartitionedPolicy(ldistro->numsegments);

				original_policy = rel->rd_cdbpolicy;
				/*
				 * break the link to avoid original_policy from getting deleted if
				 * relcache invalidation happens.
				 */
				rel->rd_cdbpolicy = NULL;
				/* update the catalog first and then assign the policy to rd_cdbpolicy */
				GpPolicyReplace(RelationGetRelid(rel), random_policy);

				oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));
				rel->rd_cdbpolicy = GpPolicyCopy(random_policy);
				MemoryContextSwitchTo(oldcontext);
			}

			/* Step (b) - build CTAS */
			queryDesc = build_ctas_with_dist(rel, ldistro,
											 untransformRelOptions(get_rel_opts(rel)),
											 &tmprv,
											 true);

			/* 
			 * We need to update our snapshot here to make sure we see all
			 * committed work. We have an exclusive lock on the table so no one
			 * will be able to access the table now.
			 */
			PushActiveSnapshot(GetLatestSnapshot());

			/* Step (c) - run on all nodes */
			queryDesc->ddesc = makeNode(QueryDispatchDesc);
			queryDesc->ddesc->useChangedAOOpts = false;
		
			/* GPDB hook for collecting query info */
			if (query_info_collect_hook)
				(*query_info_collect_hook)(METRICS_QUERY_SUBMIT, queryDesc);

			check_and_unassign_from_resgroup(queryDesc->plannedstmt);
			queryDesc->plannedstmt->query_mem =
				ResourceManagerGetQueryMemoryLimit(queryDesc->plannedstmt);

			ExecutorStart(queryDesc, 0);
			ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

			if (Gp_role == GP_ROLE_DISPATCH)
				autostats_get_cmdtype(queryDesc, &cmdType, &relationOid);

			queryDesc->dest->rDestroy(queryDesc->dest);
			ExecutorFinish(queryDesc);
			ExecutorEnd(queryDesc);

			if (Gp_role == GP_ROLE_DISPATCH)
			{
				bool inFunction = already_under_executor_run() || utility_nested();
				auto_stats(cmdType, relationOid, queryDesc->es_processed, inFunction);
			}

			FreeQueryDesc(queryDesc);

			/* Restore the old snapshot */
			PopActiveSnapshot();
			optimizer = saveOptimizerGucValue;
			optimizer_replicated_table_insert = save_optimizer_replicated_table_insert;
			gp_force_random_redistribution = saveRedistributeGucValue;

			CommandCounterIncrement(); /* see the effects of the command */

			if (original_policy)
			{
				/*
				 * update catalog first and then update the rd_cdbpolicy. This order
				 * avoids original_policy from getting freed before we use it for
				 * GpPolicyReplace() if relcache invalidation happens. Also, helps
				 * to have the rd_cdbpolicy current instead of reverse order which
				 * can invalidate our assignment to rd_cdbpolicy.
				 */
				GpPolicyReplace(RelationGetRelid(rel), original_policy);
				rel->rd_cdbpolicy = original_policy;
			}

			/*
			 * Step (d) - tell the seg nodes about the temporary relation. We use
			 * the global 'qe_data' variable to pass this information up to the
			 * caller, so that it can be included when the command is dispatched.
			 */
			cmd->backendId = MyBackendId + 1;
		}
	}
	else
	{
		int			backend_id;

		/*
		 * Extract the already-transformed representation of the DistributedBy
		 * policy that the QD should have included for us.
		 */
		policy = cmd->policy;

		if (policy)
			GpPolicyReplace(RelationGetRelid(rel), policy);

		/*
		 * Set to random distribution on master with no reorganisation.
		 * Or this is a partitioned table, with no data.
		 */
		if (cmd->backendId == 0)
		{
			goto l_distro_fini;			
		}

		backend_id = cmd->backendId - 1;
		tmprv = make_temp_table_name(rel, backend_id);

		need_reorg = true;
	}

	if (need_reorg)
	{
		ReindexParams params = {0};

		/*
		 * Step (e) - Correct ownership on temporary table:
		 *   necessary so that the toast tables/indices have the correct
		 *   owner after we swap them.
		 *
		 * Note: ATExecChangeOwner does NOT dispatch, so this does not
		 * belong in the dispatch block above (MPP-9663).
		 */
		ATExecChangeOwner(RangeVarGetRelid(tmprv, NoLock, false),
						  rel->rd_rel->relowner, true, AccessExclusiveLock);
		CommandCounterIncrement(); /* see the effects of the command */

		/*
		 * Update pg_attribute for dropped columns. The temp table we built
		 * uses int4 to stand in for any dropped columns, so we need to update
		 * the original table's definition to match the new contents.
		 */
		change_dropped_col_datatypes(rel);

		/*
		 * Step (f) - swap relfilenodes and MORE !!!
		 */
		rel = NULL;
		tmprelid = RangeVarGetRelid(tmprv, NoLock, false);
		swap_relation_files(tarrelid, tmprelid,
							false, /* target_is_pg_class */
							false, /* swap_toast_by_content */
							false, /* swap_stats */
							true,
							RecentXmin,
							ReadNextMultiXactId(),
							NULL);

		/* Make changes from swapping relation files visible. */
		CommandCounterIncrement();

		/* now, reindex */
		reindex_relation(tarrelid, 0, &params);
	}

	/* Step (g) */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (change_policy)
			GpPolicyReplace(tarrelid, policy);

		cmd->policy = policy;
	}

	/* Step (h) Drop the table */
	if (need_reorg)
	{
		ObjectAddress object;
		object.classId = RelationRelationId;
		object.objectId = tmprelid;
		object.objectSubId = 0;

		performDeletion(&object, DROP_RESTRICT, 0);
	}
	
l_distro_fini:

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char *distro_str = make_distro_str(lwith, ldistro);
		/* don't check relkind - must be a table */
		MetaTrackUpdObject(RelationRelationId, tarrelid, GetUserId(), "ALTER",
						   distro_str);
	}
}

/* ALTER TABLE ... SPLIT PARTITION */

/* Given a Relation, make a DISTRIBUTED BY (...) clause for parser consumption. */
DistributedBy *
make_distributedby_for_rel(Relation rel)
{
	GpPolicy *policy = rel->rd_cdbpolicy;
	int			i;
	DistributedBy *dist;

	dist = makeNode(DistributedBy);

	if (Gp_role == GP_ROLE_UTILITY)
	{
		Assert(policy->ptype == POLICYTYPE_ENTRY);
		return NULL;
	}

	Assert(policy->ptype != POLICYTYPE_ENTRY);

	if (GpPolicyIsReplicated(policy))
	{
		/* must be random distribution */
		dist->ptype = POLICYTYPE_REPLICATED;
		dist->numsegments = policy->numsegments;
		dist->keyCols = NIL;
	}
	else
	{
		TupleDesc tupdesc = RelationGetDescr(rel);
		List 		*keys = NIL;

		for (i = 0; i < policy->nattrs; i++)
		{
			int			attno = policy->attrs[i];
			Oid			opclassoid = policy->opclasses[i];
			char	   *attname = pstrdup(NameStr(TupleDescAttr(tupdesc, attno - 1)->attname));
			DistributionKeyElem *dkelem;
			HeapTuple	ht_opc;
			Form_pg_opclass opcrec;
			char	   *opcname;
			char	   *nspname;

			ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
			if (!HeapTupleIsValid(ht_opc))
				elog(ERROR, "cache lookup failed for opclass %u", opclassoid);
			opcrec = (Form_pg_opclass) GETSTRUCT(ht_opc);
			nspname = get_namespace_name(opcrec->opcnamespace);
			opcname = pstrdup(NameStr(opcrec->opcname));

			dkelem = makeNode(DistributionKeyElem);
			dkelem->name = attname;
			dkelem->opclass = list_make2(makeString(nspname), makeString(opcname));
			dkelem->location = -1;

			keys = lappend(keys, dkelem);

			ReleaseSysCache(ht_opc);
		}

		dist->ptype = POLICYTYPE_PARTITIONED;
		dist->numsegments = policy->numsegments;
		dist->keyCols = keys;
	}

	return dist;
}

/*
 * ALTER TABLE OF
 *
 * Attach a table to a composite type, as though it had been created with CREATE
 * TABLE OF.  All attname, atttypid, atttypmod and attcollation must match.  The
 * subject table must not have inheritance parents.  These restrictions ensure
 * that you cannot create a configuration impossible with CREATE TABLE OF alone.
 *
 * The address of the type is returned.
 */
static ObjectAddress
ATExecAddOf(Relation rel, const TypeName *ofTypename, LOCKMODE lockmode)
{
	Oid			relid = RelationGetRelid(rel);
	Type		typetuple;
	Form_pg_type typeform;
	Oid			typeid;
	Relation	inheritsRelation,
				relationRelation;
	SysScanDesc scan;
	ScanKeyData key;
	AttrNumber	table_attno,
				type_attno;
	TupleDesc	typeTupleDesc,
				tableTupleDesc;
	ObjectAddress tableobj,
				typeobj;
	HeapTuple	classtuple;

	/* Validate the type. */
	typetuple = typenameType(NULL, ofTypename, NULL);
	check_of_type(typetuple);
	typeform = (Form_pg_type) GETSTRUCT(typetuple);
	typeid = typeform->oid;

	/* Fail if the table has any inheritance parents. */
	inheritsRelation = table_open(InheritsRelationId, AccessShareLock);
	ScanKeyInit(&key,
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(inheritsRelation, InheritsRelidSeqnoIndexId,
							  true, NULL, 1, &key);
	if (HeapTupleIsValid(systable_getnext(scan)))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("typed tables cannot inherit")));
	systable_endscan(scan);
	table_close(inheritsRelation, AccessShareLock);

	/*
	 * Check the tuple descriptors for compatibility.  Unlike inheritance, we
	 * require that the order also match.  However, attnotnull need not match.
	 */
	typeTupleDesc = lookup_rowtype_tupdesc(typeid, -1);
	tableTupleDesc = RelationGetDescr(rel);
	table_attno = 1;
	for (type_attno = 1; type_attno <= typeTupleDesc->natts; type_attno++)
	{
		Form_pg_attribute type_attr,
					table_attr;
		const char *type_attname,
				   *table_attname;

		/* Get the next non-dropped type attribute. */
		type_attr = TupleDescAttr(typeTupleDesc, type_attno - 1);
		if (type_attr->attisdropped)
			continue;
		type_attname = NameStr(type_attr->attname);

		/* Get the next non-dropped table attribute. */
		do
		{
			if (table_attno > tableTupleDesc->natts)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table is missing column \"%s\"",
								type_attname)));
			table_attr = TupleDescAttr(tableTupleDesc, table_attno - 1);
			table_attno++;
		} while (table_attr->attisdropped);
		table_attname = NameStr(table_attr->attname);

		/* Compare name. */
		if (strncmp(table_attname, type_attname, NAMEDATALEN) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table has column \"%s\" where type requires \"%s\"",
							table_attname, type_attname)));

		/* Compare type. */
		if (table_attr->atttypid != type_attr->atttypid ||
			table_attr->atttypmod != type_attr->atttypmod ||
			table_attr->attcollation != type_attr->attcollation)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table \"%s\" has different type for column \"%s\"",
							RelationGetRelationName(rel), type_attname)));
	}
	DecrTupleDescRefCount(typeTupleDesc);

	/* Any remaining columns at the end of the table had better be dropped. */
	for (; table_attno <= tableTupleDesc->natts; table_attno++)
	{
		Form_pg_attribute table_attr = TupleDescAttr(tableTupleDesc,
													 table_attno - 1);

		if (!table_attr->attisdropped)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table has extra column \"%s\"",
							NameStr(table_attr->attname))));
	}

	/* If the table was already typed, drop the existing dependency. */
	if (rel->rd_rel->reloftype)
		drop_parent_dependency(relid, TypeRelationId, rel->rd_rel->reloftype,
							   DEPENDENCY_NORMAL);

	/* Record a dependency on the new type. */
	tableobj.classId = RelationRelationId;
	tableobj.objectId = relid;
	tableobj.objectSubId = 0;
	typeobj.classId = TypeRelationId;
	typeobj.objectId = typeid;
	typeobj.objectSubId = 0;
	recordDependencyOn(&tableobj, &typeobj, DEPENDENCY_NORMAL);

	/* Update pg_class.reloftype */
	relationRelation = table_open(RelationRelationId, RowExclusiveLock);
	classtuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(classtuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	((Form_pg_class) GETSTRUCT(classtuple))->reloftype = typeid;
	CatalogTupleUpdate(relationRelation, &classtuple->t_self, classtuple);

	InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

	heap_freetuple(classtuple);
	table_close(relationRelation, RowExclusiveLock);

	ReleaseSysCache(typetuple);

	return typeobj;
}

/*
 * ALTER TABLE NOT OF
 *
 * Detach a typed table from its originating type.  Just clear reloftype and
 * remove the dependency.
 */
static void
ATExecDropOf(Relation rel, LOCKMODE lockmode)
{
	Oid			relid = RelationGetRelid(rel);
	Relation	relationRelation;
	HeapTuple	tuple;

	if (!OidIsValid(rel->rd_rel->reloftype))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a typed table",
						RelationGetRelationName(rel))));

	/*
	 * We don't bother to check ownership of the type --- ownership of the
	 * table is presumed enough rights.  No lock required on the type, either.
	 */

	drop_parent_dependency(relid, TypeRelationId, rel->rd_rel->reloftype,
						   DEPENDENCY_NORMAL);

	/* Clear pg_class.reloftype */
	relationRelation = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	((Form_pg_class) GETSTRUCT(tuple))->reloftype = InvalidOid;
	CatalogTupleUpdate(relationRelation, &tuple->t_self, tuple);

	InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

	heap_freetuple(tuple);
	table_close(relationRelation, RowExclusiveLock);
}

/*
 * relation_mark_replica_identity: Update a table's replica identity
 *
 * Iff ri_type = REPLICA_IDENTITY_INDEX, indexOid must be the Oid of a suitable
 * index. Otherwise, it should be InvalidOid.
 */
static void
relation_mark_replica_identity(Relation rel, char ri_type, Oid indexOid,
							   bool is_internal)
{
	Relation	pg_index;
	Relation	pg_class;
	HeapTuple	pg_class_tuple;
	HeapTuple	pg_index_tuple;
	Form_pg_class pg_class_form;
	Form_pg_index pg_index_form;

	ListCell   *index;

	/*
	 * Check whether relreplident has changed, and update it if so.
	 */
	pg_class = table_open(RelationRelationId, RowExclusiveLock);
	pg_class_tuple = SearchSysCacheCopy1(RELOID,
										 ObjectIdGetDatum(RelationGetRelid(rel)));
	if (!HeapTupleIsValid(pg_class_tuple))
		elog(ERROR, "cache lookup failed for relation \"%s\"",
			 RelationGetRelationName(rel));
	pg_class_form = (Form_pg_class) GETSTRUCT(pg_class_tuple);
	if (pg_class_form->relreplident != ri_type)
	{
		pg_class_form->relreplident = ri_type;
		CatalogTupleUpdate(pg_class, &pg_class_tuple->t_self, pg_class_tuple);
	}
	table_close(pg_class, RowExclusiveLock);
	heap_freetuple(pg_class_tuple);

	/*
	 * Check whether the correct index is marked indisreplident; if so, we're
	 * done.
	 */
	if (OidIsValid(indexOid))
	{
		Assert(ri_type == REPLICA_IDENTITY_INDEX);

		pg_index_tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
		if (!HeapTupleIsValid(pg_index_tuple))
			elog(ERROR, "cache lookup failed for index %u", indexOid);
		pg_index_form = (Form_pg_index) GETSTRUCT(pg_index_tuple);

		if (pg_index_form->indisreplident)
		{
			ReleaseSysCache(pg_index_tuple);
			return;
		}
		ReleaseSysCache(pg_index_tuple);
	}

	/*
	 * Clear the indisreplident flag from any index that had it previously,
	 * and set it for any index that should have it now.
	 */
	pg_index = table_open(IndexRelationId, RowExclusiveLock);
	foreach(index, RelationGetIndexList(rel))
	{
		Oid			thisIndexOid = lfirst_oid(index);
		bool		dirty = false;

		pg_index_tuple = SearchSysCacheCopy1(INDEXRELID,
											 ObjectIdGetDatum(thisIndexOid));
		if (!HeapTupleIsValid(pg_index_tuple))
			elog(ERROR, "cache lookup failed for index %u", thisIndexOid);
		pg_index_form = (Form_pg_index) GETSTRUCT(pg_index_tuple);

		/*
		 * Unset the bit if set.  We know it's wrong because we checked this
		 * earlier.
		 */
		if (pg_index_form->indisreplident)
		{
			dirty = true;
			pg_index_form->indisreplident = false;
		}
		else if (thisIndexOid == indexOid)
		{
			dirty = true;
			pg_index_form->indisreplident = true;
		}

		if (dirty)
		{
			CatalogTupleUpdate(pg_index, &pg_index_tuple->t_self, pg_index_tuple);
			InvokeObjectPostAlterHookArg(IndexRelationId, thisIndexOid, 0,
										 InvalidOid, is_internal);
			/*
			 * Invalidate the relcache for the table, so that after we commit
			 * all sessions will refresh the table's replica identity index
			 * before attempting any UPDATE or DELETE on the table.
			 */
			CacheInvalidateRelcache(rel);
		}
		heap_freetuple(pg_index_tuple);
	}

	table_close(pg_index, RowExclusiveLock);
}

/*
 * ALTER TABLE <name> REPLICA IDENTITY ...
 */
static void
ATExecReplicaIdentity(Relation rel, ReplicaIdentityStmt *stmt, LOCKMODE lockmode)
{
	Oid			indexOid;
	Relation	indexRel;
	int			key;

	if (stmt->identity_type == REPLICA_IDENTITY_DEFAULT)
	{
		relation_mark_replica_identity(rel, stmt->identity_type, InvalidOid, true);
		return;
	}
	else if (stmt->identity_type == REPLICA_IDENTITY_FULL)
	{
		relation_mark_replica_identity(rel, stmt->identity_type, InvalidOid, true);
		return;
	}
	else if (stmt->identity_type == REPLICA_IDENTITY_NOTHING)
	{
		relation_mark_replica_identity(rel, stmt->identity_type, InvalidOid, true);
		return;
	}
	else if (stmt->identity_type == REPLICA_IDENTITY_INDEX)
	{
		 /* fallthrough */ ;
	}
	else
		elog(ERROR, "unexpected identity type %u", stmt->identity_type);

	/* Check that the index exists */
	indexOid = get_relname_relid(stmt->name, rel->rd_rel->relnamespace);
	if (!OidIsValid(indexOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("index \"%s\" for table \"%s\" does not exist",
						stmt->name, RelationGetRelationName(rel))));

	indexRel = index_open(indexOid, ShareLock);

	/* Check that the index is on the relation we're altering. */
	if (indexRel->rd_index == NULL ||
		indexRel->rd_index->indrelid != RelationGetRelid(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index for table \"%s\"",
						RelationGetRelationName(indexRel),
						RelationGetRelationName(rel))));
	/* The AM must support uniqueness, and the index must in fact be unique. */
	if (!indexRel->rd_indam->amcanunique ||
		!indexRel->rd_index->indisunique)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot use non-unique index \"%s\" as replica identity",
						RelationGetRelationName(indexRel))));
	/* Deferred indexes are not guaranteed to be always unique. */
	if (!indexRel->rd_index->indimmediate)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use non-immediate index \"%s\" as replica identity",
						RelationGetRelationName(indexRel))));
	/* Expression indexes aren't supported. */
	if (RelationGetIndexExpressions(indexRel) != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use expression index \"%s\" as replica identity",
						RelationGetRelationName(indexRel))));
	/* Predicate indexes aren't supported. */
	if (RelationGetIndexPredicate(indexRel) != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use partial index \"%s\" as replica identity",
						RelationGetRelationName(indexRel))));
	/* And neither are invalid indexes. */
	if (!indexRel->rd_index->indisvalid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use invalid index \"%s\" as replica identity",
						RelationGetRelationName(indexRel))));

	/* Check index for nullable columns. */
	for (key = 0; key < IndexRelationGetNumberOfKeyAttributes(indexRel); key++)
	{
		int16		attno = indexRel->rd_index->indkey.values[key];
		Form_pg_attribute attr;

		/*
		 * Reject any other system columns.  (Going forward, we'll disallow
		 * indexes containing such columns in the first place, but they might
		 * exist in older branches.)
		 */
		if (attno <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("index \"%s\" cannot be used as replica identity because column %d is a system column",
							RelationGetRelationName(indexRel), attno)));

		attr = TupleDescAttr(rel->rd_att, attno - 1);
		if (!attr->attnotnull)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("index \"%s\" cannot be used as replica identity because column \"%s\" is nullable",
							RelationGetRelationName(indexRel),
							NameStr(attr->attname))));
	}

	/* This index is suitable for use as a replica identity. Mark it. */
	relation_mark_replica_identity(rel, stmt->identity_type, indexOid, true);

	index_close(indexRel, NoLock);
}

/*
 * ALTER TABLE ENABLE/DISABLE ROW LEVEL SECURITY
 */
static void
ATExecSetRowSecurity(Relation rel, bool rls)
{
	Relation	pg_class;
	Oid			relid;
	HeapTuple	tuple;

	relid = RelationGetRelid(rel);

	/* Pull the record for this relation and update it */
	pg_class = table_open(RelationRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	((Form_pg_class) GETSTRUCT(tuple))->relrowsecurity = rls;
	CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);

	table_close(pg_class, RowExclusiveLock);
	heap_freetuple(tuple);
}

/*
 * ALTER TABLE FORCE/NO FORCE ROW LEVEL SECURITY
 */
static void
ATExecForceNoForceRowSecurity(Relation rel, bool force_rls)
{
	Relation	pg_class;
	Oid			relid;
	HeapTuple	tuple;

	relid = RelationGetRelid(rel);

	pg_class = table_open(RelationRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	((Form_pg_class) GETSTRUCT(tuple))->relforcerowsecurity = force_rls;
	CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);

	table_close(pg_class, RowExclusiveLock);
	heap_freetuple(tuple);
}

/*
 * ALTER FOREIGN TABLE <name> OPTIONS (...)
 */
static void
ATExecGenericOptions(Relation rel, List *options)
{
	Relation	ftrel;
	ForeignServer *server;
	ForeignDataWrapper *fdw;
	HeapTuple	tuple;
	bool		isnull;
	Datum		repl_val[Natts_pg_foreign_table];
	bool		repl_null[Natts_pg_foreign_table];
	bool		repl_repl[Natts_pg_foreign_table];
	Datum		datum;
	Form_pg_foreign_table tableform;

	if (options == NIL)
		return;

	ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopy1(FOREIGNTABLEREL, rel->rd_id);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("foreign table \"%s\" does not exist",
						RelationGetRelationName(rel))));
	tableform = (Form_pg_foreign_table) GETSTRUCT(tuple);
	server = GetForeignServer(tableform->ftserver);
	fdw = GetForeignDataWrapper(server->fdwid);

	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	/* Extract the current options */
	datum = SysCacheGetAttr(FOREIGNTABLEREL,
							tuple,
							Anum_pg_foreign_table_ftoptions,
							&isnull);
	if (isnull)
		datum = PointerGetDatum(NULL);

	/* Transform the options */
	datum = transformGenericOptions(ForeignTableRelationId,
									datum,
									options,
									fdw->fdwvalidator);

	if (PointerIsValid(DatumGetPointer(datum)))
		repl_val[Anum_pg_foreign_table_ftoptions - 1] = datum;
	else
		repl_null[Anum_pg_foreign_table_ftoptions - 1] = true;

	repl_repl[Anum_pg_foreign_table_ftoptions - 1] = true;

	/* Everything looks good - update the tuple */

	tuple = heap_modify_tuple(tuple, RelationGetDescr(ftrel),
							  repl_val, repl_null, repl_repl);

	CatalogTupleUpdate(ftrel, &tuple->t_self, tuple);

	/*
	 * Invalidate relcache so that all sessions will refresh any cached plans
	 * that might depend on the old options.
	 */
	CacheInvalidateRelcache(rel);

	InvokeObjectPostAlterHook(ForeignTableRelationId,
							  RelationGetRelid(rel), 0);

	table_close(ftrel, RowExclusiveLock);

	heap_freetuple(tuple);
}

/*
 * ALTER TABLE ALTER COLUMN SET COMPRESSION
 *
 * Return value is the address of the modified column
 */
static ObjectAddress
ATExecSetCompression(AlteredTableInfo *tab,
					 Relation rel,
					 const char *column,
					 Node *newValue,
					 LOCKMODE lockmode)
{
	Relation	attrel;
	HeapTuple	tuple;
	Form_pg_attribute atttableform;
	AttrNumber	attnum;
	char	   *compression;
	char		cmethod;
	ObjectAddress address;

	Assert(IsA(newValue, String));
	compression = strVal(newValue);

	attrel = table_open(AttributeRelationId, RowExclusiveLock);

	/* copy the cache entry so we can scribble on it below */
	tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), column);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						column, RelationGetRelationName(rel))));

	/* prevent them from altering a system attribute */
	atttableform = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = atttableform->attnum;
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"", column)));

	/*
	 * Check that column type is compressible, then get the attribute
	 * compression method code
	 */
	cmethod = GetAttributeCompression(atttableform->atttypid, compression);

	/* update pg_attribute entry */
	atttableform->attcompression = cmethod;
	CatalogTupleUpdate(attrel, &tuple->t_self, tuple);

	InvokeObjectPostAlterHook(RelationRelationId,
							  RelationGetRelid(rel),
							  attnum);

	/*
	 * Apply the change to indexes as well (only for simple index columns,
	 * matching behavior of index.c ConstructTupleDescriptor()).
	 */
	SetIndexStorageProperties(rel, attrel, attnum,
							  false, 0,
							  true, cmethod,
							  lockmode);

	heap_freetuple(tuple);

	table_close(attrel, RowExclusiveLock);

	/* make changes visible */
	CommandCounterIncrement();

	ObjectAddressSubSet(address, RelationRelationId,
						RelationGetRelid(rel), attnum);
	return address;
}


/*
 * Preparation phase for SET LOGGED/UNLOGGED
 *
 * This verifies that we're not trying to change a temp table.  Also,
 * existing foreign key constraints are checked to avoid ending up with
 * permanent tables referencing unlogged tables.
 *
 * Return value is false if the operation is a no-op (in which case the
 * checks are skipped), otherwise true.
 */
static bool
ATPrepChangePersistence(Relation rel, bool toLogged)
{
	Relation	pg_constraint;
	HeapTuple	tuple;
	SysScanDesc scan;
	ScanKeyData skey[1];

	/*
	 * Disallow changing status for a temp table.  Also verify whether we can
	 * get away with doing nothing; in such cases we don't need to run the
	 * checks below, either.
	 */
	switch (rel->rd_rel->relpersistence)
	{
		case RELPERSISTENCE_TEMP:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot change logged status of table \"%s\" because it is temporary",
							RelationGetRelationName(rel)),
					 errtable(rel)));
			break;
		case RELPERSISTENCE_PERMANENT:
			if (toLogged)
				/* nothing to do */
				return false;
			break;
		case RELPERSISTENCE_UNLOGGED:
			if (!toLogged)
				/* nothing to do */
				return false;
			break;
	}

	/*
	 * Check that the table is not part any publication when changing to
	 * UNLOGGED as UNLOGGED tables can't be published.
	 */
	if (!toLogged &&
		list_length(GetRelationPublications(RelationGetRelid(rel))) > 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot change table \"%s\" to unlogged because it is part of a publication",
						RelationGetRelationName(rel)),
				 errdetail("Unlogged relations cannot be replicated.")));

	/*
	 * Check existing foreign key constraints to preserve the invariant that
	 * permanent tables cannot reference unlogged ones.  Self-referencing
	 * foreign keys can safely be ignored.
	 */
	pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

	/*
	 * Scan conrelid if changing to permanent, else confrelid.  This also
	 * determines whether a useful index exists.
	 */
	ScanKeyInit(&skey[0],
				toLogged ? Anum_pg_constraint_conrelid :
				Anum_pg_constraint_confrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	scan = systable_beginscan(pg_constraint,
							  toLogged ? ConstraintRelidTypidNameIndexId : InvalidOid,
							  true, NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);

		if (con->contype == CONSTRAINT_FOREIGN)
		{
			Oid			foreignrelid;
			Relation	foreignrel;

			/* the opposite end of what we used as scankey */
			foreignrelid = toLogged ? con->confrelid : con->conrelid;

			/* ignore if self-referencing */
			if (RelationGetRelid(rel) == foreignrelid)
				continue;

			foreignrel = relation_open(foreignrelid, AccessShareLock);

			if (toLogged)
			{
				if (!RelationIsPermanent(foreignrel))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("could not change table \"%s\" to logged because it references unlogged table \"%s\"",
									RelationGetRelationName(rel),
									RelationGetRelationName(foreignrel)),
							 errtableconstraint(rel, NameStr(con->conname))));
			}
			else
			{
				if (RelationIsPermanent(foreignrel))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("could not change table \"%s\" to unlogged because it references logged table \"%s\"",
									RelationGetRelationName(rel),
									RelationGetRelationName(foreignrel)),
							 errtableconstraint(rel, NameStr(con->conname))));
			}

			relation_close(foreignrel, AccessShareLock);
		}
	}

	systable_endscan(scan);

	table_close(pg_constraint, AccessShareLock);

	return true;
}

/*
 * Execute ALTER TABLE SET SCHEMA
 */
ObjectAddress
AlterTableNamespace(AlterObjectSchemaStmt *stmt, Oid *oldschema)
{
	Relation	rel;
	Oid			relid;
	Oid			oldNspOid;
	Oid			nspOid;
	RangeVar   *newrv;
	ObjectAddresses *objsMoved;
	ObjectAddress myself;

	relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
									 stmt->missing_ok ? RVR_MISSING_OK : 0,
									 RangeVarCallbackForAlterRelation,
									 (void *) stmt);

	if (!OidIsValid(relid))
	{
		ereport((Gp_role == GP_ROLE_EXECUTE) ? DEBUG1 : NOTICE,
				(errmsg("relation \"%s\" does not exist, skipping",
						stmt->relation->relname)));
		return InvalidObjectAddress;
	}

	rel = relation_open(relid, NoLock);

	oldNspOid = RelationGetNamespace(rel);

	/* If it's an owned sequence, disallow moving it by itself. */
	if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
	{
		Oid			tableId;
		int32		colId;

		if (sequenceIsOwned(relid, DEPENDENCY_AUTO, &tableId, &colId) ||
			sequenceIsOwned(relid, DEPENDENCY_INTERNAL, &tableId, &colId))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot move an owned sequence into another schema"),
					 errdetail("Sequence \"%s\" is linked to table \"%s\".",
							   RelationGetRelationName(rel),
							   get_rel_name(tableId))));
	}

	/* Get and lock schema OID and check its permissions. */
	newrv = makeRangeVar(stmt->newschema, RelationGetRelationName(rel), -1);
	nspOid = RangeVarGetAndCheckCreationNamespace(newrv, NoLock, NULL);

	/* common checks on switching namespaces */
	CheckSetNamespace(oldNspOid, nspOid);

	objsMoved = new_object_addresses();
	AlterTableNamespaceInternal(rel, oldNspOid, nspOid, objsMoved);
	free_object_addresses(objsMoved);

	ObjectAddressSet(myself, RelationRelationId, relid);

	if (oldschema)
		*oldschema = oldNspOid;

	/* close rel, but keep lock until commit */
	relation_close(rel, NoLock);

	return myself;
}

/*
 * The guts of relocating a table to another namespace: besides moving
 * the table itself, its dependent objects are relocated to the new schema.
 */
void
AlterTableNamespaceInternal(Relation rel, Oid oldNspOid, Oid nspOid,
							ObjectAddresses *objsMoved)
{
	Relation	classRel;

	Assert(objsMoved != NULL);

	/* OK, modify the pg_class row and pg_depend entry */
	classRel = table_open(RelationRelationId, RowExclusiveLock);

	AlterRelationNamespaceInternal(classRel, RelationGetRelid(rel), oldNspOid,
								   nspOid, true, objsMoved);

	/* Fix the table's row type too, if it has one */
	if (OidIsValid(rel->rd_rel->reltype))
		AlterTypeNamespaceInternal(rel->rd_rel->reltype,
								   nspOid, false, false, objsMoved);

	/* Fix other dependent stuff */
	if (rel->rd_rel->relkind == RELKIND_RELATION ||
		rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE ||
		rel->rd_rel->relkind == RELKIND_MATVIEW ||
		rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		AlterIndexNamespaces(classRel, rel, oldNspOid, nspOid, objsMoved);
		AlterSeqNamespaces(classRel, rel, oldNspOid, nspOid,
						   objsMoved, AccessExclusiveLock);
		AlterConstraintNamespaces(RelationGetRelid(rel), oldNspOid, nspOid,
								  false, objsMoved);
	}

	table_close(classRel, RowExclusiveLock);
}

/*
 * The guts of relocating a relation to another namespace: fix the pg_class
 * entry, and the pg_depend entry if any.  Caller must already have
 * opened and write-locked pg_class.
 */
void
AlterRelationNamespaceInternal(Relation classRel, Oid relOid,
							   Oid oldNspOid, Oid newNspOid,
							   bool hasDependEntry,
							   ObjectAddresses *objsMoved)
{
	HeapTuple	classTup;
	Form_pg_class classForm;
	ObjectAddress thisobj;
	bool		already_done = false;

	classTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid));
	if (!HeapTupleIsValid(classTup))
		elog(ERROR, "cache lookup failed for relation %u", relOid);
	classForm = (Form_pg_class) GETSTRUCT(classTup);

	Assert(classForm->relnamespace == oldNspOid);

	thisobj.classId = RelationRelationId;
	thisobj.objectId = relOid;
	thisobj.objectSubId = 0;

	/*
	 * If the object has already been moved, don't move it again.  If it's
	 * already in the right place, don't move it, but still fire the object
	 * access hook.
	 */
	already_done = object_address_present(&thisobj, objsMoved);
	if (!already_done && oldNspOid != newNspOid)
	{
		/* check for duplicate name (more friendly than unique-index failure) */
		if (get_relname_relid(NameStr(classForm->relname),
							  newNspOid) != InvalidOid)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists in schema \"%s\"",
							NameStr(classForm->relname),
							get_namespace_name(newNspOid))));

		/* classTup is a copy, so OK to scribble on */
		classForm->relnamespace = newNspOid;

		CatalogTupleUpdate(classRel, &classTup->t_self, classTup);

		/* Update dependency on schema if caller said so */
		if (hasDependEntry &&
			changeDependencyFor(RelationRelationId,
								relOid,
								NamespaceRelationId,
								oldNspOid,
								newNspOid) != 1)
			elog(ERROR, "failed to change schema dependency for relation \"%s\"",
				 NameStr(classForm->relname));
	}
	if (!already_done)
	{
		add_exact_object_address(&thisobj, objsMoved);

		InvokeObjectPostAlterHook(RelationRelationId, relOid, 0);
	}

	heap_freetuple(classTup);
}

/*
 * Move all indexes for the specified relation to another namespace.
 *
 * Note: we assume adequate permission checking was done by the caller,
 * and that the caller has a suitable lock on the owning relation.
 */
static void
AlterIndexNamespaces(Relation classRel, Relation rel,
					 Oid oldNspOid, Oid newNspOid, ObjectAddresses *objsMoved)
{
	List	   *indexList;
	ListCell   *l;

	indexList = RelationGetIndexList(rel);

	foreach(l, indexList)
	{
		Oid			indexOid = lfirst_oid(l);
		ObjectAddress thisobj;

		thisobj.classId = RelationRelationId;
		thisobj.objectId = indexOid;
		thisobj.objectSubId = 0;

		/*
		 * Note: currently, the index will not have its own dependency on the
		 * namespace, so we don't need to do changeDependencyFor(). There's no
		 * row type in pg_type, either.
		 *
		 * XXX this objsMoved test may be pointless -- surely we have a single
		 * dependency link from a relation to each index?
		 */
		if (!object_address_present(&thisobj, objsMoved))
		{
			AlterRelationNamespaceInternal(classRel, indexOid,
										   oldNspOid, newNspOid,
										   false, objsMoved);
			add_exact_object_address(&thisobj, objsMoved);
		}
	}

	list_free(indexList);
}

/*
 * Move all identity and SERIAL-column sequences of the specified relation to another
 * namespace.
 *
 * Note: we assume adequate permission checking was done by the caller,
 * and that the caller has a suitable lock on the owning relation.
 */
static void
AlterSeqNamespaces(Relation classRel, Relation rel,
				   Oid oldNspOid, Oid newNspOid, ObjectAddresses *objsMoved,
				   LOCKMODE lockmode)
{
	Relation	depRel;
	SysScanDesc scan;
	ScanKeyData key[2];
	HeapTuple	tup;

	/*
	 * SERIAL sequences are those having an auto dependency on one of the
	 * table's columns (we don't care *which* column, exactly).
	 */
	depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	/* we leave refobjsubid unspecified */

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depForm = (Form_pg_depend) GETSTRUCT(tup);
		Relation	seqRel;

		/* skip dependencies other than auto dependencies on columns */
		if (depForm->refobjsubid == 0 ||
			depForm->classid != RelationRelationId ||
			depForm->objsubid != 0 ||
			!(depForm->deptype == DEPENDENCY_AUTO || depForm->deptype == DEPENDENCY_INTERNAL))
			continue;

		/* Use relation_open just in case it's an index */
		seqRel = relation_open(depForm->objid, lockmode);

		/* skip non-sequence relations */
		if (RelationGetForm(seqRel)->relkind != RELKIND_SEQUENCE)
		{
			/* No need to keep the lock */
			relation_close(seqRel, lockmode);
			continue;
		}

		/* Fix the pg_class and pg_depend entries */
		AlterRelationNamespaceInternal(classRel, depForm->objid,
									   oldNspOid, newNspOid,
									   true, objsMoved);

		/*
		 * Sequences used to have entries in pg_type, but no longer do.  If we
		 * ever re-instate that, we'll need to move the pg_type entry to the
		 * new namespace, too (using AlterTypeNamespaceInternal).
		 */
		Assert(RelationGetForm(seqRel)->reltype == InvalidOid);

		/* Now we can close it.  Keep the lock till end of transaction. */
		relation_close(seqRel, NoLock);
	}

	systable_endscan(scan);

	relation_close(depRel, AccessShareLock);
}


/*
 * This code supports
 *	CREATE TEMP TABLE ... ON COMMIT { DROP | PRESERVE ROWS | DELETE ROWS }
 *
 * Because we only support this for TEMP tables, it's sufficient to remember
 * the state in a backend-local data structure.
 */

/*
 * Register a newly-created relation's ON COMMIT action.
 */
void
register_on_commit_action(Oid relid, OnCommitAction action)
{
	OnCommitItem *oc;
	MemoryContext oldcxt;

	/*
	 * We needn't bother registering the relation unless there is an ON COMMIT
	 * action we need to take.
	 */
	if (action == ONCOMMIT_NOOP || action == ONCOMMIT_PRESERVE_ROWS)
		return;

	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	oc = (OnCommitItem *) palloc(sizeof(OnCommitItem));
	oc->relid = relid;
	oc->oncommit = action;
	oc->creating_subid = GetCurrentSubTransactionId();
	oc->deleting_subid = InvalidSubTransactionId;

	/*
	 * We use lcons() here so that ON COMMIT actions are processed in reverse
	 * order of registration.  That might not be essential but it seems
	 * reasonable.
	 */
	on_commits = lcons(oc, on_commits);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Unregister any ON COMMIT action when a relation is deleted.
 *
 * Actually, we only mark the OnCommitItem entry as to be deleted after commit.
 */
void
remove_on_commit_action(Oid relid)
{
	ListCell   *l;

	foreach(l, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(l);

		if (oc->relid == relid)
		{
			oc->deleting_subid = GetCurrentSubTransactionId();
			break;
		}
	}
}

/*
 * Perform ON COMMIT actions.
 *
 * This is invoked just before actually committing, since it's possible
 * to encounter errors.
 */
void
PreCommit_on_commit_actions(void)
{
	ListCell   *l;
	List	   *oids_to_truncate = NIL;
	List	   *oids_to_drop = NIL;

	/*
	 * We skip this operation in the catchup handler, especially
	 * between prepare and commit state, as we may not see the heap
	 * that has just been created in the prepared transaction that
	 * is not visible yet.  Skipping this under the catchup handler
	 * should be ok in known cases.
	 */
	if (in_process_catchup_event)
		return;

	foreach(l, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(l);

		/* Ignore entry if already dropped in this xact */
		if (oc->deleting_subid != InvalidSubTransactionId)
			continue;

		switch (oc->oncommit)
		{
			case ONCOMMIT_NOOP:
			case ONCOMMIT_PRESERVE_ROWS:
				/* Do nothing (there shouldn't be such entries, actually) */
				break;
			case ONCOMMIT_DELETE_ROWS:
#if 0
				/*
				 * If this transaction hasn't accessed any temporary
				 * relations, we can skip truncating ON COMMIT DELETE ROWS
				 * tables, as they must still be empty.
				 */
				if ((MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE))
#endif
				oids_to_truncate = lappend_oid(oids_to_truncate, oc->relid);
				break;
			case ONCOMMIT_DROP:
				oids_to_drop = lappend_oid(oids_to_drop, oc->relid);
				break;
		}
	}

	/*
	 * Truncate relations before dropping so that all dependencies between
	 * relations are removed after they are worked on.  Doing it like this
	 * might be a waste as it is possible that a relation being truncated will
	 * be dropped anyway due to its parent being dropped, but this makes the
	 * code more robust because of not having to re-check that the relation
	 * exists at truncation time.
	 */
	if (oids_to_truncate != NIL)
		heap_truncate(oids_to_truncate);

	if (oids_to_drop != NIL)
	{
		ObjectAddresses *targetObjects = new_object_addresses();
		ListCell   *l;

		foreach(l, oids_to_drop)
		{
			ObjectAddress object;

			object.classId = RelationRelationId;
			object.objectId = lfirst_oid(l);
			object.objectSubId = 0;

			Assert(!object_address_present(&object, targetObjects));

			add_exact_object_address(&object, targetObjects);
		}

		/*
		 * Since this is an automatic drop, rather than one directly initiated
		 * by the user, we pass the PERFORM_DELETION_INTERNAL flag.
		 */
		performMultipleDeletions(targetObjects, DROP_CASCADE,
								 PERFORM_DELETION_INTERNAL | PERFORM_DELETION_QUIETLY);

#ifdef USE_ASSERT_CHECKING

		/*
		 * Note that table deletion will call remove_on_commit_action, so the
		 * entry should get marked as deleted.
		 */
		foreach(l, on_commits)
		{
			OnCommitItem *oc = (OnCommitItem *) lfirst(l);

			if (oc->oncommit != ONCOMMIT_DROP)
				continue;

			Assert(oc->deleting_subid != InvalidSubTransactionId);
		}
#endif
	}
}

/*
 * Post-commit or post-abort cleanup for ON COMMIT management.
 *
 * All we do here is remove no-longer-needed OnCommitItem entries.
 *
 * During commit, remove entries that were deleted during this transaction;
 * during abort, remove those created during this transaction.
 */
void
AtEOXact_on_commit_actions(bool isCommit)
{
	ListCell   *cur_item;

	foreach(cur_item, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(cur_item);

		if (isCommit ? oc->deleting_subid != InvalidSubTransactionId :
			oc->creating_subid != InvalidSubTransactionId)
		{
			/* cur_item must be removed */
			on_commits = foreach_delete_current(on_commits, cur_item);
			pfree(oc);
		}
		else
		{
			/* cur_item must be preserved */
			oc->creating_subid = InvalidSubTransactionId;
			oc->deleting_subid = InvalidSubTransactionId;
		}
	}
}

/*
 * Post-subcommit or post-subabort cleanup for ON COMMIT management.
 *
 * During subabort, we can immediately remove entries created during this
 * subtransaction.  During subcommit, just relabel entries marked during
 * this subtransaction as being the parent's responsibility.
 */
void
AtEOSubXact_on_commit_actions(bool isCommit, SubTransactionId mySubid,
							  SubTransactionId parentSubid)
{
	ListCell   *cur_item;

	foreach(cur_item, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(cur_item);

		if (!isCommit && oc->creating_subid == mySubid)
		{
			/* cur_item must be removed */
			on_commits = foreach_delete_current(on_commits, cur_item);
			pfree(oc);
		}
		else
		{
			/* cur_item must be preserved */
			if (oc->creating_subid == mySubid)
				oc->creating_subid = parentSubid;
			if (oc->deleting_subid == mySubid)
				oc->deleting_subid = isCommit ? parentSubid : InvalidSubTransactionId;
		}
	}
}

/*
 * This is intended as a callback for RangeVarGetRelidExtended().  It allows
 * the relation to be locked only if (1) it's a plain table, materialized
 * view, or TOAST table and (2) the current user is the owner (or the
 * superuser).  This meets the permission-checking needs of CLUSTER, REINDEX
 * TABLE, and REFRESH MATERIALIZED VIEW; we expose it here so that it can be
 * used by all.
 */
void
RangeVarCallbackOwnsTable(const RangeVar *relation,
						  Oid relId, Oid oldRelId, void *arg)
{
	char		relkind;

	/* Nothing to do if the relation was not found. */
	if (!OidIsValid(relId))
		return;

	/*
	 * If the relation does exist, check whether it's an index.  But note that
	 * the relation might have been dropped between the time we did the name
	 * lookup and now.  In that case, there's nothing to do.
	 */
	relkind = get_rel_relkind(relId);
	if (!relkind)
		return;
	if (relkind != RELKIND_RELATION && relkind != RELKIND_TOASTVALUE &&
		relkind != RELKIND_MATVIEW && relkind != RELKIND_PARTITIONED_TABLE &&
		relkind != RELKIND_DIRECTORY_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table, directory table or materialized view", relation->relname)));

	/* Check permissions */
	if (!pg_class_ownercheck(relId, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relId)), relation->relname);
}

/*
 * Callback to RangeVarGetRelidExtended() for TRUNCATE processing.
 */
static void
RangeVarCallbackForTruncate(const RangeVar *relation,
							Oid relId, Oid oldRelId, void *arg)
{
	HeapTuple	tuple;

	/* Nothing to do if the relation was not found. */
	if (!OidIsValid(relId))
		return;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))	/* should not happen */
		elog(ERROR, "cache lookup failed for relation %u", relId);

	truncate_check_rel(relId, (Form_pg_class) GETSTRUCT(tuple));
	truncate_check_perms(relId, (Form_pg_class) GETSTRUCT(tuple));

	ReleaseSysCache(tuple);
}

/*
 * Callback to RangeVarGetRelidExtended(), similar to
 * RangeVarCallbackOwnsTable() but without checks on the type of the relation.
 */
void
RangeVarCallbackOwnsRelation(const RangeVar *relation,
							 Oid relId, Oid oldRelId, void *arg)
{
	HeapTuple	tuple;

	/* Nothing to do if the relation was not found. */
	if (!OidIsValid(relId))
		return;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))	/* should not happen */
		elog(ERROR, "cache lookup failed for relation %u", relId);

	if (!pg_class_ownercheck(relId, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relId)),
					   relation->relname);

	if (!allowSystemTableMods &&
		IsSystemClass(relId, (Form_pg_class) GETSTRUCT(tuple)))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						relation->relname)));

	ReleaseSysCache(tuple);
}

/*
 * Common RangeVarGetRelid callback for rename, set schema, and alter table
 * processing.
 */
static void
RangeVarCallbackForAlterRelation(const RangeVar *rv, Oid relid, Oid oldrelid,
								 void *arg)
{
	Node	   *stmt = (Node *) arg;
	ObjectType	reltype;
	HeapTuple	tuple;
	Form_pg_class classform;
	AclResult	aclresult;
	char		relkind;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return;					/* concurrently dropped */
	classform = (Form_pg_class) GETSTRUCT(tuple);
	relkind = classform->relkind;

	/* Must own relation. */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relid)), rv->relname);

	/* No system table modifications unless explicitly allowed. */
	if (!allowSystemTableMods && IsSystemClass(relid, classform))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						rv->relname)));

	/*
	 * Extract the specified relation type from the statement parse tree.
	 *
	 * Also, for ALTER .. RENAME, check permissions: the user must (still)
	 * have CREATE rights on the containing namespace.
	 */
	if (IsA(stmt, RenameStmt))
	{
		aclresult = pg_namespace_aclcheck(classform->relnamespace,
										  GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_SCHEMA,
						   get_namespace_name(classform->relnamespace));
		reltype = ((RenameStmt *) stmt)->renameType;
	}
	else if (IsA(stmt, AlterObjectSchemaStmt))
		reltype = ((AlterObjectSchemaStmt *) stmt)->objectType;

	else if (IsA(stmt, AlterTableStmt))
		reltype = ((AlterTableStmt *) stmt)->objtype;
	else
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(stmt));
		reltype = OBJECT_TABLE; /* placate compiler */
	}

	/*
	 * For compatibility with prior releases, we allow ALTER TABLE to be used
	 * with most other types of relations (but not composite types). We allow
	 * similar flexibility for ALTER INDEX in the case of RENAME, but not
	 * otherwise.  Otherwise, the user must select the correct form of the
	 * command for the relation at issue.
	 */
	if (reltype == OBJECT_SEQUENCE && relkind != RELKIND_SEQUENCE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sequence", rv->relname)));

	if (reltype == OBJECT_VIEW && relkind != RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a view", rv->relname)));

	if (reltype == OBJECT_MATVIEW && relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a materialized view", rv->relname)));

	if (reltype == OBJECT_FOREIGN_TABLE && relkind != RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a foreign table", rv->relname)));

	if (reltype == OBJECT_TYPE && relkind != RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a composite type", rv->relname)));

	if (reltype == OBJECT_DIRECTORY_TABLE && relkind != RELKIND_DIRECTORY_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a directory table", rv->relname)));

	if (reltype == OBJECT_INDEX && relkind != RELKIND_INDEX &&
		relkind != RELKIND_PARTITIONED_INDEX
		&& !IsA(stmt, RenameStmt))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index", rv->relname)));

	/*
	 * Don't allow ALTER TABLE on composite types. We want people to use ALTER
	 * TYPE for that.
	 */
	if (reltype != OBJECT_TYPE && relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type", rv->relname),
				 errhint("Use ALTER TYPE instead.")));

	/*
	 * Don't allow ALTER TABLE .. SET SCHEMA on relations that can't be moved
	 * to a different schema, such as indexes and TOAST tables.
	 */
	if (IsA(stmt, AlterObjectSchemaStmt) &&
		relkind != RELKIND_RELATION &&
		relkind != RELKIND_DIRECTORY_TABLE &&
		relkind != RELKIND_VIEW &&
		relkind != RELKIND_MATVIEW &&
		relkind != RELKIND_SEQUENCE &&
		relkind != RELKIND_FOREIGN_TABLE &&
		relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table, directory table, view, materialized view, sequence, or foreign table",
						rv->relname)));

	ReleaseSysCache(tuple);
}

/*
 * Transform any expressions present in the partition key
 *
 * Returns a transformed PartitionSpec, as well as the strategy code
 */
static PartitionSpec *
transformPartitionSpec(Relation rel, PartitionSpec *partspec, char *strategy)
{
	PartitionSpec *newspec;
	ParseState *pstate;
	ParseNamespaceItem *nsitem;
	ListCell   *l;

	newspec = makeNode(PartitionSpec);

	newspec->strategy = partspec->strategy;
	newspec->partParams = NIL;
	newspec->location = partspec->location;
	newspec->gpPartDef = partspec->gpPartDef;
	newspec->subPartSpec = partspec->subPartSpec;

	/* Parse partitioning strategy name */
	if (pg_strcasecmp(partspec->strategy, "hash") == 0)
		*strategy = PARTITION_STRATEGY_HASH;
	else if (pg_strcasecmp(partspec->strategy, "list") == 0)
		*strategy = PARTITION_STRATEGY_LIST;
	else if (pg_strcasecmp(partspec->strategy, "range") == 0)
		*strategy = PARTITION_STRATEGY_RANGE;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized partitioning strategy \"%s\"",
						partspec->strategy)));

	/* Check valid number of columns for strategy */
	if (*strategy == PARTITION_STRATEGY_LIST &&
		list_length(partspec->partParams) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("cannot use \"list\" partition strategy with more than one column")));

	/*
	 * In GPDB, the dispatcher does the transformation and the QEs get
	 * already-transformed expressions. So all we had to do here was parse
	 * the strategy name
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
		return partspec;

	/*
	 * Create a dummy ParseState and insert the target relation as its sole
	 * rangetable entry.  We need a ParseState for transformExpr.
	 */
	pstate = make_parsestate(NULL);
	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
										   NULL, false, true);
	addNSItemToQuery(pstate, nsitem, true, true, true);

	/* take care of any partition expressions */
	foreach(l, partspec->partParams)
	{
		PartitionElem *pelem = castNode(PartitionElem, lfirst(l));

		if (pelem->expr)
		{
			/* Copy, to avoid scribbling on the input */
			pelem = copyObject(pelem);

			/* Now do parse transformation of the expression */
			pelem->expr = transformExpr(pstate, pelem->expr,
										EXPR_KIND_PARTITION_EXPRESSION);

			/* we have to fix its collations too */
			assign_expr_collations(pstate, pelem->expr);
		}

		newspec->partParams = lappend(newspec->partParams, pelem);
	}

	return newspec;
}

/*
 * Compute per-partition-column information from a list of PartitionElems.
 * Expressions in the PartitionElems must be parse-analyzed already.
 */
void
ComputePartitionAttrs(ParseState *pstate, Relation rel, List *partParams, AttrNumber *partattrs,
					  List **partexprs, Oid *partopclass, Oid *partcollation,
					  char strategy)
{
	int			attn;
	ListCell   *lc;
	Oid			am_oid;

	attn = 0;
	foreach(lc, partParams)
	{
		PartitionElem *pelem = castNode(PartitionElem, lfirst(lc));
		Oid			atttype;
		Oid			attcollation;

		if (pelem->name != NULL)
		{
			/* Simple attribute reference */
			HeapTuple	atttuple;
			Form_pg_attribute attform;

			atttuple = SearchSysCacheAttName(RelationGetRelid(rel),
											 pelem->name);
			if (!HeapTupleIsValid(atttuple))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" named in partition key does not exist",
								pelem->name),
						 parser_errposition(pstate, pelem->location)));
			attform = (Form_pg_attribute) GETSTRUCT(atttuple);

			if (attform->attnum <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("cannot use system column \"%s\" in partition key",
								pelem->name),
						 parser_errposition(pstate, pelem->location)));

			/*
			 * Generated columns cannot work: They are computed after BEFORE
			 * triggers, but partition routing is done before all triggers.
			 */
			if (attform->attgenerated)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("cannot use generated column in partition key"),
						 errdetail("Column \"%s\" is a generated column.",
								   pelem->name),
						 parser_errposition(pstate, pelem->location)));

			partattrs[attn] = attform->attnum;
			atttype = attform->atttypid;
			attcollation = attform->attcollation;
			ReleaseSysCache(atttuple);
		}
		else
		{
			/* Expression */
			Node	   *expr = pelem->expr;
			char		partattname[16];

			Assert(expr != NULL);
			atttype = exprType(expr);
			attcollation = exprCollation(expr);

			/*
			 * The expression must be of a storable type (e.g., not RECORD).
			 * The test is the same as for whether a table column is of a safe
			 * type (which is why we needn't check for the non-expression
			 * case).
			 */
			snprintf(partattname, sizeof(partattname), "%d", attn + 1);
			CheckAttributeType(partattname,
							   atttype, attcollation,
							   NIL, CHKATYPE_IS_PARTKEY);

			/*
			 * Strip any top-level COLLATE clause.  This ensures that we treat
			 * "x COLLATE y" and "(x COLLATE y)" alike.
			 */
			while (IsA(expr, CollateExpr))
				expr = (Node *) ((CollateExpr *) expr)->arg;

			if (IsA(expr, Var) &&
				((Var *) expr)->varattno > 0)
			{
				/*
				 * User wrote "(column)" or "(column COLLATE something)".
				 * Treat it like simple attribute anyway.
				 */
				partattrs[attn] = ((Var *) expr)->varattno;
			}
			else
			{
				Bitmapset  *expr_attrs = NULL;
				int			i;

				partattrs[attn] = 0;	/* marks the column as expression */
				*partexprs = lappend(*partexprs, expr);

				/*
				 * Try to simplify the expression before checking for
				 * mutability.  The main practical value of doing it in this
				 * order is that an inline-able SQL-language function will be
				 * accepted if its expansion is immutable, whether or not the
				 * function itself is marked immutable.
				 *
				 * Note that expression_planner does not change the passed in
				 * expression destructively and we have already saved the
				 * expression to be stored into the catalog above.
				 */
				expr = (Node *) expression_planner((Expr *) expr);

				/*
				 * Partition expression cannot contain mutable functions,
				 * because a given row must always map to the same partition
				 * as long as there is no change in the partition boundary
				 * structure.
				 */
				if (contain_mutable_functions(expr))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("functions in partition key expression must be marked IMMUTABLE")));

				/*
				 * transformPartitionSpec() should have already rejected
				 * subqueries, aggregates, window functions, and SRFs, based
				 * on the EXPR_KIND_ for partition expressions.
				 */

				/*
				 * Cannot allow system column references, since that would
				 * make partition routing impossible: their values won't be
				 * known yet when we need to do that.
				 */
				pull_varattnos(expr, 1, &expr_attrs);
				for (i = FirstLowInvalidHeapAttributeNumber; i < 0; i++)
				{
					if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
									  expr_attrs))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("partition key expressions cannot contain system column references")));
				}

				/*
				 * Generated columns cannot work: They are computed after
				 * BEFORE triggers, but partition routing is done before all
				 * triggers.
				 */
				i = -1;
				while ((i = bms_next_member(expr_attrs, i)) >= 0)
				{
					AttrNumber	attno = i + FirstLowInvalidHeapAttributeNumber;

					if (attno > 0 &&
						TupleDescAttr(RelationGetDescr(rel), attno - 1)->attgenerated)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("cannot use generated column in partition key"),
								 errdetail("Column \"%s\" is a generated column.",
										   get_attname(RelationGetRelid(rel), attno, false)),
								 parser_errposition(pstate, pelem->location)));
				}

				/*
				 * While it is not exactly *wrong* for a partition expression
				 * to be a constant, it seems better to reject such keys.
				 */
				if (IsA(expr, Const))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("cannot use constant expression as partition key")));
			}
		}

		/*
		 * Apply collation override if any
		 */
		if (pelem->collation)
			attcollation = get_collation_oid(pelem->collation, false);

		/*
		 * Check we have a collation iff it's a collatable type.  The only
		 * expected failures here are (1) COLLATE applied to a noncollatable
		 * type, or (2) partition expression had an unresolved collation. But
		 * we might as well code this to be a complete consistency check.
		 */
		if (type_is_collatable(atttype))
		{
			if (!OidIsValid(attcollation))
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg("could not determine which collation to use for partition expression"),
						 errhint("Use the COLLATE clause to set the collation explicitly.")));
		}
		else
		{
			if (OidIsValid(attcollation))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("collations are not supported by type %s",
								format_type_be(atttype))));
		}

		partcollation[attn] = attcollation;

		/*
		 * Identify the appropriate operator class.  For list and range
		 * partitioning, we use a btree operator class; hash partitioning uses
		 * a hash operator class.
		 */
		if (strategy == PARTITION_STRATEGY_HASH)
			am_oid = HASH_AM_OID;
		else
			am_oid = BTREE_AM_OID;

		if (!pelem->opclass)
		{
			partopclass[attn] = GetDefaultOpClass(atttype, am_oid);

			if (!OidIsValid(partopclass[attn]))
			{
				if (strategy == PARTITION_STRATEGY_HASH)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("data type %s has no default operator class for access method \"%s\"",
									format_type_be(atttype), "hash"),
							 errhint("You must specify a hash operator class or define a default hash operator class for the data type.")));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("data type %s has no default operator class for access method \"%s\"",
									format_type_be(atttype), "btree"),
							 errhint("You must specify a btree operator class or define a default btree operator class for the data type.")));

			}
		}
		else
			partopclass[attn] = ResolveOpClass(pelem->opclass,
											   atttype,
											   am_oid == HASH_AM_OID ? "hash" : "btree",
											   am_oid);

		attn++;
	}
}

/*
 * PartConstraintImpliedByRelConstraint
 *		Do scanrel's existing constraints imply the partition constraint?
 *
 * "Existing constraints" include its check constraints and column-level
 * NOT NULL constraints.  partConstraint describes the partition constraint,
 * in implicit-AND form.
 */
bool
PartConstraintImpliedByRelConstraint(Relation scanrel,
									 List *partConstraint)
{
	List	   *existConstraint = NIL;
	TupleConstr *constr = RelationGetDescr(scanrel)->constr;
	int			i;

	if (constr && constr->has_not_null)
	{
		int			natts = scanrel->rd_att->natts;

		for (i = 1; i <= natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(scanrel->rd_att, i - 1);

			if (att->attnotnull && !att->attisdropped)
			{
				NullTest   *ntest = makeNode(NullTest);

				ntest->arg = (Expr *) makeVar(1,
											  i,
											  att->atttypid,
											  att->atttypmod,
											  att->attcollation,
											  0);
				ntest->nulltesttype = IS_NOT_NULL;

				/*
				 * argisrow=false is correct even for a composite column,
				 * because attnotnull does not represent a SQL-spec IS NOT
				 * NULL test in such a case, just IS DISTINCT FROM NULL.
				 */
				ntest->argisrow = false;
				ntest->location = -1;
				existConstraint = lappend(existConstraint, ntest);
			}
		}
	}

	return ConstraintImpliedByRelConstraint(scanrel, partConstraint, existConstraint);
}

/*
 * ConstraintImpliedByRelConstraint
 *		Do scanrel's existing constraints imply the given constraint?
 *
 * testConstraint is the constraint to validate. provenConstraint is a
 * caller-provided list of conditions which this function may assume
 * to be true. Both provenConstraint and testConstraint must be in
 * implicit-AND form, must only contain immutable clauses, and must
 * contain only Vars with varno = 1.
 */
bool
ConstraintImpliedByRelConstraint(Relation scanrel, List *testConstraint, List *provenConstraint)
{
	List	   *existConstraint = list_copy(provenConstraint);
	TupleConstr *constr = RelationGetDescr(scanrel)->constr;
	int			num_check,
				i;

	num_check = (constr != NULL) ? constr->num_check : 0;
	for (i = 0; i < num_check; i++)
	{
		Node	   *cexpr;

		/*
		 * If this constraint hasn't been fully validated yet, we must ignore
		 * it here.
		 */
		if (!constr->check[i].ccvalid)
			continue;

		cexpr = stringToNode(constr->check[i].ccbin);

		/*
		 * Run each expression through const-simplification and
		 * canonicalization.  It is necessary, because we will be comparing it
		 * to similarly-processed partition constraint expressions, and may
		 * fail to detect valid matches without this.
		 */
		cexpr = eval_const_expressions(NULL, cexpr);
		cexpr = (Node *) canonicalize_qual((Expr *) cexpr, true);

		existConstraint = list_concat(existConstraint,
									  make_ands_implicit((Expr *) cexpr));
	}

	/*
	 * Try to make the proof.  Since we are comparing CHECK constraints, we
	 * need to use weak implication, i.e., we assume existConstraint is
	 * not-false and try to prove the same for testConstraint.
	 *
	 * Note that predicate_implied_by assumes its first argument is known
	 * immutable.  That should always be true for both NOT NULL and partition
	 * constraints, so we don't test it here.
	 */
	return predicate_implied_by(testConstraint, existConstraint, true);
}

/*
 * QueuePartitionConstraintValidation
 *
 * Add an entry to wqueue to have the given partition constraint validated by
 * Phase 3, for the given relation, and all its children.
 *
 * We first verify whether the given constraint is implied by pre-existing
 * relation constraints; if it is, there's no need to scan the table to
 * validate, so don't queue in that case.
 */
static void
QueuePartitionConstraintValidation(List **wqueue, Relation scanrel,
								   List *partConstraint,
								   bool validate_default)
{
	/*
	 * Based on the table's existing constraints, determine whether or not we
	 * may skip scanning the table.
	 */
	if (PartConstraintImpliedByRelConstraint(scanrel, partConstraint))
	{
		if (!validate_default)
			ereport(DEBUG1,
					(errmsg_internal("partition constraint for table \"%s\" is implied by existing constraints",
									 RelationGetRelationName(scanrel))));
		else
			ereport(DEBUG1,
					(errmsg_internal("updated partition constraint for default partition \"%s\" is implied by existing constraints",
									 RelationGetRelationName(scanrel))));
		return;
	}

	/*
	 * Constraints proved insufficient. For plain relations, queue a
	 * validation item now; for partitioned tables, recurse to process each
	 * partition.
	 */
	if (scanrel->rd_rel->relkind == RELKIND_RELATION)
	{
		AlteredTableInfo *tab;

		/* Grab a work queue entry. */
		tab = ATGetQueueEntry(wqueue, scanrel);

		if (Gp_role == GP_ROLE_EXECUTE)
		{
			/*
			 * In the QE, we receive these from the QD. We should reach
			 * the same conclusions if we re-did the work here.
			 */
			Assert(equal(tab->partition_constraint, linitial(partConstraint)));
			Assert(tab->validate_default == validate_default);
		}
		else
		{
			Assert(tab->partition_constraint == NULL);
			tab->partition_constraint = (Expr *) linitial(partConstraint);
			tab->validate_default = validate_default;
		}
	}
	else if (scanrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		PartitionDesc partdesc = RelationGetPartitionDesc(scanrel, true);
		int			i;

		for (i = 0; i < partdesc->nparts; i++)
		{
			Relation	part_rel;
			List	   *thisPartConstraint;

			/*
			 * This is the minimum lock we need to prevent deadlocks.
			 */
			part_rel = table_open(partdesc->oids[i], AccessExclusiveLock);

			/*
			 * Adjust the constraint for scanrel so that it matches this
			 * partition's attribute numbers.
			 */
			thisPartConstraint =
				map_partition_varattnos(partConstraint, 1,
										part_rel, scanrel);

			QueuePartitionConstraintValidation(wqueue, part_rel,
											   thisPartConstraint,
											   validate_default);
			table_close(part_rel, NoLock);	/* keep lock till commit */
		}
	}
}

/*
 * ALTER TABLE <name> ATTACH PARTITION <partition-name> FOR VALUES
 *
 * Return the address of the newly attached partition.
 */
static ObjectAddress
ATExecAttachPartition(List **wqueue, Relation rel, PartitionCmd *cmd,
					  AlterTableUtilityContext *context)
{
	Relation	attachrel,
				catalog;
	List	   *attachrel_children;
	List	   *partConstraint;
	SysScanDesc scan;
	ScanKeyData skey;
	AttrNumber	attno;
	int			natts;
	TupleDesc	tupleDesc;
	ObjectAddress address;
	const char *trigger_name;
	Oid			defaultPartOid;
	List	   *partBoundConstraint;
	ParseState *pstate = make_parsestate(NULL);

	pstate->p_sourcetext = context->queryString;

	/*
	 * We must lock the default partition if one exists, because attaching a
	 * new partition will change its partition constraint.
	 */
	defaultPartOid =
		get_default_oid_from_partdesc(RelationGetPartitionDesc(rel, true));
	if (OidIsValid(defaultPartOid))
		LockRelationOid(defaultPartOid, AccessExclusiveLock);

	attachrel = table_openrv(cmd->name, AccessExclusiveLock);

	/*
	 * XXX I think it'd be a good idea to grab locks on all tables referenced
	 * by FKs at this point also.
	 */

	/*
	 * Must be owner of both parent and source table -- parent was checked by
	 * ATSimplePermissions call in ATPrepCmd
	 */
	ATSimplePermissions(attachrel, ATT_TABLE | ATT_FOREIGN_TABLE);

	/* A partition can only have one parent */
	if (attachrel->rd_rel->relispartition)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is already a partition",
						RelationGetRelationName(attachrel))));

	if (OidIsValid(attachrel->rd_rel->reloftype))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot attach a typed table as partition")));

	/*
	 * Table being attached should not already be part of inheritance; either
	 * as a child table...
	 */
	catalog = table_open(InheritsRelationId, AccessShareLock);
	ScanKeyInit(&skey,
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(attachrel)));
	scan = systable_beginscan(catalog, InheritsRelidSeqnoIndexId, true,
							  NULL, 1, &skey);
	if (HeapTupleIsValid(systable_getnext(scan)))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot attach inheritance child as partition")));
	systable_endscan(scan);

	/* ...or as a parent table (except the case when it is partitioned) */
	ScanKeyInit(&skey,
				Anum_pg_inherits_inhparent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(attachrel)));
	scan = systable_beginscan(catalog, InheritsParentIndexId, true, NULL,
							  1, &skey);
	if (HeapTupleIsValid(systable_getnext(scan)) &&
		attachrel->rd_rel->relkind == RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot attach inheritance parent as partition")));
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);

	/*
	 * Prevent circularity by seeing if rel is a partition of attachrel. (In
	 * particular, this disallows making a rel a partition of itself.)
	 *
	 * We do that by checking if rel is a member of the list of attachrel's
	 * partitions provided the latter is partitioned at all.  We want to avoid
	 * having to construct this list again, so we request the strongest lock
	 * on all partitions.  We need the strongest lock, because we may decide
	 * to scan them if we find out that the table being attached (or its leaf
	 * partitions) may contain rows that violate the partition constraint. If
	 * the table has a constraint that would prevent such rows, which by
	 * definition is present in all the partitions, we need not scan the
	 * table, nor its partitions.  But we cannot risk a deadlock by taking a
	 * weaker lock now and the stronger one only when needed.
	 */
	attachrel_children = find_all_inheritors(RelationGetRelid(attachrel),
											 AccessExclusiveLock, NULL);
	if (list_member_oid(attachrel_children, RelationGetRelid(rel)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("circular inheritance not allowed"),
				 errdetail("\"%s\" is already a child of \"%s\".",
						   RelationGetRelationName(rel),
						   RelationGetRelationName(attachrel))));

	/* If the parent is permanent, so must be all of its partitions. */
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP &&
		attachrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot attach a temporary relation as partition of permanent relation \"%s\"",
						RelationGetRelationName(rel))));

	/* Temp parent cannot have a partition that is itself not a temp */
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
		attachrel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot attach a permanent relation as partition of temporary relation \"%s\"",
						RelationGetRelationName(rel))));

	/* If the parent is temp, it must belong to this session */
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
		!rel->rd_islocaltemp)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot attach as partition of temporary relation of another session")));

	/* Ditto for the partition */
	if (attachrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
		!attachrel->rd_islocaltemp)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot attach temporary relation of another session as partition")));

	/* Check if there are any columns in attachrel that aren't in the parent */
	tupleDesc = RelationGetDescr(attachrel);
	natts = tupleDesc->natts;
	for (attno = 1; attno <= natts; attno++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc, attno - 1);
		char	   *attributeName = NameStr(attribute->attname);

		/* Ignore dropped */
		if (attribute->attisdropped)
			continue;

		/* Try to find the column in parent (matching on column name) */
		if (!SearchSysCacheExists2(ATTNAME,
								   ObjectIdGetDatum(RelationGetRelid(rel)),
								   CStringGetDatum(attributeName)))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table \"%s\" contains column \"%s\" not found in parent \"%s\"",
							RelationGetRelationName(attachrel), attributeName,
							RelationGetRelationName(rel)),
					 errdetail("The new partition may contain only the columns present in parent.")));
	}

	/*
	 * Check that the distribution policy matches. The columns might be in
	 * different order, so use GpPolicyEqualByName() rather than just
	 * GpPolicyEqual() here.
	 */
	if (attachrel->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
		!GpPolicyEqualByName(RelationGetDescr(rel),
							 rel->rd_cdbpolicy,
							 RelationGetDescr(attachrel),
							 attachrel->rd_cdbpolicy))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("distribution policy for \"%s\" must be the same as that for \"%s\"",
						RelationGetRelationName(attachrel),
						RelationGetRelationName(rel))));
	}

	/*
	 * If child_rel has row-level triggers with transition tables, we
	 * currently don't allow it to become a partition.  See also prohibitions
	 * in ATExecAddInherit() and CreateTrigger().
	 */
	trigger_name = FindTriggerIncompatibleWithInheritance(attachrel->trigdesc);
	if (trigger_name != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("trigger \"%s\" prevents table \"%s\" from becoming a partition",
						trigger_name, RelationGetRelationName(attachrel)),
				 errdetail("ROW triggers with transition tables are not supported on partitions")));

	/*
	 * Check that the new partition's bound is valid and does not overlap any
	 * of existing partitions of the parent - note that it does not return on
	 * error.
	 */
	check_new_partition_bound(RelationGetRelationName(attachrel), rel,
							  cmd->bound, pstate);

	/* OK to create inheritance.  Rest of the checks performed there */
	CreateInheritance(attachrel, rel);

	/* Update the pg_class entry. */
	StorePartitionBound(attachrel, rel, cmd->bound);

	/* Ensure there exists a correct set of indexes in the partition. */
	AttachPartitionEnsureIndexes(rel, attachrel);

	/* and triggers */
	CloneRowTriggersToPartition(rel, attachrel);

	/*
	 * Clone foreign key constraints.  Callee is responsible for setting up
	 * for phase 3 constraint verification.
	 */
	CloneForeignKeyConstraints(wqueue, rel, attachrel);

	/*
	 * Generate partition constraint from the partition bound specification.
	 * If the parent itself is a partition, make sure to include its
	 * constraint as well.
	 */
	partBoundConstraint = get_qual_from_partbound(attachrel, rel, cmd->bound);
	partConstraint = list_concat(partBoundConstraint,
								 RelationGetPartitionQual(rel));

	/* Skip validation if there are no constraints to validate. */
	if (partConstraint)
	{
		/*
		 * Run the partition quals through const-simplification similar to
		 * check constraints.  We skip canonicalize_qual, though, because
		 * partition quals should be in canonical form already.
		 */
		partConstraint =
			(List *) eval_const_expressions(NULL,
											(Node *) partConstraint);

		/* XXX this sure looks wrong */
		partConstraint = list_make1(make_ands_explicit(partConstraint));

		/*
		 * Adjust the generated constraint to match this partition's attribute
		 * numbers.
		 */
		partConstraint = map_partition_varattnos(partConstraint, 1, attachrel,
												 rel);

		/* Validate partition constraints against the table being attached. */
		QueuePartitionConstraintValidation(wqueue, attachrel, partConstraint,
										   false);
	}

	/*
	 * If we're attaching a partition other than the default partition and a
	 * default one exists, then that partition's partition constraint changes,
	 * so add an entry to the work queue to validate it, too.  (We must not do
	 * this when the partition being attached is the default one; we already
	 * did it above!)
	 */
	if (OidIsValid(defaultPartOid))
	{
		Relation	defaultrel;
		List	   *defPartConstraint;

		Assert(!cmd->bound->is_default);

		/* we already hold a lock on the default partition */
		defaultrel = table_open(defaultPartOid, NoLock);
		defPartConstraint =
			get_proposed_default_constraint(partBoundConstraint);

		/*
		 * Map the Vars in the constraint expression from rel's attnos to
		 * defaultrel's.
		 */
		defPartConstraint =
			map_partition_varattnos(defPartConstraint,
									1, defaultrel, rel);
		QueuePartitionConstraintValidation(wqueue, defaultrel,
										   defPartConstraint, true);

		/* keep our lock until commit. */
		table_close(defaultrel, NoLock);
	}

	/* MPP-6929: metadata tracking */
	MetaTrackUpdObject(RelationRelationId,
					   RelationGetRelid(attachrel),
					   GetUserId(),
					   "PARTITION", "ATTACH");

	ObjectAddressSet(address, RelationRelationId, RelationGetRelid(attachrel));

	/*
	 * If the partition we just attached is partitioned itself, invalidate
	 * relcache for all descendent partitions too to ensure that their
	 * rd_partcheck expression trees are rebuilt; partitions already locked
	 * at the beginning of this function.
	 */
	if (attachrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		ListCell *l;

		foreach(l, attachrel_children)
		{
			CacheInvalidateRelcacheByRelid(lfirst_oid(l));
		}
	}

	/* keep our lock until commit */
	table_close(attachrel, NoLock);

	return address;
}

/*
 * AttachPartitionEnsureIndexes
 *		subroutine for ATExecAttachPartition to create/match indexes
 *
 * Enforce the indexing rule for partitioned tables during ALTER TABLE / ATTACH
 * PARTITION: every partition must have an index attached to each index on the
 * partitioned table.
 */
static void
AttachPartitionEnsureIndexes(Relation rel, Relation attachrel)
{
	List	   *idxes;
	List	   *attachRelIdxs;
	Relation   *attachrelIdxRels;
	IndexInfo **attachInfos;
	int			i;
	ListCell   *cell;
	MemoryContext cxt;
	MemoryContext oldcxt;

	cxt = AllocSetContextCreate(CurrentMemoryContext,
								"AttachPartitionEnsureIndexes",
								ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);

	idxes = RelationGetIndexList(rel);
	attachRelIdxs = RelationGetIndexList(attachrel);
	attachrelIdxRels = palloc(sizeof(Relation) * list_length(attachRelIdxs));
	attachInfos = palloc(sizeof(IndexInfo *) * list_length(attachRelIdxs));

	/* Build arrays of all existing indexes and their IndexInfos */
	i = 0;
	foreach(cell, attachRelIdxs)
	{
		Oid			cldIdxId = lfirst_oid(cell);

		attachrelIdxRels[i] = index_open(cldIdxId, AccessShareLock);
		attachInfos[i] = BuildIndexInfo(attachrelIdxRels[i]);
		i++;
	}

	/*
	 * If we're attaching a foreign table, we must fail if any of the indexes
	 * is a constraint index; otherwise, there's nothing to do here.  Do this
	 * before starting work, to avoid wasting the effort of building a few
	 * non-unique indexes before coming across a unique one.
	 */
	if (attachrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		foreach(cell, idxes)
		{
			Oid			idx = lfirst_oid(cell);
			Relation	idxRel = index_open(idx, AccessShareLock);

			if (idxRel->rd_index->indisunique ||
				idxRel->rd_index->indisprimary)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot attach foreign table \"%s\" as partition of partitioned table \"%s\"",
								RelationGetRelationName(attachrel),
								RelationGetRelationName(rel)),
						 errdetail("Partitioned table \"%s\" contains unique indexes.",
								   RelationGetRelationName(rel))));
			index_close(idxRel, AccessShareLock);
		}

		goto out;
	}

	/*
	 * For each index on the partitioned table, find a matching one in the
	 * partition-to-be; if one is not found, create one.
	 */
	foreach(cell, idxes)
	{
		Oid			idx = lfirst_oid(cell);
		Relation	idxRel = index_open(idx, AccessShareLock);
		IndexInfo  *info;
		AttrMap    *attmap;
		bool		found = false;
		Oid			constraintOid;

		/*
		 * Ignore indexes in the partitioned table other than partitioned
		 * indexes.
		 */
		if (idxRel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		{
			index_close(idxRel, AccessShareLock);
			continue;
		}

		/* construct an indexinfo to compare existing indexes against */
		info = BuildIndexInfo(idxRel);
		attmap = build_attrmap_by_name(RelationGetDescr(attachrel),
									   RelationGetDescr(rel));
		constraintOid = get_relation_idx_constraint_oid(RelationGetRelid(rel), idx);

		/*
		 * Scan the list of existing indexes in the partition-to-be, and mark
		 * the first matching, unattached one we find, if any, as partition of
		 * the parent index.  If we find one, we're done.
		 */
		for (i = 0; i < list_length(attachRelIdxs); i++)
		{
			Oid			cldIdxId = RelationGetRelid(attachrelIdxRels[i]);
			Oid			cldConstrOid = InvalidOid;

			/* does this index have a parent?  if so, can't use it */
			if (attachrelIdxRels[i]->rd_rel->relispartition)
				continue;

			if (CompareIndexInfo(attachInfos[i], info,
								 attachrelIdxRels[i]->rd_indcollation,
								 idxRel->rd_indcollation,
								 attachrelIdxRels[i]->rd_opfamily,
								 idxRel->rd_opfamily,
								 attmap))
			{
				/*
				 * If this index is being created in the parent because of a
				 * constraint, then the child needs to have a constraint also,
				 * so look for one.  If there is no such constraint, this
				 * index is no good, so keep looking.
				 */
				if (OidIsValid(constraintOid))
				{
					cldConstrOid =
						get_relation_idx_constraint_oid(RelationGetRelid(attachrel),
														cldIdxId);
					/* no dice */
					if (!OidIsValid(cldConstrOid))
						continue;
				}

				/* bingo. */
				IndexSetParentIndex(attachrelIdxRels[i], idx);
				if (OidIsValid(constraintOid))
					ConstraintSetParentConstraint(cldConstrOid, constraintOid,
												  RelationGetRelid(attachrel));
				found = true;

				CommandCounterIncrement();
				break;
			}
		}

		/*
		 * If no suitable index was found in the partition-to-be, create one
		 * now.
		 */
		if (!found)
		{
			IndexStmt  *stmt;
			Oid			constraintOid;

			stmt = generateClonedIndexStmt(NULL,
										   idxRel, attmap,
										   &constraintOid);
			/*
			 * Also don't dispatch this if it's part of an ALTER TABLE. We will dispatch
			 * the whole ALTER TABLE command later.
			 */
			HOLD_DISPATCH();
			DefineIndex(RelationGetRelid(attachrel), stmt, InvalidOid,
						RelationGetRelid(idxRel),
						constraintOid,
						true, false, false, false, false);
			RESUME_DISPATCH();
		}

		index_close(idxRel, AccessShareLock);
	}

out:
	/* Clean up. */
	for (i = 0; i < list_length(attachRelIdxs); i++)
		index_close(attachrelIdxRels[i], AccessShareLock);
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);
}

/*
 * CloneRowTriggersToPartition
 *		subroutine for ATExecAttachPartition/DefineRelation to create row
 *		triggers on partitions
 */
static void
CloneRowTriggersToPartition(Relation parent, Relation partition)
{
	Relation	pg_trigger;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;
	MemoryContext perTupCxt;

	ScanKeyInit(&key, Anum_pg_trigger_tgrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(parent)));
	pg_trigger = table_open(TriggerRelationId, RowExclusiveLock);
	scan = systable_beginscan(pg_trigger, TriggerRelidNameIndexId,
							  true, NULL, 1, &key);

	perTupCxt = AllocSetContextCreate(CurrentMemoryContext,
									  "clone trig", ALLOCSET_SMALL_SIZES);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_trigger trigForm = (Form_pg_trigger) GETSTRUCT(tuple);
		CreateTrigStmt *trigStmt;
		Node	   *qual = NULL;
		Datum		value;
		bool		isnull;
		List	   *cols = NIL;
		List	   *trigargs = NIL;
		MemoryContext oldcxt;

		/*
		 * Ignore statement-level triggers; those are not cloned.
		 */
		if (!TRIGGER_FOR_ROW(trigForm->tgtype))
			continue;

		/*
		 * Internal triggers require careful examination.  Ideally, we don't
		 * clone them.  However, if our parent is itself a partition, there
		 * might be internal triggers that must not be skipped; for example,
		 * triggers on our parent that are in turn clones from its parent (our
		 * grandparent) are marked internal, yet they are to be cloned.
		 *
		 * Note we dare not verify that the other trigger belongs to an
		 * ancestor relation of our parent, because that creates deadlock
		 * opportunities.
		 */
		if (trigForm->tgisinternal &&
			(!parent->rd_rel->relispartition ||
			 !OidIsValid(trigForm->tgparentid)))
			continue;

		/*
		 * Complain if we find an unexpected trigger type.
		 */
		if (!TRIGGER_FOR_BEFORE(trigForm->tgtype) &&
			!TRIGGER_FOR_AFTER(trigForm->tgtype))
			elog(ERROR, "unexpected trigger \"%s\" found",
				 NameStr(trigForm->tgname));

		/* Use short-lived context for CREATE TRIGGER */
		oldcxt = MemoryContextSwitchTo(perTupCxt);

		/*
		 * If there is a WHEN clause, generate a 'cooked' version of it that's
		 * appropriate for the partition.
		 */
		value = heap_getattr(tuple, Anum_pg_trigger_tgqual,
							 RelationGetDescr(pg_trigger), &isnull);
		if (!isnull)
		{
			qual = stringToNode(TextDatumGetCString(value));
			qual = (Node *) map_partition_varattnos((List *) qual, PRS2_OLD_VARNO,
													partition, parent);
			qual = (Node *) map_partition_varattnos((List *) qual, PRS2_NEW_VARNO,
													partition, parent);
		}

		/*
		 * If there is a column list, transform it to a list of column names.
		 * Note we don't need to map this list in any way ...
		 */
		if (trigForm->tgattr.dim1 > 0)
		{
			int			i;

			for (i = 0; i < trigForm->tgattr.dim1; i++)
			{
				Form_pg_attribute col;

				col = TupleDescAttr(parent->rd_att,
									trigForm->tgattr.values[i] - 1);
				cols = lappend(cols,
							   makeString(pstrdup(NameStr(col->attname))));
			}
		}

		/* Reconstruct trigger arguments list. */
		if (trigForm->tgnargs > 0)
		{
			char	   *p;

			value = heap_getattr(tuple, Anum_pg_trigger_tgargs,
								 RelationGetDescr(pg_trigger), &isnull);
			if (isnull)
				elog(ERROR, "tgargs is null for trigger \"%s\" in partition \"%s\"",
					 NameStr(trigForm->tgname), RelationGetRelationName(partition));

			p = (char *) VARDATA_ANY(DatumGetByteaPP(value));

			for (int i = 0; i < trigForm->tgnargs; i++)
			{
				trigargs = lappend(trigargs, makeString(pstrdup(p)));
				p += strlen(p) + 1;
			}
		}

		trigStmt = makeNode(CreateTrigStmt);
		trigStmt->replace = false;
		trigStmt->isconstraint = OidIsValid(trigForm->tgconstraint);
		trigStmt->trigname = NameStr(trigForm->tgname);
		trigStmt->relation = NULL;
		trigStmt->funcname = NULL;	/* passed separately */
		trigStmt->args = trigargs;
		trigStmt->row = true;
		trigStmt->timing = trigForm->tgtype & TRIGGER_TYPE_TIMING_MASK;
		trigStmt->events = trigForm->tgtype & TRIGGER_TYPE_EVENT_MASK;
		trigStmt->columns = cols;
		trigStmt->whenClause = NULL;	/* passed separately */
		trigStmt->transitionRels = NIL; /* not supported at present */
		trigStmt->deferrable = trigForm->tgdeferrable;
		trigStmt->initdeferred = trigForm->tginitdeferred;
		trigStmt->constrrel = NULL; /* passed separately */

		CreateTriggerFiringOn(trigStmt, NULL, RelationGetRelid(partition),
							  trigForm->tgconstrrelid, InvalidOid, InvalidOid,
							  trigForm->tgfoid, trigForm->oid, qual,
							  false, true, trigForm->tgenabled);

		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(perTupCxt);
	}

	MemoryContextDelete(perTupCxt);

	systable_endscan(scan);
	table_close(pg_trigger, RowExclusiveLock);
}

/*
 * ALTER TABLE DETACH PARTITION
 *
 * Return the address of the relation that is no longer a partition of rel.
 *
 * If concurrent mode is requested, we run in two transactions.  A side-
 * effect is that this command cannot run in a multi-part ALTER TABLE.
 * Currently, that's enforced by the grammar.
 *
 * The strategy for concurrency is to first modify the partition's
 * pg_inherit catalog row to make it visible to everyone that the
 * partition is detached, lock the partition against writes, and commit
 * the transaction; anyone who requests the partition descriptor from
 * that point onwards has to ignore such a partition.  In a second
 * transaction, we wait until all transactions that could have seen the
 * partition as attached are gone, then we remove the rest of partition
 * metadata (pg_inherits and pg_class.relpartbounds).
 */
static ObjectAddress
ATExecDetachPartition(List **wqueue, AlteredTableInfo *tab, Relation rel,
					  RangeVar *name, bool concurrent)
{
	Relation	partRel;
	ObjectAddress address;
	Oid			defaultPartOid;

	/*
	 * We must lock the default partition, because detaching this partition
	 * will change its partition constraint.
	 */
	defaultPartOid =
		get_default_oid_from_partdesc(RelationGetPartitionDesc(rel, true));

	/* GPDB_14_MERGE_FIXME: detach partition is not supported now. We need to
	 * implement how to control cmd which will be executed in two transactions.
	 */
	if (concurrent)
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot support detach partitions concurrently now")));

	if (OidIsValid(defaultPartOid))
	{
		/*
		 * Concurrent detaching when a default partition exists is not
		 * supported. The main problem is that the default partition
		 * constraint would change.  And there's a definitional problem: what
		 * should happen to the tuples that are being inserted that belong to
		 * the partition being detached?  Putting them on the partition being
		 * detached would be wrong, since they'd become "lost" after the
		 * detaching completes but we cannot put them in the default partition
		 * either until we alter its partition constraint.
		 *
		 * I think we could solve this problem if we effected the constraint
		 * change before committing the first transaction.  But the lock would
		 * have to remain AEL and it would cause concurrent query planning to
		 * be blocked, so changing it that way would be even worse.
		 */
		if (concurrent)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot detach partitions concurrently when a default partition exists")));
		LockRelationOid(defaultPartOid, AccessExclusiveLock);
	}

	/*
	 * In concurrent mode, the partition is locked with share-update-exclusive
	 * in the first transaction.  This allows concurrent transactions to be
	 * doing DML to the partition.
	 */
	partRel = table_openrv(name, concurrent ? ShareUpdateExclusiveLock :
						   AccessExclusiveLock);

	/*
	 * Check inheritance conditions and either delete the pg_inherits row (in
	 * non-concurrent mode) or just set the inhdetachpending flag.
	 */
	if (!concurrent)
		RemoveInheritance(partRel, rel, false);
	else
		MarkInheritDetached(partRel, rel);

	/*
	 * Ensure that foreign keys still hold after this detach.  This keeps
	 * locks on the referencing tables, which prevents concurrent transactions
	 * from adding rows that we wouldn't see.  For this to work in concurrent
	 * mode, it is critical that the partition appears as no longer attached
	 * for the RI queries as soon as the first transaction commits.
	 */
	ATDetachCheckNoForeignKeyRefs(partRel);

	/*
	 * Concurrent mode has to work harder; first we add a new constraint to
	 * the partition that matches the partition constraint.  Then we close our
	 * existing transaction, and in a new one wait for all processes to catch
	 * up on the catalog updates we've done so far; at that point we can
	 * complete the operation.
	 */
	if (concurrent)
	{
		Oid			partrelid,
					parentrelid;
		LOCKTAG		tag;
		char	   *parentrelname;
		char	   *partrelname;

		/*
		 * Add a new constraint to the partition being detached, which
		 * supplants the partition constraint (unless there is one already).
		 */
		DetachAddConstraintIfNeeded(wqueue, partRel);

		/*
		 * We're almost done now; the only traces that remain are the
		 * pg_inherits tuple and the partition's relpartbounds.  Before we can
		 * remove those, we need to wait until all transactions that know that
		 * this is a partition are gone.
		 */

		/*
		 * Remember relation OIDs to re-acquire them later; and relation names
		 * too, for error messages if something is dropped in between.
		 */
		partrelid = RelationGetRelid(partRel);
		parentrelid = RelationGetRelid(rel);
		parentrelname = MemoryContextStrdup(PortalContext,
											RelationGetRelationName(rel));
		partrelname = MemoryContextStrdup(PortalContext,
										  RelationGetRelationName(partRel));

		/* Invalidate relcache entries for the parent -- must be before close */
		CacheInvalidateRelcache(rel);

		table_close(partRel, NoLock);
		table_close(rel, NoLock);
		tab->rel = NULL;

		/* Make updated catalog entry visible */
		PopActiveSnapshot();
		CommitTransactionCommand();

		StartTransactionCommand();

		/*
		 * Now wait.  This ensures that all queries that were planned
		 * including the partition are finished before we remove the rest of
		 * catalog entries.  We don't need or indeed want to acquire this
		 * lock, though -- that would block later queries.
		 *
		 * We don't need to concern ourselves with waiting for a lock on the
		 * partition itself, since we will acquire AccessExclusiveLock below.
		 */
		SET_LOCKTAG_RELATION(tag, MyDatabaseId, parentrelid);
		WaitForLockersMultiple(list_make1(&tag), AccessExclusiveLock, false);

		/*
		 * Now acquire locks in both relations again.  Note they may have been
		 * removed in the meantime, so care is required.
		 */
		rel = try_relation_open(parentrelid, ShareUpdateExclusiveLock, false);
		partRel = try_relation_open(partrelid, AccessExclusiveLock, false);

		/* If the relations aren't there, something bad happened; bail out */
		if (rel == NULL)
		{
			if (partRel != NULL)	/* shouldn't happen */
				elog(WARNING, "dangling partition \"%s\" remains, can't fix",
					 partrelname);
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("partitioned table \"%s\" was removed concurrently",
							parentrelname)));
		}
		if (partRel == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("partition \"%s\" was removed concurrently", partrelname)));

		tab->rel = rel;
	}

	/* Do the final part of detaching */
	DetachPartitionFinalize(rel, partRel, concurrent, defaultPartOid);

	ObjectAddressSet(address, RelationRelationId, RelationGetRelid(partRel));

	/* keep our lock until commit */
	table_close(partRel, NoLock);

	return address;
}

/*
 * Second part of ALTER TABLE .. DETACH.
 *
 * This is separate so that it can be run independently when the second
 * transaction of the concurrent algorithm fails (crash or abort).
 */
static void
DetachPartitionFinalize(Relation rel, Relation partRel, bool concurrent,
						Oid defaultPartOid)
{
	Relation	classRel;
	List	   *fks;
	ListCell   *cell;
	List	   *indexes;
	Datum		new_val[Natts_pg_class];
	bool		new_null[Natts_pg_class],
				new_repl[Natts_pg_class];
	HeapTuple	tuple,
				newtuple;

	if (concurrent)
	{
		/*
		 * We can remove the pg_inherits row now. (In the non-concurrent case,
		 * this was already done).
		 */
		RemoveInheritance(partRel, rel, true);
	}

	/* Drop any triggers that were cloned on creation/attach. */
	DropClonedTriggersFromPartition(RelationGetRelid(partRel));

	/*
	 * Detach any foreign keys that are inherited.  This includes creating
	 * additional action triggers.
	 */
	fks = copyObject(RelationGetFKeyList(partRel));
	foreach(cell, fks)
	{
		ForeignKeyCacheInfo *fk = lfirst(cell);
		HeapTuple	contup;
		Form_pg_constraint conform;
		Constraint *fkconstraint;

		contup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(fk->conoid));
		if (!HeapTupleIsValid(contup))
			elog(ERROR, "cache lookup failed for constraint %u", fk->conoid);
		conform = (Form_pg_constraint) GETSTRUCT(contup);

		/* consider only the inherited foreign keys */
		if (conform->contype != CONSTRAINT_FOREIGN ||
			!OidIsValid(conform->conparentid))
		{
			ReleaseSysCache(contup);
			continue;
		}

		/* unset conparentid and adjust conislocal, coninhcount, etc. */
		ConstraintSetParentConstraint(fk->conoid, InvalidOid, InvalidOid);

		/*
		 * Make the action triggers on the referenced relation.  When this was
		 * a partition the action triggers pointed to the parent rel (they
		 * still do), but now we need separate ones of our own.
		 */
		fkconstraint = makeNode(Constraint);
		fkconstraint->conname = pstrdup(NameStr(conform->conname));
		fkconstraint->fk_upd_action = conform->confupdtype;
		fkconstraint->fk_del_action = conform->confdeltype;
		fkconstraint->deferrable = conform->condeferrable;
		fkconstraint->initdeferred = conform->condeferred;

		createForeignKeyActionTriggers(partRel, conform->confrelid,
									   fkconstraint, fk->conoid,
									   conform->conindid);

		ReleaseSysCache(contup);
	}
	list_free_deep(fks);

	/*
	 * Any sub-constraints that are in the referenced-side of a larger
	 * constraint have to be removed.  This partition is no longer part of the
	 * key space of the constraint.
	 */
	foreach(cell, GetParentedForeignKeyRefs(partRel))
	{
		Oid			constrOid = lfirst_oid(cell);
		ObjectAddress constraint;

		ConstraintSetParentConstraint(constrOid, InvalidOid, InvalidOid);
		deleteDependencyRecordsForClass(ConstraintRelationId,
										constrOid,
										ConstraintRelationId,
										DEPENDENCY_INTERNAL);
		CommandCounterIncrement();

		ObjectAddressSet(constraint, ConstraintRelationId, constrOid);
		performDeletion(&constraint, DROP_RESTRICT, 0);
	}

	/* Now we can detach indexes */
	indexes = RelationGetIndexList(partRel);
	foreach(cell, indexes)
	{
		Oid			idxid = lfirst_oid(cell);
		Relation	idx;
		Oid			constrOid;

		if (!has_superclass(idxid))
			continue;

		Assert((IndexGetRelation(get_partition_parent(idxid, false), false) ==
				RelationGetRelid(rel)));

		idx = index_open(idxid, AccessExclusiveLock);
		IndexSetParentIndex(idx, InvalidOid);

		/* If there's a constraint associated with the index, detach it too */
		constrOid = get_relation_idx_constraint_oid(RelationGetRelid(partRel),
													idxid);
		if (OidIsValid(constrOid))
			ConstraintSetParentConstraint(constrOid, InvalidOid, InvalidOid);

		index_close(idx, NoLock);
	}

	/* Update pg_class tuple */
	classRel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(partRel)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(partRel));
	Assert(((Form_pg_class) GETSTRUCT(tuple))->relispartition);

	/* Clear relpartbound and reset relispartition */
	memset(new_val, 0, sizeof(new_val));
	memset(new_null, false, sizeof(new_null));
	memset(new_repl, false, sizeof(new_repl));
	new_val[Anum_pg_class_relpartbound - 1] = (Datum) 0;
	new_null[Anum_pg_class_relpartbound - 1] = true;
	new_repl[Anum_pg_class_relpartbound - 1] = true;
	newtuple = heap_modify_tuple(tuple, RelationGetDescr(classRel),
								 new_val, new_null, new_repl);

	((Form_pg_class) GETSTRUCT(newtuple))->relispartition = false;
	CatalogTupleUpdate(classRel, &newtuple->t_self, newtuple);
	heap_freetuple(newtuple);
	table_close(classRel, RowExclusiveLock);

	if (OidIsValid(defaultPartOid))
	{
		/*
		 * If the relation being detached is the default partition itself,
		 * remove it from the parent's pg_partitioned_table entry.
		 *
		 * If not, we must invalidate default partition's relcache entry, as
		 * in StorePartitionBound: its partition constraint depends on every
		 * other partition's partition constraint.
		 */
		if (RelationGetRelid(partRel) == defaultPartOid)
			update_default_partition_oid(RelationGetRelid(rel), InvalidOid);
		else
			CacheInvalidateRelcacheByRelid(defaultPartOid);
	}

	/*
	 * Invalidate the parent's relcache so that the partition is no longer
	 * included in its partition descriptor.
	 */
	CacheInvalidateRelcache(rel);

	/* MPP-6929: metadata tracking */
	MetaTrackUpdObject(RelationRelationId,
					   RelationGetRelid(partRel),
					   GetUserId(),
					   "PARTITION", "DETACH");

	/*
	 * If the partition we just detached is partitioned itself, invalidate
	 * relcache for all descendent partitions too to ensure that their
	 * rd_partcheck expression trees are rebuilt; must lock partitions
	 * before doing so, using the same lockmode as what partRel has been
	 * locked with by the caller.
	 */
	if (partRel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		List   *children;

		children = find_all_inheritors(RelationGetRelid(partRel),
									   AccessExclusiveLock, NULL);
		foreach(cell, children)
		{
			CacheInvalidateRelcacheByRelid(lfirst_oid(cell));
		}
	}
}

/*
 * ALTER TABLE ... DETACH PARTITION ... FINALIZE
 *
 * To use when a DETACH PARTITION command previously did not run to
 * completion; this completes the detaching process.
 */
static ObjectAddress
ATExecDetachPartitionFinalize(Relation rel, RangeVar *name)
{
	Relation	partRel;
	ObjectAddress address;
	Snapshot	snap = GetActiveSnapshot();

	partRel = table_openrv(name, AccessExclusiveLock);

	/*
	 * Wait until existing snapshots are gone.  This is important if the
	 * second transaction of DETACH PARTITION CONCURRENTLY is canceled: the
	 * user could immediately run DETACH FINALIZE without actually waiting for
	 * existing transactions.  We must not complete the detach action until
	 * all such queries are complete (otherwise we would present them with an
	 * inconsistent view of catalogs).
	 */
	WaitForOlderSnapshots(snap->xmin, false);

	DetachPartitionFinalize(rel, partRel, true, InvalidOid);

	ObjectAddressSet(address, RelationRelationId, RelationGetRelid(partRel));

	table_close(partRel, NoLock);

	return address;
}

/*
 * DetachAddConstraintIfNeeded
 *		Subroutine for ATExecDetachPartition.  Create a constraint that
 *		takes the place of the partition constraint, but avoid creating
 *		a dupe if an constraint already exists which implies the needed
 *		constraint.
 */
static void
DetachAddConstraintIfNeeded(List **wqueue, Relation partRel)
{
	List	   *constraintExpr;

	constraintExpr = RelationGetPartitionQual(partRel);
	constraintExpr = (List *) eval_const_expressions(NULL, (Node *) constraintExpr);

	/*
	 * Avoid adding a new constraint if the needed constraint is implied by an
	 * existing constraint
	 */
	if (!PartConstraintImpliedByRelConstraint(partRel, constraintExpr))
	{
		AlteredTableInfo *tab;
		Constraint *n;

		tab = ATGetQueueEntry(wqueue, partRel);

		/* Add constraint on partition, equivalent to the partition constraint */
		n = makeNode(Constraint);
		n->contype = CONSTR_CHECK;
		n->conname = NULL;
		n->location = -1;
		n->is_no_inherit = false;
		n->raw_expr = NULL;
		n->cooked_expr = nodeToString(make_ands_explicit(constraintExpr));
		n->initially_valid = true;
		n->skip_validation = true;
		/* It's a re-add, since it nominally already exists */
		ATAddCheckConstraint(wqueue, tab, partRel, n,
							 true, false, true, ShareUpdateExclusiveLock);
	}
}

/*
 * DropClonedTriggersFromPartition
 *		subroutine for ATExecDetachPartition to remove any triggers that were
 *		cloned to the partition when it was created-as-partition or attached.
 *		This undoes what CloneRowTriggersToPartition did.
 */
static void
DropClonedTriggersFromPartition(Oid partitionId)
{
	ScanKeyData skey;
	SysScanDesc scan;
	HeapTuple	trigtup;
	Relation	tgrel;
	ObjectAddresses *objects;

	objects = new_object_addresses();

	/*
	 * Scan pg_trigger to search for all triggers on this rel.
	 */
	ScanKeyInit(&skey, Anum_pg_trigger_tgrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(partitionId));
	tgrel = table_open(TriggerRelationId, RowExclusiveLock);
	scan = systable_beginscan(tgrel, TriggerRelidNameIndexId,
							  true, NULL, 1, &skey);
	while (HeapTupleIsValid(trigtup = systable_getnext(scan)))
	{
		Form_pg_trigger pg_trigger = (Form_pg_trigger) GETSTRUCT(trigtup);
		ObjectAddress trig;

		/* Ignore triggers that weren't cloned */
		if (!OidIsValid(pg_trigger->tgparentid))
			continue;

		/*
		 * This is ugly, but necessary: remove the dependency markings on the
		 * trigger so that it can be removed.
		 */
		deleteDependencyRecordsForClass(TriggerRelationId, pg_trigger->oid,
										TriggerRelationId,
										DEPENDENCY_PARTITION_PRI);
		deleteDependencyRecordsForClass(TriggerRelationId, pg_trigger->oid,
										RelationRelationId,
										DEPENDENCY_PARTITION_SEC);

		/* remember this trigger to remove it below */
		ObjectAddressSet(trig, TriggerRelationId, pg_trigger->oid);
		add_exact_object_address(&trig, objects);
	}

	/* make the dependency removal visible to the deletion below */
	CommandCounterIncrement();
	performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	/* done */
	free_object_addresses(objects);
	systable_endscan(scan);
	table_close(tgrel, RowExclusiveLock);
}

/*
 * Before acquiring lock on an index, acquire the same lock on the owning
 * table.
 */
struct AttachIndexCallbackState
{
	Oid			partitionOid;
	Oid			parentTblOid;
	bool		lockedParentTbl;
};

static void
RangeVarCallbackForAttachIndex(const RangeVar *rv, Oid relOid, Oid oldRelOid,
							   void *arg)
{
	struct AttachIndexCallbackState *state;
	Form_pg_class classform;
	HeapTuple	tuple;

	state = (struct AttachIndexCallbackState *) arg;

	if (!state->lockedParentTbl)
	{
		LockRelationOid(state->parentTblOid, AccessShareLock);
		state->lockedParentTbl = true;
	}

	/*
	 * If we previously locked some other heap, and the name we're looking up
	 * no longer refers to an index on that relation, release the now-useless
	 * lock.  XXX maybe we should do *after* we verify whether the index does
	 * not actually belong to the same relation ...
	 */
	if (relOid != oldRelOid && OidIsValid(state->partitionOid))
	{
		UnlockRelationOid(state->partitionOid, AccessShareLock);
		state->partitionOid = InvalidOid;
	}

	/* Didn't find a relation, so no need for locking or permission checks. */
	if (!OidIsValid(relOid))
		return;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
	if (!HeapTupleIsValid(tuple))
		return;					/* concurrently dropped, so nothing to do */
	classform = (Form_pg_class) GETSTRUCT(tuple);
	if (classform->relkind != RELKIND_PARTITIONED_INDEX &&
		classform->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("\"%s\" is not an index", rv->relname)));
	ReleaseSysCache(tuple);

	/*
	 * Since we need only examine the heap's tupledesc, an access share lock
	 * on it (preventing any DDL) is sufficient.
	 */
	state->partitionOid = IndexGetRelation(relOid, false);
	LockRelationOid(state->partitionOid, AccessShareLock);
}

/*
 * ALTER INDEX i1 ATTACH PARTITION i2
 */
static ObjectAddress
ATExecAttachPartitionIdx(List **wqueue, Relation parentIdx, RangeVar *name)
{
	Relation	partIdx;
	Relation	partTbl;
	Relation	parentTbl;
	ObjectAddress address;
	Oid			partIdxId;
	Oid			currParent;
	struct AttachIndexCallbackState state;

	/*
	 * We need to obtain lock on the index 'name' to modify it, but we also
	 * need to read its owning table's tuple descriptor -- so we need to lock
	 * both.  To avoid deadlocks, obtain lock on the table before doing so on
	 * the index.  Furthermore, we need to examine the parent table of the
	 * partition, so lock that one too.
	 */
	state.partitionOid = InvalidOid;
	state.parentTblOid = parentIdx->rd_index->indrelid;
	state.lockedParentTbl = false;
	partIdxId =
		RangeVarGetRelidExtended(name, AccessExclusiveLock, 0,
								 RangeVarCallbackForAttachIndex,
								 (void *) &state);
	/* Not there? */
	if (!OidIsValid(partIdxId))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("index \"%s\" does not exist", name->relname)));

	/* no deadlock risk: RangeVarGetRelidExtended already acquired the lock */
	partIdx = relation_open(partIdxId, AccessExclusiveLock);

	/* we already hold locks on both tables, so this is safe: */
	parentTbl = relation_open(parentIdx->rd_index->indrelid, AccessShareLock);
	partTbl = relation_open(partIdx->rd_index->indrelid, NoLock);

	ObjectAddressSet(address, RelationRelationId, RelationGetRelid(partIdx));

	/* Silently do nothing if already in the right state */
	currParent = partIdx->rd_rel->relispartition ?
		get_partition_parent(partIdxId, false) : InvalidOid;
	if (currParent != RelationGetRelid(parentIdx))
	{
		IndexInfo  *childInfo;
		IndexInfo  *parentInfo;
		AttrMap    *attmap;
		bool		found;
		int			i;
		PartitionDesc partDesc;
		Oid			constraintOid,
					cldConstrId = InvalidOid;

		/*
		 * If this partition already has an index attached, refuse the
		 * operation.
		 */
		refuseDupeIndexAttach(parentIdx, partIdx, partTbl);

		if (OidIsValid(currParent))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot attach index \"%s\" as a partition of index \"%s\"",
							RelationGetRelationName(partIdx),
							RelationGetRelationName(parentIdx)),
					 errdetail("Index \"%s\" is already attached to another index.",
							   RelationGetRelationName(partIdx))));

		/* Make sure it indexes a partition of the other index's table */
		partDesc = RelationGetPartitionDesc(parentTbl, true);
		found = false;
		for (i = 0; i < partDesc->nparts; i++)
		{
			if (partDesc->oids[i] == state.partitionOid)
			{
				found = true;
				break;
			}
		}
		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot attach index \"%s\" as a partition of index \"%s\"",
							RelationGetRelationName(partIdx),
							RelationGetRelationName(parentIdx)),
					 errdetail("Index \"%s\" is not an index on any partition of table \"%s\".",
							   RelationGetRelationName(partIdx),
							   RelationGetRelationName(parentTbl))));

		/* Ensure the indexes are compatible */
		childInfo = BuildIndexInfo(partIdx);
		parentInfo = BuildIndexInfo(parentIdx);
		attmap = build_attrmap_by_name(RelationGetDescr(partTbl),
									   RelationGetDescr(parentTbl));
		if (!CompareIndexInfo(childInfo, parentInfo,
							  partIdx->rd_indcollation,
							  parentIdx->rd_indcollation,
							  partIdx->rd_opfamily,
							  parentIdx->rd_opfamily,
							  attmap))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cannot attach index \"%s\" as a partition of index \"%s\"",
							RelationGetRelationName(partIdx),
							RelationGetRelationName(parentIdx)),
					 errdetail("The index definitions do not match.")));

		/*
		 * If there is a constraint in the parent, make sure there is one in
		 * the child too.
		 */
		constraintOid = get_relation_idx_constraint_oid(RelationGetRelid(parentTbl),
														RelationGetRelid(parentIdx));

		if (OidIsValid(constraintOid))
		{
			cldConstrId = get_relation_idx_constraint_oid(RelationGetRelid(partTbl),
														  partIdxId);
			if (!OidIsValid(cldConstrId))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("cannot attach index \"%s\" as a partition of index \"%s\"",
								RelationGetRelationName(partIdx),
								RelationGetRelationName(parentIdx)),
						 errdetail("The index \"%s\" belongs to a constraint in table \"%s\" but no constraint exists for index \"%s\".",
								   RelationGetRelationName(parentIdx),
								   RelationGetRelationName(parentTbl),
								   RelationGetRelationName(partIdx))));
		}

		/* All good -- do it */
		IndexSetParentIndex(partIdx, RelationGetRelid(parentIdx));
		if (OidIsValid(constraintOid))
			ConstraintSetParentConstraint(cldConstrId, constraintOid,
										  RelationGetRelid(partTbl));

		free_attrmap(attmap);

		validatePartitionedIndex(parentIdx, parentTbl);
	}

	relation_close(parentTbl, AccessShareLock);
	/* keep these locks till commit */
	relation_close(partTbl, NoLock);
	relation_close(partIdx, NoLock);

	return address;
}

/*
 * Verify whether the given partition already contains an index attached
 * to the given partitioned index.  If so, raise an error.
 */
static void
refuseDupeIndexAttach(Relation parentIdx, Relation partIdx, Relation partitionTbl)
{
	Oid			existingIdx;

	existingIdx = index_get_partition(partitionTbl,
									  RelationGetRelid(parentIdx));
	if (OidIsValid(existingIdx))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot attach index \"%s\" as a partition of index \"%s\"",
						RelationGetRelationName(partIdx),
						RelationGetRelationName(parentIdx)),
				 errdetail("Another index is already attached for partition \"%s\".",
						   RelationGetRelationName(partitionTbl))));
}

/*
 * Verify whether the set of attached partition indexes to a parent index on
 * a partitioned table is complete.  If it is, mark the parent index valid.
 *
 * This should be called each time a partition index is attached.
 */
static void
validatePartitionedIndex(Relation partedIdx, Relation partedTbl)
{
	Relation	inheritsRel;
	SysScanDesc scan;
	ScanKeyData key;
	int			tuples = 0;
	HeapTuple	inhTup;
	bool		updated = false;

	Assert(partedIdx->rd_rel->relkind == RELKIND_PARTITIONED_INDEX);

	/*
	 * Scan pg_inherits for this parent index.  Count each valid index we find
	 * (verifying the pg_index entry for each), and if we reach the total
	 * amount we expect, we can mark this parent index as valid.
	 */
	inheritsRel = table_open(InheritsRelationId, AccessShareLock);
	ScanKeyInit(&key, Anum_pg_inherits_inhparent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(partedIdx)));
	scan = systable_beginscan(inheritsRel, InheritsParentIndexId, true,
							  NULL, 1, &key);
	while ((inhTup = systable_getnext(scan)) != NULL)
	{
		Form_pg_inherits inhForm = (Form_pg_inherits) GETSTRUCT(inhTup);
		HeapTuple	indTup;
		Form_pg_index indexForm;

		indTup = SearchSysCache1(INDEXRELID,
								 ObjectIdGetDatum(inhForm->inhrelid));
		if (!HeapTupleIsValid(indTup))
			elog(ERROR, "cache lookup failed for index %u", inhForm->inhrelid);
		indexForm = (Form_pg_index) GETSTRUCT(indTup);
		if (indexForm->indisvalid)
			tuples += 1;
		ReleaseSysCache(indTup);
	}

	/* Done with pg_inherits */
	systable_endscan(scan);
	table_close(inheritsRel, AccessShareLock);

	/*
	 * If we found as many inherited indexes as the partitioned table has
	 * partitions, we're good; update pg_index to set indisvalid.
	 */
	if (tuples == RelationGetPartitionDesc(partedTbl, true)->nparts)
	{
		Relation	idxRel;
		HeapTuple	newtup;

		idxRel = table_open(IndexRelationId, RowExclusiveLock);

		newtup = heap_copytuple(partedIdx->rd_indextuple);
		((Form_pg_index) GETSTRUCT(newtup))->indisvalid = true;
		updated = true;

		CatalogTupleUpdate(idxRel, &partedIdx->rd_indextuple->t_self, newtup);

		table_close(idxRel, RowExclusiveLock);
	}

	/*
	 * If this index is in turn a partition of a larger index, validating it
	 * might cause the parent to become valid also.  Try that.
	 */
	if (updated && partedIdx->rd_rel->relispartition)
	{
		Oid			parentIdxId,
					parentTblId;
		Relation	parentIdx,
					parentTbl;

		/* make sure we see the validation we just did */
		CommandCounterIncrement();

		parentIdxId = get_partition_parent(RelationGetRelid(partedIdx), false);
		parentTblId = get_partition_parent(RelationGetRelid(partedTbl), false);
		parentIdx = relation_open(parentIdxId, AccessExclusiveLock);
		parentTbl = relation_open(parentTblId, AccessExclusiveLock);
		Assert(!parentIdx->rd_index->indisvalid);

		validatePartitionedIndex(parentIdx, parentTbl);

		relation_close(parentIdx, AccessExclusiveLock);
		relation_close(parentTbl, AccessExclusiveLock);
	}
}

/*
 * Return an OID list of constraints that reference the given relation
 * that are marked as having a parent constraints.
 */
static List *
GetParentedForeignKeyRefs(Relation partition)
{
	Relation	pg_constraint;
	HeapTuple	tuple;
	SysScanDesc scan;
	ScanKeyData key[2];
	List	   *constraints = NIL;

	/*
	 * If no indexes, or no columns are referenceable by FKs, we can avoid the
	 * scan.
	 */
	if (RelationGetIndexList(partition) == NIL ||
		bms_is_empty(RelationGetIndexAttrBitmap(partition,
												INDEX_ATTR_BITMAP_KEY)))
		return NIL;

	/* Search for constraints referencing this table */
	pg_constraint = table_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_constraint_confrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(partition)));
	ScanKeyInit(&key[1],
				Anum_pg_constraint_contype, BTEqualStrategyNumber,
				F_CHAREQ, CharGetDatum(CONSTRAINT_FOREIGN));

	/* XXX This is a seqscan, as we don't have a usable index */
	scan = systable_beginscan(pg_constraint, InvalidOid, true, NULL, 2, key);
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_constraint constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

		/*
		 * We only need to process constraints that are part of larger ones.
		 */
		if (!OidIsValid(constrForm->conparentid))
			continue;

		constraints = lappend_oid(constraints, constrForm->oid);
	}

	systable_endscan(scan);
	table_close(pg_constraint, AccessShareLock);

	return constraints;
}

/*
 * During DETACH PARTITION, verify that any foreign keys pointing to the
 * partitioned table would not become invalid.  An error is raised if any
 * referenced values exist.
 */
static void
ATDetachCheckNoForeignKeyRefs(Relation partition)
{
	List	   *constraints;
	ListCell   *cell;

	constraints = GetParentedForeignKeyRefs(partition);

	foreach(cell, constraints)
	{
		Oid			constrOid = lfirst_oid(cell);
		HeapTuple	tuple;
		Form_pg_constraint constrForm;
		Relation	rel;
		Trigger		trig;

		tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constrOid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for constraint %u", constrOid);
		constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

		Assert(OidIsValid(constrForm->conparentid));
		Assert(constrForm->confrelid == RelationGetRelid(partition));

		/* prevent data changes into the referencing table until commit */
		rel = table_open(constrForm->conrelid, ShareLock);

		MemSet(&trig, 0, sizeof(trig));
		trig.tgoid = InvalidOid;
		trig.tgname = NameStr(constrForm->conname);
		trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
		trig.tgisinternal = true;
		trig.tgconstrrelid = RelationGetRelid(partition);
		trig.tgconstrindid = constrForm->conindid;
		trig.tgconstraint = constrForm->oid;
		trig.tgdeferrable = false;
		trig.tginitdeferred = false;
		/* we needn't fill in remaining fields */

		RI_PartitionRemove_Check(&trig, rel, partition);

		ReleaseSysCache(tuple);

		table_close(rel, NoLock);
	}
}

/*
 * resolve column compression specification to compression method.
 */
static char
GetAttributeCompression(Oid atttypid, char *compression)
{
	char		cmethod;

	if (compression == NULL || strcmp(compression, "default") == 0)
		return InvalidCompressionMethod;

	/*
	 * To specify a nondefault method, the column data type must be toastable.
	 * Note this says nothing about whether the column's attstorage setting
	 * permits compression; we intentionally allow attstorage and
	 * attcompression to be independent.  But with a non-toastable type,
	 * attstorage could not be set to a value that would permit compression.
	 *
	 * We don't actually need to enforce this, since nothing bad would happen
	 * if attcompression were non-default; it would never be consulted.  But
	 * it seems more user-friendly to complain about a certainly-useless
	 * attempt to set the property.
	 */
	if (!TypeIsToastable(atttypid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("column data type %s does not support compression",
						format_type_be(atttypid))));

	cmethod = CompressionNameToMethod(compression);
	if (!CompressionMethodIsValid(cmethod))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid compression method \"%s\"", compression)));

	return cmethod;
}

static void
ATSetTags(Relation rel, List *tags, bool unset)
{
	Oid relid;
	
	relid = RelationGetRelid(rel);
	
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		return;

	if (!unset)
	{
		AlterTagDescriptions(tags,
							 MyDatabaseId,
							 RelationRelationId,
							 relid,
							 RelationGetRelationName(rel));
	}
	else
	{
		UnsetTagDescriptions(tags,
							 MyDatabaseId,
							 RelationRelationId,
							 relid,
							 RelationGetRelationName(rel));
	}
}
