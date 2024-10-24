/*-------------------------------------------------------------------------
 *
 * nodes.h
 *	  Definitions for tagged nodes.
 *
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/nodes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODES_H
#define NODES_H

/*
 * The first field of every node is NodeTag. Each node created (with makeNode)
 * will have one of the following tags as the value of its first field.
 *
 * Note that inserting or deleting node types changes the numbers of other
 * node types later in the list.  This is no problem during development, since
 * the node numbers are never stored on disk.  But don't do it in a released
 * branch, because that would represent an ABI break for extensions.
 */
typedef enum NodeTag
{
	T_Invalid = 0,

	/*
	 * TAGS FOR EXECUTOR NODES (execnodes.h)
	 */
	T_IndexInfo,
	T_ExprContext,
	T_ProjectionInfo,
	T_JunkFilter,
	T_OnConflictSetState,
	T_ResultRelInfo,
	T_EState,
	T_TupleTableSlot,
	T_CdbProcess,
	T_SliceTable,
	T_CursorPosInfo,
	T_PartitionState,
	T_QueryDispatchDesc,
	T_OidAssignment,

	/*
	 * TAGS FOR PLAN NODES (plannodes.h)
	 */
	T_Plan,
	T_Scan,
	T_Join,

	/* Real plan node starts below.  Scan and Join are "Virtual nodes",
	 * It will take the form of IndexScan, SeqScan, etc.
	 * CteScan will take the form of SubqueryScan.
	 */
	T_Result,
	T_Plan_Start = T_Result,
	T_ProjectSet,
	T_ModifyTable,
	T_Append,
	T_MergeAppend,
	T_RecursiveUnion,
	T_Sequence,
	T_BitmapAnd,
	T_BitmapOr,
	T_SeqScan,
	T_SampleScan,
	T_IndexScan,
	T_IndexOnlyScan,
	T_BitmapIndexScan,
	T_BitmapHeapScan,
	T_TidScan,
	T_TidRangeScan,
	T_SubqueryScan,
	T_FunctionScan,
	T_TableFunctionScan,
	T_ValuesScan,
	T_TableFuncScan,
	T_CteScan,
	T_NamedTuplestoreScan,
	T_WorkTableScan,
	T_ForeignScan,
	T_CustomScan,
	T_NestLoop,
	T_MergeJoin,
	T_HashJoin,
	T_Material,
	T_Memoize,
	T_Sort,
	T_IncrementalSort,
	T_Group,
	T_Agg,
	T_TupleSplit,
	T_WindowAgg,
	T_Unique,
	T_Gather,
	T_GatherMerge,
	T_Hash,
	T_RuntimeFilter,
	T_SetOp,
	T_LockRows,
	T_Limit,
	T_Motion,
	T_ShareInputScan,
	T_SplitUpdate,
	T_AssertOp,
	T_PartitionSelector,
	T_Plan_End,
	/* these aren't subclasses of Plan: */
	T_NestLoopParam,
	T_PlanRowMark,
	T_PartitionPruneInfo,
	T_PartitionedRelPruneInfo,
	T_PartitionPruneStepOp,
	T_PartitionPruneStepCombine,
	T_PlanInvalItem,

	/*
	 * TAGS FOR PLAN STATE NODES (execnodes.h)
	 *
	 * These should correspond one-to-one with Plan node types.
	 */
	T_PlanState,
	T_ScanState,
	T_JoinState,

	/* Real plan node starts below.  Scan and Join are "Virtal nodes",
	 * It will take the form of IndexScan, SeqScan, etc.
	 */
	T_ResultState,
	T_ProjectSetState,
	T_ModifyTableState,
	T_AppendState,
	T_MergeAppendState,
	T_RecursiveUnionState,
	T_SequenceState,
	T_BitmapAndState,
	T_BitmapOrState,
	T_SeqScanState,
	T_SampleScanState,
	T_IndexScanState,
	T_IndexOnlyScanState,
	T_BitmapIndexScanState,
	T_BitmapHeapScanState,
	T_TidScanState,
	T_TidRangeScanState,
	T_SubqueryScanState,
	T_FunctionScanState,
	T_TableFunctionState,
	T_TableFuncScanState,
	T_ValuesScanState,
	T_CteScanState,
	T_NamedTuplestoreScanState,
	T_WorkTableScanState,
	T_ForeignScanState,
	T_CustomScanState,
	T_NestLoopState,
	T_MergeJoinState,
	T_HashJoinState,
	T_MaterialState,
	T_MemoizeState,
	T_SortState,
	T_IncrementalSortState,
	T_GroupState,
	T_AggState,
	T_TupleSplitState,
	T_WindowAggState,
	T_UniqueState,
	T_GatherState,
	T_GatherMergeState,
	T_HashState,
	T_RuntimeFilterState,
	T_SetOpState,
	T_LockRowsState,
	T_LimitState,
	T_MotionState,
	T_ShareInputScanState,
	T_SplitUpdateState,
	T_AssertOpState,
	T_PartitionSelectorState,

	/*
	 * TupleDesc and ParamListInfo are not Nodes as such, but you can wrap
	 * them in TupleDescNode and SerializedParams structs for serialization.
	 */
	T_TupleDescNode,
	T_SerializedParams,

	/*
	 * TAGS FOR PRIMITIVE NODES (primnodes.h)
	 */
	T_Alias,
	T_RangeVar,
	T_TableFunc,
	T_Expr,
	T_Var,
	T_Const,
	T_Param,
	T_DQAExpr,
	T_Aggref,
	T_GroupingFunc,
	T_WindowFunc,
	T_SubscriptingRef,
	T_FuncExpr,
	T_NamedArgExpr,
	T_OpExpr,
	T_DistinctExpr,
	T_NullIfExpr,
	T_ScalarArrayOpExpr,
	T_BoolExpr,
	T_SubLink,
	T_SubPlan,
	T_AlternativeSubPlan,
	T_FieldSelect,
	T_FieldStore,
	T_RelabelType,
	T_CoerceViaIO,
	T_ArrayCoerceExpr,
	T_ConvertRowtypeExpr,
	T_CollateExpr,
	T_CaseExpr,
	T_CaseWhen,
	T_CaseTestExpr,
	T_ArrayExpr,
	T_RowExpr,
	T_RowCompareExpr,
	T_CoalesceExpr,
	T_MinMaxExpr,
	T_SQLValueFunction,
	T_XmlExpr,
	T_NullTest,
	T_BooleanTest,
	T_CoerceToDomain,
	T_CoerceToDomainValue,
	T_SetToDefault,
	T_CurrentOfExpr,
	T_NextValueExpr,
	T_InferenceElem,
	T_TargetEntry,
	T_RangeTblRef,
	T_JoinExpr,
	T_FromExpr,
	T_OnConflictExpr,
	T_IntoClause,
	T_CopyIntoClause,
	T_RefreshClause,
	T_Flow,
	T_GroupId,
	T_GroupingSetId,
	T_AggExprId,
	T_RowIdExpr,
	T_DistributedBy,
	T_DMLActionExpr,

	/*
	 * TAGS FOR EXPRESSION STATE NODES (execnodes.h)
	 *
	 * ExprState represents the evaluation state for a whole expression tree.
	 * Most Expr-based plan nodes do not have a corresponding expression state
	 * node, they're fully handled within execExpr* - but sometimes the state
	 * needs to be shared with other parts of the executor, as for example
	 * with SubPlanState, which nodeSubplan.c has to modify.
	 */
	T_ExprState,
	T_WindowFuncExprState,
	T_SetExprState,
	T_SubPlanState,
	T_DomainConstraintState,
	T_AggExprIdState,
	T_RowIdExprState,

	/*
	 * TAGS FOR PLANNER NODES (pathnodes.h)
	 */
	T_PlannerInfo,
	T_PlannerGlobal,
	T_RelOptInfo,
	T_IndexOptInfo,
	T_ForeignKeyOptInfo,
	T_ParamPathInfo,
	T_RelAggInfo,
	T_Path,
	T_AppendOnlyPath,
	T_AOCSPath,
	T_ExternalPath,
	T_CtePath,
	T_IndexPath,
	T_BitmapHeapPath,
	T_BitmapAndPath,
	T_BitmapOrPath,
	T_TidPath,
	T_TidRangePath,
	T_SubqueryScanPath,
	T_TableFunctionScanPath,
	T_ForeignPath,
	T_CustomPath,
	T_NestPath,
	T_MergePath,
	T_HashPath,
	T_RuntimeFilterPath,
	T_AppendPath,
	T_MergeAppendPath,
	T_GroupResultPath,
	T_MaterialPath,
	T_MemoizePath,
	T_UniquePath,
	T_GatherPath,
	T_GatherMergePath,
	T_ProjectionPath,
	T_ProjectSetPath,
	T_SortPath,
	T_IncrementalSortPath,
	T_GroupPath,
	T_UpperUniquePath,
	T_AggPath,
	T_GroupingSetsPath,
	T_MinMaxAggPath,
	T_WindowAggPath,
	T_TupleSplitPath,
	T_SetOpPath,
	T_RecursiveUnionPath,
	T_LockRowsPath,
	T_ModifyTablePath,
	T_LimitPath,
	/* these aren't subclasses of Path: */
	T_EquivalenceClass,
	T_EquivalenceMember,
	T_PathKey,
	T_PathTarget,
	T_RestrictInfo,
	T_IndexClause,
	T_PlaceHolderVar,
	T_SpecialJoinInfo,
	T_AppendRelInfo,
	T_RowIdentityVarInfo,
	T_PlaceHolderInfo,
	T_GroupedVarInfo,
	T_MinMaxAggInfo,
	T_SegfileMapNode,
	T_PlannerParamItem,
	T_RollupData,
	T_GroupingSetData,
	T_StatisticExtInfo,

    /* Tags for MPP planner nodes (relation.h) */
    T_CdbMotionPath = 580,
	T_PartitionSelectorPath,
	T_SplitUpdatePath,
    T_CdbRelColumnInfo,
	T_DistributionKey,

	/*
	 * TAGS FOR MEMORY NODES (memnodes.h)
	 */
	T_MemoryContext,
	T_AllocSetContext,
	T_SlabContext,
	T_GenerationContext,
	T_MemoryAccount,

	/*
	 * TAGS FOR VALUE NODES (value.h)
	 */
	T_Value,
	T_Integer,
	T_Float,
	T_String,
	T_BitString,
	T_Null,

	/*
	 * TAGS FOR LIST NODES (pg_list.h)
	 */
	T_List,
	T_IntList,
	T_OidList,

	/*
	 * TAGS FOR EXTENSIBLE NODES (extensible.h)
	 */
	T_ExtensibleNode,

	/*
	 * TAGS FOR STATEMENT NODES (mostly in parsenodes.h)
	 */
	T_RawStmt,
	T_Query,
	T_PlannedStmt,
	T_InsertStmt,
	T_DeleteStmt,
	T_UpdateStmt,
	T_SelectStmt,
	T_ReturnStmt,
	T_PLAssignStmt,
	T_AlterTableStmt,
	T_AlterTableCmd,
	T_AlterDomainStmt,
	T_SetOperationStmt,
	T_GrantStmt,
	T_GrantRoleStmt,
	T_AlterDefaultPrivilegesStmt,
	T_ClosePortalStmt,
	T_ClusterStmt,
	T_CopyStmt,
	T_CreateStmt,
	T_SingleRowErrorDesc,
	T_ExtTableTypeDesc,
	T_CreateExternalStmt,
	T_DefineStmt,
	T_DropStmt,
	T_TruncateStmt,
	T_CommentStmt,
	T_FetchStmt,
	T_IndexStmt,
	T_CreateFunctionStmt,
	T_AlterFunctionStmt,
	T_DoStmt,
	T_RenameStmt,
	T_RuleStmt,
	T_NotifyStmt,
	T_ListenStmt,
	T_UnlistenStmt,
	T_TransactionStmt,
	T_ViewStmt,
	T_LoadStmt,
	T_CreateDomainStmt,
	T_CreatedbStmt,
	T_DropdbStmt,
	T_VacuumStmt,
	T_ExplainStmt,
	T_CreateTableAsStmt,
	T_CreateSeqStmt,
	T_AlterSeqStmt,
	T_VariableSetStmt,
	T_VariableShowStmt,
	T_DiscardStmt,
	T_CreateTrigStmt,
	T_CreatePLangStmt,
	T_CreateRoleStmt,
	T_AlterRoleStmt,
	T_DropRoleStmt,
	T_CreateProfileStmt,
	T_AlterProfileStmt,
	T_DropProfileStmt,
	T_CreateQueueStmt,
	T_AlterQueueStmt,
	T_DropQueueStmt,
	T_CreateResourceGroupStmt,
	T_DropResourceGroupStmt,
	T_AlterResourceGroupStmt,
	T_LockStmt,
	T_ConstraintsSetStmt,
	T_ReindexStmt,
	T_CheckPointStmt,
	T_CreateSchemaStmt,
	T_AlterSchemaStmt,
	T_CreateTagStmt,
	T_AlterTagStmt,
	T_DropTagStmt,
	T_AlterDatabaseStmt,
	T_AlterDatabaseSetStmt,
	T_AlterRoleSetStmt,
	T_CreateConversionStmt,
	T_CreateCastStmt,
	T_CreateOpClassStmt,
	T_CreateOpFamilyStmt,
	T_AlterOpFamilyStmt,
	T_PrepareStmt,
	T_ExecuteStmt,
	T_DeallocateStmt,
	T_DeclareCursorStmt,
	T_CreateTableSpaceStmt,
	T_DropTableSpaceStmt,
	T_AlterObjectDependsStmt,
	T_AlterObjectSchemaStmt,
	T_AlterOwnerStmt,
	T_AlterOperatorStmt,
	T_AlterTypeStmt,
	T_DropOwnedStmt,
	T_ReassignOwnedStmt,
	T_CompositeTypeStmt,
	T_CreateEnumStmt,
	T_CreateRangeStmt,
	T_AlterEnumStmt,
	T_AlterTSDictionaryStmt,
	T_AlterTSConfigurationStmt,
	T_CreateFdwStmt,
	T_AlterFdwStmt,
	T_CreateForeignServerStmt,
	T_AlterForeignServerStmt,
	T_CreateStorageServerStmt,
	T_AlterStorageServerStmt,
	T_DropStorageServerStmt,
	T_CreateUserMappingStmt,
	T_AlterUserMappingStmt,
	T_DropUserMappingStmt,
	T_CreateStorageUserMappingStmt,
	T_AlterStorageUserMappingStmt,
	T_DropStorageUserMappingStmt,
	T_AlterTableSpaceOptionsStmt,
	T_AlterTableMoveAllStmt,
	T_SecLabelStmt,
	T_CreateForeignTableStmt,
	T_ImportForeignSchemaStmt,
	T_CreateExtensionStmt,
	T_AlterExtensionStmt,
	T_AlterExtensionContentsStmt,
	T_CreateEventTrigStmt,
	T_AlterEventTrigStmt,
	T_RefreshMatViewStmt,
	T_ReplicaIdentityStmt,
	T_AlterSystemStmt,
	T_CreatePolicyStmt,
	T_AlterPolicyStmt,
	T_CreateTransformStmt,
	T_CreateAmStmt,
	T_CreatePublicationStmt,
	T_AlterPublicationStmt,
	T_CreateSubscriptionStmt,
	T_AlterSubscriptionStmt,
	T_DropSubscriptionStmt,
	T_CreateStatsStmt,
	T_AlterCollationStmt,
	T_CallStmt,
	T_AlterStatsStmt,
	T_CreateTaskStmt,
	T_AlterTaskStmt,
	T_DropTaskStmt,

	/* GPDB additions */
	T_PartitionBy,
	T_PartitionRangeItem,
	T_PartitionValuesSpec,
	T_CreateDirectoryTableStmt,
	T_AlterDirectoryTableStmt,
	T_DropDirectoryTableStmt,
	T_CreateFileSpaceStmt,
	T_FileSpaceEntry,
	T_DropFileSpaceStmt,
	T_TableValueExpr,
	T_DenyLoginInterval,
	T_DenyLoginPoint,
	T_AlteredTableInfo,
	T_NewConstraint,
	T_NewColumnValue,
	T_GpPartitionDefinition,
	T_GpPartDefElem,
	T_GpPartitionRangeItem,
	T_GpPartitionRangeSpec,
	T_GpPartitionListSpec,
	T_GpAlterPartitionId,
	T_GpDropPartitionCmd,
	T_GpSplitPartitionCmd,
	T_GpAlterPartitionCmd,
	T_CreateWarehouseStmt,
	T_DropWarehouseStmt,
	T_AddForeignSegStmt,

	/*
	 * TAGS FOR PARSE TREE NODES (parsenodes.h)
	 */
	T_A_Expr,
	T_ColumnRef,
	T_ParamRef,
	T_A_Const,
	T_FuncCall,
	T_A_Star,
	T_A_Indices,
	T_A_Indirection,
	T_A_ArrayExpr,
	T_ResTarget,
	T_MultiAssignRef,
	T_TypeCast,
	T_CollateClause,
	T_SortBy,
	T_WindowDef,
	T_RangeSubselect,
	T_RangeFunction,
	T_RangeTableSample,
	T_RangeTableFunc,
	T_RangeTableFuncCol,
	T_TypeName,
	T_ColumnDef,
	T_IndexElem,
	T_StatsElem,
	T_Constraint,
	T_DefElem,
	T_RangeTblEntry,
	T_RangeTblFunction,
	T_TableSampleClause,
	T_WithCheckOption,
	T_SortGroupClause,
	T_GroupingSet,
	T_WindowClause,
	T_ObjectWithArgs,
	T_AccessPriv,
	T_CreateOpClassItem,
	T_TableLikeClause,
	T_FunctionParameter,
	T_LockingClause,
	T_RowMarkClause,
	T_XmlSerialize,
	T_WithClause,
	T_InferClause,
	T_OnConflictClause,
	T_CTESearchClause,
	T_CTECycleClause,
	T_CommonTableExpr,
	T_ColumnReferenceStorageDirective,
	T_DistributionKeyElem,
	T_RoleSpec,
	T_TriggerTransition,
	T_PartitionElem,
	T_PartitionSpec,
	T_PartitionBoundSpec,
	T_PartitionRangeDatum,
	T_PartitionCmd,
	T_VacuumRelation,

	/*
	 * TAGS FOR REPLICATION GRAMMAR PARSE NODES (replnodes.h)
	 */
	T_IdentifySystemCmd,
	T_BaseBackupCmd,
	T_CreateReplicationSlotCmd,
	T_DropReplicationSlotCmd,
	T_StartReplicationCmd,
	T_TimeLineHistoryCmd,
	T_SQLCmd,

	/*
	 * TAGS FOR RANDOM OTHER STUFF
	 *
	 * These are objects that aren't part of parse/plan/execute node tree
	 * structures, but we give them NodeTags anyway for identification
	 * purposes (usually because they are involved in APIs where we want to
	 * pass multiple object types through the same pointer).
	 */
	T_TriggerData,				/* in commands/trigger.h */
	T_EventTriggerData,			/* in commands/event_trigger.h */
	T_ReturnSetInfo,			/* in nodes/execnodes.h */
	T_WindowObjectData,			/* private in nodeWindowAgg.c */
	T_TIDBitmap,				/* in nodes/tidbitmap.h */
	T_InlineCodeBlock,			/* in nodes/parsenodes.h */
	T_FdwRoutine,				/* in foreign/fdwapi.h */
	T_IndexAmRoutine,			/* in access/amapi.h */
	T_TableAmRoutine,			/* in access/tableam.h */
	T_TsmRoutine,				/* in access/tsmapi.h */
	T_ForeignKeyCacheInfo,		/* in utils/rel.h */
	T_CallContext,				/* in nodes/parsenodes.h */
	T_SupportRequestSimplify,	/* in nodes/supportnodes.h */
	T_SupportRequestSelectivity,	/* in nodes/supportnodes.h */
	T_SupportRequestCost,		/* in nodes/supportnodes.h */
	T_SupportRequestRows,		/* in nodes/supportnodes.h */
	T_SupportRequestIndexCondition	/* in nodes/supportnodes.h */

	,
    T_StreamBitmap,             /* in nodes/tidbitmap.h */
	T_FormatterData,            /* in access/formatter.h */
	T_ExtProtocolData,          /* in access/extprotocol.h */
	T_ExtProtocolValidatorData, /* in access/extprotocol.h */
	T_ExternalScanInfo,			/* in access/plannodes.h */
	T_CookedConstraint,			/* in catalog/heap.h */

    /* CDB: tags for random other stuff */
    T_CdbExplain_StatHdr = 1000,             /* in cdb/cdbexplain.c */
	T_GpPolicy,					/* in catalog/gp_distribution_policy.h */
	T_RetrieveStmt,
	T_ReindexIndexInfo,			/* in nodes/parsenodes.h */
	T_EphemeralNamedRelationInfo, /* utils/queryenvironment.h */

} NodeTag;

/*
 * The first field of a node of any type is guaranteed to be the NodeTag.
 * Hence the type of any node can be gotten by casting it to Node. Declaring
 * a variable to be of Node * (instead of void *) can also facilitate
 * debugging.
 */
typedef struct Node
{
	NodeTag		type;
} Node;

#define nodeTag(nodeptr)		(((const Node*)(nodeptr))->type)

/*
 * newNode -
 *	  create a new node of the specified size and tag the node with the
 *	  specified tag.
 *
 * !WARNING!: Avoid using newNode directly. You should be using the
 *	  macro makeNode.  eg. to create a Query node, use makeNode(Query)
 *
 * Note: the size argument should always be a compile-time constant, so the
 * apparent risk of multiple evaluation doesn't matter in practice.
 */
#ifdef __GNUC__

/* With GCC, we can use a compound statement within an expression */
#define newNode(size, tag) \
({	Node   *_result; \
	AssertMacro((size) >= sizeof(Node));		/* need the tag, at least */ \
	_result = (Node *) palloc0fast(size); \
	_result->type = (tag); \
	_result; \
})
#else

/*
 *	There is no way to dereference the palloc'ed pointer to assign the
 *	tag, and also return the pointer itself, so we need a holder variable.
 *	Fortunately, this macro isn't recursive so we just define
 *	a global variable for this purpose.
 */
extern PGDLLIMPORT Node *newNodeMacroHolder;

#define newNode(size, tag) \
( \
	AssertMacro((size) >= sizeof(Node)),		/* need the tag, at least */ \
	newNodeMacroHolder = (Node *) palloc0fast(size), \
	newNodeMacroHolder->type = (tag), \
	newNodeMacroHolder \
)
#endif							/* __GNUC__ */


#define makeNode(_type_)		((_type_ *) newNode(sizeof(_type_),T_##_type_))
#define NodeSetTag(nodeptr,t)	(((Node*)(nodeptr))->type = (t))

#define IsA(nodeptr,_type_)		(nodeTag(nodeptr) == T_##_type_)

/*
 * castNode(type, ptr) casts ptr to "type *", and if assertions are enabled,
 * verifies that the node has the appropriate type (using its nodeTag()).
 *
 * Use an inline function when assertions are enabled, to avoid multiple
 * evaluations of the ptr argument (which could e.g. be a function call).
 */
#ifdef USE_ASSERT_CHECKING
static inline Node *
castNodeImpl(NodeTag type, void *ptr)
{
	Assert(ptr == NULL || nodeTag(ptr) == type);
	return (Node *) ptr;
}
#define castNode(_type_, nodeptr) ((_type_ *) castNodeImpl(T_##_type_, nodeptr))
#else
#define castNode(_type_, nodeptr) ((_type_ *) (nodeptr))
#endif							/* USE_ASSERT_CHECKING */


/* ----------------------------------------------------------------
 *					  extern declarations follow
 * ----------------------------------------------------------------
 */

/*
 * nodes/{outfuncs.c,print.c}
 */
struct Bitmapset;				/* not to include bitmapset.h here */
struct StringInfoData;			/* not to include stringinfo.h here */

extern void outNode(struct StringInfoData *str, const void *obj);
extern void outToken(struct StringInfoData *str, const char *s);
extern void outBitmapset(struct StringInfoData *str,
						 const struct Bitmapset *bms);
extern void outDatum(struct StringInfoData *str, uintptr_t value,
					 int typlen, bool typbyval);
extern char *nodeToString(const void *obj);
extern char *bmsToString(const struct Bitmapset *bms);

/*
 * nodes/outfast.c. This special version of nodeToString is only used by serializeNode.
 * It's a quick hack that allocates 8K buffer for StringInfo struct through initStringIinfoSizeOf
 */
extern char *nodeToBinaryStringFast(void *obj, int *length);

extern Node *readNodeFromBinaryString(const char *str, int len);

/*
 * nodes/{readfuncs.c,read.c}
 */
extern void save_strtok_states(const char ** save_ptr, const char ** save_begin);
extern void set_strtok_states(const char *ptr, const char *begin);
extern void *stringToNode(const char *str);
#ifdef WRITE_READ_PARSE_PLAN_TREES
extern void *stringToNodeWithLocations(const char *str);
#endif
extern struct Bitmapset *readBitmapset(void);
extern uintptr_t readDatum(bool typbyval);
extern bool *readBoolCols(int numCols);
extern int *readIntCols(int numCols);
extern Oid *readOidCols(int numCols);
extern int16 *readAttrNumberCols(int numCols);

/*
 * nodes/copyfuncs.c
 */
extern void *copyObjectImpl(const void *obj);

/* cast result back to argument type, if supported by compiler */
#ifdef HAVE_TYPEOF
#define copyObject(obj) ((typeof(obj)) copyObjectImpl(obj))
#else
#define copyObject(obj) copyObjectImpl(obj)
#endif

/*
 * nodes/equalfuncs.c
 */
extern bool equal(const void *a, const void *b);


/*
 * Typedefs for identifying qualifier selectivities and plan costs as such.
 * These are just plain "double"s, but declaring a variable as Selectivity
 * or Cost makes the intent more obvious.
 *
 * These could have gone into plannodes.h or some such, but many files
 * depend on them...
 */
typedef double Selectivity;		/* fraction of tuples a qualifier will pass */
typedef double Cost;			/* execution cost (in page-access units) */


/*
 * CmdType -
 *	  enums for type of operation represented by a Query or PlannedStmt
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum CmdType
{
	CMD_UNKNOWN,
	CMD_SELECT,					/* select stmt */
	CMD_UPDATE,					/* update stmt */
	CMD_INSERT,					/* insert stmt */
	CMD_DELETE,
	CMD_UTILITY,				/* cmds like create, destroy, copy, vacuum,
								 * etc. */
	CMD_NOTHING					/* dummy command for instead nothing rules
								 * with qual */
} CmdType;


/*
 * JoinType -
 *	  enums for types of relation joins
 *
 * JoinType determines the exact semantics of joining two relations using
 * a matching qualification.  For example, it tells what to do with a tuple
 * that has no match in the other relation.
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum JoinType
{
	/*
	 * The canonical kinds of joins according to the SQL JOIN syntax. Only
	 * these codes can appear in parser output (e.g., JoinExpr nodes).
	 */
	JOIN_INNER,					/* matching tuple pairs only */
	JOIN_LEFT,					/* pairs + unmatched LHS tuples */
	JOIN_FULL,					/* pairs + unmatched LHS + unmatched RHS */
	JOIN_RIGHT,					/* pairs + unmatched RHS tuples */

	/*
	 * Semijoins and anti-semijoins (as defined in relational theory) do not
	 * appear in the SQL JOIN syntax, but there are standard idioms for
	 * representing them (e.g., using EXISTS).  The planner recognizes these
	 * cases and converts them to joins.  So the planner and executor must
	 * support these codes.  NOTE: in JOIN_SEMI output, it is unspecified
	 * which matching RHS row is joined to.  In JOIN_ANTI output, the row is
	 * guaranteed to be null-extended.
	 */
	JOIN_SEMI,					/* 1 copy of each LHS row that has match(es) */
	JOIN_ANTI,					/* 1 copy of each LHS row that has no match */
	JOIN_LASJ_NOTIN,			/* Left Anti Semi Join with Not-In semantics:
									If any NULL values are produced by inner side,
									return no join results. Otherwise, same as LASJ */

	/*
	 * These codes are used internally in the planner, but are not supported
	 * by the executor (nor, indeed, by most of the planner).
	 */
	JOIN_UNIQUE_OUTER,			/* LHS path must be made unique */
	JOIN_UNIQUE_INNER,			/* RHS path must be made unique */

	/*
	 * GPDB: Like JOIN_UNIQUE_OUTER/INNER, these codes are used internally
	 * in the planner, but are not supported by the executor or by most of the
	 * planner. A JOIN_DEDUP_SEMI join indicates a semi-join, but to be
	 * implemented by performing a normal inner join, and eliminating the
	 * duplicates with a UniquePath above the join. That can be useful in
	 * an MPP environment, if performing the join as an inner join avoids
	 * moving the larger of the two relations.
	 */
	JOIN_DEDUP_SEMI,			/* inner join, LHS path must be made unique afterwards */
	JOIN_DEDUP_SEMI_REVERSE		/* inner join, RHS path must be made unique afterwards */

	/*
	 * We might need additional join types someday.
	 */
} JoinType;

/*
 * OUTER joins are those for which pushed-down quals must behave differently
 * from the join's own quals.  This is in fact everything except INNER and
 * SEMI joins.  However, this macro must also exclude the JOIN_UNIQUE symbols
 * since those are temporary proxies for what will eventually be an INNER
 * join.
 *
 * Note: semijoins are a hybrid case, but we choose to treat them as not
 * being outer joins.  This is okay principally because the SQL syntax makes
 * it impossible to have a pushed-down qual that refers to the inner relation
 * of a semijoin; so there is no strong need to distinguish join quals from
 * pushed-down quals.  This is convenient because for almost all purposes,
 * quals attached to a semijoin can be treated the same as innerjoin quals.
 */
#define IS_OUTER_JOIN(jointype) \
	(((1 << (jointype)) & \
	  ((1 << JOIN_LEFT) | \
	   (1 << JOIN_FULL) | \
	   (1 << JOIN_RIGHT) | \
	   (1 << JOIN_ANTI) | \
	   (1 << JOIN_LASJ_NOTIN))) != 0)

/*
 * AggStrategy -
 *	  overall execution strategies for Agg plan nodes
 *
 * This is needed in both pathnodes.h and plannodes.h, so put it here...
 */
typedef enum AggStrategy
{
	AGG_PLAIN,					/* simple agg across all input rows */
	AGG_SORTED,					/* grouped agg, input must be sorted */
	AGG_HASHED,					/* grouped agg, use internal hashtable */
	AGG_MIXED					/* grouped agg, hash and sort both used */
} AggStrategy;

/*
 * AggSplit -
 *	  splitting (partial aggregation) modes for Agg plan nodes
 *
 * This is needed in both pathnodes.h and plannodes.h, so put it here...
 */

/* Primitive options supported by nodeAgg.c: */
#define AGGSPLITOP_COMBINE		0x01	/* substitute combinefn for transfn */
#define AGGSPLITOP_SKIPFINAL	0x02	/* skip finalfn, return state as-is */
#define AGGSPLITOP_SERIALIZE	0x04	/* apply serialfn to output */
#define AGGSPLITOP_DESERIALIZE	0x08	/* apply deserialfn to input */

#define AGGSPLITOP_DEDUPLICATED	0x100

/* Supported operating modes (i.e., useful combinations of these options): */
typedef enum AggSplit
{
	/* Basic, non-split aggregation: */
	AGGSPLIT_SIMPLE = 0,
	/* Initial phase of partial aggregation, with serialization: */
	AGGSPLIT_INITIAL_SERIAL = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE,
	/* Final phase of partial aggregation, with deserialization: */
	AGGSPLIT_FINAL_DESERIAL = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE,

	/*
	 * The inputs have already been deduplicated for DISTINCT.
	 * This is internal to the planner, it is never set on Aggrefs, and is
	 * stripped away from Aggs in setrefs.c.
	 */
	AGGSPLIT_DEDUPLICATED = AGGSPLITOP_DEDUPLICATED,
} AggSplit;

/* Test whether an AggSplit value selects each primitive option: */
#define DO_AGGSPLIT_COMBINE(as)		(((as) & AGGSPLITOP_COMBINE) != 0)
#define DO_AGGSPLIT_SKIPFINAL(as)	(((as) & AGGSPLITOP_SKIPFINAL) != 0)
#define DO_AGGSPLIT_SERIALIZE(as)	(((as) & AGGSPLITOP_SERIALIZE) != 0)
#define DO_AGGSPLIT_DESERIALIZE(as) (((as) & AGGSPLITOP_DESERIALIZE) != 0)

#define DO_AGGSPLIT_DEDUPLICATED(as) (((as) & AGGSPLITOP_DEDUPLICATED) != 0)

/*
 * SetOpCmd and SetOpStrategy -
 *	  overall semantics and execution strategies for SetOp plan nodes
 *
 * This is needed in both pathnodes.h and plannodes.h, so put it here...
 */
typedef enum SetOpCmd
{
	SETOPCMD_INTERSECT,
	SETOPCMD_INTERSECT_ALL,
	SETOPCMD_EXCEPT,
	SETOPCMD_EXCEPT_ALL
} SetOpCmd;

typedef enum SetOpStrategy
{
	SETOP_SORTED,				/* input must be sorted */
	SETOP_HASHED				/* use internal hashtable */
} SetOpStrategy;

/*
 * OnConflictAction -
 *	  "ON CONFLICT" clause type of query
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum OnConflictAction
{
	ONCONFLICT_NONE,			/* No "ON CONFLICT" clause */
	ONCONFLICT_NOTHING,			/* ON CONFLICT ... DO NOTHING */
	ONCONFLICT_UPDATE			/* ON CONFLICT ... DO UPDATE */
} OnConflictAction;

/*
 * LimitOption -
 *	LIMIT option of query
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum LimitOption
{
	LIMIT_OPTION_COUNT,			/* FETCH FIRST... ONLY */
	LIMIT_OPTION_WITH_TIES,		/* FETCH FIRST... WITH TIES */
	LIMIT_OPTION_DEFAULT,		/* No limit present */
} LimitOption;

#endif							/* NODES_H */
