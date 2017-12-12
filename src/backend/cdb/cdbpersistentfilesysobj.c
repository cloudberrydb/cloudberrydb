/*-------------------------------------------------------------------------
 *
 * cdbpersistentfilesysobj.c
 *
 * Portions Copyright (c) 2009-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbpersistentfilesysobj.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "access/persistentfilesysobjname.h"
#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/gp_persistent.h"
#include "catalog/gp_segment_config.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentfilespace.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbmirroredbufferpool.h"
#include "cdb/cdbmirroredappendonly.h"
#include "cdb/cdbpersistentrecovery.h"
#include "cdb/cdbpersistentrelation.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbglobalsequence.h"
#include "postmaster/primary_mirror_mode.h"

#include "storage/itemptr.h"
#include "storage/lmgr.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"

typedef struct PersistentFileSysObjPrivateSharedMemory
{
	bool	needInitScan;
} PersistentFileSysObjPrivateSharedMemory;

typedef struct ReadTupleForUpdateInfo
{
	PersistentFsObjType 		fsObjType;
	PersistentFileSysObjName 	*fsObjName;
	ItemPointerData				persistentTid;
	int64						persistentSerialNum;
} ReadTupleForUpdateInfo;

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE \
	ErrorContextCallback readTupleForUpdateErrContext; \
	ReadTupleForUpdateInfo readTupleForUpdateInfo;

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(name, tid, serialNum) \
{ \
	readTupleForUpdateInfo.fsObjType = (name)->type; \
    readTupleForUpdateInfo.fsObjName = (name); \
	readTupleForUpdateInfo.persistentTid = *(tid); \
	readTupleForUpdateInfo.persistentSerialNum = serialNum; \
\
	/* Setup error traceback support for ereport() */ \
	readTupleForUpdateErrContext.callback = PersistentFileSysObj_ReadForUpdateErrContext; \
	readTupleForUpdateErrContext.arg = (void *) &readTupleForUpdateInfo; \
	readTupleForUpdateErrContext.previous = error_context_stack; \
	error_context_stack = &readTupleForUpdateErrContext; \
}

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHTYPE(type, tid, serialNum) \
{ \
	readTupleForUpdateInfo.fsObjType = (type); \
    readTupleForUpdateInfo.fsObjName = NULL; \
	readTupleForUpdateInfo.persistentTid = *(tid); \
	readTupleForUpdateInfo.persistentSerialNum = (serialNum); \
\
	/* Setup error traceback support for ereport() */ \
	readTupleForUpdateErrContext.callback = PersistentFileSysObj_ReadForUpdateErrContext; \
	readTupleForUpdateErrContext.arg = (void *) &readTupleForUpdateInfo; \
	readTupleForUpdateErrContext.previous = error_context_stack; \
	error_context_stack = &readTupleForUpdateErrContext; \
}


#define READTUPLE_FOR_UPDATE_ERRCONTEXT_POP \
{ \
	/* Pop the error context stack */ \
	error_context_stack = readTupleForUpdateErrContext.previous; \
}

static PersistentFileSysObjPrivateSharedMemory *persistentFileSysObjPrivateSharedData = NULL;

void PersistentFileSysObj_InitShared(
	PersistentFileSysObjSharedData 	*fileSysObjSharedData)
{
	PersistentStore_InitShared(&fileSysObjSharedData->storeSharedData);
}

static PersistentFileSysObjData* fileSysObjDataArray[CountPersistentFsObjType];

static PersistentFileSysObjSharedData* fileSysObjSharedDataArray[CountPersistentFsObjType];

static void PersistentFileSysObj_PrintRelationFile(
	int						elevel,
	char					*prefix,
	ItemPointer 			persistentTid,
	Datum					*values)
{
	RelFileNode		relFileNode;
	int32			segmentFileNum;

	PersistentFileSysRelStorageMgr relationStorageManager;

	PersistentFileSysState	state;

	int64			createMirrorDataLossTrackingSessionNum;

	MirroredObjectExistenceState		mirrorExistenceState;

	MirroredRelDataSynchronizationState mirrorDataSynchronizationState;
	bool							mirrorBufpoolMarkedForScanIncrementalResync;
	int64							mirrorBufpoolResyncChangedPageCount;
	XLogRecPtr						mirrorBufpoolResyncCkptLoc;
	BlockNumber 					mirrorBufpoolResyncCkptBlockNum;

	int64					mirrorAppendOnlyLossEof;
	int64					mirrorAppendOnlyNewEof;

	PersistentFileSysRelBufpoolKind relBufpoolKind;

	TransactionId			parentXid;
	int64					persistentSerialNum;

	GpPersistentRelationNode_GetValues(
									values,
									&relFileNode.spcNode,
									&relFileNode.dbNode,
									&relFileNode.relNode,
									&segmentFileNum,
									&relationStorageManager,
									&state,
									&createMirrorDataLossTrackingSessionNum,
									&mirrorExistenceState,
									&mirrorDataSynchronizationState,
									&mirrorBufpoolMarkedForScanIncrementalResync,
									&mirrorBufpoolResyncChangedPageCount,
									&mirrorBufpoolResyncCkptLoc,
									&mirrorBufpoolResyncCkptBlockNum,
									&mirrorAppendOnlyLossEof,
									&mirrorAppendOnlyNewEof,
									&relBufpoolKind,
									&parentXid,
									&persistentSerialNum);

	if (relationStorageManager == PersistentFileSysRelStorageMgr_BufferPool)
	{
		elog(elevel,
			 "%s gp_persistent_relation_node %s: %u/%u/%u, segment file #%d, relation storage manager '%s', persistent state '%s', create mirror data loss tracking session num " INT64_FORMAT ", "
			 "mirror existence state '%s', data synchronization state '%s', "
			 "Buffer Pool (marked for scan incremental resync = %s, "
			 "resync changed page count " INT64_FORMAT ", "
			 "resync checkpoint loc %s, resync checkpoint block num %u), "
			 "relation buffer pool kind %u, parent xid %u, "
			 "persistent serial num " INT64_FORMAT,
			 prefix,
			 ItemPointerToString(persistentTid),
			 relFileNode.spcNode,
			 relFileNode.dbNode,
			 relFileNode.relNode,
			 segmentFileNum,
			 PersistentFileSysRelStorageMgr_Name(relationStorageManager),
			 PersistentFileSysObjState_Name(state),
			 createMirrorDataLossTrackingSessionNum,
			 MirroredObjectExistenceState_Name(mirrorExistenceState),
			 MirroredRelDataSynchronizationState_Name(mirrorDataSynchronizationState),
			 (mirrorBufpoolMarkedForScanIncrementalResync ? "true" : "false"),
			 mirrorBufpoolResyncChangedPageCount,
			 XLogLocationToString(&mirrorBufpoolResyncCkptLoc),
			 (unsigned int)mirrorBufpoolResyncCkptBlockNum,
			 relBufpoolKind,
			 parentXid,
			 persistentSerialNum);
	}
	else
	{
		Assert(relationStorageManager == PersistentFileSysRelStorageMgr_AppendOnly);
		elog(elevel,
			 "%s gp_persistent_relation_node %s: %u/%u/%u, segment file #%d, relation storage manager '%s', persistent state '%s', create mirror data loss tracking session num " INT64_FORMAT ", "
			 "mirror existence state '%s', data synchronization state '%s', "
			 "Append-Only (loss EOF " INT64_FORMAT ", new EOF " INT64_FORMAT ", equal = %s), "
			 "relation buffer pool kind %u, parent xid %u, "
			 "persistent serial num " INT64_FORMAT ,
			 prefix,
			 ItemPointerToString(persistentTid),
			 relFileNode.spcNode,
			 relFileNode.dbNode,
			 relFileNode.relNode,
			 segmentFileNum,
			 PersistentFileSysRelStorageMgr_Name(relationStorageManager),
			 PersistentFileSysObjState_Name(state),
			 createMirrorDataLossTrackingSessionNum,
			 MirroredObjectExistenceState_Name(mirrorExistenceState),
			 MirroredRelDataSynchronizationState_Name(mirrorDataSynchronizationState),
			 mirrorAppendOnlyLossEof,
			 mirrorAppendOnlyNewEof,
			 ((mirrorAppendOnlyLossEof == mirrorAppendOnlyNewEof) ? "true" : "false"),
			 relBufpoolKind,
			 parentXid,
			 persistentSerialNum);
	}
}

static void PersistentFileSysObj_PrintDatabaseDir(
	int						elevel,
	char					*prefix,
	ItemPointer 			persistentTid,
	Datum					*values)
{
	DbDirNode		dbDirNode;

	PersistentFileSysState	state;

	int64			createMirrorDataLossTrackingSessionNum;

	MirroredObjectExistenceState		mirrorExistenceState;

	int32					reserved;
	TransactionId			parentXid;
	int64					persistentSerialNum;

	GpPersistentDatabaseNode_GetValues(
									values,
									&dbDirNode.tablespace,
									&dbDirNode.database,
									&state,
									&createMirrorDataLossTrackingSessionNum,
									&mirrorExistenceState,
									&reserved,
									&parentXid,
									&persistentSerialNum);

	elog(elevel,
		 "%s gp_persistent_database_node %s: %u/%u, persistent state '%s', create mirror data loss tracking session num " INT64_FORMAT ", "
		 "mirror existence state '%s', reserved %u, parent xid %u, "
		 "persistent serial num " INT64_FORMAT ,
		 prefix,
		 ItemPointerToString(persistentTid),
		 dbDirNode.tablespace,
		 dbDirNode.database,
		 PersistentFileSysObjState_Name(state),
		 createMirrorDataLossTrackingSessionNum,
		 MirroredObjectExistenceState_Name(mirrorExistenceState),
		 reserved,
		 parentXid,
		 persistentSerialNum);
}

static void PersistentFileSysObj_PrintTablespaceDir(
	int						elevel,
	char					*prefix,
	ItemPointer 			persistentTid,
	Datum					*values)
{
	Oid		filespaceOid;
	Oid		tablespaceOid;

	PersistentFileSysState	state;

	int64	createMirrorDataLossTrackingSessionNum;

	MirroredObjectExistenceState		mirrorExistenceState;

	int32					reserved;
	TransactionId			parentXid;
	int64					persistentSerialNum;

	GpPersistentTablespaceNode_GetValues(
									values,
									&filespaceOid,
									&tablespaceOid,
									&state,
									&createMirrorDataLossTrackingSessionNum,
									&mirrorExistenceState,
									&reserved,
									&parentXid,
									&persistentSerialNum);

	elog(elevel,
		 "%s gp_persistent_tablespace_node %s: tablespace %u (filespace %u), persistent state '%s', create mirror data loss tracking session num " INT64_FORMAT ", "
		 "mirror existence state '%s', reserved %u, parent xid %u, "
		 "persistent serial num " INT64_FORMAT ,
		 prefix,
		 ItemPointerToString(persistentTid),
		 tablespaceOid,
		 filespaceOid,
		 PersistentFileSysObjState_Name(state),
		 createMirrorDataLossTrackingSessionNum,
		 MirroredObjectExistenceState_Name(mirrorExistenceState),
		 reserved,
		 parentXid,
		 persistentSerialNum);
}

static void PersistentFileSysObj_PrintFilespaceDir(
	int						elevel,
	char					*prefix,
	ItemPointer 			persistentTid,
	Datum					*values)
{
	Oid		filespaceOid;

	int16	dbId1;
	char	locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen];

	int16	dbId2;
	char	locationBlankPadded2[FilespaceLocationBlankPaddedWithNullTermLen];

	PersistentFileSysState	state;

	int64	createMirrorDataLossTrackingSessionNum;

	MirroredObjectExistenceState	mirrorExistenceState;

	int32					reserved;
	TransactionId			parentXid;
	int64					persistentSerialNum;

	GpPersistentFilespaceNode_GetValues(
									values,
									&filespaceOid,
									&dbId1,
									locationBlankPadded1,
									&dbId2,
									locationBlankPadded2,
									&state,
									&createMirrorDataLossTrackingSessionNum,
									&mirrorExistenceState,
									&reserved,
									&parentXid,
									&persistentSerialNum);

	char *filespaceLocation1;
	char *filespaceLocation2;

	PersistentFilespace_ConvertBlankPaddedLocation(
											&filespaceLocation1,
											locationBlankPadded1,
											/* isPrimary */ false);	// Always say false so we don't error out here.
	PersistentFilespace_ConvertBlankPaddedLocation(
											&filespaceLocation2,
											locationBlankPadded2,
											/* isPrimary */ false);	// Always say false so we don't error out here.
	elog(elevel,
		 "%s gp_persistent_filespace_node %s: filespace %u, #1 (dbid %u, location '%s'), #2 (dbid %u, location '%s'), persistent state '%s', create mirror data loss tracking session num " INT64_FORMAT ", "
		 "mirror existence state '%s', reserved %u, parent xid %u, "
		 "persistent serial num " INT64_FORMAT ,
		 prefix,
		 ItemPointerToString(persistentTid),
		 filespaceOid,
		 dbId1,
		 (filespaceLocation1 == NULL ? "<empty>" : filespaceLocation1),
		 dbId2,
		 (filespaceLocation2 == NULL ? "<empty>" : filespaceLocation2),
		 PersistentFileSysObjState_Name(state),
		 createMirrorDataLossTrackingSessionNum,
		 MirroredObjectExistenceState_Name(mirrorExistenceState),
		 reserved,
		 parentXid,
		 persistentSerialNum);

	if (filespaceLocation1 != NULL)
		pfree(filespaceLocation1);
	if (filespaceLocation2 != NULL)
		pfree(filespaceLocation2);
}

static void PersistentFileSysObj_InitOurs(
	PersistentFileSysObjData 	*fileSysObjData,
	PersistentFileSysObjSharedData 	*fileSysObjSharedData,
	PersistentFsObjType			fsObjType,
	int 						attNumPersistentState,
	int 						attNumMirrorExistenceState,
	int 						attNumParentXid)
{
	if (fsObjType < PersistentFsObjType_First ||
		fsObjType > PersistentFsObjType_Last)
		elog(ERROR, "Persistent file-system object type %d out-of-range",
		     fsObjType);

	fileSysObjDataArray[fsObjType] = fileSysObjData;
	fileSysObjSharedDataArray[fsObjType] = fileSysObjSharedData;

	fileSysObjData->fsObjType = fsObjType;
	fileSysObjData->attNumPersistentState = attNumPersistentState;
	fileSysObjData->attNumMirrorExistenceState = attNumMirrorExistenceState;
	fileSysObjData->attNumParentXid = attNumParentXid;
}

void PersistentFileSysObj_Init(
	PersistentFileSysObjData 			*fileSysObjData,
	PersistentFileSysObjSharedData 		*fileSysObjSharedData,
	PersistentFsObjType					fsObjType,
	PersistentStoreScanTupleCallback	scanTupleCallback)
{
	PersistentStoreData *storeData = &fileSysObjData->storeData;

	fileSysObjData->fsObjType = fsObjType;

	switch (fsObjType)
	{
	case PersistentFsObjType_RelationFile:
		PersistentStore_Init(
						storeData,
						"gp_persistent_relation_node",
						GpGlobalSequence_PersistentRelation,
						DirectOpen_GpPersistentRelationNodeOpenShared,
						DirectOpen_GpPersistentRelationNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintRelationFile,
						Natts_gp_persistent_relation_node,
						Anum_gp_persistent_relation_node_persistent_serial_num);

		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_relation_node_persistent_state,
						Anum_gp_persistent_relation_node_mirror_existence_state,
						Anum_gp_persistent_relation_node_parent_xid);
		break;

	case PersistentFsObjType_DatabaseDir:
		PersistentStore_Init(
						storeData,
						"gp_persistent_database_node",
						GpGlobalSequence_PersistentDatabase,
						DirectOpen_GpPersistentDatabaseNodeOpenShared,
						DirectOpen_GpPersistentDatabaseNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintDatabaseDir,
						Natts_gp_persistent_database_node,
						Anum_gp_persistent_database_node_persistent_serial_num);
		
		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_database_node_persistent_state,
						Anum_gp_persistent_database_node_mirror_existence_state,
						Anum_gp_persistent_database_node_parent_xid);
		break;

	case PersistentFsObjType_TablespaceDir:
		PersistentStore_Init(
						storeData,
						"gp_persistent_tablespace_node",
						GpGlobalSequence_PersistentTablespace,
						DirectOpen_GpPersistentTableSpaceNodeOpenShared,
						DirectOpen_GpPersistentTableSpaceNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintTablespaceDir,
						Natts_gp_persistent_tablespace_node,
						Anum_gp_persistent_tablespace_node_persistent_serial_num);

		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_tablespace_node_persistent_state,
						Anum_gp_persistent_tablespace_node_mirror_existence_state,
						Anum_gp_persistent_tablespace_node_parent_xid);
		break;

	case PersistentFsObjType_FilespaceDir:
		PersistentStore_Init(
						storeData,
						"gp_persistent_filespace_node",
						GpGlobalSequence_PersistentFilespace,
						DirectOpen_GpPersistentFileSpaceNodeOpenShared,
						DirectOpen_GpPersistentFileSpaceNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintFilespaceDir,
						Natts_gp_persistent_filespace_node,
						Anum_gp_persistent_filespace_node_persistent_serial_num);
		
		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_filespace_node_persistent_state,
						Anum_gp_persistent_filespace_node_mirror_existence_state,
						Anum_gp_persistent_filespace_node_parent_xid);
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjType);
	}
}

/*
 * Reset the persistent shared-memory free list heads and shared-memory hash tables.
 */
void PersistentFileSysObj_Reset(void)
{
	PersistentFsObjType	fsObjType;

	for (fsObjType = PersistentFsObjType_First;
		 fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
	{
		PersistentFileSysObjData		 *fileSysObjData;
		PersistentFileSysObjSharedData  *fileSysObjSharedData;

		PersistentFileSysObj_GetDataPtrs(
									 fsObjType,
									 &fileSysObjData,
									 &fileSysObjSharedData);

		switch (fsObjType)
		{
		case PersistentFsObjType_RelationFile:
			PersistentRelation_Reset();
			break;

		case PersistentFsObjType_DatabaseDir:
			PersistentDatabase_Reset();
			break;

		case PersistentFsObjType_TablespaceDir:
			PersistentTablespace_Reset();
			break;

		case PersistentFsObjType_FilespaceDir:
			/*
			 * Filespace persistent table is not reset (PersistentFilespace_Reset(); is not called)
			 * since it cannot be re-built on segments.
			 * 'gp_filespace' table does not exist on the segments by design.
			 */
			break;

		default:
			elog(ERROR, "Unexpected persistent file-system object type: %d",
				 fsObjType);
		}
	}
}

void PersistentFileSysObj_GetDataPtrs(
	PersistentFsObjType				fsObjType,

	PersistentFileSysObjData		**fileSysObjData,

	PersistentFileSysObjSharedData	**fileSysObjSharedData)
{
	if (fsObjType < PersistentFsObjType_First ||
		fsObjType > PersistentFsObjType_Last)
		elog(PANIC, "Persistent file-system object type %d out-of-range",
		     fsObjType);

	*fileSysObjData = fileSysObjDataArray[fsObjType];
	if (*fileSysObjData == NULL)
		elog(PANIC, "Persistent file-system object type %d memory not initialized",
		     fsObjType);
	if ((*fileSysObjData)->fsObjType != fsObjType)
		elog(PANIC, "Persistent file-system data's type %d doesn't match expected %d",
		     (*fileSysObjData)->fsObjType, fsObjType);

	*fileSysObjSharedData = fileSysObjSharedDataArray[fsObjType];
	if (*fileSysObjSharedData == NULL)
		elog(PANIC, "Persistent file-system object type %d shared-memory not initialized",
		     fsObjType);
}

static void PersistentFileSysObj_DoInitScan(void)
{
	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData		*fileSysObjData;
	PersistentFileSysObjSharedData	*fileSysObjSharedData;

	Assert(persistentFileSysObjPrivateSharedData->needInitScan);

	for (fsObjType = PersistentFsObjType_First;
		 fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
	{
		 PersistentFileSysObj_GetDataPtrs(
									 fsObjType,
									 &fileSysObjData,
									 &fileSysObjSharedData);
		 PersistentStore_InitScanUnderLock(
						 &fileSysObjData->storeData,
						 &fileSysObjSharedData->storeSharedData);
	}

	 persistentFileSysObjPrivateSharedData->needInitScan = false;
}

void PersistentFileSysObj_BuildInitScan(void)
{
	if (persistentFileSysObjPrivateSharedData == NULL)
		elog(PANIC, "Persistent File-System Object shared-memory not initialized");

	if (!persistentFileSysObjPrivateSharedData->needInitScan)
	{
		// gp_persistent_build_db can be called multiple times.
		return;
	}

	PersistentFileSysObj_DoInitScan();
}

void PersistentFileSysObj_StartupInitScan(void)
{
	if (persistentFileSysObjPrivateSharedData == NULL)
		elog(PANIC, "Persistent File-System Object shared-memory not initialized");

	/*
	 * We only scan persistent meta-data from with Startup phase 1.
	 * Build is only done in special states.
	 */
	if (!persistentFileSysObjPrivateSharedData->needInitScan)
	{
		elog(PANIC, "We only scan persistent meta-data once during Startup Pass 1");
	}

	PersistentFileSysObj_DoInitScan();
}

void PersistentFileSysObj_VerifyInitScan(void)
{
	if (persistentFileSysObjPrivateSharedData == NULL)
		elog(PANIC, "Persistent File-System Object shared-memory not initialized");

	if (persistentFileSysObjPrivateSharedData->needInitScan)
	{
		elog(PANIC, "Persistent File-System Object meta-data not scanned into memory");
	}
}

void PersistentFileSysObj_FlushXLog(void)
{
	PersistentStore_FlushXLog();
}

void PersistentFileSysObj_Scan(
	PersistentFsObjType			fsObjType,

	PersistentStoreScanTupleCallback	scanTupleCallback)
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	PersistentStore_Scan(
					&(fileSysObjDataArray[fsObjType]->storeData),
					&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
					scanTupleCallback);

}

int64 PersistentFileSysObj_MyHighestSerialNum(
	PersistentFsObjType 	fsObjType)
{
	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	return PersistentStore_MyHighestSerialNum(
								&fileSysObjData->storeData);
}

int64 PersistentFileSysObj_CurrentMaxSerialNum(
	PersistentFsObjType 	fsObjType)
{
	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	return PersistentStore_CurrentMaxSerialNum(
								&fileSysObjSharedData->storeSharedData);
}

void PersistentFileSysObj_UpdateTuple(
	PersistentFsObjType		fsObjType,

	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */

	Datum 					*values,

	bool					flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	PersistentStore_UpdateTuple(
							&(fileSysObjDataArray[fsObjType]->storeData),
							&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
							persistentTid,
							values,
							flushToXLog);
}

void PersistentFileSysObj_ReplaceTuple(
	PersistentFsObjType		fsObjType,

	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */

	HeapTuple				tuple,

	Datum 					*newValues,

	bool					*replaces,

	bool					flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	PersistentStore_ReplaceTuple(
							&(fileSysObjDataArray[fsObjType]->storeData),
							&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
							persistentTid,
							tuple,
							newValues,
							replaces,
							flushToXLog);
}


void PersistentFileSysObj_ReadTuple(
	PersistentFsObjType			fsObjType,

	ItemPointer					readTid,

	Datum						*values,

	HeapTuple					*tupleCopy)
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	if (PersistentStore_IsZeroTid(readTid))
		elog(ERROR, "TID for fetch persistent '%s' tuple is invalid (0,0)",
			 PersistentFileSysObjName_TypeName(fsObjType));

	PersistentStore_ReadTuple(
						&(fileSysObjDataArray[fsObjType]->storeData),
						&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
						readTid,
						values,
						tupleCopy);
}

void PersistentFileSysObj_AddTuple(
	PersistentFsObjType			fsObjType,

	Datum						*values,

	bool						flushToXLog,
				/* When true, the XLOG record for this change will be flushed to disk. */

	ItemPointer 				persistentTid,
				/* TID of the stored tuple. */

	int64					*persistentSerialNum)
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	PersistentStore_AddTuple(
						&(fileSysObjDataArray[fsObjType]->storeData),
						&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
						values,
						flushToXLog,
						persistentTid,
						persistentSerialNum);
}

void PersistentFileSysObj_FreeTuple(
	PersistentFileSysObjData 		*fileSysObjData,

	PersistentFileSysObjSharedData 	*fileSysObjSharedData,

	PersistentFsObjType			fsObjType,

	ItemPointer 				persistentTid,
				/* TID of the stored tuple. */

	bool						flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */
{
	Datum *freeValues;

	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	freeValues = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	switch (fsObjType)
	{
	case PersistentFsObjType_RelationFile:
		{
			XLogRecPtr mirrorBufpoolResyncCkptLoc;

			MemSet(&mirrorBufpoolResyncCkptLoc, 0, sizeof(XLogRecPtr));
			GpPersistentRelationNode_SetDatumValues(
												freeValues,
												/* tablespaceOid */ 0,
												/* databaseOid */ 0,
												/* relFileNodeOid */ 0,
												/* segmentFileNum */ 0,
												PersistentFileSysRelStorageMgr_None,
												PersistentFileSysState_Free,
												/* createMirrorDataLossTrackingSessionNum */ 0,
												MirroredObjectExistenceState_None,
												MirroredRelDataSynchronizationState_None,
												/* mirrorBufpoolMarkedForScanIncrementalResync */ false,
												/* mirrorBufpoolResyncChangedPageCount */ 0,
												&mirrorBufpoolResyncCkptLoc,
												/* mirrorBufpoolResyncCkptBlockNum */ 0,
												/* mirrorAppendOnlyLossEof */ 0,
												/* mirrorAppendOnlyNewEof */ 0,
												PersistentFileSysRelBufpoolKind_None,
												InvalidTransactionId,
												/* persistentSerialNum */ 0);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		GpPersistentDatabaseNode_SetDatumValues(
											freeValues,
											/* tablespaceOid */ 0,
											/* databaseOid */ 0,
											PersistentFileSysState_Free,
											/* createMirrorDataLossTrackingSessionNum */ 0,
											MirroredObjectExistenceState_None,
											/* reserved */ 0,
											InvalidTransactionId,
											/* persistentSerialNum */ 0);
		break;

	case PersistentFsObjType_TablespaceDir:
		GpPersistentTablespaceNode_SetDatumValues(
											freeValues,
											/* filespaceOid */ 0,
											/* tablespaceOid */ 0,
											PersistentFileSysState_Free,
											/* createMirrorDataLossTrackingSessionNum */ 0,
											MirroredObjectExistenceState_None,
											/* reserved */ 0,
											InvalidTransactionId,
											/* persistentSerialNum */ 0);
		break;

	case PersistentFsObjType_FilespaceDir:
		{
			char	locationBlankFilled[FilespaceLocationBlankPaddedWithNullTermLen];

			MemSet(locationBlankFilled, ' ', FilespaceLocationBlankPaddedWithNullTermLen - 1);
			locationBlankFilled[FilespaceLocationBlankPaddedWithNullTermLen - 1] = '\0';

			GpPersistentFilespaceNode_SetDatumValues(
												freeValues,
												/* filespaceOid */ 0,
												/* dbId1 */ 0,
												locationBlankFilled,
												/* dbId2 */ 0,
												locationBlankFilled,
												PersistentFileSysState_Free,
												/* createMirrorDataLossTrackingSessionNum */ 0,
												MirroredObjectExistenceState_None,
												/* reserved */ 0,
												InvalidTransactionId,
												/* persistentSerialNum */ 0);
		}
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjType);
	}

	PersistentStore_FreeTuple(
						&fileSysObjData->storeData,
						&fileSysObjSharedData->storeSharedData,
						persistentTid,
						freeValues,
						flushToXLog);
}

static void
PersistentFileSysObj_ReadForUpdateErrContext(void *arg)
{
	ReadTupleForUpdateInfo *info = (ReadTupleForUpdateInfo*) arg;

	if (info->fsObjName != NULL)
	{
		errcontext(
			 "Reading tuple TID %s for possible update to persistent file-system object %s, persistent serial number " INT64_FORMAT,
			 ItemPointerToString(&info->persistentTid),
			 PersistentFileSysObjName_TypeAndObjectName(info->fsObjName),
			 info->persistentSerialNum);
	}
	else
	{
		errcontext(
			 "Reading tuple TID %s for possible update to persistent file-system object type %s, persistent serial number " INT64_FORMAT,
			 ItemPointerToString(&info->persistentTid),
			 PersistentFileSysObjName_TypeName(info->fsObjType),
			 info->persistentSerialNum);
	}
}

/*
 * errcontext_persistent_relation_state_change
 *
 * Add an errcontext() line showing the expected relation path, serial number,
 * expected state(s), next state, and persistent TID.
 */
static int
errcontext_persistent_relation_state_change(
	PersistentFileSysObjName	*fsObjName,

	ItemPointer 			persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64					serialNum,

	PersistentFileSysState	expectedState1,

	PersistentFileSysState	expectedState2,

	PersistentFileSysState	expectedState3,

	PersistentFileSysState	nextState)

{
	char states[100];

	if ((int)expectedState2 == -1)
		sprintf(states, "state '%s'",
		        PersistentFileSysObjState_Name(expectedState1));
	else if ((int)expectedState3 == -1)
		sprintf(states, "states '%s' or '%s'",
			    PersistentFileSysObjState_Name(expectedState1),
			    PersistentFileSysObjState_Name(expectedState2));
	else
		sprintf(states, "states '%s', '%s', or '%s'",
			    PersistentFileSysObjState_Name(expectedState1),
			    PersistentFileSysObjState_Name(expectedState2),
			    PersistentFileSysObjState_Name(expectedState3));

	errcontext(
		 "State change is for persistent '%s' to go from %s to state '%s' with serial number " INT64_FORMAT " at TID %s",
		 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
		 states,
		 PersistentFileSysObjState_Name(nextState),
		 serialNum,
		 ItemPointerToString(persistentTid));

	return 0;
}

static PersistentFileSysObjVerifyExpectedResult PersistentFileSysObj_VerifyExpected(
	PersistentFileSysObjData 		*fileSysObjData,

	PersistentFileSysObjSharedData 	*fileSysObjSharedData,

	PersistentFileSysObjName		*fsObjName,

	ItemPointer 					persistentTid,
								/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64							serialNum,

	Datum 							*values,

	PersistentFileSysState			expectedState1,

	PersistentFileSysState			expectedState2,

	PersistentFileSysState			expectedState3,

	PersistentFileSysState			nextState,

	bool							retryPossible,

	PersistentFileSysState			*oldState,

	MirroredObjectExistenceState	*currentMirrorExistenceState)
{
	PersistentFsObjType				fsObjType;

	PersistentFileSysObjName 		actualFsObjName;
	PersistentFileSysState			actualState;

	TransactionId				actualParentXid;
	int64						actualSerialNum;

	bool expectedStateMatch;

	int elevel;

	elevel = (gp_persistent_statechange_suppress_error ? WARNING : ERROR);

	fsObjType = fsObjName->type;

	if (Debug_persistent_print)
		(*fileSysObjData->storeData.printTupleCallback)(
									Persistent_DebugPrintLevel(),
									"VEFIFY EXPECTED",
									persistentTid,
									values);

	GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&actualFsObjName,
							&actualState,
							currentMirrorExistenceState,
							&actualParentXid,
							&actualSerialNum);

	if (oldState != NULL)
		*oldState = actualState;

	expectedStateMatch = (actualState == expectedState1);
	if (!expectedStateMatch && ((int)expectedState2 != -1))
		expectedStateMatch = (actualState == expectedState2);
	if (!expectedStateMatch && ((int)expectedState3 != -1))
		expectedStateMatch = (actualState == expectedState3);

	if (actualState == PersistentFileSysState_Free)
	{
		if ((nextState == PersistentFileSysState_AbortingCreate ||
			 nextState == PersistentFileSysState_DropPending)
			 &&
			 retryPossible)
		{
			return PersistentFileSysObjVerifyExpectedResult_DeleteUnnecessary;
		}

		ereport(elevel,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Did not expect to find a 'Free' entry"),
				 errcontext_persistent_relation_state_change(
								 						fsObjName,
								 						persistentTid,
								 						serialNum,
								 						expectedState1,
								 						expectedState2,
								 						expectedState3,
								 						nextState)));
		return PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed;
	}

	if (actualSerialNum != serialNum)
	{
		if ((nextState == PersistentFileSysState_AbortingCreate ||
			 nextState == PersistentFileSysState_DropPending)
			 &&
			 retryPossible)
		{
			return PersistentFileSysObjVerifyExpectedResult_DeleteUnnecessary;
		}

		ereport(elevel,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Found different serial number " INT64_FORMAT " than expected (persistent file-system object found is '%s', state '%s')",
				        actualSerialNum,
						PersistentFileSysObjName_TypeAndObjectName(&actualFsObjName),
				        PersistentFileSysObjState_Name(actualState)),
				 errcontext_persistent_relation_state_change(
								 						fsObjName,
								 						persistentTid,
								 						serialNum,
								 						expectedState1,
								 						expectedState2,
														expectedState3,
								 						nextState)));
		return PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed;
	}

	if (!expectedStateMatch)
	{
		if (actualState == nextState
			&&
			retryPossible)
		{
			return PersistentFileSysObjVerifyExpectedResult_StateChangeAlreadyDone;
		}

		ereport(elevel,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Found persistent file-system object in unexpected state '%s'",
						PersistentFileSysObjState_Name(actualState)),
				 errcontext_persistent_relation_state_change(
														fsObjName,
														persistentTid,
														serialNum,
														expectedState1,
														expectedState2,
														expectedState3,
														nextState)));
		return PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed;
	}

	return PersistentFileSysObjVerifyExpectedResult_StateChangeNeeded;
}

extern char *PersistentFileSysObjStateChangeResult_Name(
	PersistentFileSysObjStateChangeResult result)
{
	switch (result)
	{
	case PersistentFileSysObjStateChangeResult_DeleteUnnecessary:
		return "Delete Unnecessary";

	case PersistentFileSysObjStateChangeResult_StateChangeOk:
		return "State-Change OK";

	case PersistentFileSysObjStateChangeResult_ErrorSuppressed:
		return "Error Suppressed";

	default:
		return "Unknown";
	}
}

PersistentFileSysObjStateChangeResult PersistentFileSysObj_StateChange(
	PersistentFileSysObjName 	*fsObjName,

	ItemPointer					persistentTid,

	int64						persistentSerialNum,

	PersistentFileSysState		nextState,

	bool						retryPossible,

	bool						flushToXLog,

	PersistentFileSysState		*oldState,

	PersistentFileSysObjVerifiedActionCallback verifiedActionCallback)
{
	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;
	PersistentFileSysRelStorageMgr   relStorageMgr = PersistentFileSysRelStorageMgr_None;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysState	expectedState1;
	PersistentFileSysState	expectedState2 = -1;	// -1 means no second possibility.
	PersistentFileSysState	expectedState3 = -1;	// -1 means no third possibility.

	MirroredObjectExistenceState	currentMirrorExistenceState;
	MirroredObjectExistenceState	newMirrorExistenceState;

	PersistentFileSysObjVerifyExpectedResult verifyExpectedResult;

	PersistentFileSysState localOldState;

	bool badMirrorExistenceStateTransition;

	MirroredRelDataSynchronizationState newDataSynchronizationState = (MirroredRelDataSynchronizationState)-1;
	bool resetParentXid;

	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent '%s' state-change tuple is invalid (0,0)",
			 PersistentFileSysObjName_TypeName(fsObjName->type));

	if (persistentSerialNum == 0)
		elog(ERROR, "Persistent '%s' serial number for state-change is invalid (0)",
			 PersistentFileSysObjName_TypeName(fsObjName->type));

	if (oldState != NULL)
		*oldState = (PersistentFileSysState)-1;

	fsObjType = fsObjName->type;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(fsObjName, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	if (fsObjType == PersistentFsObjType_RelationFile)
		relStorageMgr =
		(PersistentFileSysRelStorageMgr)DatumGetInt16(values[Anum_gp_persistent_relation_node_relation_storage_manager
		- 1]);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	switch (nextState)
	{
	case PersistentFileSysState_CreatePending:
		Assert (fsObjType == PersistentFsObjType_RelationFile);

		expectedState1 = PersistentFileSysState_BulkLoadCreatePending;
		break;

	case PersistentFileSysState_Created:
		expectedState1 = PersistentFileSysState_CreatePending;

		if (fsObjType == PersistentFsObjType_DatabaseDir)
			expectedState2 = PersistentFileSysState_JustInTimeCreatePending;
		break;

	case PersistentFileSysState_DropPending:
		expectedState1 = PersistentFileSysState_Created;
		break;

	case PersistentFileSysState_AbortingCreate:
		expectedState1 = PersistentFileSysState_CreatePending;
		if (fsObjType == PersistentFsObjType_RelationFile)
		{
			expectedState2 = PersistentFileSysState_BulkLoadCreatePending;
		}
		break;

	case PersistentFileSysState_Free:
		expectedState1 = PersistentFileSysState_DropPending;
		expectedState2 = PersistentFileSysState_AbortingCreate;
		if (fsObjType == PersistentFsObjType_DatabaseDir)
			expectedState3 = PersistentFileSysState_JustInTimeCreatePending;
		break;

	default:
		elog(ERROR, "Unexpected persistent object next state: %d",
			 nextState);
        expectedState1 = MaxPersistentFileSysState; /* to appease optimized compilation */
	}

	verifyExpectedResult =
		PersistentFileSysObj_VerifyExpected(
									fileSysObjData,
									fileSysObjSharedData,
									fsObjName,
									persistentTid,
									persistentSerialNum,
									values,
									expectedState1,
									expectedState2,
									expectedState3,
									nextState,
									retryPossible,
									&localOldState,
									&currentMirrorExistenceState);
	if (oldState != NULL)
		*oldState = localOldState;

	if (verifiedActionCallback != NULL)
	{
		(*verifiedActionCallback)(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								verifyExpectedResult,
								relStorageMgr);
	}

	switch (verifyExpectedResult)
	{
	case PersistentFileSysObjVerifyExpectedResult_DeleteUnnecessary:
		return PersistentFileSysObjStateChangeResult_DeleteUnnecessary;

	case PersistentFileSysObjVerifyExpectedResult_StateChangeAlreadyDone:
		return PersistentFileSysObjStateChangeResult_StateChangeOk;

	case PersistentFileSysObjVerifyExpectedResult_StateChangeNeeded:
		break;

	case PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed:
		return PersistentFileSysObjStateChangeResult_ErrorSuppressed;

	default:
		elog(ERROR, "Unexpected persistent object verify expected result: %d",
			 verifyExpectedResult);
	}

	newMirrorExistenceState = currentMirrorExistenceState;	// Assume no change.
	badMirrorExistenceStateTransition = false;
	resetParentXid = false;	// Assume.

	switch (nextState)
	{
	case PersistentFileSysState_CreatePending:
		Assert (fsObjType == PersistentFsObjType_RelationFile);

		if (currentMirrorExistenceState == MirroredObjectExistenceState_NotMirrored ||
			currentMirrorExistenceState == MirroredObjectExistenceState_MirrorCreatePending ||
			currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownBeforeCreate ||
			currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownDuringCreate)
		{
			// Leave current mirror existence state alone.
		}
		else
		{
			badMirrorExistenceStateTransition = true;
		}

		// Keep parent XID since we are still in '*Create Pending' state.
		break;

	case PersistentFileSysState_Created:
		if (currentMirrorExistenceState == MirroredObjectExistenceState_MirrorCreatePending)
		{
			newMirrorExistenceState = MirroredObjectExistenceState_MirrorCreated;
		}
		else if (currentMirrorExistenceState == MirroredObjectExistenceState_NotMirrored ||
				 currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownBeforeCreate ||
				 currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownDuringCreate)
		{
			// Leave current mirror existence state alone.
		}
		else
		{
			badMirrorExistenceStateTransition = true;
		}

		resetParentXid = true;
		break;

	case PersistentFileSysState_DropPending:
		if (currentMirrorExistenceState == MirroredObjectExistenceState_MirrorCreated ||
			currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownDuringCreate)
		{
			newMirrorExistenceState = MirroredObjectExistenceState_MirrorDropPending;
		}
		else if (currentMirrorExistenceState == MirroredObjectExistenceState_NotMirrored ||
				 currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownBeforeCreate)
		{
			// Leave current mirror existence state alone.
		}
		else
		{
			badMirrorExistenceStateTransition = true;
		}

		if (fsObjType == PersistentFsObjType_RelationFile)
		{
			newDataSynchronizationState = MirroredRelDataSynchronizationState_None;
		}

		resetParentXid = true;
		break;

	case PersistentFileSysState_AbortingCreate:
		if (currentMirrorExistenceState == MirroredObjectExistenceState_MirrorCreatePending ||
			currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownDuringCreate)
		{
			newMirrorExistenceState = MirroredObjectExistenceState_MirrorDropPending;
		}
		else if (currentMirrorExistenceState == MirroredObjectExistenceState_NotMirrored ||
				 currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownBeforeCreate)
		{
			// Leave current mirror existence state alone.
		}
		else
		{
			badMirrorExistenceStateTransition = true;
		}

		if (fsObjType == PersistentFsObjType_RelationFile)
		{
			newDataSynchronizationState = MirroredRelDataSynchronizationState_None;
		}

		resetParentXid = true;
		break;

	case PersistentFileSysState_Free:
		if (currentMirrorExistenceState == MirroredObjectExistenceState_NotMirrored ||
			currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDownBeforeCreate ||
			currentMirrorExistenceState == MirroredObjectExistenceState_MirrorDropPending ||
			currentMirrorExistenceState == MirroredObjectExistenceState_OnlyMirrorDropRemains)
		{
			// These current mirror existence states are ok.
		}
		else
		{
			badMirrorExistenceStateTransition = true;
		}
		break;

	default:
		elog(ERROR, "Unexpected persistent object next state: %d",
			 nextState);
	}

	if (badMirrorExistenceStateTransition)
	{
		int elevel;

		elevel = (gp_persistent_statechange_suppress_error ? WARNING : ERROR);

		elog(elevel,
			 "In state transition of %s from current '%s' persistent state of to new '%s' persistent state "
			 " found unexpected current mirror existence state '%s' (persistent serial number " INT64_FORMAT ", TID %s)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
			 PersistentFileSysObjState_Name(localOldState),
			 PersistentFileSysObjState_Name(nextState),
			 MirroredObjectExistenceState_Name(currentMirrorExistenceState),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid));
		return PersistentFileSysObjStateChangeResult_ErrorSuppressed;
	}

	if (nextState != PersistentFileSysState_Free)
	{
		Datum *newValues;
		bool *replaces;

		newValues = (Datum*)palloc0(fileSysObjData->storeData.numAttributes * sizeof(Datum));
		replaces = (bool*)palloc0(fileSysObjData->storeData.numAttributes * sizeof(bool));

		replaces[fileSysObjData->attNumPersistentState - 1] = true;
		newValues[fileSysObjData->attNumPersistentState - 1] = Int16GetDatum(nextState);

		if (newMirrorExistenceState != currentMirrorExistenceState)
		{
			replaces[fileSysObjData->attNumMirrorExistenceState - 1] = true;
			newValues[fileSysObjData->attNumMirrorExistenceState - 1] = Int16GetDatum(newMirrorExistenceState);
		}

		if (newDataSynchronizationState != (MirroredRelDataSynchronizationState)-1)
		{
			Assert (fsObjType == PersistentFsObjType_RelationFile);

			replaces[Anum_gp_persistent_relation_node_mirror_data_synchronization_state - 1] = true;
			newValues[Anum_gp_persistent_relation_node_mirror_data_synchronization_state - 1] =
														 Int16GetDatum(newDataSynchronizationState);
		}

		if (resetParentXid)
		{
			replaces[fileSysObjData->attNumParentXid - 1] = true;
			newValues[fileSysObjData->attNumParentXid - 1] = Int32GetDatum(InvalidTransactionId);
		}

		PersistentFileSysObj_ReplaceTuple(
									fsObjType,
									persistentTid,
									tupleCopy,
									newValues,
									replaces,
									flushToXLog);

		pfree(newValues);
		pfree(replaces);
	}
	else
	{
		heap_freetuple(tupleCopy);

		PersistentFileSysObj_FreeTuple(
									fileSysObjData,
									fileSysObjSharedData,
									fsObjType,
									persistentTid,
									flushToXLog);
	}

	if (Debug_persistent_print)
	{
		PersistentFileSysObj_ReadTuple(
								fsObjType,
								persistentTid,
								values,
								&tupleCopy);

		(*fileSysObjData->storeData.printTupleCallback)(
									Persistent_DebugPrintLevel(),
									"STATE CHANGE",
									persistentTid,
									values);

		heap_freetuple(tupleCopy);
	}

	pfree(values);

	return PersistentFileSysObjStateChangeResult_StateChangeOk;
}

void
PersistentFileSysObj_RemoveSegment(PersistentFileSysObjName *fsObjName,
								   ItemPointer persistentTid,
								   int64 persistentSerialNum,
								   int16 dbid,
								   bool ismirror,
								   bool flushToXLog)
{
	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysObjName		actualFsObjName;
	PersistentFileSysState			state;

	MirroredObjectExistenceState	mirrorExistenceState;

	TransactionId					parentXid;
	int64							serialNum;

	fsObjType = fsObjName->type;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes *
							sizeof(Datum));

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(fsObjName, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	if (tupleCopy != NULL)
	{
		GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&actualFsObjName,
							&state,
							&mirrorExistenceState,
							&parentXid,
							&serialNum);

		Datum *newValues;
		bool *replaces;

		if (ismirror
			&&
			(state == PersistentFileSysState_DropPending ||
			 state == PersistentFileSysState_AbortingCreate)
			 &&
			 mirrorExistenceState == MirroredObjectExistenceState_OnlyMirrorDropRemains)
		{
			PersistentFileSysState oldState;

			pfree(values);
			values = NULL;
			heap_freetuple(tupleCopy);

			/*
			 * The primary enters this state when it has successfully dropped the primary object but
			 * was unable to drop the mirror object because the mirror was down.
			 *
			 * Since we are letting go of the mirror, we can safely delete this persistent object.
			 */
			if (Debug_persistent_print)
				elog(Persistent_DebugPrintLevel(),
					 "PersistentFileSysObj_RemoveSegment: Removing '%s' as result of removing mirror, persistent state '%s', mirror existence state '%s', serial number " INT64_FORMAT " at TID %s",
					 PersistentFileSysObjName_ObjectName(fsObjName),
					 PersistentFileSysObjState_Name(state),
					 MirroredObjectExistenceState_Name(mirrorExistenceState),
					 persistentSerialNum,
					 ItemPointerToString(persistentTid));

			PersistentFileSysObj_StateChange(
										fsObjName,
										persistentTid,
										persistentSerialNum,
										PersistentFileSysState_Free,
										/* retryPossible */ false,
										/* flushToXLog */ false,
										&oldState,
										/* verifiedActionCallback */ NULL);
			return;
		}

		newValues = (Datum*)palloc0(fileSysObjData->storeData.numAttributes *
									sizeof(Datum));
		replaces = (bool*)palloc0(fileSysObjData->storeData.numAttributes *
								  sizeof(bool));

		if (ismirror)
		{
			replaces[fileSysObjData->attNumMirrorExistenceState - 1] = true;
			newValues[fileSysObjData->attNumMirrorExistenceState - 1] =
				Int32GetDatum(MirroredObjectExistenceState_NotMirrored);
		}


		if (fsObjType == PersistentFsObjType_FilespaceDir)
		{
			int dbidattnum;
			int locattnum;
			int dbid_1_attnum = Anum_gp_persistent_filespace_node_db_id_1;
			char ep[FilespaceLocationBlankPaddedWithNullTermLen];

			MemSet(ep, ' ', FilespaceLocationBlankPaddedWithNullTermLen - 1);
			ep[FilespaceLocationBlankPaddedWithNullTermLen - 1] = '\0';

			if (DatumGetInt16(values[dbid_1_attnum - 1]) == dbid)
			{
				dbidattnum = Anum_gp_persistent_filespace_node_db_id_1;
				locattnum = Anum_gp_persistent_filespace_node_location_1;
			}
			else
			{
				dbidattnum = Anum_gp_persistent_filespace_node_db_id_2;
				locattnum = Anum_gp_persistent_filespace_node_location_2;
			}

			replaces[dbidattnum - 1] = true;
			newValues[dbidattnum - 1] = Int16GetDatum(0);

			/*
			 * Although ep is allocated on the stack, CStringGetTextDatum()
			 * duplicates this and puts the result in palloc() allocated memory.
			 */
			replaces[locattnum - 1] = true;
			newValues[locattnum - 1] = CStringGetTextDatum(ep);
		}

		PersistentFileSysObj_ReplaceTuple(
									fsObjType,
									persistentTid,
									tupleCopy,
									newValues,
									replaces,
									flushToXLog);

		pfree(newValues);
		pfree(replaces);
	}
	pfree(values);
}

void
PersistentFileSysObj_ActivateStandby(PersistentFileSysObjName *fsObjName,
								   ItemPointer persistentTid,
								   int64 persistentSerialNum,
								   int16 oldmaster,
								   int16 newmaster,
								   bool flushToXLog)
{
	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysObjName		actualFsObjName;
	PersistentFileSysState			state;

	MirroredObjectExistenceState	mirrorExistenceState;

	TransactionId					parentXid;
	int64							serialNum;

	fsObjType = fsObjName->type;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes *
							sizeof(Datum));

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(fsObjName, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;


	if (tupleCopy != NULL)
	{
		GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&actualFsObjName,
							&state,
							&mirrorExistenceState,
							&parentXid,
							&serialNum);

		Datum *newValues;
		bool *replaces;
		int olddbidattnum;
		int newdbidattnum;

		newValues = (Datum*)palloc0(fileSysObjData->storeData.numAttributes *
									sizeof(Datum));
		replaces = (bool*)palloc0(fileSysObjData->storeData.numAttributes *
								  sizeof(bool));

		replaces[fileSysObjData->attNumParentXid - 1] = true;
		newValues[fileSysObjData->attNumParentXid - 1] =
			Int32GetDatum(InvalidTransactionId);

		replaces[fileSysObjData->attNumMirrorExistenceState - 1] = true;
		newValues[fileSysObjData->attNumMirrorExistenceState - 1] =
			Int32GetDatum(MirroredObjectExistenceState_NotMirrored);


		if (fsObjType == PersistentFsObjType_FilespaceDir)
		{
			int locattnum;
			int newLocattnum;
			char ep[FilespaceLocationBlankPaddedWithNullTermLen];
			int dbid_1_attnum = Anum_gp_persistent_filespace_node_db_id_1;
			int dbid_2_attnum = Anum_gp_persistent_filespace_node_db_id_2;

			MemSet(ep, ' ', FilespaceLocationBlankPaddedWithNullTermLen - 1);
			ep[FilespaceLocationBlankPaddedWithNullTermLen - 1] = '\0';

			if (DatumGetInt16(values[dbid_1_attnum - 1]) == oldmaster)
			{
				olddbidattnum = dbid_1_attnum;
				newdbidattnum = dbid_2_attnum;
				locattnum = Anum_gp_persistent_filespace_node_location_1;
				newLocattnum = Anum_gp_persistent_filespace_node_location_2;
			}
			else if (DatumGetInt16(values[dbid_2_attnum - 1]) == oldmaster)
			{
				olddbidattnum = dbid_2_attnum;
				newdbidattnum = dbid_1_attnum;
				locattnum = Anum_gp_persistent_filespace_node_location_2;
				newLocattnum = Anum_gp_persistent_filespace_node_location_1;
			}
			else
			{
				Insist(false);
			}

			/*
			 * Although ep is allocated on the stack, CStringGetTextDatum()
			 * duplicates this and puts the result in palloc() allocated memory.
			 */

			/* Replace the new master filespace location with the standby value. */
			replaces[newLocattnum - 1] = true;
			newValues[newLocattnum - 1] = values[locattnum - 1];

			replaces[locattnum - 1] = true;
			newValues[locattnum - 1] = CStringGetTextDatum(ep);
			replaces[olddbidattnum - 1] = true;
			newValues[olddbidattnum - 1] = Int16GetDatum(InvalidDbid);
			replaces[newdbidattnum - 1] = true;
			newValues[newdbidattnum - 1] = Int16GetDatum(newmaster);
		}

		PersistentFileSysObj_ReplaceTuple(
									fsObjType,
									persistentTid,
									tupleCopy,
									newValues,
									replaces,
									flushToXLog);

		pfree(newValues);
		pfree(replaces);
	}
	pfree(values);
}

void
PersistentFileSysObj_AddMirror(PersistentFileSysObjName *fsObjName,
							   ItemPointer persistentTid,
							   int64 persistentSerialNum,
							   int16 pridbid,
							   int16 mirdbid,
							   void *arg,
							   bool set_mirror_existence,
							   bool	flushToXLog)
{
	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysObjName		actualFsObjName;
	PersistentFileSysState			state;

	MirroredObjectExistenceState	mirrorExistenceState;

	TransactionId					parentXid;
	int64							serialNum;

	fsObjType = fsObjName->type;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(fsObjName, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	if (tupleCopy != NULL)
	{
		GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&actualFsObjName,
							&state,
							&mirrorExistenceState,
							&parentXid,
							&serialNum);

		Datum *newValues;
		bool *replaces;
		bool needs_update = false;

		newValues = (Datum*)palloc0(fileSysObjData->storeData.numAttributes *
									sizeof(Datum));
		replaces = (bool*)palloc0(fileSysObjData->storeData.numAttributes *
								  sizeof(bool));

		if (set_mirror_existence)
		{
			replaces[fileSysObjData->attNumMirrorExistenceState - 1] = true;
			newValues[fileSysObjData->attNumMirrorExistenceState - 1] =
				Int32GetDatum(MirroredObjectExistenceState_MirrorDownBeforeCreate);
			needs_update = true;
		}

		if (fsObjType == PersistentFsObjType_FilespaceDir)
		{
			int dbidattnum;
			int locattnum;
			int dbid_1_attnum = Anum_gp_persistent_filespace_node_db_id_1;

			if (DatumGetInt16(values[dbid_1_attnum - 1]) == pridbid)
			{
				dbidattnum = Anum_gp_persistent_filespace_node_db_id_2;
				locattnum = Anum_gp_persistent_filespace_node_location_2;
			}
			else
			{
				dbidattnum = Anum_gp_persistent_filespace_node_db_id_1;
				locattnum = Anum_gp_persistent_filespace_node_location_1;
			}

			replaces[dbidattnum - 1] = true;
			newValues[dbidattnum - 1] = Int16GetDatum(mirdbid);

			/* arg must be of the fixed filespace location length */
			Assert(strlen((char *)arg) == FilespaceLocationBlankPaddedWithNullTermLen - 1);

			replaces[locattnum - 1] = true;
			newValues[locattnum - 1] = CStringGetTextDatum((char *)arg);

			needs_update = true;
		}

		/* only replace the tuple if we've changed it */
		if (needs_update)
		{
			PersistentFileSysObj_ReplaceTuple(fsObjType,
											  persistentTid,
											  tupleCopy,
											  newValues,
											  replaces,
											  flushToXLog);
		}
		else
		{
			heap_freetuple(tupleCopy);
		}

		pfree(newValues);
		pfree(replaces);
	}
	pfree(values);
}

void PersistentFileSysObj_RepairDelete(
	PersistentFsObjType			fsObjType,
	ItemPointer					persistentTid)
{
	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent repair delete tuple is invalid (0,0)");

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	/*
	 * There used to be a check here to perform ReadTuple and then
	 * validate if its free then error out. Removed the check as now
	 * simple_heap_delete can error out if trying to delete non
	 * existent or invisible tuple.
	 */
	PersistentFileSysObj_FreeTuple(
								fileSysObjData,
								fileSysObjSharedData,
								fsObjType,
								persistentTid,
								/* flushToXLog */ true);
}

void PersistentFileSysObj_Created(
	PersistentFileSysObjName 	*fsObjName,
	ItemPointer				persistentTid,
	int64					persistentSerialNum,
	bool					retryPossible)
{
	if (!Persistent_BeforePersistenceWork())
	{
		if (PersistentStore_IsZeroTid(persistentTid))
			elog(ERROR, "TID for persistent '%s' tuple for created is invalid (0,0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));

		if (persistentSerialNum == 0)
			elog(ERROR, "Persistent '%s' serial number for created is invalid (0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));
	}

	switch (fsObjName->type)
	{
	case PersistentFsObjType_RelationFile:
		{
			RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
			int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

			PersistentRelation_Created(
								relFileNode,
								segmentFileNum,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		PersistentDatabase_Created(
							&fsObjName->variant.dbDirNode,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_TablespaceDir:
		PersistentTablespace_Created(
							fsObjName->variant.tablespaceOid,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_FilespaceDir:
		PersistentFilespace_Created(
							fsObjName->variant.filespaceOid,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;


	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjName->type);
	}
}

PersistentFileSysObjStateChangeResult PersistentFileSysObj_MarkAbortingCreate(
	PersistentFileSysObjName 	*fsObjName,
	ItemPointer				persistentTid,
	int64					persistentSerialNum,
	bool					retryPossible)
{
	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (!Persistent_BeforePersistenceWork())
	{
		if (PersistentStore_IsZeroTid(persistentTid))
			elog(ERROR, "TID for persistent '%s' tuple for mark aborting CREATE is invalid (0,0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));

		if (persistentSerialNum == 0)
			elog(ERROR, "Persistent '%s' serial number for mark aborting CREATE is invalid (0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));
	}

	switch (fsObjName->type)
	{
	case PersistentFsObjType_RelationFile:
		{
			RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
			int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

			stateChangeResult =
				PersistentRelation_MarkAbortingCreate(
								relFileNode,
								segmentFileNum,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		stateChangeResult =
			PersistentDatabase_MarkAbortingCreate(
							&fsObjName->variant.dbDirNode,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_TablespaceDir:
		stateChangeResult =
			PersistentTablespace_MarkAbortingCreate(
							fsObjName->variant.tablespaceOid,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_FilespaceDir:
		stateChangeResult =
			PersistentFilespace_MarkAbortingCreate(
							fsObjName->variant.filespaceOid,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjName->type);
		stateChangeResult = PersistentFileSysObjStateChangeResult_None;
	}

	return stateChangeResult;
}

PersistentFileSysObjStateChangeResult PersistentFileSysObj_MarkDropPending(
	PersistentFileSysObjName 	*fsObjName,
	ItemPointer				persistentTid,
	int64					persistentSerialNum,
	bool					retryPossible)
{
	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (!Persistent_BeforePersistenceWork())
	{
		if (PersistentStore_IsZeroTid(persistentTid))
			elog(ERROR, "TID for persistent '%s' tuple for mark DROP pending is invalid (0,0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));

		if (persistentSerialNum == 0)
			elog(ERROR, "Persistent '%s' serial number for mark DROP pending is invalid (0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));
	}

	switch (fsObjName->type)
	{
	case PersistentFsObjType_RelationFile:
		{
			RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
			int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

			stateChangeResult =
				PersistentRelation_MarkDropPending(
								relFileNode,
								segmentFileNum,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		stateChangeResult =
			PersistentDatabase_MarkDropPending(
							&fsObjName->variant.dbDirNode,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_TablespaceDir:
		stateChangeResult =
			PersistentTablespace_MarkDropPending(
							fsObjName->variant.tablespaceOid,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_FilespaceDir:
		stateChangeResult =
			PersistentFilespace_MarkDropPending(
							fsObjName->variant.tablespaceOid,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjName->type);
		stateChangeResult = PersistentFileSysObjStateChangeResult_None;
	}

	return stateChangeResult;
}

/*
 * Error context callback for errors occurring during PersistentFileSysObj_DropObject().
 */
static void
PersistentFileSysObj_DropObjectErrorCallback(void *arg)
{
	PersistentFileSysObjName *fsObjName = (PersistentFileSysObjName *) arg;

	errcontext("Dropping file-system object -- %s",
		       PersistentFileSysObjName_TypeAndObjectName(fsObjName));
}


void PersistentFileSysObj_DropObject(
	PersistentFileSysObjName 	*fsObjName,
	PersistentFileSysRelStorageMgr relStorageMgr,
	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	ItemPointer					persistentTid,
	int64						persistentSerialNum,
	bool 						ignoreNonExistence,
	bool						debugPrint,
	int							debugPrintLevel)
{
	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysObjName 		actualFsObjName;
	PersistentFileSysState		  	actualState;

	MirroredObjectExistenceState  	mirrorExistenceState;

	TransactionId					actualParentXid;
	int64							actualSerialNum;

	bool							suppressMirror;

	ErrorContextCallback errcontext;

	bool mirrorDataLossOccurred;

	/* Setup error traceback support for ereport() */
	errcontext.callback = PersistentFileSysObj_DropObjectErrorCallback;
	errcontext.arg = (void *) fsObjName;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	if (!Persistent_BeforePersistenceWork())
	{
		if (PersistentStore_IsZeroTid(persistentTid))
			elog(ERROR, "TID for persistent '%s' tuple for DROP is invalid (0,0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));

		if (persistentSerialNum == 0)
			elog(ERROR, "Persistent '%s' serial number for DROP is invalid (0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));
	}

	fsObjType = fsObjName->type;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(fsObjName, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	if (tupleCopy == NULL)
		elog(ERROR, "Expected persistent entry '%s' not to be in 'Free' state for DROP at TID %s",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
			 ItemPointerToString(persistentTid));

	GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&actualFsObjName,
							&actualState,
							&mirrorExistenceState,
							&actualParentXid,
							&actualSerialNum);

	pfree(values);

	heap_freetuple(tupleCopy);

	if (persistentSerialNum != actualSerialNum)
		elog(ERROR, "Expected persistent entry '%s' serial number mismatch for DROP (expected " INT64_FORMAT ", found " INT64_FORMAT "), at TID %s",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
			 persistentSerialNum,
			 actualSerialNum,
			 ItemPointerToString(persistentTid));


	/*
	 * If we didn't try to create a mirror object because the mirror was already known to
	 * be down, then we can safely drop just the primary object.
	 */
	suppressMirror = (mirrorExistenceState == MirroredObjectExistenceState_MirrorDownBeforeCreate);

	if (debugPrint)
		elog(debugPrintLevel,
			 "PersistentFileSysObj_DropObject: before drop of %s, persistent serial number " INT64_FORMAT ", TID %s, suppressMirror %s",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 (suppressMirror ? "true" : "false"));

	switch (fsObjType)
	{
	case PersistentFsObjType_RelationFile:
		{
			RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
			int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

			if (!PersistentFileSysRelStorageMgr_IsValid(relStorageMgr))
				elog(ERROR, "Relation storage manager for persistent '%s' tuple for DROP is invalid (%d)",
					 PersistentFileSysObjName_TypeName(fsObjName->type),
					 relStorageMgr);

			MirroredFileSysObj_DropRelFile(
								relFileNode,
								segmentFileNum,
								relStorageMgr,
								relationName,
								/* isLocalBuf */ false,
								/* primaryOnly */ suppressMirror,
								ignoreNonExistence,
								&mirrorDataLossOccurred);


			if (debugPrint)
				elog(debugPrintLevel,
					 "PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s, suppressMirror %s and mirrorDataLossOccurred %s",
					 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
					 persistentSerialNum,
					 ItemPointerToString(persistentTid),
					 (suppressMirror ? "true" : "false"),
					 (mirrorDataLossOccurred ? "true" : "false"));

			if (suppressMirror || !mirrorDataLossOccurred)
				PersistentRelation_Dropped(
									relFileNode,
									segmentFileNum,
									persistentTid,
									persistentSerialNum);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		{
			DbDirNode *dbDirNode = &fsObjName->variant.dbDirNode;

			MirroredFileSysObj_DropDbDir(
								dbDirNode,
								/* primaryOnly */ suppressMirror,
								/* mirrorOnly */ false,
								ignoreNonExistence,
								&mirrorDataLossOccurred);

			if (debugPrint)
				elog(debugPrintLevel,
					 "PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s, suppressMirror %s and mirrorDataLossOccurred %s",
					 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
					 persistentSerialNum,
					 ItemPointerToString(persistentTid),
					 (suppressMirror ? "true" : "false"),
					 (mirrorDataLossOccurred ? "true" : "false"));

			if (suppressMirror || !mirrorDataLossOccurred)
				PersistentDatabase_Dropped(
									dbDirNode,
									persistentTid,
									persistentSerialNum);
		}
		break;

	case PersistentFsObjType_TablespaceDir:
		MirroredFileSysObj_DropTablespaceDir(
							fsObjName->variant.tablespaceOid,
							/* primaryOnly */ suppressMirror,
							/* mirrorOnly */ false,
							ignoreNonExistence,
							&mirrorDataLossOccurred);

		if (debugPrint)
			elog(debugPrintLevel,
				 "PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s, suppressMirror %s and mirrorDataLossOccurred %s",
				 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
				 persistentSerialNum,
				 ItemPointerToString(persistentTid),
				 (suppressMirror ? "true" : "false"),
				 (mirrorDataLossOccurred ? "true" : "false"));

		if (suppressMirror || !mirrorDataLossOccurred)
			PersistentTablespace_Dropped(
								fsObjName->variant.tablespaceOid,
								persistentTid,
								persistentSerialNum);
		break;

	case PersistentFsObjType_FilespaceDir:
		{
			char *primaryFilespaceLocation;
			char *mirrorFilespaceLocation;

			PersistentFilespace_GetPrimaryAndMirror(
								fsObjName->variant.filespaceOid,
								&primaryFilespaceLocation,
								&mirrorFilespaceLocation);

			MirroredFileSysObj_DropFilespaceDir(
								fsObjName->variant.filespaceOid,
								primaryFilespaceLocation,
								mirrorFilespaceLocation,
								/* primaryOnly */ suppressMirror,
								/* mirrorOnly */ false,
								ignoreNonExistence,
								&mirrorDataLossOccurred);

			if (debugPrint)
				elog(debugPrintLevel,
					 "PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s, suppressMirror %s and mirrorDataLossOccurred %s",
					 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
					 persistentSerialNum,
					 ItemPointerToString(persistentTid),
					 (suppressMirror ? "true" : "false"),
					 (mirrorDataLossOccurred ? "true" : "false"));

			if (suppressMirror || !mirrorDataLossOccurred)
				PersistentFilespace_Dropped(
									fsObjName->variant.filespaceOid,
									persistentTid,
									persistentSerialNum);

			if (primaryFilespaceLocation != NULL)
				pfree(primaryFilespaceLocation);
			if (mirrorFilespaceLocation != NULL)
				pfree(mirrorFilespaceLocation);
		}
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjName->type);
	}

	/* Pop the error context stack */
	error_context_stack = errcontext.previous;
}

void PersistentFileSysObj_EndXactDrop(
	PersistentFileSysObjName 	*fsObjName,
	PersistentFileSysRelStorageMgr relStorageMgr,
	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	ItemPointer				persistentTid,
	int64					persistentSerialNum,
	bool					ignoreNonExistence)
{
	// NOTE: The caller must already have the MirroredLock.

	if (fsObjName->type == PersistentFsObjType_RelationFile)
	{
		/*
		 * We use this lock to guard data resynchronization.
		 */
		LockRelationForResynchronize(
						PersistentFileSysObjName_GetRelFileNodePtr(fsObjName),
						AccessExclusiveLock);
	}

	PersistentFileSysObj_DropObject(
							fsObjName,
							relStorageMgr,
							relationName,
							persistentTid,
							persistentSerialNum,
							ignoreNonExistence,
							Debug_persistent_print,
							Persistent_DebugPrintLevel());

	if (fsObjName->type == PersistentFsObjType_RelationFile)
	{
		UnlockRelationForResynchronize(
						PersistentFileSysObjName_GetRelFileNodePtr(fsObjName),
						AccessExclusiveLock);
	}
}

static void PersistentFileSysObj_UpdateTupleAppendOnlyMirrorResyncEofs(
	PersistentFileSysObjData 		*fileSysObjData,
	PersistentFileSysObjSharedData 	*fileSysObjSharedData,
	HeapTuple					tuple,
	ItemPointer 				persistentTid,
	int64						persistentSerialNum,
	int64						mirrorLossEof,
	int64						mirrorNewEof,
	bool						flushToXLog)
{
	Datum newValues[Natts_gp_persistent_relation_node];
	bool replaces[Natts_gp_persistent_relation_node];

	MemSet(newValues, 0, Natts_gp_persistent_relation_node * sizeof(Datum));
	MemSet(replaces, false, Natts_gp_persistent_relation_node * sizeof(bool));

	replaces[Anum_gp_persistent_relation_node_mirror_append_only_loss_eof - 1] = true;
	newValues[Anum_gp_persistent_relation_node_mirror_append_only_loss_eof - 1] =
													Int64GetDatum(mirrorLossEof);

	replaces[Anum_gp_persistent_relation_node_mirror_append_only_new_eof - 1] = true;
	newValues[Anum_gp_persistent_relation_node_mirror_append_only_new_eof - 1] =
													Int64GetDatum(mirrorNewEof);

	PersistentFileSysObj_ReplaceTuple(
								PersistentFsObjType_RelationFile,
								persistentTid,
								tuple,
								newValues,
								replaces,
								flushToXLog);
}

void PersistentFileSysObj_UpdateAppendOnlyMirrorResyncEofs(
	RelFileNode					*relFileNode,
	int32						segmentFileNum,
	ItemPointer 				persistentTid,
	int64						persistentSerialNum,
	bool						mirrorCatchupRequired,
	int64						mirrorNewEof,
	bool						recovery,
	bool						flushToXLog)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFileSysObjData		*fileSysObjData;
	PersistentFileSysObjSharedData	*fileSysObjSharedData;

	Datum values[Natts_gp_persistent_relation_node];

	HeapTuple tupleCopy;

	RelFileNode 					actualRelFileNode;
	int32							actualSegmentFileNum;

	PersistentFileSysRelStorageMgr	relationStorageManager;
	PersistentFileSysState			persistentState;
	int64							createMirrorDataLossTrackingSessionNum;
	MirroredObjectExistenceState	mirrorExistenceState;
	MirroredRelDataSynchronizationState mirrorDataSynchronizationState;
	bool							mirrorBufpoolMarkedForScanIncrementalResync;
	int64							mirrorBufpoolResyncChangedPageCount;
	XLogRecPtr						mirrorBufpoolResyncCkptLoc;
	BlockNumber 					mirrorBufpoolResyncCkptBlockNum;
	int64							mirrorAppendOnlyLossEof;
	int64							mirrorAppendOnlyNewEof;
	PersistentFileSysRelBufpoolKind relBufpoolKind;
	TransactionId					parentXid;
	int64							actualPersistentSerialNum;
	int64							updateMirrorAppendOnlyLossEof;

	PersistentFileSysObj_GetDataPtrs(
								PersistentFsObjType_RelationFile,
								&fileSysObjData,
								&fileSysObjSharedData);

	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent %u/%u/%u, segment file #%d, tuple for update Append-Only mirror resync EOFs is invalid (0,0)",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum);

	if (persistentSerialNum == 0)
		elog(ERROR, "Persistent %u/%u/%u, segment file #%d, serial number for update Append-Only mirror resync EOFs is invalid (0)",
		relFileNode->spcNode,
		relFileNode->dbNode,
		relFileNode->relNode,
		segmentFileNum);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHTYPE(PersistentFsObjType_RelationFile, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							PersistentFsObjType_RelationFile,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	if (tupleCopy == NULL)
		elog(ERROR, "Persistent %u/%u/%u, segment file #%d, tuple at TID %s not found for update Append-Only mirror resync EOFs",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum,
			 ItemPointerToString(persistentTid));

	GpPersistentRelationNode_GetValues(
									values,
									&actualRelFileNode.spcNode,
									&actualRelFileNode.dbNode,
									&actualRelFileNode.relNode,
									&actualSegmentFileNum,
									&relationStorageManager,
									&persistentState,
									&createMirrorDataLossTrackingSessionNum,
									&mirrorExistenceState,
									&mirrorDataSynchronizationState,
									&mirrorBufpoolMarkedForScanIncrementalResync,
									&mirrorBufpoolResyncChangedPageCount,
									&mirrorBufpoolResyncCkptLoc,
									&mirrorBufpoolResyncCkptBlockNum,
									&mirrorAppendOnlyLossEof,
									&mirrorAppendOnlyNewEof,
									&relBufpoolKind,
									&parentXid,
									&actualPersistentSerialNum);

	if (persistentSerialNum != actualPersistentSerialNum)
		elog(ERROR, "Persistent %u/%u/%u, segment file #%d, serial number mismatch for update Append-Only mirror resync EOFs (expected " INT64_FORMAT ", found " INT64_FORMAT "), at TID %s",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum,
			 persistentSerialNum,
			 actualPersistentSerialNum,
			 ItemPointerToString(persistentTid));

	if (relationStorageManager != PersistentFileSysRelStorageMgr_AppendOnly)
		elog(ERROR, "Persistent %u/%u/%u, segment file #%d, relation storage manager mismatch for update Append-Only mirror resync EOFs (expected '%s', found '%s'), at TID %s",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum,
			 PersistentFileSysRelStorageMgr_Name(PersistentFileSysRelStorageMgr_AppendOnly),
			 PersistentFileSysRelStorageMgr_Name(relationStorageManager),
			 ItemPointerToString(persistentTid));

	if (!recovery)
	{
		if (mirrorAppendOnlyNewEof >= mirrorNewEof)
			elog(ERROR, "Persistent %u/%u/%u, segment file #%d, current new EOF is greater than or equal to update new EOF for Append-Only mirror resync EOFs (current new EOF " INT64_FORMAT ", update new EOF " INT64_FORMAT "), persistent serial num " INT64_FORMAT " at TID %s",
				 relFileNode->spcNode,
				 relFileNode->dbNode,
				 relFileNode->relNode,
				 segmentFileNum,
				 mirrorAppendOnlyNewEof,
				 mirrorNewEof,
				 persistentSerialNum,
				 ItemPointerToString(persistentTid));
	}
	else
	{
		int level = ERROR;

		if (gp_crash_recovery_suppress_ao_eof)
		{
			level = WARNING;
		}

		if (mirrorAppendOnlyNewEof > mirrorNewEof)
		{
			elog(level,
				 "Persistent %u/%u/%u, segment file #%d, "
				 "current new EOF is greater than update new EOF for Append-Only mirror resync EOFs recovery "
				 "(current new EOF " INT64_FORMAT ", update new EOF " INT64_FORMAT "), persistent serial num " INT64_FORMAT " at TID %s",
				 relFileNode->spcNode,
				 relFileNode->dbNode,
				 relFileNode->relNode,
				 segmentFileNum,
				 mirrorAppendOnlyNewEof,
				 mirrorNewEof,
				 persistentSerialNum,
				 ItemPointerToString(persistentTid));

			WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

			return;
		}

		if (mirrorAppendOnlyNewEof == mirrorNewEof)
		{
			if (Debug_persistent_print ||
				Debug_persistent_appendonly_commit_count_print)
				elog(Persistent_DebugPrintLevel(),
					 "Persistent %u/%u/%u, segment file #%d, current new EOF equal to update new EOF for Append-Only mirror resync EOFs recovery (EOF " INT64_FORMAT "), persistent serial num " INT64_FORMAT " at TID %s",
					 relFileNode->spcNode,
					 relFileNode->dbNode,
					 relFileNode->relNode,
					 segmentFileNum,
					 mirrorAppendOnlyNewEof,
					 persistentSerialNum,
					 ItemPointerToString(persistentTid));

			WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

			return;		// Old news.
		}
	}

	if (mirrorCatchupRequired)
	{
		updateMirrorAppendOnlyLossEof = mirrorAppendOnlyLossEof;	// No change.
	}
	else
	{
		updateMirrorAppendOnlyLossEof = mirrorNewEof;				// No loss.
	}

	PersistentFileSysObj_UpdateTupleAppendOnlyMirrorResyncEofs(
												fileSysObjData,
												fileSysObjSharedData,
												tupleCopy,
												persistentTid,
												persistentSerialNum,
												updateMirrorAppendOnlyLossEof,
												mirrorNewEof,
												flushToXLog);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print ||
		Debug_persistent_appendonly_commit_count_print)
		elog(Persistent_DebugPrintLevel(),
			 "PersistentFileSysObj_UpdateAppendOnlyMirrorResyncEofs: %u/%u/%u, segment file #%d, Append-Only Mirror Resync EOFs (recovery %s) -- persistent serial num " INT64_FORMAT ", TID %s, "
			 "mirror catchup required %s"
			 ", original mirror loss EOF " INT64_FORMAT ", update mirror loss EOF " INT64_FORMAT
			 ", original mirror new EOF " INT64_FORMAT ", update mirror new EOF " INT64_FORMAT,
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum,
			 (recovery ? "true" : "false"),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 (mirrorCatchupRequired ? "true" : "false"),
			 mirrorAppendOnlyLossEof,
			 updateMirrorAppendOnlyLossEof,
			 mirrorAppendOnlyNewEof,
			 mirrorNewEof);
}

void PersistentFileSysObj_PreparedEndXactAction(
	TransactionId 					preparedXid,
	const char 						*gid,
	PersistentEndXactRecObjects 	*persistentObjects,
	bool							isCommit,
	int								prepareAppendOnlyIntentCount)
{
	MIRRORED_LOCK_DECLARE;

	PersistentFileSysObjStateChangeResult *stateChangeResults;

	int i;

	if (Debug_persistent_print ||
		Debug_persistent_appendonly_commit_count_print)
		elog(Persistent_DebugPrintLevel(),
			 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): commit %s, file-system action count %d, shared-memory Append-Only intent count %d, xlog Append-Only mirror resync EOFs count %d",
			 gid,
			 preparedXid,
			 (isCommit ? "true" : "false"),
			 persistentObjects->typed.fileSysActionInfosCount,
			 prepareAppendOnlyIntentCount,
			 persistentObjects->typed.appendOnlyMirrorResyncEofsCount);

	stateChangeResults =
			(PersistentFileSysObjStateChangeResult*)
					palloc0(persistentObjects->typed.fileSysActionInfosCount * sizeof(PersistentFileSysObjStateChangeResult));

	/*
	 * We need to do the transition to 'Aborting Create' or 'Drop Pending' and perform
	 * the file-system drop while under one acquistion of the MirroredLock.  Otherwise,
	 * we could race with resynchronize's ReDrop.
	 */
	MIRRORED_LOCK;

	/*
	 * We need to complete this work, or let Crash Recovery complete it.
	 */
	START_CRIT_SECTION();

	/*
	 * End transaction State-Changes.
	 */
	for (i = 0; i < persistentObjects->typed.fileSysActionInfosCount; i++)
	{
		PersistentEndXactFileSysActionInfo	*fileSysActionInfo =
					&persistentObjects->typed.fileSysActionInfos[i];

		PersistentEndXactFileSysAction action;

		action = fileSysActionInfo->action;

		switch (action)
		{
		case PersistentEndXactFileSysAction_Create:
			if (isCommit)
			{
				/*
				 * 'Create Pending' --> 'Created'.
				 */
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, 'Create Pending' --> 'Created'",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid));

				PersistentFileSysObj_Created(
										&fileSysActionInfo->fsObjName,
										&fileSysActionInfo->persistentTid,
										fileSysActionInfo->persistentSerialNum,
										/* retryPossible */ true);
			}
			else
			{
				/*
				 * 'Create Pending' --> 'Aborting Create'.
				 */
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, 'Create Pending' --> 'Aborting Create'",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid));

				stateChangeResults[i] =
					PersistentFileSysObj_MarkAbortingCreate(
											&fileSysActionInfo->fsObjName,
											&fileSysActionInfo->persistentTid,
											fileSysActionInfo->persistentSerialNum,
											/* retryPossible */ true);
			}
			break;

		case PersistentEndXactFileSysAction_Drop:
			if (isCommit)
			{
				/*
				 * 'Created' --> 'Drop Pending'.
				 */
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, 'Created' --> 'Drop Pending'",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid));

				stateChangeResults[i] =
					PersistentFileSysObj_MarkDropPending(
										&fileSysActionInfo->fsObjName,
										&fileSysActionInfo->persistentTid,
										fileSysActionInfo->persistentSerialNum,
										/* retryPossible */ true);
			}
			break;

		case PersistentEndXactFileSysAction_AbortingCreateNeeded:
			if (Debug_persistent_print)
				elog(Persistent_DebugPrintLevel(),
					 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, 'Create Pending' --> 'Aborting Create'",
					 gid,
					 preparedXid,
					 i,
					 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
					 fileSysActionInfo->persistentSerialNum,
					 ItemPointerToString(&fileSysActionInfo->persistentTid));

			stateChangeResults[i] =
				PersistentFileSysObj_MarkAbortingCreate(
									&fileSysActionInfo->fsObjName,
									&fileSysActionInfo->persistentTid,
									fileSysActionInfo->persistentSerialNum,
									/* retryPossible */ true);
			break;

		default:
			elog(ERROR, "Unexpected persistent end transaction file-system action: %d",
				 action);
		}
	}

	/*
	 * Make the above State-Changes permanent.
	 */
	PersistentFileSysObj_FlushXLog();

	/*
	 * Do the physically drop of the file-system objects.
	 */
	for (i = 0; i < persistentObjects->typed.fileSysActionInfosCount; i++)
	{
		PersistentEndXactFileSysActionInfo	*fileSysActionInfo =
					&persistentObjects->typed.fileSysActionInfos[i];

		PersistentEndXactFileSysAction action;

		bool dropPending;
		bool abortingCreate;

		action = fileSysActionInfo->action;

		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(),
				 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, "
				 "looking at persistent end transaction action '%s' where isCommit %s and State-Change result '%s'",
				 gid,
				 preparedXid,
				 i,
				 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
				 fileSysActionInfo->persistentSerialNum,
				 ItemPointerToString(&fileSysActionInfo->persistentTid),
				 PersistentEndXactFileSysAction_Name(action),
				 (isCommit ? "true" : "false"),
				 PersistentFileSysObjStateChangeResult_Name(stateChangeResults[i]));

		dropPending = false;		// Assume.
		abortingCreate = false;		// Assume.

		switch (action)
		{
		case PersistentEndXactFileSysAction_Create:
			if (!isCommit)
			{
				abortingCreate = true;
			}
#ifdef FAULT_INJECTOR
				if (FaultInjector_InjectFaultIfSet(
											   isCommit ?
											   FinishPreparedTransactionCommitPass1FromCreatePendingToCreated :
											   FinishPreparedTransactionAbortPass1FromCreatePendingToAbortingCreate,
											   DDLNotSpecified,
												   "" /* databaseName */,
												   "" /* tableName */) == TRUE)
					goto injectfaultexit;  // skip pass2
#endif
			break;

		case PersistentEndXactFileSysAction_Drop:
			if (isCommit)
			{
				dropPending = true;
#ifdef FAULT_INJECTOR
				if (FaultInjector_InjectFaultIfSet(
											   FinishPreparedTransactionCommitPass1FromDropInMemoryToDropPending,
											   DDLNotSpecified,
											   "" /* databaseName */,
											   "" /* tableName */) == TRUE)
				goto injectfaultexit;  // skip pass2
#endif
			}
			break;

		case PersistentEndXactFileSysAction_AbortingCreateNeeded:
			abortingCreate = true;
#ifdef FAULT_INJECTOR
				if (FaultInjector_InjectFaultIfSet(
											   isCommit ?
											   FinishPreparedTransactionCommitPass1AbortingCreateNeeded:
											   FinishPreparedTransactionAbortPass1AbortingCreateNeeded,
											   DDLNotSpecified,
											   "" /* databaseName */,
											   "" /* tableName */) == TRUE)
				goto injectfaultexit;  // skip pass2
#endif
			break;

		default:
			elog(ERROR, "Unexpected persistent end transaction file-system action: %d",
				 action);
		}

		if (abortingCreate || dropPending)
		{
			if (stateChangeResults[i] == PersistentFileSysObjStateChangeResult_StateChangeOk)
			{
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, "
						 "going to drop object where abortingCreate %s or dropPending %s",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid),
						 (abortingCreate ? "true" : "false"),
						 (dropPending ? "true" : "false"));

				PersistentFileSysObj_EndXactDrop(
										&fileSysActionInfo->fsObjName,
										fileSysActionInfo->relStorageMgr,
										/* relationName */ NULL,		// Ok to be NULL -- we don't know the name here.
										&fileSysActionInfo->persistentTid,
										fileSysActionInfo->persistentSerialNum,
										/* ignoreNonExistence */ abortingCreate);
			}
			else
			{
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, "
						 "skipping to drop object where abortingCreate %s or dropPending %s with State-Change result '%s'",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid),
						 (abortingCreate ? "true" : "false"),
						 (dropPending ? "true" : "false"),
						 PersistentFileSysObjStateChangeResult_Name(stateChangeResults[i]));
			}
		}

#ifdef FAULT_INJECTOR
		switch (action)
		{
			case PersistentEndXactFileSysAction_Create:
				if (FaultInjector_InjectFaultIfSet(
												   isCommit ?
												   FinishPreparedTransactionCommitPass2FromCreatePendingToCreated :
												   FinishPreparedTransactionAbortPass2FromCreatePendingToAbortingCreate,
												   DDLNotSpecified,
												   "" /* databaseName */,
												   "" /* tableName */) == TRUE)
					goto injectfaultexit;  // skip physical drop in pass2

				break;

			case PersistentEndXactFileSysAction_Drop:
				if (isCommit)
				{
					if (FaultInjector_InjectFaultIfSet(
												   FinishPreparedTransactionCommitPass2FromDropInMemoryToDropPending,
												   DDLNotSpecified,
												   "" /* databaseName */,
												   "" /* tableName */) == TRUE)
					goto injectfaultexit;  // skip physical drop in pass2
				}
				break;

			case PersistentEndXactFileSysAction_AbortingCreateNeeded:
				if (FaultInjector_InjectFaultIfSet(
											   isCommit ?
											   FinishPreparedTransactionCommitPass2AbortingCreateNeeded :
											   FinishPreparedTransactionAbortPass2AbortingCreateNeeded,
											   DDLNotSpecified,
											   "" /* databaseName */,
											   "" /* tableName */) == TRUE)
				goto injectfaultexit;  // skip physical drop in pass2
				break;

			default:
				break;
		}

#endif
	}

	if (isCommit)
	{
		/*
		 * Append-Only mirror resync EOFs.
		 */
		for (i = 0; i < persistentObjects->typed.appendOnlyMirrorResyncEofsCount; i++)
		{
			bool mirrorCatchupRequired;

			PersistentEndXactAppendOnlyMirrorResyncEofs *eofs =
						&persistentObjects->typed.appendOnlyMirrorResyncEofs[i];

			if (eofs->mirrorLossEof == INT64CONST(-1))
			{
				mirrorCatchupRequired = true;
			}
			else
			{
				if (eofs->mirrorLossEof != eofs->mirrorNewEof)
					elog(ERROR, "mirror loss EOF " INT64_FORMAT " doesn't match new EOF " INT64_FORMAT,
						 eofs->mirrorLossEof,
						 eofs->mirrorNewEof);
				mirrorCatchupRequired = false;
			}

			SIMPLE_FAULT_INJECTOR(UpdateCommittedEofInPersistentTable);

			PersistentFileSysObj_UpdateAppendOnlyMirrorResyncEofs(
															&eofs->relFileNode,
															eofs->segmentFileNum,
															&eofs->persistentTid,
															eofs->persistentSerialNum,
															mirrorCatchupRequired,
															eofs->mirrorNewEof,
															/* recovery */ false,
															/* flushToXLog */ false);
		}
	}

	goto jumpoverinjectfaultexit;

injectfaultexit:

	if (Debug_persistent_print ||
		Debug_persistent_appendonly_commit_count_print)
	{
		int systemAppendOnlyCommitWorkCount;

		LWLockAcquire(FileRepAppendOnlyCommitCountLock , LW_EXCLUSIVE);

		systemAppendOnlyCommitWorkCount = 0;

		elog(Persistent_DebugPrintLevel(),
			 "PersistentFileSysObj_PreparedEndXactAction: Append-Only Mirror Resync EOFs commit count %d at fault-injection "
			 "(shared-memory count %d, xlog count %d). "
			 "Distributed transaction id %s (local prepared xid %u)",
			 systemAppendOnlyCommitWorkCount,
			 prepareAppendOnlyIntentCount,
			 persistentObjects->typed.appendOnlyMirrorResyncEofsCount,
			 gid,
			 preparedXid);

		LWLockRelease(FileRepAppendOnlyCommitCountLock);
	}

jumpoverinjectfaultexit:

	PersistentFileSysObj_FlushXLog();

	MIRRORED_UNLOCK;

	END_CRIT_SECTION();

	pfree(stateChangeResults);
}

bool PersistentFileSysObj_ScanForRelation(
	RelFileNode 		*relFileNode,
				/* The tablespace, database, and relation OIDs for the create. */

	int32				segmentFileNum,

	ItemPointer			persistentTid,
				/* Resulting TID of the gp_persistent_rel_files tuple for the relation. */

	int64				*persistentSerialNum)
				/* Resulting serial number for the relation.  Distinquishes the uses of the tuple. */
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjData *fileSysObjData;
	PersistentFileSysObjSharedData	*fileSysObjSharedData;

	PersistentStoreScan storeScan;

	Datum values[Natts_gp_persistent_relation_node];

	bool found;

	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	if (Persistent_BeforePersistenceWork())
	{
		elog(ERROR,
		     "Cannot scan for persistent relation %u/%u/%u, segment file #%d because we are before persistence work",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum);
	}

	PersistentFileSysObj_VerifyInitScan();

	PersistentFileSysObj_GetDataPtrs(
								PersistentFsObjType_RelationFile,
								&fileSysObjData,
								&fileSysObjSharedData);

	found = false;
	MemSet(persistentTid, 0, sizeof(ItemPointerData));
	*persistentSerialNum = 0;

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	PersistentStore_BeginScan(
						&fileSysObjData->storeData,
						&fileSysObjSharedData->storeSharedData,
						&storeScan);

	while (PersistentStore_GetNext(
							&storeScan,
							values,
							persistentTid,
							persistentSerialNum))
	{
		RelFileNode 					candidateRelFileNode;
		int32 							candidateSegmentFileNum;

		PersistentFileSysRelStorageMgr	relationStorageManager;
		PersistentFileSysState			persistentState;
		int64							createMirrorDataLossTrackingSessionNum;
		MirroredObjectExistenceState	mirrorExistenceState;
		MirroredRelDataSynchronizationState mirrorDataSynchronizationState;
		bool							mirrorBufpoolMarkedForScanIncrementalResync;
		int64							mirrorBufpoolResyncChangedPageCount;
		XLogRecPtr						mirrorBufpoolResyncCkptLoc;
		BlockNumber 					mirrorBufpoolResyncCkptBlockNum;
		int64							mirrorAppendOnlyLossEof;
		int64							mirrorAppendOnlyNewEof;
		PersistentFileSysRelBufpoolKind relBufpoolKind;
		TransactionId					parentXid;
		int64							serialNum;

		GpPersistentRelationNode_GetValues(
										values,
										&candidateRelFileNode.spcNode,
										&candidateRelFileNode.dbNode,
										&candidateRelFileNode.relNode,
										&candidateSegmentFileNum,
										&relationStorageManager,
										&persistentState,
										&createMirrorDataLossTrackingSessionNum,
										&mirrorExistenceState,
										&mirrorDataSynchronizationState,
										&mirrorBufpoolMarkedForScanIncrementalResync,
										&mirrorBufpoolResyncChangedPageCount,
										&mirrorBufpoolResyncCkptLoc,
										&mirrorBufpoolResyncCkptBlockNum,
										&mirrorAppendOnlyLossEof,
										&mirrorAppendOnlyNewEof,
										&relBufpoolKind,
										&parentXid,
										&serialNum);

		if (persistentState != PersistentFileSysState_BulkLoadCreatePending &&
			persistentState != PersistentFileSysState_CreatePending &&
			persistentState != PersistentFileSysState_Created)
			continue;

		if (RelFileNodeEquals(candidateRelFileNode, *relFileNode) &&
			candidateSegmentFileNum == segmentFileNum)
		{
			found = true;
			break;
		}

		READ_PERSISTENT_STATE_ORDERED_UNLOCK;

		READ_PERSISTENT_STATE_ORDERED_LOCK;

	}

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;

	PersistentStore_EndScan(&storeScan);

	if (found && Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
		     "Scan found persistent relation %u/%u/%u, segment file #%d with serial number " INT64_FORMAT " at TID %s",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum,
			 *persistentSerialNum,
			 ItemPointerToString(persistentTid));

	return found;
}

static void PersistentFileSysObj_StartupIntegrityCheckRelation(void)
{
	PersistentFileSysObjData *fileSysObjData;
	PersistentFileSysObjSharedData	*fileSysObjSharedData;

	PersistentStoreScan storeScan;

	Datum values[Natts_gp_persistent_relation_node];

	ItemPointerData persistentTid;
	int64			persistentSerialNum;

	PersistentFileSysObj_GetDataPtrs(
								PersistentFsObjType_RelationFile,
								&fileSysObjData,
								&fileSysObjSharedData);


	PersistentStore_BeginScan(
						&fileSysObjData->storeData,
						&fileSysObjSharedData->storeSharedData,
						&storeScan);

	while (PersistentStore_GetNext(
							&storeScan,
							values,
							&persistentTid,
							&persistentSerialNum))
	{
		RelFileNode 					relFileNode;
		int32 							segmentFileNum;

		PersistentFileSysRelStorageMgr	relationStorageManager;
		PersistentFileSysState			persistentState;
		int64							createMirrorDataLossTrackingSessionNum;
		MirroredObjectExistenceState	mirrorExistenceState;
		MirroredRelDataSynchronizationState mirrorDataSynchronizationState;
		bool							mirrorBufpoolMarkedForScanIncrementalResync;
		int64							mirrorBufpoolResyncChangedPageCount;
		XLogRecPtr						mirrorBufpoolResyncCkptLoc;
		BlockNumber 					mirrorBufpoolResyncCkptBlockNum;
		int64							mirrorAppendOnlyLossEof;
		int64							mirrorAppendOnlyNewEof;
		PersistentFileSysRelBufpoolKind relBufpoolKind;
		TransactionId					parentXid;
		int64							serialNum;

		DbDirNode dbDirNode;

		GpPersistentRelationNode_GetValues(
										values,
										&relFileNode.spcNode,
										&relFileNode.dbNode,
										&relFileNode.relNode,
										&segmentFileNum,
										&relationStorageManager,
										&persistentState,
										&createMirrorDataLossTrackingSessionNum,
										&mirrorExistenceState,
										&mirrorDataSynchronizationState,
										&mirrorBufpoolMarkedForScanIncrementalResync,
										&mirrorBufpoolResyncChangedPageCount,
										&mirrorBufpoolResyncCkptLoc,
										&mirrorBufpoolResyncCkptBlockNum,
										&mirrorAppendOnlyLossEof,
										&mirrorAppendOnlyNewEof,
										&relBufpoolKind,
										&parentXid,
										&serialNum);

		if (persistentState == PersistentFileSysState_Free)
			continue;

		/*
		 * Verify this relation has a database directory parent.
		 */
		dbDirNode.tablespace = relFileNode.spcNode;
		dbDirNode.database = relFileNode.dbNode;

		if (dbDirNode.database == 0)
		{
			/*
			 * We don't represent the shared database directory in
			 * gp_persistent_database_node since it cannot be dropped.
			 */
			Assert(dbDirNode.tablespace == GLOBALTABLESPACE_OID);
			continue;
		}

		if (!PersistentDatabase_DbDirExistsUnderLock(&dbDirNode))
		{
			elog(LOG,
				 "Database directory not found for relation %u/%u/%u",
				 relFileNode.spcNode,
				 relFileNode.dbNode,
				 relFileNode.relNode);
		}
	}

	PersistentStore_EndScan(&storeScan);
}

static void PersistentFileSysObj_StartupIntegrityCheckPrintDbDirs(void)
{
	DbDirNode dbDirNode;
	PersistentFileSysState state;

	ItemPointerData persistentTid;
	int64 persistentSerialNum;

	PersistentDatabase_DirIterateInit();
	while (PersistentDatabase_DirIterateNext(
									&dbDirNode,
									&state,
									&persistentTid,
									&persistentSerialNum))
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "STARTUP INTEGRITY: Database database %u/%u in state '%s'",
			 dbDirNode.tablespace,
			 dbDirNode.database,
			 PersistentFileSysObjState_Name(state));
	}
}

void PersistentFileSysObj_StartupIntegrityCheck(void)
{
	/*
	 * Show the existing database directories.
	 */
	if (Debug_persistent_recovery_print)
	{
		PersistentFileSysObj_StartupIntegrityCheckPrintDbDirs();
	}

	/*Verify each relation has a database directory parent.*/
	PersistentFileSysObj_StartupIntegrityCheckRelation();

	/*
	 * Show the existing tablespace directories.
	 */
	if (Debug_persistent_recovery_print)
	{
//		PersistentFileSysObj_StartupIntegrityCheckPrintTablespaceDirs();
	}

}

static Size PersistentFileSysObj_SharedDataSize(void)
{
	return MAXALIGN(sizeof(PersistentFileSysObjPrivateSharedMemory));
}

/*
 * Return the required shared-memory size for this module.
 */
Size PersistentFileSysObj_ShmemSize(void)
{
	Size size = 0;

	/* The shared-memory structure. */
	size = add_size(size, PersistentFileSysObj_SharedDataSize());

	return size;
}

/*
 * Initialize the shared-memory for this module.
 */
void PersistentFileSysObj_ShmemInit(void)
{
	bool found;

	/* Create the shared-memory structure. */
	persistentFileSysObjPrivateSharedData =
		(PersistentFileSysObjPrivateSharedMemory *)
						ShmemInitStruct("Persist File-Sys Data",
										PersistentFileSysObj_SharedDataSize(),
										&found);

	if (!found)
	{
		persistentFileSysObjPrivateSharedData->needInitScan = true;
	}
}

/* 
 * Check for inconsistencies in global sequence number and then update to the right value if necessary 
 * WARNING : This is a special routine called during repair only under guc gp_persistent_repair_global_sequence
 */
void 
PersistentFileSysObj_DoGlobalSequenceScan(void)
{

	PersistentFsObjType             fsObjType;
	int64                   	globalSequenceNum;

	PersistentFileSysObjData        *fileSysObjData;
	PersistentFileSysObjSharedData  *fileSysObjSharedData;

	Assert(!persistentFileSysObjPrivateSharedData->needInitScan);

	if (gp_persistent_repair_global_sequence)
	{
	
		for (fsObjType = PersistentFsObjType_First;
			fsObjType <= PersistentFsObjType_Last;
			fsObjType++)
		{
                 	PersistentFileSysObj_GetDataPtrs(
                                                                         fsObjType,
                                                                         &fileSysObjData,
                                                                         &fileSysObjSharedData);

			globalSequenceNum = GlobalSequence_Current(fileSysObjData->storeData.gpGlobalSequence);

			if (globalSequenceNum < fileSysObjSharedData->storeSharedData.maxInUseSerialNum)
			{
				/*
				 * We seem to have a corruption problem.
				 *
				 * Use the gp_persistent_repair_global_sequence GUC to get the system up.
				 */
				GlobalSequence_Set(fileSysObjData->storeData.gpGlobalSequence, fileSysObjSharedData->storeSharedData.maxInUseSerialNum);

				elog(LOG, "Repaired global sequence number " INT64_FORMAT " so use scanned maximum value " INT64_FORMAT " ('%s')",
                                 	globalSequenceNum,
                                 	fileSysObjSharedData->storeSharedData.maxInUseSerialNum,
                                 	fileSysObjData->storeData.tableName);
			}

		}
	}
}
