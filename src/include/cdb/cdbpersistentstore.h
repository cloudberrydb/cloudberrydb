/*-------------------------------------------------------------------------
 *
 * cdbpersistentstore.h
 *
 * Portions Copyright (c) 2009-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbpersistentstore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPERSISTENTSTORE_H
#define CDBPERSISTENTSTORE_H

#include "miscadmin.h"
#include "access/persistentfilesysobjname.h"
#include "catalog/gp_global_sequence.h"
#include "storage/itemptr.h"
#include "storage/proc.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "utils/relcache.h"
#include "cdb/cdbdirectopen.h"
#include "utils/guc.h"

/*
 * Can be used to suppress errcontext when doing low-level tracing where we
 * don't want "dynamic" errcontext like that used by the executor which executes a
 * lot of code to determine the errcontext string to recurse and cause stack overflow.
 */
#define SUPPRESS_ERRCONTEXT_DECLARE \
	ErrorContextCallback *suppressErrContext = NULL;

#define SUPPRESS_ERRCONTEXT_PUSH() \
{ \
	suppressErrContext = error_context_stack; \
	error_context_stack = NULL; \
}

#define SUPPRESS_ERRCONTEXT_POP() \
{ \
	error_context_stack = suppressErrContext; \
}


#define PersistentStoreSharedDataEyecatcher "PersistentStoreSharedData"

// Length includes 1 extra for NUL.
#define PersistentStoreSharedDataEyecatcher_Len 26

typedef struct PersistentStoreSharedData
{
	char	eyecatcher[PersistentStoreSharedDataEyecatcher_Len];
	bool				needToScanIntoSharedMemory;
	int64				maxInUseSerialNum;
		/* Value of persistent serial number that was assigned to last tuple */

	int64				inUseCount;
		/* Current number of tuples in persistent table */
} PersistentStoreSharedData;

inline static bool PersistentStoreSharedData_EyecatcherIsValid(
	PersistentStoreSharedData *storeSharedData)
{
	return (strcmp(storeSharedData->eyecatcher, PersistentStoreSharedDataEyecatcher) == 0);
}

typedef Relation (*PersistentStoreOpenRel) (void);
typedef void (*PersistentStoreCloseRel) (Relation relation);
typedef bool (*PersistentStoreScanTupleCallback) (
								ItemPointer 			persistentTid,
								int64					persistentSerialNum,
								Datum					*values);
typedef void (*PersistentStorePrintTupleCallback) (
								int						elevel,
								char					*prefix,
								ItemPointer 			persistentTid,
								Datum					*values);

#define PersistentStoreData_StaticInit {NULL,0,NULL,NULL,NULL,NULL,0,0}

typedef struct PersistentStoreData
{
	char								*tableName;
	GpGlobalSequence					gpGlobalSequence;
	PersistentStoreOpenRel				openRel;
	PersistentStoreCloseRel				closeRel;
	PersistentStoreScanTupleCallback	scanTupleCallback;
	PersistentStorePrintTupleCallback	printTupleCallback;
	int64			myHighestSerialNum;
	int				numAttributes;
	int				attNumPersistentSerialNum;
} PersistentStoreData;

typedef struct PersistentStoreScan
{
	PersistentStoreData 		*storeData;
	PersistentStoreSharedData 	*storeSharedData;
	Relation					persistentRel;
	HeapScanDesc				scan;
	HeapTuple					tuple;
} PersistentStoreScan;

inline static bool PersistentStore_IsZeroTid(
	ItemPointer		testTid)
{
	static ItemPointerData zeroTid = {{0,0},0};
	return (ItemPointerCompare(testTid, &zeroTid) == 0);
}

extern void PersistentStore_InitShared(
	PersistentStoreSharedData 	*storeSharedData);

extern void PersistentStore_Init(
	PersistentStoreData 		*storeData,
	char						*tableName,
	GpGlobalSequence			gpGlobalSequence,
	PersistentStoreOpenRel		openRel,
	PersistentStoreCloseRel		closeRel,
	PersistentStoreScanTupleCallback	scanTupleCallback,
	PersistentStorePrintTupleCallback	printTupleCallback,
	int 						numAttributes,
	int 						attNumPersistentSerialNum);

extern void PersistentStore_InitScanUnderLock(
	PersistentStoreData 		*storeData,
	PersistentStoreSharedData 	*storeSharedData);

extern void PersistentStore_Scan(
	PersistentStoreData 		*storeData,
	PersistentStoreSharedData	*storeSharedData,
	PersistentStoreScanTupleCallback	scanTupleCallback);

extern void PersistentStore_BeginScan(
	PersistentStoreData 		*storeData,
	PersistentStoreSharedData	*storeSharedData,
	PersistentStoreScan			*storeScan);

extern void PersistentStore_DeformTuple(
	PersistentStoreData 	*storeData,
	TupleDesc				tupleDesc,			
						/* tuple descriptor */
    HeapTuple 				tuple,
	Datum					*values);

extern bool PersistentStore_GetNext(
	PersistentStoreScan			*storeScan,
	Datum						*values,
	ItemPointer					persistentTid,
	int64						*persistentSerialNum);

extern HeapTuple PersistentStore_GetScanTupleCopy(
	PersistentStoreScan			*storeScan);

extern void PersistentStore_EndScan(
	PersistentStoreScan			*storeScan);

extern void PersistentStore_FlushXLog(void);

extern int64 PersistentStore_MyHighestSerialNum(
	PersistentStoreData 	*storeData);

extern int64 PersistentStore_CurrentMaxSerialNum(
	PersistentStoreSharedData 	*storeSharedData);

typedef enum PersistentTidIsKnownResult
{
	PersistentTidIsKnownResult_None = 0,
	PersistentTidIsKnownResult_BeforePersistenceWork,
	PersistentTidIsKnownResult_ScanNotPerformedYet,
	PersistentTidIsKnownResult_MaxTidIsZero,
	PersistentTidIsKnownResult_Known,
	PersistentTidIsKnownResult_NotKnown,
	MaxPersistentTidIsKnownResult		/* must always be last */
} PersistentTidIsKnownResult;

extern void PersistentStore_UpdateTuple(
	PersistentStoreData 		*storeData,
	PersistentStoreSharedData	*storeSharedData,
	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */
	Datum					*values,
	bool					flushToXLog);
				/* When true, the XLOG record for this change will be flushed to disk. */

extern void PersistentStore_ReplaceTuple(
	PersistentStoreData 		*storeData,
	PersistentStoreSharedData	*storeSharedData,
	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */
	HeapTuple				tuple,
	Datum					*newValues,
	bool					*replaces,
	bool					flushToXLog);
				/* When true, the XLOG record for this change will be flushed to disk. */

extern void PersistentStore_ReadTuple(
	PersistentStoreData 		*storeData,
	PersistentStoreSharedData 	*storeSharedData,
	ItemPointer					readTid,
	Datum						*values,
	HeapTuple					*tupleCopy);

extern void PersistentStore_AddTuple(
	PersistentStoreData 		*storeData,
	PersistentStoreSharedData 	*storeSharedData,
	Datum					*values,
	bool					flushToXLog,
				/* When true, the XLOG record for this change will be flushed to disk. */
	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */
	int64					*persistentSerialNum);

extern void PersistentStore_FreeTuple(
	PersistentStoreData 		*storeData,
	PersistentStoreSharedData 	*storeSharedData,
	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */
	Datum					*freeValues,
	bool					flushToXLog);
				/* When true, the XLOG record for this change will be flushed to disk. */

#endif   /* CDBPERSISTENTSTORE_H */

