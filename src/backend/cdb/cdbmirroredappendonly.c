/*-------------------------------------------------------------------------
 *
 * cdbmirroredappendonly.c
 *
 * Portions Copyright (c) 2009-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbmirroredappendonly.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/file.h>

#include "access/xlogmm.h"
#include "utils/palloc.h"
#include "cdb/cdbmirroredappendonly.h"
#include "storage/fd.h"
#include "catalog/catalog.h"
#include "cdb/cdbpersistenttablespace.h"
#include "storage/smgr.h"
#include "storage/smgr_ao.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentrecovery.h"
#include "postmaster/primary_mirror_mode.h"

#ifdef USE_SEGWALREP
#include "access/xlogutils.h"
#include "cdb/cdbappendonlyam.h"

static void xlog_ao_insert(MirroredAppendOnlyOpen *open, int64 offset,
						   void *buffer, int32 bufferLen);
#endif		/* USE_SEGWALREP */

/*
 * Open a relation for mirrored write.
 */
static void
MirroredAppendOnly_DoOpen(
						  MirroredAppendOnlyOpen *open,
 /* The resulting open struct. */

						  RelFileNode *relFileNode,
 /* The tablespace, database, and relation OIDs for the open. */

						  int32 segmentFileNum,

						  char *relationName,
 /* For tracing only.  Can be NULL in some execution paths. */

						  int64 logicalEof,
 /* The file name path. */

						  MirrorDataLossTrackingState mirrorDataLossTrackingState,

						  int64 mirrorDataLossTrackingSessionNum,

						  bool create,

						  bool primaryOnlyToLetResynchronizeWork,

						  bool mirrorOnly,

						  bool copyToMirror,

						  bool guardOtherCallsWithMirroredLock,

						  bool traceOpenFlags,

						  int *primaryError,

						  bool *mirrorDataLossOccurred)
{
	int			fileFlags = O_RDWR | PG_BINARY;
	int			fileMode = 0600;

	/*
	 * File mode is S_IRUSR 00400 user has read permission + S_IWUSR 00200
	 * user has write permission
	 */

	char	   *primaryFilespaceLocation;
	char	   *mirrorFilespaceLocation;

	Assert(open != NULL);

	*primaryError = 0;
	*mirrorDataLossOccurred = false;

	if (create)
		fileFlags |= O_CREAT;

	MemSet(open, 0, sizeof(MirroredAppendOnlyOpen));

	open->relFileNode = *relFileNode;

	open->segmentFileNum = segmentFileNum;

	open->mirrorDataLossTrackingState = mirrorDataLossTrackingState;
	open->mirrorDataLossTrackingSessionNum = mirrorDataLossTrackingSessionNum;

	open->create = create;
	open->primaryOnlyToLetResynchronizeWork = primaryOnlyToLetResynchronizeWork;
	open->mirrorOnly = mirrorOnly;
	open->copyToMirror = copyToMirror;
	open->guardOtherCallsWithMirroredLock = guardOtherCallsWithMirroredLock;
	open->mirrorMode = StorageManagerMirrorMode_PrimaryOnly;
	open->mirrorDataLossOccurred = false;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(
													   relFileNode->spcNode,
													   &primaryFilespaceLocation,
													   &mirrorFilespaceLocation);

	if (mirrorFilespaceLocation == NULL)
		sprintf(open->mirrorFilespaceLocation, "%s", "");
	else
		sprintf(open->mirrorFilespaceLocation, "%s", mirrorFilespaceLocation);

	char	   *dbPath;
	char	   *path;

	dbPath = (char *) palloc(MAXPGPATH + 1);
	path = (char *) palloc(MAXPGPATH + 1);

	FormDatabasePath(
		dbPath,
		primaryFilespaceLocation,
		relFileNode->spcNode,
		relFileNode->dbNode);

	if (segmentFileNum == 0)
		sprintf(path, "%s/%u", dbPath, relFileNode->relNode);
	else
		sprintf(path, "%s/%u.%u", dbPath, relFileNode->relNode, segmentFileNum);

	errno = 0;

	open->primaryFile = PathNameOpenFile(path, fileFlags, fileMode);

	if (open->primaryFile < 0)
	{
		*primaryError = errno;
	}

	pfree(dbPath);
	pfree(path);

	if (*primaryError != 0)
	{
		open->isActive = false;
	}
	else
	{
		open->isActive = true;
	}

	*mirrorDataLossOccurred = open->mirrorDataLossOccurred;

	if (primaryFilespaceLocation != NULL)
		pfree(primaryFilespaceLocation);
	if (mirrorFilespaceLocation != NULL)
		pfree(mirrorFilespaceLocation);
}

/*
 * Call MirroredAppendOnly_Create with the MirroredLock already held.
 */
void
MirroredAppendOnly_Create(
						  RelFileNode *relFileNode,
 /* The tablespace, database, and relation OIDs for the open. */

						  int32 segmentFileNum,
 /* Which segment file. */

						  char *relationName,
 /* For tracing only.  Can be NULL in some execution paths. */

						  MirrorDataLossTrackingState mirrorDataLossTrackingState,

						  int64 mirrorDataLossTrackingSessionNum,

						  int *primaryError,

						  bool *mirrorDataLossOccurred)
{
	MirroredAppendOnlyOpen mirroredOpen;

	bool		mirrorCatchupRequired;

	MirrorDataLossTrackingState originalMirrorDataLossTrackingState;
	int64		originalMirrorDataLossTrackingSessionNum;

	*primaryError = 0;
	*mirrorDataLossOccurred = false;

	MirroredAppendOnly_DoOpen(
							  &mirroredOpen,
							  relFileNode,
							  segmentFileNum,
							  relationName,
							   /* logicalEof */ 0,
							  mirrorDataLossTrackingState,
							  mirrorDataLossTrackingSessionNum,
							   /* create */ true,
							   /* primaryOnlyToLetResynchronizeWork */ false,
							   /* mirrorOnly */ false,
							   /* copyToMirror */ false,
							   /* guardOtherCallsWithMirroredLock */ false,
							   /* traceOpenFlags */ false,
							  primaryError,
							  mirrorDataLossOccurred);
	if ((*primaryError) != 0)
		return;

	MirroredAppendOnly_FlushAndClose(
									 &mirroredOpen,
									 primaryError,
									 mirrorDataLossOccurred,
									 &mirrorCatchupRequired,
									 &originalMirrorDataLossTrackingState,
									 &originalMirrorDataLossTrackingSessionNum);
}

/*
 * MirroredAppendOnly_OpenReadWrite will acquire and release the MirroredLock.
 *
 * Use aaa after writing data to determine if mirror loss occurred and
 * mirror catchup must be performed.
 */
void
MirroredAppendOnly_OpenReadWrite(
								 MirroredAppendOnlyOpen *open,
 /* The resulting open struct. */

								 RelFileNode *relFileNode,
 /* The tablespace, database, and relation OIDs for the open. */

								 int32 segmentFileNum,
 /* Which segment file. */

								 char *relationName,
 /* For tracing only.  Can be NULL in some execution paths. */

								 int64 logicalEof,
 /* The logical EOF to begin appending the new data. */

								 bool traceOpenFlags,

								 ItemPointer persistentTid,

								 int64 persistentSerialNum,

								 int *primaryError)
{
	MIRRORED_LOCK_DECLARE;

	MirrorDataLossTrackingState mirrorDataLossTrackingState;
	int64		mirrorDataLossTrackingSessionNum;

	bool		primaryOnlyToLetResynchronizeWork = false;

	bool		mirrorDataLossOccurred;

	*primaryError = 0;

	MIRRORED_LOCK;

	if (smgrgetappendonlyinfo(
							  relFileNode,
							  segmentFileNum,
							  relationName,
							   /* OUT mirrorCatchupRequired */ &primaryOnlyToLetResynchronizeWork,
							  &mirrorDataLossTrackingState,
							  &mirrorDataLossTrackingSessionNum))
	{
		/*
		 * If we have already written to the same Append-Only segment file in
		 * this transaction, use the FileRep state information it collected.
		 */
	}
	else
	{
		/*
		 * Make this call while under the MirroredLock (unless we are a resync
		 * worker).
		 */
		mirrorDataLossTrackingState = MirrorDataLossTrackingState_MirrorNotConfigured;
		mirrorDataLossTrackingSessionNum = getChangeTrackingSessionId();
		primaryOnlyToLetResynchronizeWork = false;
	}
	MirroredAppendOnly_DoOpen(
							  open,
							  relFileNode,
							  segmentFileNum,
							  relationName,
							  logicalEof,
							  mirrorDataLossTrackingState,
							  mirrorDataLossTrackingSessionNum,
							   /* create */ false,
							  primaryOnlyToLetResynchronizeWork,
							   /* mirrorOnly */ false,
							   /* copyToMirror */ false,
							   /* guardOtherCallsWithMirroredLock */ true,
							  traceOpenFlags,
							  primaryError,
							  &mirrorDataLossOccurred);

	MIRRORED_UNLOCK;
}

/*
 * Flush and close a bulk relation file.
 *
 * If the flush is unable to complete on the mirror, then this relation will be marked in the
 * commit, distributed commit, distributed prepared and commit prepared records as having
 * un-mirrored bulk initial data.
 */
void
MirroredAppendOnly_FlushAndClose(
								 MirroredAppendOnlyOpen *open,
 /* The open struct. */

								 int *primaryError,

								 bool *mirrorDataLossOccurred,

								 bool *mirrorCatchupRequired,

								 MirrorDataLossTrackingState *originalMirrorDataLossTrackingState,

								 int64 *originalMirrorDataLossTrackingSessionNum)
{
	MIRRORED_LOCK_DECLARE;

	Assert(open != NULL);
	Assert(open->isActive);

	*primaryError = 0;
	*mirrorDataLossOccurred = false;
	*mirrorCatchupRequired = false;
	*originalMirrorDataLossTrackingState = (MirrorDataLossTrackingState) -1;
	*originalMirrorDataLossTrackingSessionNum = 0;

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_LOCK;
	}

	errno = 0;

	if (FileSync(open->primaryFile) != 0)
		*primaryError = errno;
	FileClose(open->primaryFile);

	*mirrorDataLossOccurred = open->mirrorDataLossOccurred;
	/* Keep reporting-- it may have occurred anytime during the open session. */

	/*
	 * Evaluate whether we need to catchup the mirror.
	 */
	*mirrorCatchupRequired = (open->primaryOnlyToLetResynchronizeWork || open->mirrorDataLossOccurred);
	*originalMirrorDataLossTrackingState = open->mirrorDataLossTrackingState;
	*originalMirrorDataLossTrackingSessionNum = open->mirrorDataLossTrackingSessionNum;

	open->isActive = false;
	open->primaryFile = 0;

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_UNLOCK;
	}
}

/*
 * Flush and close a bulk relation file.
 *
 * If the flush is unable to complete on the mirror, then this relation will be marked in the
 * commit, distributed commit, distributed prepared and commit prepared records as having
 * un-mirrored bulk initial data.
 */
void
MirroredAppendOnly_Flush(
						 MirroredAppendOnlyOpen *open,
 /* The open struct. */

						 int *primaryError,

						 bool *mirrorDataLossOccurred)
{
	MIRRORED_LOCK_DECLARE;

	Assert(open != NULL);
	Assert(open->isActive);

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_LOCK;
	}

	errno = 0;

	if (FileSync(open->primaryFile) != 0)
	{
		*primaryError = errno;
	}

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_UNLOCK;
	}

	*mirrorDataLossOccurred = open->mirrorDataLossOccurred;
	/* Keep reporting-- it may have occurred anytime during the open session. */
}

/*
 * Close a bulk relation file.
 *
 */
void
MirroredAppendOnly_Close(
						 MirroredAppendOnlyOpen *open,
 /* The open struct. */

						 bool *mirrorDataLossOccurred)
{
	MIRRORED_LOCK_DECLARE;

	Assert(open != NULL);
	Assert(open->isActive);

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_LOCK;
	}

	/* No primary error to report. */
	errno = 0;

	FileClose(open->primaryFile);

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_UNLOCK;
	}

	*mirrorDataLossOccurred = open->mirrorDataLossOccurred;
	/* Keep reporting-- it may have occurred anytime during the open session. */

	open->isActive = false;
	open->primaryFile = 0;
}

void
MirroredAppendOnly_AddMirrorResyncEofs(
									   RelFileNode *relFileNode,

									   int32 segmentFileNum,

									   char *relationName,

									   ItemPointer persistentTid,

									   int64 persistentSerialNum,

									   MirroredLockLocalVars *mirroredLockByRefVars,

									   bool originalMirrorCatchupRequired,

									   MirrorDataLossTrackingState originalMirrorDataLossTrackingState,

									   int64 originalMirrorDataLossTrackingSessionNum,

									   int64 mirrorNewEof)
{
	bool		flushMirrorCatchupRequired;
	MirrorDataLossTrackingState flushMirrorDataLossTrackingState;
	int64		flushMirrorDataLossTrackingSessionNum;
	int64		startEof;

	/*
	 * Start these values with the original ones of the writer.
	 */
	flushMirrorCatchupRequired = originalMirrorCatchupRequired;
	flushMirrorDataLossTrackingState = originalMirrorDataLossTrackingState;
	flushMirrorDataLossTrackingSessionNum = originalMirrorDataLossTrackingSessionNum;

	while (true)
	{
		/*
		 * We already have the MirroredLock for safely evaluating the FileRep
		 * mirror data loss tracking state.
		 */
		if (!flushMirrorCatchupRequired)
		{
			/*
			 * No data loss.  Report new EOFs now.
			 */
			if (Debug_persistent_print ||
				Debug_persistent_appendonly_commit_count_print)
			{
				SUPPRESS_ERRCONTEXT_DECLARE;

				SUPPRESS_ERRCONTEXT_POP();

				elog(Persistent_DebugPrintLevel(),
					 "MirroredAppendOnly_AddMirrorResyncEofs %u/%u/%u, segment file #%d, relation name '%s': no mirror data loss occurred --> report new EOF (EOF " INT64_FORMAT ")",
					 relFileNode->spcNode,
					 relFileNode->dbNode,
					 relFileNode->relNode,
					 segmentFileNum,
					 (relationName == NULL ? "<null>" : relationName),
					 mirrorNewEof);

				SUPPRESS_ERRCONTEXT_PUSH();
			}

			smgrappendonlymirrorresynceofs(
										   relFileNode,
										   segmentFileNum,
										   relationName,
										   persistentTid,
										   persistentSerialNum,
										    /* mirrorCatchupRequired */ false,
										   flushMirrorDataLossTrackingState,
										   flushMirrorDataLossTrackingSessionNum,
										   mirrorNewEof);
			return;
		}

		flushMirrorDataLossTrackingState = MirrorDataLossTrackingState_MirrorNotConfigured;
		flushMirrorDataLossTrackingSessionNum = getChangeTrackingSessionId();

		switch (flushMirrorDataLossTrackingState)
		{
			case MirrorDataLossTrackingState_MirrorNotConfigured:

				/*
				 * We were not configured with a mirror or we've dropped the
				 * mirror.
				 */
				if (Debug_persistent_print ||
					Debug_persistent_appendonly_commit_count_print)
				{
					SUPPRESS_ERRCONTEXT_DECLARE;

					SUPPRESS_ERRCONTEXT_POP();

					elog(Persistent_DebugPrintLevel(),
						 "MirroredAppendOnly_AddMirrorResyncEofs %u/%u/%u, segment file #%d, relation name '%s': mirror no longer configured (mirror data loss tracking serial num " INT64_FORMAT ")-- reporting no data loss occurred --> report new EOF (EOF " INT64_FORMAT ")",
						 relFileNode->spcNode,
						 relFileNode->dbNode,
						 relFileNode->relNode,
						 segmentFileNum,
						 (relationName == NULL ? "<null>" : relationName),
						 flushMirrorDataLossTrackingSessionNum,
						 mirrorNewEof);

					SUPPRESS_ERRCONTEXT_PUSH();
				}

				smgrappendonlymirrorresynceofs(
											   relFileNode,
											   segmentFileNum,
											   relationName,
											   persistentTid,
											   persistentSerialNum,
											    /* mirrorCatchupRequired */ false,
											   flushMirrorDataLossTrackingState,
											   flushMirrorDataLossTrackingSessionNum,
											   mirrorNewEof);
				return;

			default:
				elog(ERROR, "Unexpected mirror data loss tracking state: %d",
					 flushMirrorDataLossTrackingState);
				startEof = 0;
		}
	}
}

void
MirroredAppendOnly_EndXactCatchup(
								  int entryIndex,

								  RelFileNode *relFileNode,

								  int32 segmentFileNum,

								  int nestLevel,

								  char *relationName,

								  ItemPointer persistentTid,

								  int64 persistentSerialNum,

								  MirroredLockLocalVars *mirroredLockByRefVars,

								  bool lastMirrorCatchupRequired,

								  MirrorDataLossTrackingState lastMirrorDataLossTrackingState,

								  int64 lastMirrorDataLossTrackingSessionNum,

								  int64 mirrorNewEof)
{
	MirrorDataLossTrackingState currentMirrorDataLossTrackingState;
	int64		currentMirrorDataLossTrackingSessionNum;

	bool		flushMirrorCatchupRequired;
	MirrorDataLossTrackingState flushMirrorDataLossTrackingState;
	int64		flushMirrorDataLossTrackingSessionNum;
	int64		startEof;

	currentMirrorDataLossTrackingState = MirrorDataLossTrackingState_MirrorNotConfigured;
	currentMirrorDataLossTrackingSessionNum = getChangeTrackingSessionId();

	if (Debug_persistent_print ||
		Debug_persistent_appendonly_commit_count_print)
	{
		elog(Persistent_DebugPrintLevel(),
			 "MirroredAppendOnly_EndXactCatchup: Evaluate Append-Only mirror resync eofs list entry #%d for loss: %u/%u/%u, segment file #%d, relation name '%s' "
			 "(transaction nest level %d, persistent TID %s, persistent serial number " INT64_FORMAT ", "
			 "mirror catchup required %s, "
			 "last mirror data loss tracking (state '%s', session num " INT64_FORMAT "), "
			 "current mirror data loss tracking (state '%s', session num " INT64_FORMAT "), "
			 "mirror new EOF " INT64_FORMAT ")",
			 entryIndex,
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum,
			 (relationName == NULL ? "<null>" : relationName),
			 nestLevel,
			 ItemPointerToString(persistentTid),
			 persistentSerialNum,
			 (lastMirrorCatchupRequired ? "true" : "false"),
			 MirrorDataLossTrackingState_Name(lastMirrorDataLossTrackingState),
			 lastMirrorDataLossTrackingSessionNum,
			 MirrorDataLossTrackingState_Name(currentMirrorDataLossTrackingState),
			 currentMirrorDataLossTrackingSessionNum,
			 mirrorNewEof);
	}

	if (lastMirrorCatchupRequired)
	{
		flushMirrorCatchupRequired = true;
		flushMirrorDataLossTrackingState = currentMirrorDataLossTrackingState;
		flushMirrorDataLossTrackingSessionNum = currentMirrorDataLossTrackingSessionNum;
	}
	else
	{
		flushMirrorCatchupRequired = false;
		/* Assume. */
		if (lastMirrorDataLossTrackingSessionNum != currentMirrorDataLossTrackingSessionNum)
		{
			Assert(currentMirrorDataLossTrackingSessionNum > lastMirrorDataLossTrackingSessionNum);

			flushMirrorCatchupRequired = true;
		}
		else if (lastMirrorDataLossTrackingState != currentMirrorDataLossTrackingState)
		{
			/*
			 * Same mirror data loss tracking number, but different state.
			 */

			if (lastMirrorDataLossTrackingState == MirrorDataLossTrackingState_MirrorDown)
			{
				/*
				 * We started with the mirror down and ended with the mirror
				 * up in either Resynchronized or Synchronized state (or not
				 * configured).
				 *
				 * So, there was mirror data loss.
				 */
				flushMirrorCatchupRequired = true;
			}
		}
		if (flushMirrorCatchupRequired)
		{
			flushMirrorDataLossTrackingState = currentMirrorDataLossTrackingState;
			flushMirrorDataLossTrackingSessionNum = currentMirrorDataLossTrackingSessionNum;
		}
		else
		{
			flushMirrorDataLossTrackingState = lastMirrorDataLossTrackingState;
			flushMirrorDataLossTrackingSessionNum = lastMirrorDataLossTrackingSessionNum;
		}
	}

	while (true)
	{
		/*
		 * We already have the MirroredLock for safely evaluating the FileRep
		 * mirror data loss tracking state.
		 */
		if (!flushMirrorCatchupRequired)
		{
			/*
			 * No data loss.
			 */
			if (Debug_persistent_print ||
				Debug_persistent_appendonly_commit_count_print)
			{
				SUPPRESS_ERRCONTEXT_DECLARE;

				SUPPRESS_ERRCONTEXT_POP();

				elog(Persistent_DebugPrintLevel(),
					 "MirroredAppendOnly_EndXactCatchup %u/%u/%u, segment file #%d, relation name '%s': no mirror data loss occurred --> report new EOF (EOF " INT64_FORMAT ")",
					 relFileNode->spcNode,
					 relFileNode->dbNode,
					 relFileNode->relNode,
					 segmentFileNum,
					 (relationName == NULL ? "<null>" : relationName),
					 mirrorNewEof);

				SUPPRESS_ERRCONTEXT_PUSH();
			}

			return;
		}

		switch (flushMirrorDataLossTrackingState)
		{
			case MirrorDataLossTrackingState_MirrorNotConfigured:

				/*
				 * We were not configured with a mirror or we've dropped the
				 * mirror.
				 */
				if (Debug_persistent_print ||
					Debug_persistent_appendonly_commit_count_print)
				{
					SUPPRESS_ERRCONTEXT_DECLARE;

					SUPPRESS_ERRCONTEXT_POP();

					elog(Persistent_DebugPrintLevel(),
						 "MirroredAppendOnly_EndXactCatchup %u/%u/%u, segment file #%d, relation name '%s': mirror no longer configured (mirror data loss tracking serial num " INT64_FORMAT ")-- reporting no data loss occurred --> report new EOF (EOF " INT64_FORMAT ")",
						 relFileNode->spcNode,
						 relFileNode->dbNode,
						 relFileNode->relNode,
						 segmentFileNum,
						 (relationName == NULL ? "<null>" : relationName),
						 flushMirrorDataLossTrackingSessionNum,
						 mirrorNewEof);

					SUPPRESS_ERRCONTEXT_PUSH();
				}
				return;

			default:
				elog(ERROR, "Unexpected mirror data loss tracking state: %d",
					 flushMirrorDataLossTrackingState);
				startEof = 0;
		}
	}
}

void
MirroredAppendOnly_Drop(
						RelFileNode *relFileNode,
						int32 segmentFileNum,
						int *primaryError)
{
	char	   *primaryFilespaceLocation;
	char	   *mirrorFilespaceLocation;

	*primaryError = 0;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(
													   relFileNode->spcNode,
													   &primaryFilespaceLocation,
													   &mirrorFilespaceLocation);

	char	   *dbPath;
	char	   *path;

	dbPath = (char *) palloc(MAXPGPATH + 1);
	path = (char *) palloc(MAXPGPATH + 1);

	FormDatabasePath(
		dbPath,
		primaryFilespaceLocation,
		relFileNode->spcNode,
		relFileNode->dbNode);

	if (segmentFileNum == 0)
		sprintf(path, "%s/%u", dbPath, relFileNode->relNode);
	else
		sprintf(path, "%s/%u.%u", dbPath, relFileNode->relNode, segmentFileNum);

	errno = 0;

	if (unlink(path) < 0)
	{
		*primaryError = errno;
	}

	pfree(dbPath);
	pfree(path);

	if (primaryFilespaceLocation != NULL)
		pfree(primaryFilespaceLocation);

	if (mirrorFilespaceLocation != NULL)
		pfree(mirrorFilespaceLocation);
}

/* ----------------------------------------------------------------------------- */
/*  Append */
/* ----------------------------------------------------------------------------- */

/*
 * Write bulk mirrored.
 */
void
MirroredAppendOnly_Append(
						  MirroredAppendOnlyOpen *open,
 /* The open struct. */

						  void *buffer,
 /* Pointer to the buffer. */

						  int32 bufferLen,
 /* Byte length of buffer. */

						  int *primaryError,

						  bool *mirrorDataLossOccurred)
{
	MIRRORED_LOCK_DECLARE;

	Assert(open != NULL);
	Assert(open->isActive);

	*primaryError = 0;
	*mirrorDataLossOccurred = false;

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_LOCK;
	}

#ifdef USE_SEGWALREP
	/*
	 * Using FileSeek to fetch the current write offset. Passing 0 offset
	 * with SEEK_CUR avoids actual disk-io, as it just returns from
	 * VFDCache the current file position value. Make sure to populate
	 * this before the FileWrite call else the file pointer has moved
	 * forward.
	 */
	int64 offset = FileSeek(open->primaryFile, 0, SEEK_CUR);
#endif

	errno = 0;

	if ((int) FileWrite(open->primaryFile, buffer, bufferLen) != bufferLen)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		*primaryError = errno;
	}
#ifdef USE_SEGWALREP
	else
	{
		/*
		 * Log each varblock to the XLog. Write to the file first, before
		 * writing the WAL record, to avoid trouble if you run out of disk
		 * space. If WAL record is written first, and then the FileWrite()
		 * fails, there's no way to "undo" the WAL record. If crash
		 * happens, crash recovery will also try to replay the WAL record,
		 * and will also run out of disk space, and will fail. As EOF
		 * controls the visibility of data in AO / CO files, writing xlog
		 * record after writing to file works fine.
		 */
		xlog_ao_insert(open, offset, buffer, bufferLen);
	}
#endif

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_UNLOCK;
	}

	*mirrorDataLossOccurred = open->mirrorDataLossOccurred;
	/* Keep reporting-- it may have occurred anytime during the open session. */

}

/* ----------------------------------------------------------------------------- */
/*  Truncate */
/* ---------------------------------------------------------------------------- */
void
MirroredAppendOnly_Truncate(
							MirroredAppendOnlyOpen *open,
 /* The open struct. */
							int64 position,
 /* The position to cutoff the data. */

							int *primaryError,

							bool *mirrorDataLossOccurred)
{
	MIRRORED_LOCK_DECLARE;

	*primaryError = 0;
	*mirrorDataLossOccurred = false;

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_LOCK;
	}

	errno = 0;
	if (FileTruncate(open->primaryFile, position) < 0)
		*primaryError = errno;

	if (open->guardOtherCallsWithMirroredLock)
	{
		MIRRORED_UNLOCK;
	}

	*mirrorDataLossOccurred = open->mirrorDataLossOccurred;
	/* Keep reporting-- it may have occurred anytime during the open session. */
}

#ifdef USE_SEGWALREP
/*
 * Insert an AO XLOG/AOCO record
 */
static void xlog_ao_insert(MirroredAppendOnlyOpen *open, int64 offset,
						   void *buffer, int32 bufferLen)
{
	xl_ao_insert	xlaoinsert;
	XLogRecData		rdata[2];

	xlaoinsert.target.node = open->relFileNode;
	xlaoinsert.target.segment_filenum = open->segmentFileNum;
	xlaoinsert.target.offset = offset;

	rdata[0].data = (char*) &xlaoinsert;
	rdata[0].len = SizeOfAOInsert;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	rdata[1].data = (char*) buffer;
	rdata[1].len = bufferLen;
	rdata[1].buffer = InvalidBuffer;
	rdata[1].next = NULL;

	XLogInsert(RM_APPEND_ONLY_ID, XLOG_APPENDONLY_INSERT, rdata);
}

void
ao_insert_replay(XLogRecord *record)
{
	char	   *primaryFilespaceLocation;
	char	   *mirrorFilespaceLocation;
	char		dbPath[MAXPGPATH + 1];
	char		path[MAXPGPATH + 1];
	int			written_len;
	int64		seek_offset;
	File		file;

	xl_ao_insert *xlrec = (xl_ao_insert *) XLogRecGetData(record);
	char	   *buffer = (char *) xlrec + SizeOfAOInsert;
	uint32		len = record->xl_len - SizeOfAOInsert;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(
							   xlrec->target.node.spcNode,
							   &primaryFilespaceLocation,
							   &mirrorFilespaceLocation);

	FormDatabasePath(
			 dbPath,
			 primaryFilespaceLocation,
			 xlrec->target.node.spcNode,
			 xlrec->target.node.dbNode);

	if (xlrec->target.segment_filenum == 0)
		snprintf(path, MAXPGPATH, "%s/%u", dbPath, xlrec->target.node.relNode);
	else
		snprintf(path, MAXPGPATH, "%s/%u.%u", dbPath, xlrec->target.node.relNode, xlrec->target.segment_filenum);

	file = PathNameOpenFile(path, O_RDWR | PG_BINARY, 0600);
	if (file < 0)
	{
		XLogAOSegmentFile(xlrec->target.node, xlrec->target.segment_filenum);
		return;
	}

	seek_offset = FileSeek(file, xlrec->target.offset, SEEK_SET);
	if (seek_offset != xlrec->target.offset)
	{
		/*
		 * FIXME: If non-existance of the segment file is handled by recording
		 * it as invalid using XLogAOSegmentFile, should this case also behave
		 * that way?  See GitHub issue
		 *    https://github.com/greenplum-db/gpdb/issues/3843
		 *
		 * Note: heap redo routines treat a non existant file and a non
		 * existant block within a file as identical.  See XLogReadBuffer.
		 */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("seeked to position " INT64_FORMAT " but expected to seek to position " INT64_FORMAT " in file \"%s\": %m",
						seek_offset,
						xlrec->target.offset,
						path)));
	}

	written_len = FileWrite(file, buffer, len);
	if (written_len < 0 || written_len != len)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("failed to write %d bytes in file \"%s\": %m",
						len,
						path)));
	}

	if (FileSync(file) != 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("failed to flush file \"%s\": %m",
						path)));
	}

	FileClose(file);
}

/*
 * AO/CO truncate xlog record insertion.
 */
void xlog_ao_truncate(MirroredAppendOnlyOpen *open, int64 offset)
{
	xl_ao_truncate	xlaotruncate;
	XLogRecData		rdata[1];

	xlaotruncate.target.node = open->relFileNode;
	xlaotruncate.target.segment_filenum = open->segmentFileNum;
	xlaotruncate.target.offset = offset;

	rdata[0].data = (char*) &xlaotruncate;
	rdata[0].len = sizeof(xl_ao_truncate);
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = NULL;

	XLogInsert(RM_APPEND_ONLY_ID, XLOG_APPENDONLY_TRUNCATE, rdata);
}

void
ao_truncate_replay(XLogRecord *record)
{
	char *primaryFilespaceLocation;
	char *mirrorFilespaceLocation;
	char dbPath[MAXPGPATH + 1];
	char path[MAXPGPATH + 1];
	File file;

	xl_ao_truncate *xlrec = (xl_ao_truncate*) XLogRecGetData(record);

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(
		xlrec->target.node.spcNode,
		&primaryFilespaceLocation,
		&mirrorFilespaceLocation);

	FormDatabasePath(
				dbPath,
				primaryFilespaceLocation,
				xlrec->target.node.spcNode,
				xlrec->target.node.dbNode);

	if (xlrec->target.segment_filenum == 0)
		snprintf(path, MAXPGPATH, "%s/%u", dbPath, xlrec->target.node.relNode);
	else
		snprintf(path, MAXPGPATH, "%s/%u.%u", dbPath, xlrec->target.node.relNode, xlrec->target.segment_filenum);

	file = PathNameOpenFile(path, O_RDWR | PG_BINARY, 0600);
	if (file < 0)
	{
		XLogAOSegmentFile(xlrec->target.node, xlrec->target.segment_filenum);
		return;
	}

	if (FileTruncate(file, xlrec->target.offset) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("failed to truncate file \"%s\" to offset:" INT64_FORMAT " : %m",
						path, xlrec->target.offset)));
	}

	FileClose(file);
}
#endif /* USE_SEGWALREP */
