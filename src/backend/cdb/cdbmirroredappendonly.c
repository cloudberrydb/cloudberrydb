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

#include "utils/palloc.h"
#include "cdb/cdbmirroredappendonly.h"
#include "storage/fd.h"
#include "catalog/catalog.h"
#include "cdb/cdbpersistenttablespace.h"
#include "storage/smgr.h"
#include "storage/smgr_ao.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "cdb/cdbpersistentstore.h"
#include "postmaster/primary_mirror_mode.h"

#include "access/xlogutils.h"
#include "cdb/cdbappendonlyam.h"


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
	Assert(open != NULL);
	Assert(open->isActive);

	errno = 0;

	if (FileSync(open->primaryFile) != 0)
	{
		*primaryError = errno;
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
	Assert(open != NULL);
	Assert(open->isActive);

	/* No primary error to report. */
	errno = 0;

	FileClose(open->primaryFile);

	*mirrorDataLossOccurred = open->mirrorDataLossOccurred;
	/* Keep reporting-- it may have occurred anytime during the open session. */

	open->isActive = false;
	open->primaryFile = 0;
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

						  int *primaryError)
{
	Assert(open != NULL);
	Assert(open->isActive);
	Assert(open->relFileNode.relNode != InvalidOid);

	*primaryError = 0;

	/*
	 * Using FileSeek to fetch the current write offset. Passing 0 offset
	 * with SEEK_CUR avoids actual disk-io, as it just returns from
	 * VFDCache the current file position value. Make sure to populate
	 * this before the FileWrite call else the file pointer has moved
	 * forward.
	 */
	int64 offset = FileSeek(open->primaryFile, 0, SEEK_CUR);

	errno = 0;

	if ((int) FileWrite(open->primaryFile, buffer, bufferLen) != bufferLen)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		*primaryError = errno;
	}
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
		xlog_ao_insert(open->relFileNode, open->segmentFileNum, offset, buffer, bufferLen);
	}
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
	*primaryError = 0;
	*mirrorDataLossOccurred = false;

	errno = 0;
	if (FileTruncate(open->primaryFile, position) < 0)
		*primaryError = errno;

	*mirrorDataLossOccurred = open->mirrorDataLossOccurred;
	/* Keep reporting-- it may have occurred anytime during the open session. */
}

/*
 * Insert an AO XLOG/AOCO record.
 *
 * This is also used with 0 length, to mark creation of a new segfile.
 */
void
xlog_ao_insert(RelFileNode relFileNode, int32 segmentFileNum,
			   int64 offset, void *buffer, int32 bufferLen)
{
	xl_ao_insert	xlaoinsert;
	XLogRecData		rdata[2];

	xlaoinsert.target.node = relFileNode;
	xlaoinsert.target.segment_filenum = segmentFileNum;
	xlaoinsert.target.offset = offset;

	rdata[0].data = (char*) &xlaoinsert;
	rdata[0].len = SizeOfAOInsert;
	rdata[0].buffer = InvalidBuffer;

	if (bufferLen == 0)
		rdata[0].next = NULL;
	else
	{
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char*) buffer;
		rdata[1].len = bufferLen;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = NULL;
	}

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
	int			fileFlags;

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

	fileFlags = O_RDWR | PG_BINARY;

	/* When writing from the beginning of the file, it might not exist yet. Create it. */
	if (xlrec->target.offset == 0)
		fileFlags |= O_CREAT;
	file = PathNameOpenFile(path, fileFlags, 0600);
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
