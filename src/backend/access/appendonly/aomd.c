/*-------------------------------------------------------------------------
 *
 * aomd.c
 *	  This code manages append only relations that reside on magnetic disk.
 *	  It serves the same general purpose as smgr/md.c however we introduce
 *    AO specific file access functions mainly because would like to bypass 
 *	  md.c's and bgwriter's fsyncing. AO relations also use a non constant
 *	  block number to file segment mapping unlike heap relations.
 *
 *	  As of now we still let md.c create and unlink AO relations for us. This
 *	  may need to change if inconsistencies arise.
 *
 * Portions Copyright (c) 2008, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/appendonly/aomd.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "access/aomd.h"
#include "access/aocssegfiles.h"
#include "access/appendonlytid.h"
#include "access/appendonlywriter.h"
#include "access/appendonly_compaction.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/pg_appendonly.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbappendonlyxlog.h"
#include "crypto/bufenc.h"
#include "commands/progress.h"
#include "common/relpath.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/sync.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"

#define SEGNO_SUFFIX_LENGTH 12

static void mdunlink_ao_base_relfile(void *ctx);
static bool mdunlink_ao_perFile(const int segno, void *ctx);
static bool copy_append_only_data_perFile(const int segno, void *ctx);
static bool truncate_ao_perFile(const int segno, void *ctx);
static uint64 ao_segfile_get_physical_size(Relation aorel, int segno, int col);

int
AOSegmentFilePathNameLen(Relation rel)
{
	char		*basepath;
	int 		len;
		
	/* Get base path for this relation file */
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	/*
	 * The basepath will be the RelFileNode number.  Optional part is dot "." plus 
	 * 6 digit segment file number.
	 */
	len = strlen(basepath) + 8;	// Generous.
	
	pfree(basepath);

	return len;
}

/*
 * Formats an Append Only relation file segment file name.
 *
 * The filepathname parameter assume sufficient space.
 */
void
FormatAOSegmentFileName(char *basepath,
						int segno,
						int col,
						int32 *fileSegNo,
						char *filepathname)
{
	int	pseudoSegNo;
	
	Assert(segno >= 0);
	Assert(segno <= AOTupleId_MaxSegmentFileNum);

	if (col < 0)
	{
		/*
		 * Row oriented Append-Only.
		 */
		pseudoSegNo = segno;		
	}
	else
	{
		/*
		 * Column oriented Append-only.
		 */
		pseudoSegNo = (col*AOTupleId_MultiplierSegmentFileNum) + segno;
	}
	
	*fileSegNo = pseudoSegNo;

	if (pseudoSegNo > 0)
	{
		sprintf(filepathname, "%s.%u", basepath, pseudoSegNo);
	}
	else
		strcpy(filepathname, basepath);
}

/*
 * Make an Append Only relation file segment file name.
 *
 * The filepathname parameter assume sufficient space.
 */
void
MakeAOSegmentFileName(Relation rel,
					  int segno,
					  int col,
					  int32 *fileSegNo,
					  char *filepathname)
{
	char	*basepath;
	int32   fileSegNoLocal;
	
	/* Get base path for this relation file */
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	FormatAOSegmentFileName(basepath, segno, col, &fileSegNoLocal, filepathname);
	
	*fileSegNo = fileSegNoLocal;
	
	pfree(basepath);
}

/*
 * Open an Append Only relation file segment
 *
 * The fd module's PathNameOpenFile() is used to open the file, so the
 * the File* routines can be used to read, write, close, etc, the file.
 */
File
OpenAOSegmentFile(Relation rel, 
				  char *filepathname, 
				  int64	logicalEof)
{
	int			fileFlags = O_RDWR | PG_BINARY;
	File		fd;

	errno = 0;

	RelationOpenSmgr(rel);

	fd = rel->rd_smgr->smgr_ao->smgr_AORelOpenSegFile(filepathname, fileFlags);
	if (fd < 0)
	{
		if (logicalEof == 0 && errno == ENOENT)
			return -1;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open Append-Only segment file \"%s\": %m",
						filepathname)));
	}
	return fd;
}


/*
 * Close an Append Only relation file segment
 */
void
CloseAOSegmentFile(File fd, Relation rel)
{
	Assert(fd > 0);
	RelationOpenSmgr(rel);

	rel->rd_smgr->smgr_ao->smgr_FileClose(fd);
}

/*
 * Truncate all bytes from offset to end of file.
 */
void
TruncateAOSegmentFile(File fd, Relation rel, int32 segFileNum, int64 offset, AOVacuumRelStats *vacrelstats)
{
	char *relname = RelationGetRelationName(rel);
	int64 filesize_before;

	Assert(fd > 0);
	Assert(offset >= 0);

	RelationOpenSmgr(rel);

	filesize_before = rel->rd_smgr->smgr_ao->smgr_FileSize(fd);
	if (filesize_before < offset)
		ereport(ERROR,
				(errmsg("\"%s\": file size smaller than logical eof: %m",
						relname)));

	/*
	 * Call the 'fd' module with a 64-bit length since AO segment files
	 * can be multi-gigabyte to the terabytes...
	 */
	if (rel->rd_smgr->smgr_ao->smgr_FileTruncate(fd, offset, WAIT_EVENT_DATA_FILE_TRUNCATE) != 0)
		ereport(ERROR,
				(errmsg("\"%s\": failed to truncate data after eof: %m",
					    relname)));
	if (vacrelstats)
	{
		/* report heap-equivalent blocks vacuumed */
		vacrelstats->nbytes_truncated += filesize_before - offset;
		pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED,
									 RelationGuessNumberOfBlocksFromSize(vacrelstats->nbytes_truncated));
	}

	if (XLogIsNeeded() && RelationNeedsWAL(rel))
		xlog_ao_truncate(rel->rd_node, segFileNum, offset);

	SIMPLE_FAULT_INJECTOR("appendonly_after_truncate_segment_file");

	if (file_truncate_hook)
	{
		RelFileNodeBackend rnode;
		rnode.node = rel->rd_node;
		rnode.backend = rel->rd_backend;
		(*file_truncate_hook)(rnode);
	}
}

struct mdunlink_ao_callback_ctx
{
	RelFileNode rnode; /* used to register forget request */
	char *segPath;
	char *segpathSuffixPosition;
	bool isRedo;
};

struct truncate_ao_callback_ctx
{
	char *segPath;
	char *segpathSuffixPosition;
	Relation rel;
};

void
mdunlink_ao(RelFileNodeBackend rnode, ForkNumber forkNumber, bool isRedo)
{
	const char *path = relpath(rnode, forkNumber);

	/*
	 * Unlogged AO tables have INIT_FORK, in addition to MAIN_FORK.  It is
	 * created once, regardless of the number of segment files (or the number
	 * of columns for column-oriented tables).  Sync requests for INIT_FORKs
	 * are not remembered, so they need not be forgotten.
	 */
	if (forkNumber == INIT_FORKNUM)
	{
		path = relpath(rnode, forkNumber);
		if (unlink(path) < 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", path)));
	}
	/* This storage manager is not concerned with forks other than MAIN_FORK */
	else if (forkNumber == MAIN_FORKNUM)
	{
		int pathSize = strlen(path);
		char *segPath = (char *) palloc(pathSize + SEGNO_SUFFIX_LENGTH);
		char *segPathSuffixPosition = segPath + pathSize;
		struct mdunlink_ao_callback_ctx unlinkFiles;
		unlinkFiles.isRedo = isRedo;
		unlinkFiles.rnode = rnode.node;

		strncpy(segPath, path, pathSize);

		unlinkFiles.segPath = segPath;
		unlinkFiles.segpathSuffixPosition = segPathSuffixPosition;

		mdunlink_ao_base_relfile(&unlinkFiles);

		ao_foreach_extent_file(mdunlink_ao_perFile, &unlinkFiles);

		pfree(segPath);
	}

	pfree((void *) path);
}

/*
 * Delete or truncate segfile 0.  Note: There is no <relfilenode>.0 file.  The
 * segfile 0 is the same as base relfilenode for row-oriented AO.  For
 * column-oriented AO, the segno 0 for the first column corresponds to base
 * relfilenode.  See also: ao_foreach_extent_file.
 */
static void
mdunlink_ao_base_relfile(void *ctx)
{
	FileTag tag;
	struct mdunlink_ao_callback_ctx *unlinkFiles =
		(struct mdunlink_ao_callback_ctx *)ctx;

	const char *baserel = unlinkFiles->segPath;

	*unlinkFiles->segpathSuffixPosition = '\0';
	if (unlinkFiles->isRedo)
	{
		/* First, forget any pending sync requests for the first segment */
		INIT_FILETAG(tag, unlinkFiles->rnode, MAIN_FORKNUM, 0,
					 SYNC_HANDLER_AO);
		RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true);

		if (unlink(baserel) != 0)
		{
			/* ENOENT is expected after the end of the extensions */
			if (errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m",
								baserel)));
		}
	}
	else
	{
		int			fd;
		int			ret;


		/* Register request to unlink first segment later */
		INIT_FILETAG(tag, unlinkFiles->rnode, MAIN_FORKNUM, 0,
					 SYNC_HANDLER_AO);
		RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */ );

		fd = OpenTransientFile(baserel, O_RDWR | PG_BINARY);
		if (fd >= 0)
		{
			int			save_errno;

			ret = ftruncate(fd, 0);
			save_errno = errno;
			CloseTransientFile(fd);
			errno = save_errno;
		}
		else
			ret = -1;

		if (ret < 0 && errno != ENOENT)
		{
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not truncate file \"%s\": %m", baserel)));

		}
	}
}

static bool
mdunlink_ao_perFile(const int segno, void *ctx)
{
	FileTag tag;
	const struct mdunlink_ao_callback_ctx *unlinkFiles = ctx;

	char *segPath = unlinkFiles->segPath;
	char *segPathSuffixPosition = unlinkFiles->segpathSuffixPosition;

	Assert (segno > 0);
	sprintf(segPathSuffixPosition, ".%u", segno);

	/* First, forget any pending sync requests for the first segment */
	INIT_FILETAG(tag, unlinkFiles->rnode, MAIN_FORKNUM, segno,
				 SYNC_HANDLER_AO);
	RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true);

	/* Next unlink the file */
	if (unlink(segPath) != 0)
	{
		/* ENOENT is expected after the end of the extensions */
		if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", segPath)));
		else
			return false;
	}
	return true;
}

static void
copy_file(char *srcsegpath, char *dstsegpath,
		  RelFileNode dst, SMgrRelation srcSMGR, SMgrRelation dstSMGR,
		  int segfilenum, bool use_wal)
{
	File		srcFile;
	File		dstFile;
	int64		left;
	off_t		offset;
	char       *buffer = palloc(BLCKSZ);
	int dstflags;

	srcFile = srcSMGR->smgr_ao->smgr_AORelOpenSegFile(srcsegpath, O_RDONLY | PG_BINARY);
	if (srcFile < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not open file %s: %m", srcsegpath))));

	dstflags = O_WRONLY | O_EXCL | PG_BINARY;
	/*
	 * .0 relfilenode is expected to exist before calling this
	 * function. Caller calls RelationCreateStorage() which creates the base
	 * file for the relation. Hence use different flag for the same.
	 */
	if (segfilenum)
		dstflags |= O_CREAT;

	dstFile = dstSMGR->smgr_ao->smgr_AORelOpenSegFile(dstsegpath, dstflags);
	if (dstFile < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not create destination file %s: %m", dstsegpath))));

	left = srcSMGR->smgr_ao->smgr_FileDiskSize(srcFile);
	if (left < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not seek to end of file %s: %m", srcsegpath))));

	offset = 0;
	while(left > 0)
	{
		int			len;

		CHECK_FOR_INTERRUPTS();

		len = Min(left, BLCKSZ);
		if (srcSMGR->smgr_ao->smgr_FileRead(srcFile, buffer, len, offset, WAIT_EVENT_DATA_FILE_READ) != len)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read %d bytes from file \"%s\": %m",
							len, srcsegpath)));

		if (dstSMGR->smgr_ao->smgr_FileWrite(dstFile, buffer, len, offset, WAIT_EVENT_DATA_FILE_WRITE) != len)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write %d bytes to file \"%s\": %m",
							len, dstsegpath)));
		if (use_wal)
			xlog_ao_insert(dst, segfilenum, offset, buffer, len);

		offset += len;
		left -= len;
	}

	if (dstSMGR->smgr_ao->smgr_FileSync(dstFile, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						dstsegpath)));
	srcSMGR->smgr_ao->smgr_FileClose(srcFile);
	dstSMGR->smgr_ao->smgr_FileClose(dstFile);
	pfree(buffer);
}

struct copy_append_only_data_callback_ctx {
	char *srcPath;
	char *dstPath;
	SMgrRelation srcSMGR;
	SMgrRelation dstSMGR;
	RelFileNode src;
	RelFileNode dst;
	bool useWal;
};

/*
 * Like copy_relation_data(), but for AO tables.
 *
 */
void
copy_append_only_data(RelFileNode src, RelFileNode dst,
		SMgrRelation srcSMGR, SMgrRelation dstSMGR,
        BackendId backendid, char relpersistence)
{
	char *srcPath;
	char *dstPath;
	bool useWal;
	struct copy_append_only_data_callback_ctx copyFiles = { 0 };
	/*
	 * We need to log the copied data in WAL iff WAL archiving/streaming is
	 * enabled AND it's a permanent relation.
	 */
	useWal = XLogIsNeeded() && relpersistence == RELPERSISTENCE_PERMANENT;

	srcPath = relpathbackend(src, backendid, MAIN_FORKNUM);
	dstPath = relpathbackend(dst, backendid, MAIN_FORKNUM);

	copy_file(srcPath, dstPath, dst, srcSMGR, dstSMGR, 0, useWal);

	copyFiles.srcPath = srcPath;
	copyFiles.dstPath = dstPath;
	copyFiles.srcSMGR = srcSMGR;
	copyFiles.dstSMGR = dstSMGR;
	copyFiles.src = src;
	copyFiles.dst = dst;
	copyFiles.useWal = useWal;

    ao_foreach_extent_file(copy_append_only_data_perFile, &copyFiles);

	if (file_extend_hook)
	{
		RelFileNodeBackend rnode;
		rnode.node = dst;
		rnode.backend = backendid;
		(*file_extend_hook)(rnode);
	}
}

static bool
copy_append_only_data_perFile(const int segno, void *ctx)
{
	const struct copy_append_only_data_callback_ctx *copyFiles = ctx;

	char srcSegPath[MAXPGPATH + 12];
	char dstSegPath[MAXPGPATH + 12];

	sprintf(srcSegPath, "%s.%u", copyFiles->srcPath, segno);
	if (access(srcSegPath, F_OK) != 0)
	{
		/* ENOENT is expected after the end of the extensions */
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("access failed for file \"%s\": %m", srcSegPath)));
		return false;
	}
	sprintf(dstSegPath, "%s.%u", copyFiles->dstPath, segno);
	copy_file(srcSegPath, dstSegPath, copyFiles->dst, copyFiles->srcSMGR, copyFiles->dstSMGR, segno, copyFiles->useWal);

	return true;
}

/*
 * ao_truncate_one_rel
 *
 * This routine deletes all data within the specified ao relation.
 */
void
ao_truncate_one_rel(Relation rel)
{
	char *basepath;
	char *segPath;
	char *segPathSuffixPosition;
	struct truncate_ao_callback_ctx truncateFiles = { 0 };
	int pathSize;

	/* Get base path for this relation file */
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	pathSize = strlen(basepath);
	segPath = (char *) palloc(pathSize + SEGNO_SUFFIX_LENGTH);
	segPathSuffixPosition = segPath + pathSize;
	strncpy(segPath, basepath, pathSize);

	truncateFiles.segPath = segPath;
	truncateFiles.segpathSuffixPosition = segPathSuffixPosition;
	truncateFiles.rel = rel;

	/*
	 * Truncate the actual file.
	 *
	 * Segfile 0 first, ao_foreach_extent_file() doesn't invoke the
	 * callback for it.
	 */
	truncate_ao_perFile(0, &truncateFiles);
	ao_foreach_extent_file(truncate_ao_perFile, &truncateFiles);

	pfree(segPath);
	pfree(basepath);
}

/*
 * Truncate a specific segment file of ao relation.
 */
static bool
truncate_ao_perFile(const int segno, void *ctx)
{
	File		fd;
	Relation aorel;

	const struct truncate_ao_callback_ctx *truncateFiles = ctx;

	char *segPath = truncateFiles->segPath;
	char *segPathSuffixPosition = truncateFiles->segpathSuffixPosition;
	aorel = truncateFiles->rel;

	if (segno > 0)
		sprintf(segPathSuffixPosition, ".%u", segno);
	else
		*segPathSuffixPosition = '\0';

	fd = OpenAOSegmentFile(aorel, segPath, 0);

	if (fd >= 0)
	{
		TruncateAOSegmentFile(fd, aorel, segno, 0, NULL);
		CloseAOSegmentFile(fd, aorel);
	}
	else
	{
		/* 
		 * we traverse possible segment files of AO/AOCS tables and call
		 * truncate_ao_perFile to truncate them. It is ok that some files do not exist
		 */
		return false;
	}

	return true;
}

/*
 * Returns the total of segment files' on-disk size for an AO/AOCO relation.
 * This is only used by AO vaccum progress reporting.
 */
uint64
ao_rel_get_physical_size(Relation aorel)
{
	Relation	pg_aoseg_rel;
	TupleDesc	pg_aoseg_dsc;
	SysScanDesc aoscan;
	HeapTuple	tuple;
	Snapshot	appendOnlyMetaDataSnapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));
	Oid			segrelid;
	uint64 		total_physical_size = 0;

	Assert(RelationIsAppendOptimized(aorel));

	GetAppendOnlyEntryAuxOids(aorel,
							  &segrelid, NULL, NULL, NULL, NULL);

	pg_aoseg_rel = heap_open(segrelid, AccessShareLock);
	pg_aoseg_dsc = RelationGetDescr(pg_aoseg_rel);

	aoscan = systable_beginscan(pg_aoseg_rel, InvalidOid, false, appendOnlyMetaDataSnapshot, 0, NULL);
	while ((tuple = systable_getnext(aoscan)) != NULL)
	{
		int			segno;
		bool		isNull;

		if (RelationIsAoRows(aorel))
		{
			segno = DatumGetInt32(fastgetattr(tuple,
											  Anum_pg_aoseg_segno,
											  pg_aoseg_dsc, &isNull));
			total_physical_size += ao_segfile_get_physical_size(aorel, segno, -1);
		}
		else
		{
			Datum		d;
			AOCSVPInfo *vpinfo;
			int			col;

			Assert(RelationIsAoCols(aorel));

			segno = DatumGetInt32(fastgetattr(tuple,
											  Anum_pg_aocs_segno,
											  pg_aoseg_dsc, &isNull));
			d = fastgetattr(tuple,
							Anum_pg_aocs_vpinfo,
							pg_aoseg_dsc, &isNull);
			vpinfo = (AOCSVPInfo *) PG_DETOAST_DATUM(d);

			for (col = 0; col < vpinfo->nEntry; ++col)
				total_physical_size += ao_segfile_get_physical_size(aorel, segno, col);

			if (DatumGetPointer(d) != (Pointer) vpinfo)
				pfree(vpinfo);
		}
	}
	systable_endscan(aoscan);

	heap_close(pg_aoseg_rel, AccessShareLock);
	UnregisterSnapshot(appendOnlyMetaDataSnapshot);
	return total_physical_size;
}

static uint64
ao_segfile_get_physical_size(Relation aorel, int segno, int col)
{
	const char *relname;
	File		fd;
	int32		fileSegNo;
	char		filenamepath[MAXPGPATH];
	uint64 		physical_size = 0;

	relname = RelationGetRelationName(aorel);

	MakeAOSegmentFileName(aorel, segno, col, &fileSegNo, filenamepath);
	elogif(Debug_appendonly_print_compaction, LOG,
		   "Opening append-optimized relation \"%s\", relation id %u, relfilenode %lu column #%d, logical segment #%d (physical segment file #%d)",
		   RelationGetRelationName(aorel),
		   aorel->rd_id,
		   aorel->rd_node.relNode,
		   col,
		   segno,
		   fileSegNo);
	fd = PathNameOpenFile(filenamepath, O_RDONLY | PG_BINARY);
	if (fd >= 0)
		physical_size = FileDiskSize(fd);
	else
		elogif(Debug_appendonly_print_compaction, LOG,
			   "No gp_relation_node entry for append-optimized relation \"%s\", relation id %u, relfilenode %lu column #%d, logical segment #%d (physical segment file #%d)",
			   relname,
			   aorel->rd_id,
			   aorel->rd_node.relNode,
			   col,
			   segno,
			   fileSegNo);
	return physical_size;
}
