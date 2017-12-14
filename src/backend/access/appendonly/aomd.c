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
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
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
#include "access/aomd.h"
#include "catalog/catalog.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "utils/guc.h"
#include "access/appendonlytid.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbmirroredappendonly.h"
#include "cdb/cdbpersistenttablespace.h"

int
AOSegmentFilePathNameLen(Relation rel)
{
	char		*basepath;
	int 		len;
		
	/* Get base path for this relation file */
	basepath = relpath(rel->rd_node, MAIN_FORKNUM);

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
FormatAOSegmentFileName(
							char *basepath, 
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
MakeAOSegmentFileName(
							Relation rel, 
							int segno, 
							int col, 
							int32 *fileSegNo,
							char *filepathname)
{
	char	*basepath;
	int32   fileSegNoLocal;
	
	/* Get base path for this relation file */
	basepath = relpath(rel->rd_node, MAIN_FORKNUM);

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
bool
OpenAOSegmentFile(Relation rel, 
				  char *filepathname, 
				  int32	segmentFileNum,
				  int64	logicalEof,
				  MirroredAppendOnlyOpen *mirroredOpen)
{	
	char	   *primaryFilespaceLocation;
	char	   *mirrorFilespaceLocation;
	char	   *dbPath;
	char	   *path;
	int			fileFlags = O_RDWR | PG_BINARY;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(
										rel->rd_node.spcNode,
										&primaryFilespaceLocation,
										&mirrorFilespaceLocation);

	dbPath = (char *) palloc(MAXPGPATH + 1);
	path = (char *) palloc(MAXPGPATH + 1);

	FormDatabasePath(dbPath,
					 primaryFilespaceLocation,
					 rel->rd_node.spcNode,
					 rel->rd_node.dbNode);

	if (segmentFileNum == 0)
		sprintf(path, "%s/%u", dbPath, rel->rd_node.relNode);
	else
		sprintf(path, "%s/%u.%u", dbPath, rel->rd_node.relNode, segmentFileNum);

	errno = 0;

	mirroredOpen->primaryFile = PathNameOpenFile(path, fileFlags, 0600);

	if (mirroredOpen->primaryFile < 0)
	{
		if (logicalEof == 0 && errno == ENOENT)
			return false;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open Append-Only segment file \"%s\": %m",
						filepathname)));
	}
	mirroredOpen->isActive = true;
	pfree(dbPath);
	pfree(path);

	return true;
}


/*
 * Close an Append Only relation file segment
 */
void
CloseAOSegmentFile(MirroredAppendOnlyOpen *mirroredOpen)
{
	bool mirrorDataLossOccurred;	// UNDONE: We need to do something now...

	Assert(mirroredOpen->primaryFile > 0);
	
	MirroredAppendOnly_Close(
						mirroredOpen,
						&mirrorDataLossOccurred);
}

/*
 * Truncate all bytes from offset to end of file.
 */
void
TruncateAOSegmentFile(MirroredAppendOnlyOpen *mirroredOpen, Relation rel, int64 offset)
{
	int primaryError;
	bool mirrorDataLossOccurred;	// We'll look at this at close time.

	char *relname = RelationGetRelationName(rel);
	
	Assert(mirroredOpen->primaryFile > 0);
	Assert(offset >= 0);

	/*
	 * Call the 'fd' module with a 64-bit length since AO segment files
	 * can be multi-gigabyte to the terabytes...
	 */
	MirroredAppendOnly_Truncate(
							mirroredOpen,
							offset,
							&primaryError,
							&mirrorDataLossOccurred);
	if (primaryError != 0)
		ereport(ERROR,
				(errmsg("\"%s\": failed to truncate data after eof: %s", 
					    relname,
					    strerror(primaryError))));
#ifdef USE_SEGWALREP
	if (!rel->rd_istemp)
		xlog_ao_truncate(mirroredOpen, offset);
#endif
}
