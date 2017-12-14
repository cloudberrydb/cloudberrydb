/*-------------------------------------------------------------------------
 *
 * cdbmirroredfilesysobj.h
 *	  Create and drop mirrored files and directories.
 *
 * Portions Copyright (c) 2009-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbmirroredfilesysobj.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBMIRROREDFILESYSOBJ_H
#define CDBMIRROREDFILESYSOBJ_H

#include "storage/relfilenode.h"
#include "storage/dbdirnode.h"
#include "storage/smgr.h"

extern void MirroredFileSysObj_TransactionCreateAppendOnlyFile(
	RelFileNode 			*relFileNode,

	int32					segmentFileNum,

	char					*relationName);

#endif	/*  CDBMIRROREDFILESYSOBJ_H */
