/*-------------------------------------------------------------------------
 *
 * cdbmirroredappendonly.h
 *
 * Portions Copyright (c) 2009-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbmirroredappendonly.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBMIRROREDAPPENDONLY_H
#define CDBMIRROREDAPPENDONLY_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"

/*
 * This module is for doing mirrored writes for Append-Only relation files privately being written
 * by a backend process.
 *
 * It is intended for Append-Only relation files not under the management of the Buffer Pool.
 */

/*
 * This structure contains write open information.  Consider the fields
 * inside to be private.
 */
typedef struct MirroredAppendOnlyOpen
{
	bool	isActive;

	RelFileNode	relFileNode;
	
	uint32		segmentFileNum;
	
	File		primaryFile;

} MirroredAppendOnlyOpen;

// -----------------------------------------------------------------------------
// Open, Flush, and Close 
// -----------------------------------------------------------------------------

/*
 * Flush an Append-Only relation file.
 */
extern void MirroredAppendOnly_Flush(MirroredAppendOnlyOpen *open,
									 int *primaryError);

/*
 * Close an Append-Only relation file.
 */
extern void MirroredAppendOnly_Close(MirroredAppendOnlyOpen *open);


extern void MirroredAppendOnly_Drop(
	RelFileNode					*relFileNode,
	int32						segmentFileNum,
	int							*primaryError);

// -----------------------------------------------------------------------------
// Append 
// -----------------------------------------------------------------------------
				
/*
 * Append mirrored.
 */
extern void MirroredAppendOnly_Append(
	MirroredAppendOnlyOpen *open,
				/* The open struct. */

	void		*appendData,
				/* Pointer to the Append-Only data. */

	int32		appendDataLen,
	
				/* The byte length of the Append-Only data. */
	int 		*primaryError);

extern void ao_create_segfile_replay(XLogRecord *record);
extern void ao_insert_replay(XLogRecord *record);
extern void xlog_ao_insert(RelFileNode relFileNode, int32 segmentFileNum,
			   int64 offset, void *buffer, int32 bufferLen);
extern void xlog_ao_truncate(MirroredAppendOnlyOpen *open, int64 offset);
extern void ao_truncate_replay(XLogRecord *record);

#endif   /* CDBMIRROREDAPPENDONLY_H */
