/*-------------------------------------------------------------------------
 *
 * bgwriter.h
 *	  Exports from postmaster/bgwriter.c.
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 *
 * $PostgreSQL: pgsql/src/include/postmaster/bgwriter.h,v 1.8 2006/03/05 15:58:58 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef _BGWRITER_H
#define _BGWRITER_H

#include "storage/block.h"
#include "storage/relfilenode.h"


/* GUC options */
extern int	BgWriterDelay;

extern void BackgroundWriterMain(void);

extern void RequestCheckpointSmgrCloseAll(void);

extern bool ForwardFsyncRequest(RelFileNode rnode, BlockNumber segno);
extern void AbsorbFsyncRequests(void);

extern Size BgWriterShmemSize(void);
extern void BgWriterShmemInit(void);

extern bool AmBackgroundWriterProcess(void);

#endif   /* _BGWRITER_H */
