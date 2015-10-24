/*-------------------------------------------------------------------------
 * cdblogsync.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBLOGSYNC_H
#define CDBLOGSYNC_H

#include "catalog/pg_control.h"

/* sync commands */
typedef enum
{
	SYNC_POSITION_TO_END,			/* position to WAL end */
	SYNC_XLOG,						/* xlog sync */
	SYNC_NEW_CHECKPOINT_LOC,		/* new checkpoint location */
	SYNC_SHUTDOWN_TOO_FAR_BEHIND,	/* shutdown -- too far behind to synchronize */
	SYNC_CLOSE						/* close */
}	syncCommand;

extern void cdb_init_log_sync(void);
extern bool cdb_sync_command(const char *);
extern void cdb_shutdown_too_far_behind(void);
extern void cdb_perform_redo(XLogRecPtr *redoCheckPointLoc, CheckPoint *redoCheckPoint, XLogRecPtr *newCheckpointLoc);

#endif   /* CDBLOGSYNC_H */
