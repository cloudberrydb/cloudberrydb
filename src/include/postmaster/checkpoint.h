/*-------------------------------------------------------------------------
 *
 * checkpoint.h
 *	  Exports from postmaster/checkpoint.c.
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef _CHECKPOINT_H
#define _CHECKPOINT_H

/* GUC options */
extern int	CheckPointTimeout;
extern int	CheckPointWarning;

extern void CheckpointMain(void);

extern void RequestCheckpoint(bool waitforit, bool warnontime);

extern Size CheckpointShmemSize(void);
extern void CheckpointShmemInit(void);

extern int checkpoint_start(void);

#endif   /* _CHECKPOINT_H */

