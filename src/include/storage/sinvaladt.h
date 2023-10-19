/*-------------------------------------------------------------------------
 *
 * sinvaladt.h
 *	  POSTGRES shared cache invalidation data manager.
 *
 * The shared cache invalidation manager is responsible for transmitting
 * invalidation messages between backends.  Any message sent by any backend
 * must be delivered to all already-running backends before it can be
 * forgotten.  (If we run out of space, we instead deliver a "RESET"
 * message to backends that have fallen too far behind.)
 *
 * The struct type SharedInvalidationMessage, defining the contents of
 * a single message, is defined in sinval.h.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/sinvaladt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SINVALADT_H
#define SINVALADT_H

#include "storage/lock.h"
#include "storage/sinval.h"


/*
 * Configurable parameters.
 *
 * MAXNUMMESSAGES: max number of shared-inval messages we can buffer.
 * Must be a power of 2 for speed.
 *
 * MSGNUMWRAPAROUND: how often to reduce MsgNum variables to avoid overflow.
 * Must be a multiple of MAXNUMMESSAGES.  Should be large.
 *
 * CLEANUP_MIN: the minimum number of messages that must be in the buffer
 * before we bother to call SICleanupQueue.
 *
 * CLEANUP_QUANTUM: how often (in messages) to call SICleanupQueue once
 * we exceed CLEANUP_MIN.  Should be a power of 2 for speed.
 *
 * SIG_THRESHOLD: the minimum number of messages a backend must have fallen
 * behind before we'll send it PROCSIG_CATCHUP_INTERRUPT.
 *
 * WRITE_QUANTUM: the max number of messages to push into the buffer per
 * iteration of SIInsertDataEntries.  Noncritical but should be less than
 * CLEANUP_QUANTUM, because we only consider calling SICleanupQueue once
 * per iteration.
 */

#define MAXNUMMESSAGES 4096
#define MSGNUMWRAPAROUND (MAXNUMMESSAGES * 262144)
#define CLEANUP_MIN (MAXNUMMESSAGES / 2)
#define CLEANUP_QUANTUM (MAXNUMMESSAGES / 16)
#define SIG_THRESHOLD (MAXNUMMESSAGES / 2)
#define WRITE_QUANTUM 64

/* Per-backend state in shared invalidation structure */
typedef struct ProcState
{
    /* procPid is zero in an inactive ProcState array entry. */
    pid_t		procPid;		/* PID of backend, for signaling */
    PGPROC	   *proc;			/* PGPROC of backend */
    /* nextMsgNum is meaningless if procPid == 0 or resetState is true. */
    int			nextMsgNum;		/* next message number to read */
    bool		resetState;		/* backend needs to reset its state */
    bool		signaled;		/* backend has been sent catchup signal */
    bool		hasMessages;	/* backend has unread messages */

    /*
     * Backend only sends invalidations, never receives them. This only makes
     * sense for Startup process during recovery because it doesn't maintain a
     * relcache, yet it fires inval messages to allow query backends to see
     * schema changes.
     */
    bool		sendOnly;		/* backend only sends, never receives */

    /*
     * Next LocalTransactionId to use for each idle backend slot.  We keep
     * this here because it is indexed by BackendId and it is convenient to
     * copy the value to and from local memory when MyBackendId is set. It's
     * meaningless in an active ProcState entry.
     */
    LocalTransactionId nextLXID;
} ProcState;

/* Shared cache invalidation memory segment */
typedef struct SISeg
{
    /*
     * General state information
     */
    int			minMsgNum;		/* oldest message still needed */
    int			maxMsgNum;		/* next message number to be assigned */
    int			nextThreshold;	/* # of messages to call SICleanupQueue */
    int			lastBackend;	/* index of last active procState entry, +1 */
    int			maxBackends;	/* size of procState array */

    slock_t		msgnumLock;		/* spinlock protecting maxMsgNum */

    /*
     * Circular buffer holding shared-inval messages
     */
    SharedInvalidationMessage buffer[MAXNUMMESSAGES];

    /*
     * Per-backend invalidation state info (has MaxBackends entries).
     */
    ProcState	procState[FLEXIBLE_ARRAY_MEMBER];
} SISeg;

extern SISeg *shmInvalBuffer;	/* pointer to the shared inval buffer */


extern LocalTransactionId nextLocalTransactionId;

/*
 * prototypes for functions in sinvaladt.c
 */
extern Size SInvalShmemSize(void);
extern void CreateSharedInvalidationState(void);
extern void SharedInvalBackendInit(bool sendOnly);
extern PGPROC *BackendIdGetProc(int backendID);
extern void BackendIdGetTransactionIds(int backendID, TransactionId *xid, TransactionId *xmin);

extern void SIInsertDataEntries(const SharedInvalidationMessage *data, int n);
extern int	SIGetDataEntries(SharedInvalidationMessage *data, int datasize);
extern void SICleanupQueue(bool callerHasWriteLock, int minFree);

extern LocalTransactionId GetNextLocalTransactionId(void);

#endif							/* SINVALADT_H */
