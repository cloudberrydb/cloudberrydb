/*-------------------------------------------------------------------------
 *
 * walreceiver.h
 *	  Exports from replication/walreceiverfuncs.c.
 *
 * Portions Copyright (c) 2010-2013, PostgreSQL Global Development Group
 *
 * src/include/replication/walreceiver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALRECEIVER_H
#define _WALRECEIVER_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "pgtime.h"

/* user-settable parameters */
extern int	wal_receiver_status_interval;
extern int	wal_receiver_timeout;
extern bool hot_standby_feedback;

/*
 * MAXCONNINFO: maximum size of a connection string.
 *
 * XXX: Should this move to pg_config_manual.h?
 */
#define MAXCONNINFO		1024

/* Can we allow the standby to accept replication connection from another standby? */
#define AllowCascadeReplication() (EnableHotStandby && max_wal_senders > 0)

/*
 * Values for WalRcv->walRcvState.
 */
typedef enum
{
	WALRCV_STOPPED,				/* stopped and mustn't start up again */
	WALRCV_STARTING,			/* launched, but the process hasn't
								 * initialized yet */
	WALRCV_STREAMING,			/* walreceiver is streaming */
	WALRCV_WAITING,				/* stopped streaming, waiting for orders */
	WALRCV_RESTARTING,			/* asked to restart streaming */
	WALRCV_STOPPING				/* requested to stop, but still running */
} WalRcvState;

/* Shared memory area for management of walreceiver process */
typedef struct
{
	/*
	 * PID of currently active walreceiver process, its current state and
	 * start time (actually, the time at which it was requested to be
	 * started).
	 */
	pid_t		pid;
	WalRcvState walRcvState;
	pg_time_t	startTime;

	/*
	 * receiveStart and receiveStartTLI indicate the first byte position and
	 * timeline that will be received. When startup process starts the
	 * walreceiver, it sets these to the point where it wants the streaming to
	 * begin.
	 */
	XLogRecPtr	receiveStart;
	TimeLineID	receiveStartTLI;

	/*
	 * receivedUpto-1 is the last byte position that has already been
	 * received, and receivedTLI is the timeline it came from.	At the first
	 * startup of walreceiver, these are set to receiveStart and
	 * receiveStartTLI. After that, walreceiver updates these whenever it
	 * flushes the received WAL to disk.
	 */
	XLogRecPtr	receivedUpto;
	TimeLineID	receivedTLI;

	/*
	 * latestChunkStart is the starting byte position of the current "batch"
	 * of received WAL.  It's actually the same as the previous value of
	 * receivedUpto before the last flush to disk.	Startup process can use
	 * this to detect whether it's keeping up or not.
	 */
	XLogRecPtr	latestChunkStart;

	/*
	 * Time of send and receive of any message received.
	 */
	TimestampTz lastMsgSendTime;
	TimestampTz lastMsgReceiptTime;

	/*
	 * Latest reported end of WAL on the sender
	 */
	XLogRecPtr	latestWalEnd;
	TimestampTz latestWalEndTime;

	/*
	 * connection string; is used for walreceiver to connect with the primary.
	 */
	char		conninfo[MAXCONNINFO];

	slock_t		mutex;			/* locks shared variables shown above */

	/*
	 * Latch used by startup process to wake up walreceiver after telling it
	 * where to start streaming (after setting receiveStart and
	 * receiveStartTLI).
	 */
	Latch		latch;
} WalRcvData;

extern WalRcvData *WalRcv;

/* libpqwalreceiver hooks */
void walrcv_connect (char *conninfo);

void walrcv_identify_system (TimeLineID *primary_tli);

void walrcv_readtimelinehistoryfile (TimeLineID tli, char **filename, char **content, int *size);

bool walrcv_startstreaming (TimeLineID tli, XLogRecPtr startpoint);

void walrcv_endstreaming (TimeLineID *next_tli);

int walrcv_receive (int timeout, char **buffer);

void walrcv_send (const char *buffer, int nbytes);

void walrcv_disconnect (void);

/* prototypes for functions in walreceiver.c */
extern void WalReceiverMain(void) __attribute__((noreturn));

/* prototypes for functions in walreceiverfuncs.c */
extern Size WalRcvShmemSize(void);
extern void WalRcvShmemInit(void);
extern void ShutdownWalRcv(void);
extern bool WalRcvStreaming(void);
extern bool WalRcvRunning(void);
extern void RequestXLogStreaming(TimeLineID tli, XLogRecPtr recptr, const char *conninfo);
extern XLogRecPtr GetWalRcvWriteRecPtr(XLogRecPtr *latestChunkStart, TimeLineID *receiveTLI);
extern int	GetReplicationApplyDelay(void);
extern int	GetReplicationTransferLatency(void);
extern const char *WalRcvGetStateString(WalRcvState state);
extern XLogRecPtr WaitNextXLogAvailable(XLogRecPtr recptr, bool *finished);

#endif   /* _WALRECEIVER_H */
