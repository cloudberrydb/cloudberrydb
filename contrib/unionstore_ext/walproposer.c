/*-------------------------------------------------------------------------
 *
 * walproposer.c
 *
 * Proposer/leader part of the total order broadcast protocol between postgres
 * and WAL safekeepers.
 *
 * We have two ways of launching WalProposer:
 *
 *   1. As a background worker which will run physical WalSender with
 *      am_wal_proposer flag set to true. WalSender in turn would handle WAL
 *      reading part and call WalProposer when ready to scatter WAL.
 *
 *   2. As a standalone utility by running `postgres --sync-safekeepers`. That
 *      is needed to create LSN from which it is safe to start postgres. More
 *      specifically it addresses following problems:
 *
 *      a) Chicken-or-the-egg problem: compute postgres needs data directory
 *         with non-rel files that are downloaded from pageserver by calling
 *         basebackup@LSN. This LSN is not arbitrary, it must include all
 *         previously committed transactions and defined through consensus
 *         voting, which happens... in walproposer, a part of compute node.
 *
 *      b) Just warranting such LSN is not enough, we must also actually commit
 *         it and make sure there is a safekeeper who knows this LSN is
 *         committed so WAL before it can be streamed to pageserver -- otherwise
 *         basebackup will hang waiting for WAL. Advancing commit_lsn without
 *         playing consensus game is impossible, so speculative 'let's just poll
 *         safekeepers, learn start LSN of future epoch and run basebackup'
 *         won't work.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include "access/distributedlog.h"
#include "access/slru.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogutils.h"
#include "access/xloginsert.h"
#include "cdb/cdbvars.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "storage/fd.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "libpq/pqformat.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "pagestore_client.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "unionstore_xlog.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#include "unionstore.h"
#include "walproposer.h"
#include "walproposer_utils.h"

static bool syncSafekeepers = false;

char	   *wal_acceptors_list;
int			wal_acceptor_reconnect_timeout;
int			wal_acceptor_connection_timeout;
int         wal_send_timeout;
bool		am_wal_proposer;

char	   *neon_timeline_walproposer = NULL;
char	   *neon_tenant_walproposer = NULL;
char	   *neon_safekeeper_token_walproposer = NULL;

#define WAL_PROPOSER_SLOT_NAME "wal_proposer_slot"

static int	n_safekeepers = 0;
static int	quorum = 0;
static Safekeeper safekeeper[MAX_SAFEKEEPERS];
static XLogRecPtr availableLsn; /* WAL has been generated up to this point */
static XLogRecPtr lastSentCommitLsn;	/* last commitLsn broadcast to*
										 * safekeepers */
static ProposerGreeting greetRequest;
static VoteRequest voteRequest; /* Vote request for safekeeper */
static WaitEventSet *waitEvents;
static AppendResponse quorumFeedback;
/*
 *  Minimal LSN which may be needed for recovery of some safekeeper,
 *  record-aligned (first record which might not yet received by someone).
 */
static XLogRecPtr truncateLsn;

/*
 * Term of the proposer. We want our term to be highest and unique,
 * so we collect terms from safekeepers quorum, choose max and +1.
 * After that our term is fixed and must not change. If we observe
 * that some safekeeper has higher term, it means that we have another
 * running compute, so we must stop immediately.
 */
static term_t propTerm;
static TermHistory propTermHistory; /* term history of the proposer */
static XLogRecPtr propEpochStartLsn;	/* epoch start lsn of the proposer */
static term_t donorEpoch;		/* Most advanced acceptor epoch */
static int	donor;				/* Most advanced acceptor */
static XLogRecPtr timelineStartLsn; /* timeline globally starts at this LSN */
static int	n_votes = 0;
static int	n_connected = 0;
static TimestampTz last_reconnect_attempt;

static WalproposerShmemState * walprop_shared;

/* Prototypes for private functions */
static void WalProposerRegister(void);
static void WalProposerInit(XLogRecPtr flushRecPtr, uint64 systemId);
static void WalProposerStart(void);
static void WalProposerLoop(void);
static void InitEventSet(void);
static void UpdateEventSet(Safekeeper *sk, uint32 events);
static void HackyRemoveWalProposerEvent(Safekeeper *to_remove);
static void ShutdownConnection(Safekeeper *sk);
static void ResetConnection(Safekeeper *sk);
static long TimeToReconnect(TimestampTz now);
static void ReconnectSafekeepers(void);
static void AdvancePollState(Safekeeper *sk, uint32 events);
static void HandleConnectionEvent(Safekeeper *sk);
static void SendStartWALPush(Safekeeper *sk);
static void RecvStartWALPushResult(Safekeeper *sk);
static void SendProposerGreeting(Safekeeper *sk);
static void RecvAcceptorGreeting(Safekeeper *sk);
static void SendVoteRequest(Safekeeper *sk);
static void RecvVoteResponse(Safekeeper *sk);
static void HandleElectedProposer(void);
static term_t GetHighestTerm(TermHistory * th);
static term_t GetEpoch(Safekeeper *sk);
static void DetermineEpochStartLsn(void);
static bool WalProposerRecovery(int donor, TimeLineID timeline, XLogRecPtr startpos, XLogRecPtr endpos);
static void SendProposerElected(Safekeeper *sk);
static void StartStreaming(Safekeeper *sk);
static void SendMessageToNode(Safekeeper *sk);
static void SendMessage(void);
static void HandleActiveState(Safekeeper *sk, uint32 events);
static bool SendAppendRequests(Safekeeper *sk);
static bool RecvAppendResponses(Safekeeper *sk);
static XLogRecPtr CalculateMinFlushLsn(void);
static XLogRecPtr GetAcknowledgedByQuorumWALPosition(void);
static void HandleSafekeeperResponse(void);
static bool AsyncRead(Safekeeper *sk, char **buf, int *buf_size);
static bool AsyncReadMessage(Safekeeper *sk, AcceptorProposerMessage * anymsg);
static bool BlockingWrite(Safekeeper *sk, void *msg, size_t msg_size, SafekeeperState success_state);
static bool AsyncWrite(Safekeeper *sk, void *msg, size_t msg_size, SafekeeperState flush_state);
static bool AsyncFlush(Safekeeper *sk);

static void nwp_shmem_startup_hook(void);
static void nwp_register_gucs(void);
static void nwp_prepare_shmem(void);
static bool UnionStoreStartupXLog(XLogRecPtr startup_lsn);
static void UnionStoreStartup(void);
static int  DistributedLog_ZeroPage_if_needed(SlruCtl ctl, int page);
static void DistributedLog_Extend_if_needed(TransactionId newestXact);
static void CLOG_Extend_if_needed(TransactionId newestXact);
static RelFileNodeId AssignNewRelFileNode(void);
static void UnionStore_start_bgworkers(bool FatalError, int pmState, start_bgworker_func startBgworkerFunc);

static shmem_startup_hook_type prev_shmem_startup_hook_type = NULL;
static Startup_hook_type prev_Startup_hook = NULL;
static start_bgworkers_hook_type prev_start_bgworkers_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void walproposer_shmem_request(void);
#endif

void
pg_init_walproposer(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	nwp_register_gucs();

	nwp_prepare_shmem();

	prev_Startup_hook = Startup_hook;
	Startup_hook = UnionStoreStartup;

	XLogInsert_hook = UnionStoreXLogInsert;

	NewSegRelfilenode_assign_hook = AssignNewRelFileNode;

	WalProposerRegister();
}

/*
 * Entry point for `postgres --sync-safekeepers`.
 */
void
WalProposerSync(int argc, char *argv[])
{
	struct stat stat_buf;

	syncSafekeepers = true;
#if PG_VERSION_NUM < 150000
	ThisTimeLineID = 1;
#endif

	/*
	 * Initialize postmaster_alive_fds as WaitEventSet checks them.
	 *
	 * Copied from InitPostmasterDeathWatchHandle()
	 */
	if (pipe(postmaster_alive_fds) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
					errmsg_internal("could not create pipe to monitor postmaster death: %m")));
	if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) == -1)
		ereport(FATAL,
				(errcode_for_socket_access(),
					errmsg_internal("could not set postmaster death monitoring pipe to nonblocking mode: %m")));

	ChangeToDataDir();

	/* Create pg_wal directory, if it doesn't exist */
	if (stat(XLOGDIR, &stat_buf) != 0)
	{
		ereport(LOG, (errmsg("creating missing WAL directory \"%s\"", XLOGDIR)));
		if (MakePGDirectory(XLOGDIR) < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
						errmsg("could not create directory \"%s\": %m",
							   XLOGDIR)));
			exit(1);
		}
	}

	WalProposerInit(0, 0);

	BackgroundWorkerUnblockSignals();

	WalProposerStart();
}

static void
nwp_register_gucs(void)
{
	DefineCustomStringVariable(
							"unionstore.safekeepers",
							"List of unionstore WAL acceptors (host:port)",
							   NULL,	/* long_desc */
							   &wal_acceptors_list, /* valueAddr */
							   "",	/* bootValue */
							   PGC_POSTMASTER,
							   GUC_LIST_INPUT,	/* extensions can't use*
												 * GUC_LIST_QUOTE */
							   NULL, NULL, NULL);

	DefineCustomIntVariable(
							"unionstore.safekeeper_reconnect_timeout",
							"Timeout for reconnecting to offline wal acceptor.",
							NULL,
							&wal_acceptor_reconnect_timeout,
							30000, 0, INT_MAX,	/* default, min, max */
							PGC_SIGHUP, /* context */
							GUC_UNIT_MS,	/* flags */
							NULL, NULL, NULL);

	DefineCustomIntVariable(
							"unionstore.wal_send_timeout",
                            "Timeout for sending wal to wal service.",
                            NULL,
                            &wal_send_timeout,
                            10, 0, INT_MAX,	/* default, min, max */
                            PGC_SIGHUP, /* context */
                            GUC_UNIT_MS,	/* flags */
                            NULL, NULL, NULL);

	DefineCustomIntVariable(
							"unionstore.safekeeper_connect_timeout",
							"Timeout for connection establishement and it's maintenance against safekeeper",
							NULL,
							&wal_acceptor_connection_timeout,
							5000, 0, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);
}

/* shmem handling */

static void
nwp_prepare_shmem(void)
{
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = walproposer_shmem_request;
#else
	RequestAddinShmemSpace(WalproposerShmemSize());
	RequestAddinShmemSpace(UnionStoreXLOGShmemSize());
	RequestNamedLWLockTranche("LastWrittenLsnLock", 1);
#endif
	prev_shmem_startup_hook_type = shmem_startup_hook;
	shmem_startup_hook = nwp_shmem_startup_hook;
}

#if PG_VERSION_NUM >= 150000
/*
 * shmem_request hook: request additional shared resources.  We'll allocate or
 * attach to the shared resources in nwp_shmem_startup_hook().
 */
static void
walproposer_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(WalproposerShmemSize());
	RequestAddinShmemSpace(UnionStoreXLOGShmemSize());
	RequestNamedLWLockTranche("LastWrittenLsnLock", 1);
}
#endif

static void
nwp_shmem_startup_hook(void)
{
	if (prev_shmem_startup_hook_type)
		prev_shmem_startup_hook_type();

	WalproposerShmemInit();
	UnionStoreXLOGShmemInit();
}

/*
 * WAL proposer bgworker entry point.
 */
void
WalProposerMain(Datum main_arg)
{
#if PG_VERSION_NUM >= 150000
	TimeLineID	tli;
#endif

	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

#if PG_VERSION_NUM >= 150000
	/* FIXME pass proper tli to WalProposerInit ? */
	GetXLogReplayRecPtr(&tli);
	WalProposerInit(GetFlushRecPtr(NULL), GetSystemIdentifier());
#else
	GetXLogReplayRecPtr(&ThisTimeLineID);
	WalProposerInit(GetFlushRecPtr(), GetSystemIdentifier());
#endif

	last_reconnect_attempt = GetCurrentTimestamp();

	WalProposerStart();
}

/*
 * Create new AppendRequest message and start sending it. This function is
 * called from walsender every time the new WAL is available.
 */
void
WalProposerBroadcast(XLogRecPtr startpos, XLogRecPtr endpos)
{
	availableLsn = endpos;
	SendMessage();
}

/*
 * Advance the WAL proposer state machine, waiting each time for events to occur.
 * Will exit only when latch is set, i.e. new WAL should be pushed from walsender
 * to walproposer.
 */
void
WalProposerPoll(void)
{
	while (true)
	{
		Safekeeper *sk;
		int			rc;
		WaitEvent	event;

		rc = WaitEventSetWait(waitEvents, wal_send_timeout,
							  &event, 1, WAIT_EVENT_WAL_SENDER_MAIN);
		sk = (Safekeeper *) event.user_data;

		/*
		 * If the event contains something that one of our safekeeper states
		 * was waiting for, we'll advance its state.
		 */
		if (rc != 0 && (event.events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)))
			AdvancePollState(sk, event.events);

		/*
		 * If the timeout expired, attempt to reconnect to any safekeepers
		 * that we dropped
		 */
		ReconnectSafekeepers();

		/*
		 * If wait is terminated by latch set (latch is set on each wal write),
		 * then write WAL records to Union Store. (no need for pm death check due to
		 * WL_EXIT_ON_PM_DEATH)
		 */
		if ((rc != 0 && (event.events & WL_LATCH_SET)) || rc == 0)
		{
			if (rc != 0)
				ResetLatch((Latch *)GetUnionStoreXLogCtlLatch());

			SendMessage();
		}

		CHECK_FOR_INTERRUPTS();

		/* Process any requests or signals received recently */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}
}

/*
 * Register a background worker proposing WAL to wal acceptors.
 */
static void
WalProposerRegister(void)
{
	BackgroundWorker bgw;

	if (*wal_acceptors_list == '\0')
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "unionstore");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "WalProposerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "WAL proposer");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "WAL proposer");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);

	prev_start_bgworkers_hook = start_bgworkers_hook;
	start_bgworkers_hook = UnionStore_start_bgworkers;
}

static void
WalProposer_shutdown(int code, Datum arg)
{
	DisownLatch((Latch *)GetUnionStoreXLogCtlLatch());
	DisownLatch((Latch *)GetUnionStoreStartupLatch());
}

static void
WalProposerInit(XLogRecPtr flushRecPtr, uint64 systemId)
{
	char	   *host;
	char	   *sep;
	char	   *port;

	/* load_file("libpqwalreceiver", false); */
	libpqwalreceiver_PG_init();
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	for (host = wal_acceptors_list; host != NULL && *host != '\0'; host = sep)
	{
		port = strchr(host, ':');
		if (port == NULL)
		{
			elog(FATAL, "port is not specified");
		}
		*port++ = '\0';
		sep = strchr(port, ',');
		if (sep != NULL)
			*sep++ = '\0';
		if (n_safekeepers + 1 >= MAX_SAFEKEEPERS)
		{
			elog(FATAL, "Too many safekeepers");
		}
		safekeeper[n_safekeepers].host = host;
		safekeeper[n_safekeepers].port = port;
		safekeeper[n_safekeepers].state = SS_OFFLINE;
		safekeeper[n_safekeepers].conn = NULL;
		safekeeper[n_safekeepers].is_safekeeper_leader = false;
		safekeeper[n_safekeepers].greetResponse.am_safekeeper_leader = false;

		{
			Safekeeper *sk = &safekeeper[n_safekeepers];
			int written = 0;

			if (neon_safekeeper_token_walproposer != NULL) {
				written = snprintf((char *) &sk->conninfo, MAXCONNINFO,
								   "host=%s port=%s password=%s dbname=replication options='-c timeline_id=%s tenant_id=%s'",
								   sk->host, sk->port, neon_safekeeper_token_walproposer, neon_timeline_walproposer,
								   neon_tenant_walproposer);
			} else {
				written = snprintf((char *) &sk->conninfo, MAXCONNINFO,
								   "host=%s port=%s dbname=replication options='-c timeline_id=%s tenant_id=%s'",
								   sk->host, sk->port, neon_timeline_walproposer, neon_tenant_walproposer);
			}

			if (written > MAXCONNINFO || written < 0)
				elog(FATAL, "could not create connection string for safekeeper %s:%s", sk->host, sk->port);
		}

		initStringInfo(&safekeeper[n_safekeepers].outbuf);
		safekeeper[n_safekeepers].xlogreader = XLogReaderAllocate(wal_segment_size, NULL, XL_ROUTINE(.segment_open = wal_segment_open,.segment_close = wal_segment_close), NULL);
		if (safekeeper[n_safekeepers].xlogreader == NULL)
			elog(FATAL, "Failed to allocate xlog reader");
		safekeeper[n_safekeepers].flushWrite = false;
		safekeeper[n_safekeepers].startStreamingAt = InvalidXLogRecPtr;
		safekeeper[n_safekeepers].streamingAt = InvalidXLogRecPtr;
		n_safekeepers += 1;
	}
	if (n_safekeepers < 1)
	{
		elog(FATAL, "Safekeepers addresses are not specified");
	}
	quorum = n_safekeepers / 2 + 1;

	/* Fill the greeting package */
	greetRequest.tag = 'g';
	greetRequest.protocolVersion = SK_PROTOCOL_VERSION;
	greetRequest.pgVersion = PG_VERSION_NUM;
	pg_strong_random(&greetRequest.proposerId, sizeof(greetRequest.proposerId));
	greetRequest.systemId = systemId;
	if (!neon_timeline_walproposer)
		elog(FATAL, "unionstore.timeline_id is not provided");
	if (*neon_timeline_walproposer != '\0' &&
		!HexDecodeString(greetRequest.timeline_id, neon_timeline_walproposer, 16))
		elog(FATAL, "Could not parse unionstore.timeline_id, %s", neon_timeline_walproposer);
	if (!neon_tenant_walproposer)
		elog(FATAL, "unionstore.tenant_id is not provided");
	if (*neon_tenant_walproposer != '\0' &&
		!HexDecodeString(greetRequest.tenant_id, neon_tenant_walproposer, 16))
		elog(FATAL, "Could not parse unionstore.tenant_id, %s", neon_tenant_walproposer);

#if PG_VERSION_NUM >= 150000
	/* FIXME don't use hardcoded timeline id */
	greetRequest.timeline = 1;
#else
	greetRequest.timeline = ThisTimeLineID;
#endif
	greetRequest.walSegSize = wal_segment_size;

	OwnLatch((Latch *)GetUnionStoreXLogCtlLatch());
	OwnLatch((Latch *)GetUnionStoreStartupLatch());
	on_shmem_exit(WalProposer_shutdown, (Datum) 0);

	InitEventSet();
}

static void
WalProposerStart(void)
{

	/* Initiate connections to all safekeeper nodes */
	for (int i = 0; i < n_safekeepers; i++)
	{
		ResetConnection(&safekeeper[i]);
	}

	WalProposerLoop();
}

static void
WalProposerLoop(void)
{
	while (true)
		WalProposerPoll();
}

/* Initializes the internal event set, provided that it is currently null */
static void
InitEventSet(void)
{
	if (waitEvents)
		elog(FATAL, "double-initialization of event set");

	waitEvents = CreateWaitEventSet(TopMemoryContext, 2 + n_safekeepers);
	AddWaitEventToSet(waitEvents, WL_LATCH_SET, PGINVALID_SOCKET,
					  (Latch *)GetUnionStoreXLogCtlLatch(), NULL);
	AddWaitEventToSet(waitEvents, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);
}

/*
 * Updates the events we're already waiting on for the safekeeper, setting it to
 * the provided `events`
 *
 * This function is called any time the safekeeper's state switches to one where
 * it has to wait to continue. This includes the full body of AdvancePollState
 * and calls to IO helper functions.
 */
static void
UpdateEventSet(Safekeeper *sk, uint32 events)
{
	/* eventPos = -1 when we don't have an event */
	Assert(sk->eventPos != -1);

	ModifyWaitEvent(waitEvents, sk->eventPos, events, NULL);
}

/* Hack: provides a way to remove the event corresponding to an individual walproposer from the set.
 *
 * Note: Internally, this completely reconstructs the event set. It should be avoided if possible.
 */
static void
HackyRemoveWalProposerEvent(Safekeeper *to_remove)
{
	/* Remove the existing event set */
	if (waitEvents)
	{
		FreeWaitEventSet(waitEvents);
		waitEvents = NULL;
	}
	/* Re-initialize it without adding any safekeeper events */
	InitEventSet();

	/*
	 * loop through the existing safekeepers. If they aren't the one we're
	 * removing, and if they have a socket we can use, re-add the applicable
	 * events.
	 */
	for (int i = 0; i < n_safekeepers; i++)
	{
		uint32		desired_events = WL_NO_EVENTS;
		Safekeeper *sk = &safekeeper[i];

		sk->eventPos = -1;

		if (sk == to_remove)
			continue;

		/* If this safekeeper isn't offline, add an event for it! */
		if (sk->conn != NULL)
		{
			desired_events = SafekeeperStateDesiredEvents(sk->state);
			sk->eventPos = AddWaitEventToSet(waitEvents, desired_events, walprop_socket(sk->conn), NULL, sk);
		}
	}
}

/* Shuts down and cleans up the connection for a safekeeper. Sets its state to SS_OFFLINE */
static void
ShutdownConnection(Safekeeper *sk)
{
	if (sk->conn)
		walprop_finish(sk->conn);
	sk->conn = NULL;
	sk->state = SS_OFFLINE;
	sk->flushWrite = false;
	sk->streamingAt = InvalidXLogRecPtr;

	if (sk->voteResponse.termHistory.entries)
		pfree(sk->voteResponse.termHistory.entries);
	sk->voteResponse.termHistory.entries = NULL;

	HackyRemoveWalProposerEvent(sk);
}

/*
 * This function is called to establish new connection or to reestablish
 * connection in case of connection failure.
 *
 * On success, sets the state to SS_CONNECTING_WRITE.
 */
static void
ResetConnection(Safekeeper *sk)
{
	pgsocket	sock;			/* socket of the new connection */

	if (sk->state != SS_OFFLINE)
	{
		ShutdownConnection(sk);
	}

	/*
	 * Try to establish new connection
	 */
	sk->conn = walprop_connect_start((char *) &sk->conninfo);

	/*
	 * "If the result is null, then libpq has been unable to allocate a new
	 * PGconn structure"
	 */
	if (!sk->conn)
		elog(FATAL, "failed to allocate new PGconn object");

	/*
	 * PQconnectStart won't actually start connecting until we run
	 * PQconnectPoll. Before we do that though, we need to check that it
	 * didn't immediately fail.
	 */
	if (walprop_status(sk->conn) == WP_CONNECTION_BAD)
	{
		/*---
		 * According to libpq docs:
		 *   "If the result is CONNECTION_BAD, the connection attempt has already failed,
		 *    typically because of invalid connection parameters."
		 * We should report this failure. Do not print the exact `conninfo` as it may
		 * contain e.g. password. The error message should already provide enough information.
		 *
		 * https://www.postgresql.org/docs/devel/libpq-connect.html#LIBPQ-PQCONNECTSTARTPARAMS
		 */
		elog(WARNING, "Immediate failure to connect with node '%s:%s':\n\terror: %s",
			 sk->host, sk->port, walprop_error_message(sk->conn));

		/*
		 * Even though the connection failed, we still need to clean up the
		 * object
		 */
		walprop_finish(sk->conn);
		sk->conn = NULL;

		/*
         * If we lose connection to safekeeper leader, we should reconnect to other safekeepers to determine
         * the new safekeeper leader.
         */
		if (sk->is_safekeeper_leader)
		{
		    for (int i = 0; i < n_safekeepers; i++)
		    {
		        if (&safekeeper[i] != sk)
		        {
		            ResetConnection(&safekeeper[i]);
		        }
		    }

		    /*
             * Reconnection means that safekeeper leader may restart/crash recovery, our last sent wal may not be
             * persisted, set the abort lsn. If any open transactions write wal before abort lsn, should abort.
             *
             * We prefer consistency to availability.
             */
		    UnionStoreSetAbortLocalLsn(-1);
		    n_connected = 0;
		    sk->is_safekeeper_leader = false;
		    WakeupUnionStoreWaiters(InvalidXLogRecPtr);

		    /*
		     * Dirty buffers already exist, we should reset those buffers(~BM_VALID) to prevent from
		     * being used by other open transactions.
		     *
		     * TODO: evict the buffers before processing transactions.
		     */
		    elog(PANIC, "Failed to write WAL into UnionStore.");
		}

		return;
	}

	/*
	 * The documentation for PQconnectStart states that we should call
	 * PQconnectPoll in a loop until it returns PGRES_POLLING_OK or
	 * PGRES_POLLING_FAILED. The other two possible returns indicate whether
	 * we should wait for reading or writing on the socket. For the first
	 * iteration of the loop, we're expected to wait until the socket becomes
	 * writable.
	 *
	 * The wording of the documentation is a little ambiguous; thankfully
	 * there's an example in the postgres source itself showing this behavior.
	 * (see libpqrcv_connect, defined in
	 * src/backend/replication/libpqwalreceiver/libpqwalreceiver.c)
	 */
	elog(LOG, "connecting with node %s:%s", sk->host, sk->port);

	sk->state = SS_CONNECTING_WRITE;
	sk->latestMsgReceivedAt = GetCurrentTimestamp();

	sock = walprop_socket(sk->conn);
	sk->eventPos = AddWaitEventToSet(waitEvents, WL_SOCKET_WRITEABLE, sock, NULL, sk);

	/*
	 * If we reconnect to old safekeeper leader, we should reconnect to other safekeepers to determine
	 * the new safekeeper leader.
	 */
	if (sk->is_safekeeper_leader)
	{
	    for (int i = 0; i < n_safekeepers; i++)
	    {
	        if (&safekeeper[i] != sk)
	        {
	            ResetConnection(&safekeeper[i]);
	        }
	    }

	    /*
	     * Reconnection means that safekeeper leader may restart/crash recovery, our last sent wal may not be
	     * persisted, set the abort lsn. If any open transactions write wal before abort lsn, should abort.
	     *
	     * We prefer consistency to availability.
	     */
	    UnionStoreSetAbortLocalLsn(-1);
	    n_connected = 0;
	    sk->is_safekeeper_leader = false;
	    WakeupUnionStoreWaiters(InvalidXLogRecPtr);

	    /*
         * Dirty buffers already exist, we should reset those buffers(~BM_VALID) to prevent from
         * being used by other open transactions.
         *
         * TODO: evict the buffers before processing transactions.
         */
	    elog(PANIC, "Failed to write WAL into UnionStore.");
	}
	return;
}

/*
 * How much milliseconds left till we should attempt reconnection to
 * safekeepers? Returns 0 if it is already high time, -1 if we never reconnect
 * (do we actually need this?).
 */
static long
TimeToReconnect(TimestampTz now)
{
	TimestampTz passed;
	TimestampTz till_reconnect;

	if (wal_acceptor_reconnect_timeout <= 0)
		return -1;

	passed = now - last_reconnect_attempt;
	till_reconnect = wal_acceptor_reconnect_timeout * 1000 - passed;
	if (till_reconnect <= 0)
		return 0;
	return (long) (till_reconnect / 1000);
}

/* If the timeout has expired, attempt to reconnect to all offline safekeepers */
static void
ReconnectSafekeepers(void)
{
	TimestampTz now = GetCurrentTimestamp();

	if (TimeToReconnect(now) == 0)
	{
		last_reconnect_attempt = now;
		for (int i = 0; i < n_safekeepers; i++)
		{
			if (safekeeper[i].state == SS_OFFLINE)
				ResetConnection(&safekeeper[i]);
		}
	}
}

/*
 * Performs the logic for advancing the state machine of the specified safekeeper,
 * given that a certain set of events has occured.
 */
static void
AdvancePollState(Safekeeper *sk, uint32 events)
{
	/*
	 * Sanity check. We assume further down that the operations don't block
	 * because the socket is ready.
	 */
	AssertEventsOkForState(events, sk);

	/* Execute the code corresponding to the current state */
	switch (sk->state)
	{
			/*
			 * safekeepers are only taken out of SS_OFFLINE by calls to
			 * ResetConnection
			 */
		case SS_OFFLINE:
			elog(FATAL, "Unexpected safekeeper %s:%s state advancement: is offline",
				 sk->host, sk->port);
			break;				/* actually unreachable, but prevents
								 * -Wimplicit-fallthrough */

			/*
			 * Both connecting states run the same logic. The only difference
			 * is the events they're expecting
			 */
		case SS_CONNECTING_READ:
		case SS_CONNECTING_WRITE:
			HandleConnectionEvent(sk);
			break;

			/*
			 * Waiting for a successful CopyBoth response.
			 */
		case SS_WAIT_EXEC_RESULT:
			RecvStartWALPushResult(sk);
			break;

			/*
			 * Finish handshake comms: receive information about the
			 * safekeeper.
			 */
		case SS_HANDSHAKE_RECV:
			RecvAcceptorGreeting(sk);
			break;

			/*
			 * Voting is an idle state - we don't expect any events to
			 * trigger. Refer to the execution of SS_HANDSHAKE_RECV to see how
			 * nodes are transferred from SS_VOTING to sending actual vote
			 * requests.
			 */
		case SS_VOTING:
			elog(WARNING, "EOF from node %s:%s in %s state", sk->host,
				 sk->port, FormatSafekeeperState(sk->state));
			ResetConnection(sk);
			return;

			/* Read the safekeeper response for our candidate */
		case SS_WAIT_VERDICT:
			RecvVoteResponse(sk);
			break;

			/* Flush proposer announcement message */
		case SS_SEND_ELECTED_FLUSH:

			/*
			 * AsyncFlush ensures we only move on to SS_ACTIVE once the flush
			 * completes. If we still have more to do, we'll wait until the
			 * next poll comes along.
			 */
			if (!AsyncFlush(sk))
				return;

			/* flush is done, event set and state will be updated later */
			StartStreaming(sk);
			break;

			/*
			 * Idle state for waiting votes from quorum.
			 */
		case SS_IDLE:
			elog(WARNING, "EOF from node %s:%s in %s state", sk->host,
				 sk->port, FormatSafekeeperState(sk->state));
			ResetConnection(sk);
			return;

			/*
			 * Active state is used for streaming WAL and receiving feedback.
			 */
		case SS_ACTIVE:
		case SS_INACTIVE:
			HandleActiveState(sk, events);
			break;
	}
}

static void
HandleConnectionEvent(Safekeeper *sk)
{
	WalProposerConnectPollStatusType result = walprop_connect_poll(sk->conn);

	/* The new set of events we'll wait on, after updating */
	uint32		new_events = WL_NO_EVENTS;

	switch (result)
	{
		case WP_CONN_POLLING_OK:
			elog(LOG, "connected with node %s:%s", sk->host,
				 sk->port);
			sk->latestMsgReceivedAt = GetCurrentTimestamp();
			/*
			 * We have to pick some event to update event set. We'll
			 * eventually need the socket to be readable, so we go with that.
			 */
			new_events = WL_SOCKET_READABLE;
			break;

			/*
			 * If we need to poll to finish connecting, continue doing that
			 */
		case WP_CONN_POLLING_READING:
			sk->state = SS_CONNECTING_READ;
			new_events = WL_SOCKET_READABLE;
			break;
		case WP_CONN_POLLING_WRITING:
			sk->state = SS_CONNECTING_WRITE;
			new_events = WL_SOCKET_WRITEABLE;
			break;

		case WP_CONN_POLLING_FAILED:
			elog(WARNING, "failed to connect to node '%s:%s': %s",
				 sk->host, sk->port, walprop_error_message(sk->conn));

			/*
			 * If connecting failed, we don't want to restart the connection
			 * because that might run us into a loop. Instead, shut it down --
			 * it'll naturally restart at a slower interval on calls to
			 * ReconnectSafekeepers.
			 */
			ShutdownConnection(sk);
			return;
	}

	/*
	 * Because PQconnectPoll can change the socket, we have to un-register the
	 * old event and re-register an event on the new socket.
	 */
	HackyRemoveWalProposerEvent(sk);
	sk->eventPos = AddWaitEventToSet(waitEvents, new_events, walprop_socket(sk->conn), NULL, sk);

	/* If we successfully connected, send START_WAL_PUSH query */
	if (result == WP_CONN_POLLING_OK)
		SendStartWALPush(sk);
}

/*
 * Send "START_WAL_PUSH" message as an empty query to the safekeeper. Performs
 * a blocking send, then immediately moves to SS_WAIT_EXEC_RESULT. If something
 * goes wrong, change state to SS_OFFLINE and shutdown the connection.
 */
static void
SendStartWALPush(Safekeeper *sk)
{
	if (!walprop_send_query(sk->conn, "START_WAL_PUSH"))
	{
		elog(WARNING, "Failed to send 'START_WAL_PUSH' query to safekeeper %s:%s: %s",
			 sk->host, sk->port, walprop_error_message(sk->conn));
		ShutdownConnection(sk);
		return;
	}
	sk->state = SS_WAIT_EXEC_RESULT;
	UpdateEventSet(sk, WL_SOCKET_READABLE);
}

static void
RecvStartWALPushResult(Safekeeper *sk)
{
	switch (walprop_get_query_result(sk->conn))
	{
			/*
			 * Successful result, move on to starting the handshake
			 */
		case WP_EXEC_SUCCESS_COPYBOTH:

			SendProposerGreeting(sk);
			sk->is_safekeeper_leader = false;
			break;

			/*
			 * Needs repeated calls to finish. Wait until the socket is
			 * readable
			 */
		case WP_EXEC_NEEDS_INPUT:

			/*
			 * SS_WAIT_EXEC_RESULT is always reached through an event, so we
			 * don't need to update the event set
			 */
			break;

		case WP_EXEC_FAILED:
			elog(WARNING, "Failed to send query to safekeeper %s:%s: %s",
				 sk->host, sk->port, walprop_error_message(sk->conn));
			ShutdownConnection(sk);
			return;

			/*
			 * Unexpected result -- funamdentally an error, but we want to
			 * produce a custom message, rather than a generic "something went
			 * wrong"
			 */
		case WP_EXEC_UNEXPECTED_SUCCESS:
			elog(WARNING, "Received bad response from safekeeper %s:%s query execution",
				 sk->host, sk->port);
			ShutdownConnection(sk);
			return;
	}
}

/*
 * Start handshake: first of all send information about the
 * safekeeper. After sending, we wait on SS_HANDSHAKE_RECV for
 * a response to finish the handshake.
 */
static void
SendProposerGreeting(Safekeeper *sk)
{
	/*
	 * On failure, logging & resetting the connection is handled. We just need
	 * to handle the control flow.
	 */
	BlockingWrite(sk, &greetRequest, sizeof(greetRequest), SS_HANDSHAKE_RECV);
}

static void
RecvAcceptorGreeting(Safekeeper *sk)
{
	/*
	 * If our reading doesn't immediately succeed, any necessary error
	 * handling or state setting is taken care of. We can leave any other work
	 * until later.
	 */
	sk->greetResponse.apm.tag = 'g';
	if (!AsyncReadMessage(sk, (AcceptorProposerMessage *) & sk->greetResponse))
		return;

	/* Protocol is all good, move to voting. */
	sk->state = SS_INACTIVE;

	/*
	 * If we have already got the leader, do nothing.
	 */
	for (int i = 0; i < n_safekeepers; i++)
	{
	    if (safekeeper[i].is_safekeeper_leader)
	    {
	        Assert(sk != &safekeeper[i]);
	        sk->greetResponse.am_safekeeper_leader = false;
	        return;
	    }
	}

	++n_connected;
	/*
	 *	Get all greeting responses from safekeepers, let's see who is the leader.
	 */
	if (n_connected >= quorum)
	{
		int safekeeper_leader = -1;
		NNodeId leader_node_id = -1;

		/*
		 * Get safekeeper's leader, send wal to leader.
		 */
		propTerm = safekeeper[0].greetResponse.term;
		propEpochStartLsn = safekeeper[0].greetResponse.flush_lsn;
		safekeeper_leader = safekeeper[0].greetResponse.am_safekeeper_leader ? 0 : -1;
		leader_node_id = safekeeper[0].greetResponse.am_safekeeper_leader ? safekeeper[0].greetResponse.nodeId : -1;
		safekeeper[0].greetResponse.am_safekeeper_leader = false;
		for (int i = 1; i < n_safekeepers; i++)
		{
		    /*
		     * In general, safekeepers should have only one leader, and we can identify the leader through greeting msg.
		     *
		     * But in some strict cases, such as network partition, we may have two leaders, one is the old leader, and
		     * one is the new elected leader. We should choose the new one by comparing the term, the new elected leader
		     * always has the larger term.
		     */
			if (safekeeper[i].greetResponse.am_safekeeper_leader)
			{
			    if (safekeeper_leader >= 0 && safekeeper[i].greetResponse.term <= propTerm)
			    {
			        elog(LOG, "safekeepers have multi leaders, check the safekeepers' status.");
			        safekeeper_leader = -1;
			        break;
			    }
			    safekeeper_leader = i;
			    leader_node_id = safekeeper[i].greetResponse.nodeId;
			    propEpochStartLsn = safekeeper[i].greetResponse.flush_lsn;
			    propTerm = safekeeper[i].greetResponse.term;
			}

			safekeeper[i].greetResponse.am_safekeeper_leader = false;
		}

		/*
		 * We have got the leader, make sanity check.
		 */
		if (safekeeper_leader >= 0)
		{
		    int agreements = 0;

		    /*
		     * We should get quorom safekeepers who agree with the leader.
		     */
		    for (int i = 0; i < n_safekeepers; i++)
		    {
		        if (safekeeper[i].state == SS_INACTIVE && safekeeper[i].greetResponse.leader_nodeId == leader_node_id)
		        {
		            agreements++;
		        }
		    }

		    /*
		     * This is not the true leader, try again.
		     */
		    if (agreements < quorum)
		    {
		        safekeeper_leader = -1;
		    }
		}
		/*
		 * Safekeeper leader is not elected yet, wait for a while and try again.
		 */
		if (safekeeper_leader == -1)
		{
            pg_usleep(1000000L);
		    for (int i = 0; i < n_safekeepers; i++)
		    {
		        if (safekeeper[i].state == SS_INACTIVE)
		        {
		            SendProposerGreeting(&safekeeper[i]);
		            safekeeper[i].is_safekeeper_leader = false;
		        }
		    }
		    n_connected = 0;
		    return;
		}

		elog(LOG, "Safekeeper leader elected, host %s port %s lsn %lu", safekeeper[safekeeper_leader].host, safekeeper[safekeeper_leader].port, propEpochStartLsn);

		/*
		 * Do some startup works.
		 */
		if (!UnionStoreStartupXLog(propEpochStartLsn))
		{
		    for (int i = 0; i < n_safekeepers; i++)
		    {
                SendProposerGreeting(&safekeeper[i]);
                safekeeper[i].is_safekeeper_leader = false;
		    }
		    n_connected = 0;

		    elog(LOG, "Promotion triggered, refresh leader info.");

		    return;
		}

		SetUnionStoreFlushedLSN(propEpochStartLsn, InvalidXLogRecPtr);

		/* start streaming */
		safekeeper[safekeeper_leader].startStreamingAt = propEpochStartLsn;
		safekeeper[safekeeper_leader].is_safekeeper_leader = true;
		StartStreaming(&safekeeper[safekeeper_leader]);
		n_connected = 0;
	}

	return;

	if (n_connected <= quorum)
	{
		/* We're still collecting terms from the majority. */
		propTerm = Max(sk->greetResponse.term, propTerm);

		/* Quorum is acquried, prepare the vote request. */
		if (n_connected == quorum)
		{
			propTerm++;
			elog(LOG, "proposer connected to quorum (%d) safekeepers, propTerm=" INT64_FORMAT, quorum, propTerm);

			voteRequest = (VoteRequest)
			{
				.tag = 'v',
					.term = propTerm
			};
			memcpy(voteRequest.proposerId.data, greetRequest.proposerId.data, UUID_LEN);
		}
	}
	else if (sk->greetResponse.term > propTerm)
	{
		/* Another compute with higher term is running. */
		elog(FATAL, "WAL acceptor %s:%s with term " INT64_FORMAT " rejects our connection request with term " INT64_FORMAT "",
			 sk->host, sk->port,
			 sk->greetResponse.term, propTerm);
	}

	/*
	 * Check if we have quorum. If there aren't enough safekeepers, wait and
	 * do nothing. We'll eventually get a task when the election starts.
	 *
	 * If we do have quorum, we can start an election.
	 */
	if (n_connected < quorum)
	{
		/*
		 * SS_VOTING is an idle state; read-ready indicates the connection
		 * closed.
		 */
		UpdateEventSet(sk, WL_SOCKET_READABLE);
	}
	else
	{
		/*
		 * Now send voting request to the cohort and wait responses
		 */
		for (int j = 0; j < n_safekeepers; j++)
		{
			/*
			 * Remember: SS_VOTING indicates that the safekeeper is
			 * participating in voting, but hasn't sent anything yet.
			 */
			if (safekeeper[j].state == SS_VOTING)
				SendVoteRequest(&safekeeper[j]);
		}
	}
}

static void
SendVoteRequest(Safekeeper *sk)
{
	/* We have quorum for voting, send our vote request */
	elog(LOG, "requesting vote from %s:%s for term " UINT64_FORMAT, sk->host, sk->port, voteRequest.term);
	/* On failure, logging & resetting is handled */
	if (!BlockingWrite(sk, &voteRequest, sizeof(voteRequest), SS_WAIT_VERDICT))
		return;

	/* If successful, wait for read-ready with SS_WAIT_VERDICT */
}

static void
RecvVoteResponse(Safekeeper *sk)
{
	sk->voteResponse.apm.tag = 'v';
	if (!AsyncReadMessage(sk, (AcceptorProposerMessage *) & sk->voteResponse))
		return;

	elog(LOG,
		 "got VoteResponse from acceptor %s:%s, voteGiven=" UINT64_FORMAT ", epoch=" UINT64_FORMAT ", flushLsn=%X/%X, truncateLsn=%X/%X, timelineStartLsn=%X/%X",
		 sk->host, sk->port, sk->voteResponse.voteGiven, GetHighestTerm(&sk->voteResponse.termHistory),
		 LSN_FORMAT_ARGS(sk->voteResponse.flushLsn),
		 LSN_FORMAT_ARGS(sk->voteResponse.truncateLsn),
		 LSN_FORMAT_ARGS(sk->voteResponse.timelineStartLsn));

	/*
	 * In case of acceptor rejecting our vote, bail out, but only if either it
	 * already lives in strictly higher term (concurrent compute spotted) or
	 * we are not elected yet and thus need the vote.
	 */
	if ((!sk->voteResponse.voteGiven) &&
		(sk->voteResponse.term > propTerm || n_votes < quorum))
	{
		elog(FATAL, "WAL acceptor %s:%s with term " INT64_FORMAT " rejects our connection request with term " INT64_FORMAT "",
			 sk->host, sk->port,
			 sk->voteResponse.term, propTerm);
	}
	Assert(sk->voteResponse.term == propTerm);

	/* Handshake completed, do we have quorum? */
	n_votes++;
	if (n_votes < quorum)
	{
		sk->state = SS_IDLE;	/* can't do much yet, no quorum */
	}
	else if (n_votes > quorum)
	{
		/* recovery already performed, just start streaming */
		SendProposerElected(sk);
	}
	else
	{
		sk->state = SS_IDLE;
		UpdateEventSet(sk, WL_SOCKET_READABLE); /* Idle states wait for
												 * read-ready */

		HandleElectedProposer();
	}
}

/*
 * Called once a majority of acceptors have voted for us and current proposer
 * has been elected.
 *
 * Sends ProposerElected message to all acceptors in SS_IDLE state and starts
 * replication from walsender.
 */
static void
HandleElectedProposer(void)
{
	DetermineEpochStartLsn();

	/*
	 * Check if not all safekeepers are up-to-date, we need to download WAL
	 * needed to synchronize them
	 */
	if (truncateLsn < propEpochStartLsn)
	{
		elog(LOG,
			 "start recovery because truncateLsn=%X/%X is not "
			 "equal to epochStartLsn=%X/%X",
			 LSN_FORMAT_ARGS(truncateLsn),
			 LSN_FORMAT_ARGS(propEpochStartLsn));
		/* Perform recovery */
		if (!WalProposerRecovery(donor, greetRequest.timeline, truncateLsn, propEpochStartLsn))
			elog(FATAL, "Failed to recover state");
	}
	else if (syncSafekeepers)
	{
		/* Sync is not needed: just exit */
		fprintf(stdout, "%X/%X\n", LSN_FORMAT_ARGS(propEpochStartLsn));
		exit(0);
	}

	for (int i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].state == SS_IDLE)
			SendProposerElected(&safekeeper[i]);
	}

	/*
	 * The proposer has been elected, and there will be no quorum waiting
	 * after this point. There will be no safekeeper with state SS_IDLE also,
	 * because that state is used only for quorum waiting.
	 */

	if (syncSafekeepers)
	{
		/*
		 * Send empty message to enforce receiving feedback even from nodes
		 * who are fully recovered; this is required to learn they switched
		 * epoch which finishes sync-safeekepers who doesn't generate any real
		 * new records. Will go away once we switch to async acks.
		 */
		SendMessage();

		/* keep polling until all safekeepers are synced */
		return;
	}
}

/* latest term in TermHistory, or 0 is there is no entries */
static term_t
GetHighestTerm(TermHistory * th)
{
	return th->n_entries > 0 ? th->entries[th->n_entries - 1].term : 0;
}

/* safekeeper's epoch is the term of the highest entry in the log */
static term_t
GetEpoch(Safekeeper *sk)
{
	return GetHighestTerm(&sk->voteResponse.termHistory);
}

/*
 * Called after majority of acceptors gave votes, it calculates the most
 * advanced safekeeper (who will be the donor) and epochStartLsn -- LSN since
 * which we'll write WAL in our term.
 *
 * Sets truncateLsn along the way (though it is not of much use at this point --
 * only for skipping recovery).
 */
static void
DetermineEpochStartLsn(void)
{
	TermHistory *dth;

	propEpochStartLsn = InvalidXLogRecPtr;
	donorEpoch = 0;
	truncateLsn = InvalidXLogRecPtr;
	timelineStartLsn = InvalidXLogRecPtr;

	for (int i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].state == SS_IDLE)
		{
			if (GetEpoch(&safekeeper[i]) > donorEpoch ||
				(GetEpoch(&safekeeper[i]) == donorEpoch &&
				 safekeeper[i].voteResponse.flushLsn > propEpochStartLsn))
			{
				donorEpoch = GetEpoch(&safekeeper[i]);
				propEpochStartLsn = safekeeper[i].voteResponse.flushLsn;
				donor = i;
			}
			truncateLsn = Max(safekeeper[i].voteResponse.truncateLsn, truncateLsn);

			if (safekeeper[i].voteResponse.timelineStartLsn != InvalidXLogRecPtr)
			{
				/* timelineStartLsn should be the same everywhere or unknown */
				if (timelineStartLsn != InvalidXLogRecPtr &&
					timelineStartLsn != safekeeper[i].voteResponse.timelineStartLsn)
				{
					elog(WARNING,
						 "inconsistent timelineStartLsn: current %X/%X, received %X/%X",
						 LSN_FORMAT_ARGS(timelineStartLsn),
						 LSN_FORMAT_ARGS(safekeeper[i].voteResponse.timelineStartLsn));
				}
				timelineStartLsn = safekeeper[i].voteResponse.timelineStartLsn;
			}
		}
	}

	/*
	 * If propEpochStartLsn is 0 everywhere, we are bootstrapping -- nothing
	 * was committed yet. Start streaming then from the basebackup LSN.
	 */
	if (propEpochStartLsn == InvalidXLogRecPtr && !syncSafekeepers)
	{
		propEpochStartLsn = truncateLsn = GetFlushRecPtr();
		if (timelineStartLsn == InvalidXLogRecPtr)
		{
			timelineStartLsn = GetFlushRecPtr();
		}
		elog(LOG, "bumped epochStartLsn to the first record %X/%X", LSN_FORMAT_ARGS(propEpochStartLsn));
	}

	/*
	 * If propEpochStartLsn is not 0, at least one msg with WAL was sent to
	 * some connected safekeeper; it must have carried truncateLsn pointing to
	 * the first record.
	 */
	Assert((truncateLsn != InvalidXLogRecPtr) ||
		   (syncSafekeepers && truncateLsn == propEpochStartLsn));

	/*
	 * We will be generating WAL since propEpochStartLsn, so we should set
	 * availableLsn to mark this LSN as the latest available position.
	 */
	availableLsn = propEpochStartLsn;

	SetUnionStoreFlushedLSN(propEpochStartLsn, InvalidXLogRecPtr);

	/*
	 * Proposer's term history is the donor's + its own entry.
	 */
	dth = &safekeeper[donor].voteResponse.termHistory;
	propTermHistory.n_entries = dth->n_entries + 1;
	propTermHistory.entries = palloc(sizeof(TermSwitchEntry) * propTermHistory.n_entries);
	memcpy(propTermHistory.entries, dth->entries, sizeof(TermSwitchEntry) * dth->n_entries);
	propTermHistory.entries[propTermHistory.n_entries - 1].term = propTerm;
	propTermHistory.entries[propTermHistory.n_entries - 1].lsn = propEpochStartLsn;

	elog(LOG, "got votes from majority (%d) of nodes, term " UINT64_FORMAT ", epochStartLsn %X/%X, donor %s:%s, truncate_lsn %X/%X",
		 quorum,
		 propTerm,
		 LSN_FORMAT_ARGS(propEpochStartLsn),
		 safekeeper[donor].host, safekeeper[donor].port,
		 LSN_FORMAT_ARGS(truncateLsn));

	/*
	 * Ensure the basebackup we are running (at RedoStartLsn) matches LSN
	 * since which we are going to write according to the consensus. If not,
	 * we must bail out, as clog and other non rel data is inconsistent.
	 */
	if (!syncSafekeepers)
	{
		walprop_shared->mineLastElectedTerm = propTerm;
	}
}

/*
 * Receive WAL from most advanced safekeeper
 */
static bool
WalProposerRecovery(int donor, TimeLineID timeline, XLogRecPtr startpos, XLogRecPtr endpos)
{
	/*
	 * Make sanity check to ensure consistency between local and remote.
	 */
	if (GetUnionStoreLocalFlushLsn() < endpos)
	{
		ereport(LOG,
				(errmsg("primary server contains no more WAL on requested timeline %u LSN %X/%08X",
						timeline, (uint32) (startpos >> 32), (uint32) startpos)));
		return false;
	}

	return true;
}

/*
 * Determine for sk the starting streaming point and send it message
 * 1) Announcing we are elected proposer (which immediately advances epoch if
 *    safekeeper is synced, being important for sync-safekeepers)
 * 2) Communicating starting streaming point -- safekeeper must truncate its WAL
 *    beyond it -- and history of term switching.
 *
 * Sets sk->startStreamingAt.
 */
static void
SendProposerElected(Safekeeper *sk)
{
	ProposerElected msg;
	TermHistory *th;
	term_t		lastCommonTerm;
	int			i;

	/*
	 * Determine start LSN by comparing safekeeper's log term switch history
	 * and proposer's, searching for the divergence point.
	 *
	 * Note: there is a vanishingly small chance of no common point even if
	 * there is some WAL on safekeeper, if immediately after bootstrap compute
	 * wrote some WAL on single sk and died; we stream since the beginning
	 * then.
	 */
	th = &sk->voteResponse.termHistory;

	/* We must start somewhere. */
	Assert(propTermHistory.n_entries >= 1);

	for (i = 0; i < Min(propTermHistory.n_entries, th->n_entries); i++)
	{
		if (propTermHistory.entries[i].term != th->entries[i].term)
			break;
		/* term must begin everywhere at the same point */
		Assert(propTermHistory.entries[i].lsn == th->entries[i].lsn);
	}
	i--;						/* step back to the last common term */
	if (i < 0)
	{
		/* safekeeper is empty or no common point, start from the beginning */
		sk->startStreamingAt = propTermHistory.entries[0].lsn;

		if (sk->startStreamingAt < truncateLsn)
		{
			/*
			 * There's a gap between the WAL starting point and a truncateLsn,
			 * which can't appear in a normal working cluster. That gap means
			 * that all safekeepers reported that they have persisted WAL up
			 * to the truncateLsn before, but now current safekeeper tells
			 * otherwise.
			 *
			 * Also we have a special condition here, which is empty
			 * safekeeper with no history. In combination with a gap, that can
			 * happen when we introduce a new safekeeper to the cluster. This
			 * is a rare case, which is triggered manually for now, and should
			 * be treated with care.
			 */

			/*
			 * truncateLsn will not change without ack from current
			 * safekeeper, and it's aligned to the WAL record, so we can
			 * safely start streaming from this point.
			 */
			sk->startStreamingAt = truncateLsn;

			elog(WARNING, "empty safekeeper joined cluster as %s:%s, historyStart=%X/%X, sk->startStreamingAt=%X/%X",
				 sk->host, sk->port, LSN_FORMAT_ARGS(propTermHistory.entries[0].lsn),
				 LSN_FORMAT_ARGS(sk->startStreamingAt));
		}
	}
	else
	{
		/*
		 * End of (common) term is the start of the next except it is the last
		 * one; there it is flush_lsn in case of safekeeper or, in case of
		 * proposer, LSN it is currently writing, but then we just pick
		 * safekeeper pos as it obviously can't be higher.
		 */
		if (propTermHistory.entries[i].term == propTerm)
		{
			sk->startStreamingAt = sk->voteResponse.flushLsn;
		}
		else
		{
			XLogRecPtr	propEndLsn = propTermHistory.entries[i + 1].lsn;
			XLogRecPtr	skEndLsn = (i + 1 < th->n_entries ? th->entries[i + 1].lsn : sk->voteResponse.flushLsn);

			sk->startStreamingAt = Min(propEndLsn, skEndLsn);
		}
	}

	Assert(sk->startStreamingAt >= truncateLsn && sk->startStreamingAt <= availableLsn);

	msg.tag = 'e';
	msg.term = propTerm;
	msg.startStreamingAt = sk->startStreamingAt;
	msg.termHistory = &propTermHistory;
	msg.timelineStartLsn = timelineStartLsn;

	lastCommonTerm = i >= 0 ? propTermHistory.entries[i].term : 0;
	elog(LOG,
		 "sending elected msg to node " UINT64_FORMAT " term=" UINT64_FORMAT ", startStreamingAt=%X/%X (lastCommonTerm=" UINT64_FORMAT "), termHistory.n_entries=%u to %s:%s, timelineStartLsn=%X/%X",
		 sk->greetResponse.nodeId, msg.term, LSN_FORMAT_ARGS(msg.startStreamingAt), lastCommonTerm, msg.termHistory->n_entries, sk->host, sk->port, LSN_FORMAT_ARGS(msg.timelineStartLsn));

	resetStringInfo(&sk->outbuf);
	pq_sendint64_le(&sk->outbuf, msg.tag);
	pq_sendint64_le(&sk->outbuf, msg.term);
	pq_sendint64_le(&sk->outbuf, msg.startStreamingAt);
	pq_sendint32_le(&sk->outbuf, msg.termHistory->n_entries);
	for (int i = 0; i < msg.termHistory->n_entries; i++)
	{
		pq_sendint64_le(&sk->outbuf, msg.termHistory->entries[i].term);
		pq_sendint64_le(&sk->outbuf, msg.termHistory->entries[i].lsn);
	}
	pq_sendint64_le(&sk->outbuf, msg.timelineStartLsn);

	if (!AsyncWrite(sk, sk->outbuf.data, sk->outbuf.len, SS_SEND_ELECTED_FLUSH))
		return;

	StartStreaming(sk);
}

/*
 * Start streaming to safekeeper sk, always updates state to SS_ACTIVE and sets
 * correct event set.
 */
static void
StartStreaming(Safekeeper *sk)
{
	/*
	 * This is the only entrypoint to state SS_ACTIVE. It's executed exactly
	 * once for a connection.
	 */
	sk->state = SS_ACTIVE;
	sk->streamingAt = sk->startStreamingAt;

	/* event set will be updated inside SendMessageToNode */
	SendMessageToNode(sk);
}

/*
 * Try to send message to the particular node. Always updates event set. Will
 * send at least one message, if socket is ready.
 *
 * Can be used only for safekeepers in SS_ACTIVE state. State can be changed
 * in case of errors.
 */
static void
SendMessageToNode(Safekeeper *sk)
{
	Assert(sk->state == SS_ACTIVE);

	/*
	 * Note: we always send everything to the safekeeper until WOULDBLOCK or
	 * nothing left to send
	 */
	HandleActiveState(sk, WL_SOCKET_WRITEABLE);
}

/*
 * Send new message to safekeeper leader
 */
static void
SendMessage()
{
	for (int i = 0; i < n_safekeepers; i++)
		if (safekeeper[i].state == SS_ACTIVE)
			SendMessageToNode(&safekeeper[i]);
}

static void
PrepareAppendRequest(AppendRequestHeader * req, XLogRecPtr beginLsn, XLogRecPtr endLsn)
{
	req->tag = 'a';
	req->term = propTerm;
	req->epochStartLsn = propEpochStartLsn;
	req->beginLsn = beginLsn;
	req->endLsn = endLsn;
	req->commitLsn = GetAcknowledgedByQuorumWALPosition();
	req->truncateLsn = truncateLsn;
	req->proposerId = greetRequest.proposerId;
}

/*
 * Process all events happened in SS_ACTIVE state, update event set after that.
 */
static void
HandleActiveState(Safekeeper *sk, uint32 events)
{
	uint32		newEvents = WL_SOCKET_READABLE;

	if (events & WL_SOCKET_WRITEABLE)
		if (!SendAppendRequests(sk))
			return;

	if (events & WL_SOCKET_READABLE)
		if (!RecvAppendResponses(sk))
			return;

	/*
	 * We should wait for WL_SOCKET_WRITEABLE event if we have unflushed data
	 * in the buffer.
	 *
	 */
	if (sk->flushWrite)
		newEvents |= WL_SOCKET_WRITEABLE;

	UpdateEventSet(sk, newEvents);
}

/*
 * Send WAL messages starting from sk->streamingAt until the end or non-writable
 * socket, whichever comes first. Caller should take care of updating event set.
 * Even if no unsent WAL is available, at least one empty message will be sent
 * as a heartbeat, if socket is ready.
 *
 * Can change state if Async* functions encounter errors and reset connection.
 * Returns false in this case, true otherwise.
 */
static bool
SendAppendRequests(Safekeeper *sk)
{
	XLogRecPtr	endLsn = InvalidXLogRecPtr;
	AppendRequestHeader *req;
	PGAsyncWriteResult writeResult;

	if (sk->flushWrite)
	{
		if (!AsyncFlush(sk))

			/*
			 * AsyncFlush failed, that could happen if the socket is closed or
			 * we have nothing to write and should wait for writeable socket.
			 */
			return sk->state == SS_ACTIVE;

		/* Event set will be updated in the end of HandleActiveState */
		sk->flushWrite = false;
	}

	/*
	 * Only safekeeper leader can send WAL to Union Store.
	 */
	if (!sk->is_safekeeper_leader)
	{
	    return true;
	}

	/*
	 * Read WAL from wal buffer, and send to Union Store.
	 */
	while (true)
	{
		req = &sk->appendRequest;
		PrepareAppendRequest(&sk->appendRequest, sk->streamingAt, endLsn);

		resetStringInfo(&sk->outbuf);

		/* write AppendRequest header */
		appendBinaryStringInfo(&sk->outbuf, (char *) req, sizeof(AppendRequestHeader));

		/* write the WAL itself */
		enlargeStringInfo(&sk->outbuf, MAX_WAL_WRITE_SIZE);

		req = (AppendRequestHeader *)sk->outbuf.data;

		/*
		 * send any records we have in buffer.
		 */
		if (!GetUnionStoreWalRecords(sk->outbuf.data + sk->outbuf.len, &req->beginLsn, &req->endLsn, true))
			break;

		sk->outbuf.len += req->endLsn - req->beginLsn;
		writeResult = walprop_async_write(sk->conn, sk->outbuf.data, sk->outbuf.len);

		/* Mark current message as sent, whatever the result is */
		sk->streamingAt = req->endLsn;

		switch (writeResult)
		{
			case PG_ASYNC_WRITE_SUCCESS:
				/* Continue writing the next message */
				AdvanceWalCounter();
				break;

			case PG_ASYNC_WRITE_TRY_FLUSH:
				/*
				 * We still need to call PQflush some more to finish the
				 * job. Caller function will handle this by setting right
				 * event* set.
				 */
				AdvanceWalCounter();
				sk->flushWrite = true;
				return true;

			case PG_ASYNC_WRITE_FAIL:
				elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
					 sk->host, sk->port, FormatSafekeeperState(sk->state),
					 walprop_error_message(sk->conn));
				ShutdownConnection(sk);
				return false;
			default:
				Assert(false);
				return false;
		}
	}

	return true;
}

/*
 * Receive and process all available feedback.
 *
 * Can change state if Async* functions encounter errors and reset connection.
 * Returns false in this case, true otherwise.
 *
 * NB: This function can call SendMessageToNode and produce new messages.
 */
static bool
RecvAppendResponses(Safekeeper *sk)
{
	XLogRecPtr	minQuorumLsn;
	bool		readAnything = false;

	while (true)
	{
		/*
		 * If our reading doesn't immediately succeed, any necessary error
		 * handling or state setting is taken care of. We can leave any other
		 * work until later.
		 */
		sk->appendResponse.apm.tag = 'a';
		if (!AsyncReadMessage(sk, (AcceptorProposerMessage *) & sk->appendResponse))
			break;

		ereport(DEBUG2,
				(errmsg("received message term=" INT64_FORMAT " flushLsn=%X/%X commitLsn=%X/%X from %s:%s",
						sk->appendResponse.term,
						LSN_FORMAT_ARGS(sk->appendResponse.flushLsn),
						LSN_FORMAT_ARGS(sk->appendResponse.commitLsn),
						sk->host, sk->port)));

		if (sk->appendResponse.term > propTerm)
		{
			/* Another compute with higher term is running. */
			elog(PANIC, "WAL acceptor %s:%s with term " INT64_FORMAT " rejected our request, our term " INT64_FORMAT "",
				 sk->host, sk->port,
				 sk->appendResponse.term, propTerm);
		}

		readAnything = true;
	}

	if (!readAnything)
		return sk->state == SS_ACTIVE;

	SetUnionStoreFlushedLSN(sk->appendResponse.flushLsn, sk->appendResponse.localLsn);

	HandleSafekeeperResponse();

	/*
	 * Also send the new commit lsn to all the safekeepers.
	 */
	minQuorumLsn = GetAcknowledgedByQuorumWALPosition();
	if (minQuorumLsn > lastSentCommitLsn)
	{
		SendMessage();
		lastSentCommitLsn = minQuorumLsn;
	}

	return sk->state == SS_ACTIVE;
}

/* Parse a ReplicationFeedback message, or the ReplicationFeedback part of an AppendResponse */
void
ParseReplicationFeedbackMessage(StringInfo reply_message, ReplicationFeedback * rf)
{
	uint8		nkeys;
	int			i;
	int32		len;

	/* get number of custom keys */
	nkeys = pq_getmsgbyte(reply_message);

	for (i = 0; i < nkeys; i++)
	{
		const char *key = pq_getmsgstring(reply_message);

		if (strcmp(key, "current_timeline_size") == 0)
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->currentClusterSize = pq_getmsgint64(reply_message);
			elog(DEBUG2, "ParseReplicationFeedbackMessage: current_timeline_size %lu",
				 rf->currentClusterSize);
		}
		else if (strcmp(key, "ps_writelsn") == 0)
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->ps_writelsn = pq_getmsgint64(reply_message);
			elog(DEBUG2, "ParseReplicationFeedbackMessage: ps_writelsn %X/%X",
				 LSN_FORMAT_ARGS(rf->ps_writelsn));
		}
		else if (strcmp(key, "ps_flushlsn") == 0)
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->ps_flushlsn = pq_getmsgint64(reply_message);
			elog(DEBUG2, "ParseReplicationFeedbackMessage: ps_flushlsn %X/%X",
				 LSN_FORMAT_ARGS(rf->ps_flushlsn));
		}
		else if (strcmp(key, "ps_applylsn") == 0)
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->ps_applylsn = pq_getmsgint64(reply_message);
			elog(DEBUG2, "ParseReplicationFeedbackMessage: ps_applylsn %X/%X",
				 LSN_FORMAT_ARGS(rf->ps_applylsn));
		}
		else if (strcmp(key, "ps_replytime") == 0)
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->ps_replytime = pq_getmsgint64(reply_message);
			{
				char	   *replyTimeStr;

				/* Copy because timestamptz_to_str returns a static buffer */
				replyTimeStr = pstrdup(timestamptz_to_str(rf->ps_replytime));
				elog(DEBUG2, "ParseReplicationFeedbackMessage: ps_replytime %lu reply_time: %s",
					 rf->ps_replytime, replyTimeStr);

				pfree(replyTimeStr);
			}
		}
		else
		{
			len = pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */

			/*
			 * Skip unknown keys to support backward compatibile protocol
			 * changes
			 */
			elog(LOG, "ParseReplicationFeedbackMessage: unknown key: %s len %d", key, len);
			pq_getmsgbytes(reply_message, len);
		};
	}
}

/*
 * Get minimum of flushed LSNs of all safekeepers, which is the LSN of the
 * last WAL record that can be safely discarded.
 */
static XLogRecPtr
CalculateMinFlushLsn(void)
{
	XLogRecPtr	lsn = n_safekeepers > 0
	? safekeeper[0].appendResponse.flushLsn
	: InvalidXLogRecPtr;

	for (int i = 1; i < n_safekeepers; i++)
	{
		lsn = Min(lsn, safekeeper[i].appendResponse.flushLsn);
	}
	return lsn;
}

/*
 * Calculate WAL position acknowledged by quorum
 */
static XLogRecPtr
GetAcknowledgedByQuorumWALPosition(void)
{
	XLogRecPtr	responses[MAX_SAFEKEEPERS];

	/*
	 * Sort acknowledged LSNs
	 */
	for (int i = 0; i < n_safekeepers; i++)
	{
		/*
		 * Like in Raft, we aren't allowed to commit entries from previous
		 * terms, so ignore reported LSN until it gets to epochStartLsn.
		 */
		responses[i] = safekeeper[i].appendResponse.flushLsn >= propEpochStartLsn ? safekeeper[i].appendResponse.flushLsn : 0;
	}
	qsort(responses, n_safekeepers, sizeof(XLogRecPtr), CompareLsn);

	/*
	 * Get the smallest LSN committed by quorum
	 */
	return responses[n_safekeepers - quorum];
}

/*
 * ReplicationFeedbackShmemSize --- report amount of shared memory space needed
 */
Size
WalproposerShmemSize(void)
{
	return sizeof(WalproposerShmemState);
}

bool
WalproposerShmemInit(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	walprop_shared = ShmemInitStruct("Walproposer shared state",
									 sizeof(WalproposerShmemState),
									 &found);

	if (!found)
	{
		memset(walprop_shared, 0, WalproposerShmemSize());
		SpinLockInit(&walprop_shared->mutex);
		pg_atomic_init_u64(&walprop_shared->backpressureThrottlingTime, 0);
	}
	LWLockRelease(AddinShmemInitLock);

	return found;
}

void
replication_feedback_set(ReplicationFeedback * rf)
{
	SpinLockAcquire(&walprop_shared->mutex);
	memcpy(&walprop_shared->feedback, rf, sizeof(ReplicationFeedback));
	SpinLockRelease(&walprop_shared->mutex);
}

void
replication_feedback_get_lsns(XLogRecPtr *writeLsn, XLogRecPtr *flushLsn, XLogRecPtr *applyLsn)
{
	SpinLockAcquire(&walprop_shared->mutex);
	*writeLsn = walprop_shared->feedback.ps_writelsn;
	*flushLsn = walprop_shared->feedback.ps_flushlsn;
	*applyLsn = walprop_shared->feedback.ps_applylsn;
	SpinLockRelease(&walprop_shared->mutex);
}

/*
 * Get ReplicationFeedback fields from the most advanced safekeeper
 */
static void
GetLatestNeonFeedback(ReplicationFeedback * rf)
{
	int			latest_safekeeper = 0;
	XLogRecPtr	ps_writelsn = InvalidXLogRecPtr;

	for (int i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].appendResponse.rf.ps_writelsn > ps_writelsn)
		{
			latest_safekeeper = i;
			ps_writelsn = safekeeper[i].appendResponse.rf.ps_writelsn;
		}
	}

	rf->currentClusterSize = safekeeper[latest_safekeeper].appendResponse.rf.currentClusterSize;
	rf->ps_writelsn = safekeeper[latest_safekeeper].appendResponse.rf.ps_writelsn;
	rf->ps_flushlsn = safekeeper[latest_safekeeper].appendResponse.rf.ps_flushlsn;
	rf->ps_applylsn = safekeeper[latest_safekeeper].appendResponse.rf.ps_applylsn;
	rf->ps_replytime = safekeeper[latest_safekeeper].appendResponse.rf.ps_replytime;

	elog(DEBUG2, "GetLatestNeonFeedback: currentClusterSize %lu,"
		 " ps_writelsn %X/%X, ps_flushlsn %X/%X, ps_applylsn %X/%X, ps_replytime %lu",
		 rf->currentClusterSize,
		 LSN_FORMAT_ARGS(rf->ps_writelsn),
		 LSN_FORMAT_ARGS(rf->ps_flushlsn),
		 LSN_FORMAT_ARGS(rf->ps_applylsn),
		 rf->ps_replytime);

	replication_feedback_set(rf);
}

static void
HandleSafekeeperResponse(void)
{
	XLogRecPtr	minQuorumLsn;
	XLogRecPtr	diskConsistentLsn;
	XLogRecPtr	minFlushLsn;

	minQuorumLsn = GetAcknowledgedByQuorumWALPosition();
	diskConsistentLsn = quorumFeedback.rf.ps_flushlsn;

	if (!syncSafekeepers)
	{
		/* Get ReplicationFeedback fields from the most advanced safekeeper */
		GetLatestNeonFeedback(&quorumFeedback.rf);
		SetZenithCurrentClusterSize(quorumFeedback.rf.currentClusterSize);
	}

	/*
	 * Try to advance truncateLsn to minFlushLsn, which is the last record
	 * flushed to all safekeepers. We must always start streaming from the
	 * beginning of the record, which simplifies decoding on the far end.
	 *
	 * Advanced truncateLsn should be not further than nearest commitLsn. This
	 * prevents surprising violation of truncateLsn <= commitLsn invariant
	 * which might occur because 1) truncateLsn can be advanced immediately
	 * once chunk is broadcast to all safekeepers, and commitLsn generally
	 * can't be advanced based on feedback from safekeeper who is still in the
	 * previous epoch (similar to 'leader can't commit entries from previous
	 * term' in Raft); 2) chunks we read from WAL and send are plain sheets of
	 * bytes, but safekeepers ack only on record boundaries.
	 */
	minFlushLsn = CalculateMinFlushLsn();
	if (minFlushLsn > truncateLsn)
	{
		truncateLsn = minFlushLsn;
	}

	/*
	 * Generally sync is done when majority switched the epoch so we committed
	 * epochStartLsn and made the majority aware of it, ensuring they are
	 * ready to give all WAL to pageserver. It would mean whichever majority
	 * is alive, there will be at least one safekeeper who is able to stream
	 * WAL to pageserver to make basebackup possible. However, since at the
	 * moment we don't have any good mechanism of defining the healthy and
	 * most advanced safekeeper who should push the wal into pageserver and
	 * basically the random one gets connected, to prevent hanging basebackup
	 * (due to pageserver connecting to not-synced-safekeeper) we currently
	 * wait for all seemingly alive safekeepers to get synced.
	 */
	if (syncSafekeepers)
	{
		int			n_synced;

		n_synced = 0;
		for (int i = 0; i < n_safekeepers; i++)
		{
			Safekeeper *sk = &safekeeper[i];
			bool		synced = sk->appendResponse.commitLsn >= propEpochStartLsn;

			/* alive safekeeper which is not synced yet; wait for it */
			if (sk->state != SS_OFFLINE && !synced)
				return;
			if (synced)
				n_synced++;
		}
		if (n_synced >= quorum)
		{
			/* All safekeepers synced! */
			fprintf(stdout, "%X/%X\n", LSN_FORMAT_ARGS(propEpochStartLsn));
			exit(0);
		}
	}
}

/*
 * Try to read CopyData message from i'th safekeeper, resetting connection on
 * failure.
 */
static bool
AsyncRead(Safekeeper *sk, char **buf, int *buf_size)
{
	switch (walprop_async_read(sk->conn, buf, buf_size))
	{
		case PG_ASYNC_READ_SUCCESS:
			return true;

		case PG_ASYNC_READ_TRY_AGAIN:
			/* WL_SOCKET_READABLE is always set during copyboth */
			return false;

		case PG_ASYNC_READ_FAIL:
			elog(WARNING, "Failed to read from node %s:%s in %s state: %s", sk->host,
				 sk->port, FormatSafekeeperState(sk->state),
				 walprop_error_message(sk->conn));
			ShutdownConnection(sk);
			return false;
	}
	Assert(false);
	return false;
}

/*
 * Read next message with known type into provided struct, by reading a CopyData
 * block from the safekeeper's postgres connection, returning whether the read
 * was successful.
 *
 * If the read needs more polling, we return 'false' and keep the state
 * unmodified, waiting until it becomes read-ready to try again. If it fully
 * failed, a warning is emitted and the connection is reset.
 */
static bool
AsyncReadMessage(Safekeeper *sk, AcceptorProposerMessage * anymsg)
{
	char	   *buf;
	int			buf_size;
	uint64		tag;
	StringInfoData s;

	if (!(AsyncRead(sk, &buf, &buf_size)))
		return false;

	/* parse it */
	s.data = buf;
	s.len = buf_size;
	s.cursor = 0;

	tag = pq_getmsgint64_le(&s);
	if (tag != anymsg->tag)
	{
		elog(WARNING, "unexpected message tag %c from node %s:%s in state %s", (char) tag, sk->host,
			 sk->port, FormatSafekeeperState(sk->state));
		ResetConnection(sk);
		return false;
	}
	sk->latestMsgReceivedAt = GetCurrentTimestamp();
	switch (tag)
	{
		case 'g':
			{
				AcceptorGreeting *msg = (AcceptorGreeting *) anymsg;

				msg->term = pq_getmsgint64_le(&s);
				msg->nodeId = pq_getmsgint64_le(&s);
				msg->leader_nodeId = pq_getmsgint64_le(&s);
				msg->flush_lsn = pq_getmsgint64_le(&s);
				msg->am_safekeeper_leader = pq_getmsgbyte(&s);
				pq_getmsgend(&s);
				return true;
			}

		case 'v':
			{
				VoteResponse *msg = (VoteResponse *) anymsg;

				msg->term = pq_getmsgint64_le(&s);
				msg->voteGiven = pq_getmsgint64_le(&s);
				msg->flushLsn = pq_getmsgint64_le(&s);
				msg->truncateLsn = pq_getmsgint64_le(&s);
				msg->termHistory.n_entries = pq_getmsgint32_le(&s);
				msg->termHistory.entries = palloc(sizeof(TermSwitchEntry) * msg->termHistory.n_entries);
				for (int i = 0; i < msg->termHistory.n_entries; i++)
				{
					msg->termHistory.entries[i].term = pq_getmsgint64_le(&s);
					msg->termHistory.entries[i].lsn = pq_getmsgint64_le(&s);
				}
				msg->timelineStartLsn = pq_getmsgint64_le(&s);
				pq_getmsgend(&s);
				return true;
			}

		case 'a':
			{
				AppendResponse *msg = (AppendResponse *) anymsg;

				msg->term = pq_getmsgint64_le(&s);
				msg->flushLsn = pq_getmsgint64_le(&s);
				msg->localLsn = pq_getmsgint64_le(&s);
				msg->commitLsn = pq_getmsgint64_le(&s);
				msg->hs.ts = pq_getmsgint64_le(&s);
				msg->hs.xmin.value = pq_getmsgint64_le(&s);
				msg->hs.catalog_xmin.value = pq_getmsgint64_le(&s);
				if (buf_size > APPENDRESPONSE_FIXEDPART_SIZE)
					ParseReplicationFeedbackMessage(&s, &msg->rf);
				pq_getmsgend(&s);
				return true;
			}

		default:
			{
				Assert(false);
				return false;
			}
	}
}

/*
 * Blocking equivalent to AsyncWrite.
 *
 * We use this everywhere messages are small enough that they should fit in a
 * single packet.
 */
static bool
BlockingWrite(Safekeeper *sk, void *msg, size_t msg_size, SafekeeperState success_state)
{
	uint32		events;

	if (!walprop_blocking_write(sk->conn, msg, msg_size))
	{
		elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
			 sk->host, sk->port, FormatSafekeeperState(sk->state),
			 walprop_error_message(sk->conn));
		ShutdownConnection(sk);
		return false;
	}

	sk->state = success_state;

	/*
	 * If the new state will be waiting for events to happen, update the event
	 * set to wait for those
	 */
	events = SafekeeperStateDesiredEvents(success_state);
	if (events)
		UpdateEventSet(sk, events);

	return true;
}

/*
 * Starts a write into the 'i'th safekeeper's postgres connection, moving to
 * flush_state (adjusting eventset) if write still needs flushing.
 *
 * Returns false if sending is unfinished (requires flushing or conn failed).
 * Upon failure, a warning is emitted and the connection is reset.
 */
static bool
AsyncWrite(Safekeeper *sk, void *msg, size_t msg_size, SafekeeperState flush_state)
{
	switch (walprop_async_write(sk->conn, msg, msg_size))
	{
		case PG_ASYNC_WRITE_SUCCESS:
			return true;
		case PG_ASYNC_WRITE_TRY_FLUSH:

			/*
			 * We still need to call PQflush some more to finish the job; go
			 * to the appropriate state. Update the event set at the bottom of
			 * this function
			 */
			sk->state = flush_state;
			UpdateEventSet(sk, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
			return false;
		case PG_ASYNC_WRITE_FAIL:
			elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
				 sk->host, sk->port, FormatSafekeeperState(sk->state),
				 walprop_error_message(sk->conn));
			ShutdownConnection(sk);
			return false;
		default:
			Assert(false);
			return false;
	}
}

/*
 * Flushes a previous call to AsyncWrite. This only needs to be called when the
 * socket becomes read or write ready *after* calling AsyncWrite.
 *
 * If flushing successfully completes returns true, otherwise false. Event set
 * is updated only if connection fails, otherwise caller should manually unset
 * WL_SOCKET_WRITEABLE.
 */
static bool
AsyncFlush(Safekeeper *sk)
{
	/*---
	 * PQflush returns:
	 *   0 if successful                    [we're good to move on]
	 *   1 if unable to send everything yet [call PQflush again]
	 *  -1 if it failed                     [emit an error]
	 */
	switch (walprop_flush(sk->conn))
	{
		case 0:
			/* flush is done */
			return true;
		case 1:
			/* Nothing to do; try again when the socket's ready */
			return false;
		case -1:
			elog(WARNING, "Failed to flush write to node %s:%s in %s state: %s",
				 sk->host, sk->port, FormatSafekeeperState(sk->state),
				 walprop_error_message(sk->conn));
			ResetConnection(sk);
			return false;
		default:
			Assert(false);
			return false;
	}
}

uint64
BackpressureThrottlingTime(void)
{
	return pg_atomic_read_u64(&walprop_shared->backpressureThrottlingTime);
}

/*
 * Copy of AdvanceNextFullTransactionIdPastXid, but called by wal proposer.
 */
static void
AdvanceNextFullTransactionId(TransactionId xid)
{
    FullTransactionId newNextXid;
    TransactionId next_xid;
    uint32		epoch;

    /* Fast return if this isn't an xid high enough to move the needle. */
    next_xid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
    if (!TransactionIdFollowsOrEquals(xid, next_xid))
        return;

    /*
	 * Compute the FullTransactionId that comes after the given xid.  To do
	 * this, we preserve the existing epoch, but detect when we've wrapped
	 * into a new epoch.  This is necessary because WAL records and 2PC state
	 * currently contain 32 bit xids.  The wrap logic is safe in those cases
	 * because the span of active xids cannot exceed one epoch at any given
	 * point in the WAL stream.
	 */
    TransactionIdAdvance(xid);
    epoch = EpochFromFullTransactionId(ShmemVariableCache->nextXid);
    if (unlikely(xid < next_xid))
        ++epoch;
    newNextXid = FullTransactionIdFromEpochAndXid(epoch, xid);

    /*
	 * We still need to take a lock to modify the value when there are
	 * concurrent readers.
	 */
    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
    ShmemVariableCache->nextXid = newNextXid;
    LWLockRelease(XidGenLock);
}

/*
 * Before doing anything, let the wal proposer finishing startup job.
 */
static bool
UnionStoreStartupXLog(XLogRecPtr startup_lsn)
{
    FullTransactionId xid;
    static bool promoted = false;

    /*
     * If promotion triggered, we just need to set the global transaction id.
     */
    if (!promoted)
    {
        while(true)
        {
            /*
             * Before doing the startup works, wait StartupXLog to finish some basic startup works,
             * such as set the global transaction ID.
             */
            int rc = WaitLatch((Latch *)GetUnionStoreStartupLatch(), WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 3000L,
                             WAIT_EVENT_MQ_INTERNAL);
            if (rc & WL_LATCH_SET)
            {
                ResetLatch((Latch *)GetUnionStoreStartupLatch());

                /*
                 * Promotion triggered, we need to refresh the leader info.
                 */
                if (PromoteIsTriggered())
                {
                    promoted = true;
                    return false;
                }

                break;
            }

            CHECK_FOR_INTERRUPTS();
        }
    }

    /*
     * In some corner cases, we may fail to get the transaction id from page server,
     * just retry.
     *
     * Corner case: get transaction id using latest LSN after page server crash recovery, and page
     * server does not have the WAL till latest LSN, page server receiving WAL from safekeeper continuously.
     * We should wait until page server reaches the latest LSN.
     */
    while(true)
    {
        PG_TRY();
        {

            /*
             * UnionStore's wal records are persisted in Wal Service, we have to get the
             * latest XID to advance the global transaction ID as Startup did to avoid
             * using the wrong XID.
             */
            xid = neon_get_latest_transaction_id(startup_lsn);

            elog(LOG, "Latest transaction id %u from union store, local xid %u", XidFromFullTransactionId(xid), XidFromFullTransactionId(ShmemVariableCache->nextXid));

            AdvanceNextFullTransactionId(XidFromFullTransactionId(xid));

            /*
             * Maybe other startup works later.
             */


            return true;
        }
        PG_CATCH();
        {
            FlushErrorState();
            pg_usleep(1000000L);
        }
        PG_END_TRY();
    }

    return true;
}

/*
 * Wait Union Store to finish the startup job.
 */
static void
UnionStoreStartup(void)
{
    /*
     * We have finished the basic startup works, wake up wal proposer.
     */
    SetLatch((Latch *)GetUnionStoreStartupLatch());

    /*
     * Wal proposer is doing the startup works, wait wal proposer to finish the job.
     */
    UnionStoreXLogFlush(DEFAULT_XLOG_SEG_SIZE);

    /*
     * nextXid is set, make sure the page of distributed log/clog which is related with nextXid does exist.
     */
    DistributedLog_Extend_if_needed(XidFromFullTransactionId(ShmemVariableCache->nextXid));
    CLOG_Extend_if_needed(XidFromFullTransactionId(ShmemVariableCache->nextXid));

    if (prev_Startup_hook)
        (*prev_Startup_hook) ();
}

#define ENTRIES_PER_PAGE (BLCKSZ / sizeof(DistributedLogEntry))
#define TransactionIdToPage(localXid) ((localXid) / (TransactionId) ENTRIES_PER_PAGE)
#define TransactionIdToEntry(localXid) ((localXid) % (TransactionId) ENTRIES_PER_PAGE)

static int
DistributedLog_ZeroPage_if_needed(SlruCtl ctl, int page)
{
	int         slotno;

	Assert(!IS_QUERY_DISPATCHER());

	elog((Debug_print_full_dtm ? LOG : DEBUG5),
	     "DistributedLog_ZeroPage zero page %d",
	     page);
	slotno = SimpleLruZeroPage(ctl, page);

	XLogBeginInsert();
	XLogRegisterData((char *) (&page), sizeof(int));
	XLogRecPtr endPos = XLogInsert(RM_DISTRIBUTEDLOG_ID, DISTRIBUTEDLOG_ZEROPAGE);
	return slotno;
}

/*
 * Make room for the newly-allocated XID if needed.
 */
static void
DistributedLog_Extend_if_needed(TransactionId newestXact)
{
	SlruCtl ctl = (SlruCtl)DistributedLog_Ctl();
	int endPage = TransactionIdToPage(newestXact);
	int         startPage;
	FullTransactionId nextXid;
	TransactionId oldestXmin;

	LWLockAcquire(DistributedLogControlLock, LW_EXCLUSIVE);
	// firstly init DistributedLog_InitOldestXmin cause DistributedLog_Startup follows Startup_hook (maybe invoke DistributedLog_InitOldestXmin twice)
	DistributedLog_InitOldestXmin();
	oldestXmin = DistributedLog_GetOldestXmin(newestXact);
	startPage = TransactionIdToPage(oldestXmin);
	endPage = TransactionIdToPage(newestXact);
	while(startPage != endPage)
	{
		if (!SimpleLruDoesPhysicalPageExist(ctl, startPage))
		{
			DistributedLog_ZeroPage_if_needed(ctl, startPage);
		}
		startPage++;
		if (startPage > TransactionIdToPage(MaxTransactionId))
			startPage = 0;
	}
	if (!SimpleLruDoesPhysicalPageExist(ctl, startPage))
	{
		DistributedLog_ZeroPage_if_needed(ctl, startPage);
	}

    LWLockRelease(DistributedLogControlLock);
}


#define CLOG_XACTS_PER_BYTE 4
#define CLOG_XACTS_PER_PAGE (BLCKSZ * CLOG_XACTS_PER_BYTE)

#define TransactionIdToCLogPage(xid)	((xid) / (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CLOG_XACTS_PER_PAGE)
/*
 * Make room for the newly-allocated XID if needed.
 */
static void
CLOG_Extend_if_needed(TransactionId newestXact)
{
    SlruCtl ctl = (SlruCtl)CLOG_Ctl();
    int page = TransactionIdToCLogPage(newestXact);

    LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);

    if (TransactionIdToPgIndex(newestXact) != 0)
    {
        if (!SimpleLruDoesPhysicalPageExist(ctl, page))
        {
            SimpleLruZeroPage(ctl, page);

            XLogBeginInsert();
            XLogRegisterData((char *) (&page), sizeof(int));
            (void) XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE);
        }
    }

    LWLockRelease(XactSLRULock);
}

#define VAR_OID_PREFETCH		8192

/*
 * Assign new relfilenode id
 */
static RelFileNodeId
AssignNewRelFileNode(void)
{
    RelFileNodeId result;

    if (ShmemVariableCache->nextRelfilenode & (1L << 63))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("could not assign new relfilenode"),
                errdetail("relfilenode exceeds the max limitation")));
    }

    /*
     * Divide Relfilenode into ranges, every range contains numsegmentsFromQD Relfilenodes.
     * Each segment gets the Relfilenode from the range at the segindex, this can make sure
     * that Relfilenode allocated in each segment is unique in cluster.
     */
    if (Gp_role == GP_ROLE_EXECUTE)
    {
        if (ShmemVariableCache->relfilenodeCount < numsegmentsFromQD)
        {
            XLogPutNextRelfilenode(ShmemVariableCache->nextRelfilenode + VAR_OID_PREFETCH);
            ShmemVariableCache->relfilenodeCount = VAR_OID_PREFETCH - (numsegmentsFromQD - ShmemVariableCache->relfilenodeCount);
        }
        else
            (ShmemVariableCache->relfilenodeCount) -= numsegmentsFromQD;

        result = ShmemVariableCache->nextRelfilenode % numsegmentsFromQD;

        result = ShmemVariableCache->nextRelfilenode - result + GpIdentity.segindex;

        (ShmemVariableCache->nextRelfilenode) += numsegmentsFromQD;

        return result;
    }

    result = ShmemVariableCache->nextRelfilenode;

    (ShmemVariableCache->nextRelfilenode)++;
    (ShmemVariableCache->relfilenodeCount)--;

    /*
	 * In utility mode, set the highest bit of new allocated relfilenode to 1, so the relfilenode allocated
	 * can be distinguished from relfilenode allocated in execute mode.
	 */
    if (Gp_role == GP_ROLE_UTILITY)
    {
        return result | 1L << 63;
    }

    return result;
}

/*
 * Start wal proposer in FatalError. We need wal proposer to fetch latest txn id from page server to
 * advance global transaction id.
 */
static void
UnionStore_start_bgworkers(bool FatalError, int pmState, start_bgworker_func startBgworkerFunc)
{
#define PM_STARTUP 1
	/*
	 * Only start wal proposer if not running in FatalError and waiting for startup subprocess.
	 * Wal proposer will be launched by postmaster in normal mode.
	 */
	if (FatalError && PM_STARTUP == pmState)
	{
		slist_mutable_iter iter;

		slist_foreach_modify(iter, &BackgroundWorkerList)
		{
			RegisteredBgWorker *rw;

			rw = slist_container(RegisteredBgWorker, rw_lnode, iter.cur);

			/* ignore if already running */
			if (rw->rw_pid != 0)
				continue;

			if (strcmp(rw->rw_worker.bgw_function_name, "WalProposerMain") == 0)
			{
				/* reset crash time before trying to start worker */
				rw->rw_crashed_at = 0;

				while(!startBgworkerFunc(rw))
				{
					pg_usleep(1000000L);
				}

				break;
			}
		}
	}

	if (prev_start_bgworkers_hook)
		(*prev_start_bgworkers_hook) (FatalError, pmState, startBgworkerFunc);
}