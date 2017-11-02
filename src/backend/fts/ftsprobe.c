/*-------------------------------------------------------------------------
 *
 * ftsprobe.c
 *	  Implementation of segment probing interface
 *
 * Portions Copyright (c) 2006-2011, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/fts/ftsprobe.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <pthread.h>
#include <limits.h>

#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include "libpq-fe.h"
#include "libpq-int.h"

#include "cdb/cdbgang.h"		/* gp_pthread_create */
#include "libpq/ip.h"
#include "postmaster/fts.h"
#include "postmaster/ftsprobe.h"

#include "executor/spi.h"
#include "postmaster/primary_mirror_mode.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#ifdef USE_TEST_UTILS
#include "utils/simex.h"
#endif /* USE_TEST_UTILS */

/*
 * CONSTANTS
 */

#ifdef USE_SEGWALREP
#define PROBE_RESPONSE_LEN  (sizeof(ProbeResponse) + sizeof(PacketLen))         /* size of segment response message */
#else
#define PROBE_RESPONSE_LEN  (20)         /* size of segment response message */
#endif


/*
 * MACROS
 */

#define SYS_ERR_TRANSIENT(errno) (errno == EINTR || errno == EAGAIN)


/*
 * STRUCTURES
 */
typedef struct ProbeMsg
{
	uint32 packetlen;
	PrimaryMirrorTransitionPacket payload;
} ProbeMsg;

/*
 * STATIC VARIABLES
 */


#ifdef USE_SEGWALREP
/* mutex used for pthread synchronization in parallel probing */
static pthread_mutex_t worker_thread_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

/*
 * FUNCTION PROTOTYPES
 */

static bool probeConnect(CdbComponentDatabaseInfo *dbInfo, ProbeConnectionInfo *probeInfo);
static bool probePollOut(ProbeConnectionInfo *probeInfo);
static bool probeSend(ProbeConnectionInfo *probeInfo);
static bool probePollIn(ProbeConnectionInfo *probeInfo);
static bool probeReceive(ProbeConnectionInfo *probeInfo);
#ifndef USE_SEGWALREP
static bool probeProcessResponse(ProbeConnectionInfo *probeInfo);
#endif
static bool probeTimeout(ProbeConnectionInfo *probeInfo, const char* calledFrom);

/*
 * strerror() is not threadsafe.  Therefore, prober threads must use
 * the correct strerror_r() available on the platform to obtain error
 * message corresponding to errno.
 */
static char *errmessage(ProbeConnectionInfo *probeInfo)
{
#if (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && ! _GNU_SOURCE
	/* POSIX compliant version (seen on OSX 10.9) */
	int ret = strerror_r(probeInfo->probe_errno,
						 probeInfo->errmsg, PROBE_ERR_MSG_LEN);
	if (ret != 0)
		probeInfo->errmsg[0] = '\0';
#else
	/* GNU version (seen on RHEL 6.2) */
	char *msg = strerror_r(probeInfo->probe_errno,
						   probeInfo->errmsg, PROBE_ERR_MSG_LEN);
	if (msg == NULL)
		probeInfo->errmsg[0] = '\0';
	else
	{
		strncpy(probeInfo->errmsg, msg, PROBE_ERR_MSG_LEN-1);
		/* In case strncpy doesn't null-terminate errmsg */
		probeInfo->errmsg[PROBE_ERR_MSG_LEN-1] = '\0';
	}
#endif
	return probeInfo->errmsg;
}

#ifdef USE_SEGWALREP
void
#else
char
#endif
probeSegmentHelper(CdbComponentDatabaseInfo *dbInfo, ProbeConnectionInfo *probeInfo)
{
	/*
	 * probe segment: open socket -> connect -> send probe msg -> receive response;
	 * on any error the connection is shut down, the socket is closed
	 * and the probe process is restarted;
	 * this is repeated until no error occurs (response is received) or timeout expires;
	 */

	int retryCnt = 0;
	/* reset timer */
	gp_set_monotonic_begin_time(&probeInfo->startTime);
	while (retryCnt < gp_fts_probe_retries && FtsIsActive() && (
			   !probeConnect(dbInfo, probeInfo) ||
			   !probePollOut(probeInfo) ||
			   !probeSend(probeInfo) ||
			   !probePollIn(probeInfo) ||
			   !probeReceive(probeInfo)
#ifdef USE_SEGWALREP
#else
			   || !probeProcessResponse(probeInfo)
#endif
		))
	{
		PQfinish(probeInfo->conn);
		probeInfo->conn = NULL;

		/* sleep for 1 second to avoid tight loops */
		pg_usleep(USECS_PER_SEC);
		retryCnt++;

		write_log("FTS: retry %d to probe segment (content=%d, dbid=%d).",
				  retryCnt - 1, probeInfo->segmentId, probeInfo->dbId);

		/* reset timer */
		gp_set_monotonic_begin_time(&probeInfo->startTime);
	}

#ifdef USE_SEGWALREP
#else
	if (!FtsIsActive())
	{
		/* the returned value will be ignored */
		probeInfo->segmentStatus = PROBE_ALIVE;
	}
#endif

	if (retryCnt == gp_fts_probe_retries)
	{
		write_log("FTS: failed to probe segment (content=%d, dbid=%d) after trying %d time(s), "
				  "maximum number of retries reached.",
				  probeInfo->segmentId,
				  probeInfo->dbId,
				  retryCnt);
#ifdef USE_SEGWALREP
		probeInfo->result->isPrimaryAlive = false;
#endif
	}

	if (probeInfo->conn)
	{
		PQfinish(probeInfo->conn);
		probeInfo->conn = NULL;
	}

#ifdef USE_SEGWALREP
#else
	return probeInfo->segmentStatus;
#endif
}

#ifdef USE_SEGWALREP
static void
probeWalRepSegment(probe_response_per_segment *response)
{
	Assert(response);
	CdbComponentDatabaseInfo *segment_db_info = response->segment_db_info;
	Assert(segment_db_info);

	if(!FtsIsSegmentAlive(response->segment_db_info))
	{
		response->result.isPrimaryAlive = false;
		response->result.isMirrorAlive = false;

		return;
	}

	/* setup probe descriptor */
	ProbeConnectionInfo probeInfo;
	memset(&probeInfo, 0, sizeof(ProbeConnectionInfo));
	probeInfo.segmentId = segment_db_info->segindex;
	probeInfo.dbId = segment_db_info->dbid;
	probeInfo.role = segment_db_info->role;
	probeInfo.mode = segment_db_info->mode;
	probeInfo.result = &(response->result);

	probeSegmentHelper(segment_db_info, &probeInfo);
}

static void *
probeWalRepSegmentFromThread(void *arg)
{
	probe_context *context = (probe_context *)arg;

	Assert(context);

	int number_of_probed_segments = 0;

	while(number_of_probed_segments < context->count)
	{
		/*
		 * Look for the unprocessed context
		 */
		int response_index = number_of_probed_segments;

		pthread_mutex_lock(&worker_thread_mutex);
		while(response_index < context->count)
		{
			if (!context->responses[response_index].isScheduled)
			{
				context->responses[response_index].isScheduled = true;
				break;
			}
			number_of_probed_segments++;
			response_index++;
		}
		pthread_mutex_unlock(&worker_thread_mutex);

		/*
		 * If probed all the segments, we are done.
		 */
		if (response_index == context->count)
			break;

		/*
		 * If FTS is shutdown, abort.
		 */
		if (!FtsIsActive())
			break;

		/* now let's probe the primary. */
		probe_response_per_segment *response = &context->responses[response_index];
		Assert(SEGMENT_IS_ACTIVE_PRIMARY(response->segment_db_info));
		probeWalRepSegment(response);
	}

	return NULL;
}

void
FtsWalRepProbeSegments(probe_context *context)
{
	int workers = gp_fts_probe_threadcount;
	pthread_t *threads = NULL;
	Assert(context);

	threads = (pthread_t *)palloc(workers * sizeof(pthread_t));

	for(int i = 0; i < workers; i++)
	{
		int ret = gp_pthread_create(&threads[i], probeWalRepSegmentFromThread, context, "FtsWalRepProbeSegments");

		if (ret)
		{
			ereport(ERROR, (errmsg("FTS: failed to create probing thread")));
		}
	}

	for(int i = 0; i < workers; i++)
	{
		int ret = pthread_join(threads[i], NULL);

		if (ret)
		{
			ereport(ERROR, (errmsg("FTS: failed to join probing thread")));
		}
	}
}
#endif

/*
 * Establish async libpq connection to a segment
 */
static bool
probeConnect(CdbComponentDatabaseInfo *dbInfo, ProbeConnectionInfo *probeInfo)
{
	char conninfo[1024];
#ifdef USE_SEGWALREP
	snprintf(conninfo, 1024, "host=%s port=%d gpconntype=%s",
			 dbInfo->hostip, dbInfo->port, GPCONN_TYPE_FTS);
	probeInfo->conn = PQconnectdb(conninfo);
#else
	snprintf(conninfo, 1024, "postgresql://%s:%d", dbInfo->hostip, dbInfo->port);
	probeInfo->conn = PQconnectStart(conninfo);
#endif
	if (probeInfo->conn == NULL)
	{
		write_log("FTS: cannot create libpq connection object, possibly out of memory"
				  " (content=%d, dbid=%d)", probeInfo->segmentId, probeInfo->dbId);
		return false;
	}
	if (probeInfo->conn->status == CONNECTION_BAD)
	{
		write_log("FTS: cannot establish libpq connection to (content=%d, dbid=%d): %s",
				  probeInfo->segmentId, probeInfo->dbId,
				  probeInfo->conn->errorMessage.data);
		return false;
	}
	return true;
}

/*
 * Wait for a socket to become available for writing.
 */
static bool
probePollOut(ProbeConnectionInfo *probeInfo)
{
	struct pollfd nfd;
	nfd.fd = PQsocket(probeInfo->conn);
	nfd.events = POLLOUT;
	int ret;

	while ((ret = poll(&nfd, 1, 1000)) < 0 &&
		   (errno == EAGAIN || errno == EINTR) &&
		   !probeTimeout(probeInfo, "probePollOut"));
	if (ret == 0)
	{
		write_log("FTS: pollout timeout waiting for libpq socket to become available,"
				  " (content=%d, dbid=%d)", probeInfo->segmentId, probeInfo->dbId);
		return false;
	}
	if (ret == -1)
	{
		probeInfo->probe_errno = errno;
		write_log("FTS: pollout error on libpq socket (content=%d, dbid=%d): %s",
				  probeInfo->segmentId, probeInfo->dbId, errmessage(probeInfo));
		return false;
	}

	PostgresPollingStatusType status = PQconnectPoll(probeInfo->conn);
	switch(status)
	{
		case PGRES_POLLING_OK:
		case PGRES_POLLING_WRITING:
			return true;
		case PGRES_POLLING_READING:
			write_log("FTS: PQconnectPoll protocol violation: received data "
					  "before sending probe (content=%d, dbid=%d)",
					  probeInfo->segmentId, probeInfo->dbId);
			break;
		case PGRES_POLLING_FAILED:
			write_log("FTS: PQconnectPoll failed (content=%d, dbid=%d): %s",
					  probeInfo->segmentId, probeInfo->dbId,
					  probeInfo->conn->errorMessage.data);
			break;
		default:
			write_log("FTS: bad status (%d) on libpq connection "
					  "(content=%d, dbid=%d)",
					  status, probeInfo->segmentId, probeInfo->dbId);
			break;
	}
	return false;
}

#ifdef USE_SEGWALREP
/*
 * Send FTS query
 */
static bool
probeSend(ProbeConnectionInfo *probeInfo)
{
	if (!PQsendQuery(probeInfo->conn, FTS_MSG_TYPE_PROBE))
	{
		write_log("FTS: failed to send query '%s' to (content=%d, dbid=%d): "
				  "connection status %d, %s",
				  FTS_MSG_TYPE_PROBE, probeInfo->segmentId, probeInfo->dbId,
				  probeInfo->conn->status, probeInfo->conn->errorMessage.data);
		return false;
	}
	return true;
}
#else
/*
 * Send the status request-startup-packet
 */
static bool
probeSend(ProbeConnectionInfo *probeInfo)
{
	char firstchar = 0;
	ProtocolVersion req;
	req = htonl(PRIMARY_MIRROR_TRANSITION_QUERY_CODE);
	if (pqPacketSend(probeInfo->conn, firstchar, &req, sizeof(req)) != STATUS_OK)
	{
		write_log("FTS: failed to send probe packet to (content=%d, dbid=%d): "
				  "connection status %d, %s",
				  probeInfo->segmentId, probeInfo->dbId, probeInfo->conn->status,
				  probeInfo->conn->errorMessage.data);
		return false;
	}
	return true;
}
#endif

/*
 * Wait for a socket to become available for reading.
 */
static bool
probePollIn(ProbeConnectionInfo *probeInfo)
{
	struct pollfd nfd;
	nfd.fd = PQsocket(probeInfo->conn);
	nfd.events = POLLIN;
	int ret;

	do
	{
		ret = poll(&nfd, 1, 1000);

		if (ret > 0)
		{
			return true;
		}

		if (ret == -1 && errno != EAGAIN && errno != EINTR)
		{
			probeInfo->probe_errno = errno;
			write_log("FTS: pollin error on libpq socket (content=%d, dbid=%d): %s",
					  probeInfo->segmentId, probeInfo->dbId, errmessage(probeInfo));

			return false;
		}
	} while (!probeTimeout(probeInfo, "probePollIn"));

	write_log("FTS: pollin timeout waiting for libpq socket to become "
					  "available (content=%d, dbid=%d)", probeInfo->segmentId,
			  probeInfo->dbId);

	return false;
}

#ifdef USE_SEGWALREP
/*
 * Record FTS handler's response from libpq result into probe_result
 */
static void
probeRecordResponse(ProbeConnectionInfo *probeInfo, PGresult *result)
{
	probeInfo->result->isPrimaryAlive = true;
	int *isMirrorAlive = (int *) PQgetvalue(result, 0,
											Anum_fts_probe_response_is_mirror_up);
	Assert (isMirrorAlive);
	probeInfo->result->isMirrorAlive = *isMirrorAlive;

	write_log("FTS: segment (content=%d, dbid=%d, role=%c) reported IsMirrorUp %d to the prober.",
			  probeInfo->segmentId, probeInfo->dbId, probeInfo->role, probeInfo->result->isMirrorAlive);
}

/*
 * Receive segment response
 */
static bool
probeReceive(ProbeConnectionInfo *probeInfo)
{
	/* last non-null result that will be returned */
	PGresult   *lastResult = NULL;

	/*
	 * Could directly use PQexec() here instead of loop and have very simple
	 * version, only hurdle is PQexec() (and PQgetResult()) has infinite wait
	 * coded in pqWait() and hence doesn't allow for checking the timeout
	 * (SLA) for FTS probe.
	 *
	 * It would have been really easy if pqWait() could have been taken
	 * timeout as connection object property instead of having -1 hard-coded,
	 * that way could have easily used the PQexec() interface.
	 */
	for (;;)
	{
		PGresult   *tmpResult = NULL;
		/*
		 * Receive data until PQgetResult is ready to get the result without
		 * blocking.
		 */
		while (PQisBusy(probeInfo->conn))
		{
			if (!probePollIn(probeInfo))
			{
				return false;	/* either timeout or error happened */
			}

			if (PQconsumeInput(probeInfo->conn) == 0)
			{
				write_log("FTS: error reading probe response from "
						  "libpq socket (content=%d, dbid=%d): %s",
						  probeInfo->segmentId, probeInfo->dbId,
						  PQerrorMessage(probeInfo->conn));
				return false;	/* trouble */
			}
		}

		/*
		 * Emulate the PQexec()'s behavior of returning the last result when
		 * there are many. Since walsender will never generate multiple
		 * results, we skip the concatenation of error messages.
		 */
		tmpResult = PQgetResult(probeInfo->conn);
		if (tmpResult == NULL)
			break;				/* response received */

		PQclear(lastResult);
		lastResult = tmpResult;

		if (PQstatus(probeInfo->conn) == CONNECTION_BAD)
		{
			write_log("FTS: error reading probe response from "
					  "libpq socket (content=%d, dbid=%d): %s",
					  probeInfo->segmentId, probeInfo->dbId,
					  PQerrorMessage(probeInfo->conn));
			return false;	/* trouble */
		}
	}
	if (PQresultStatus(lastResult) != PGRES_TUPLES_OK)
	{
		PQclear(lastResult);
		write_log("FTS: error reading probe response from "
				  "libpq socket (content=%d, dbid=%d): %s",
				  probeInfo->segmentId, probeInfo->dbId,
				  PQerrorMessage(probeInfo->conn));
		return false;
	}
	if (PQnfields(lastResult) != Natts_fts_probe_response ||
		PQntuples(lastResult) != FTS_PROBE_RESPONSE_NTUPLES)
	{
		int			ntuples = PQntuples(lastResult);
		int			nfields = PQnfields(lastResult);

		PQclear(lastResult);
		write_log("FTS: invalid probe response from (content=%d, dbid=%d):"
				  "Expected %d tuple with %d field, got %d tuples with %d fields",
				  probeInfo->segmentId, probeInfo->dbId, FTS_PROBE_RESPONSE_NTUPLES,
				  Natts_fts_probe_response, ntuples, nfields);
		return false;
	}

	probeRecordResponse(probeInfo, lastResult);
	return true;
}
#else
/*
 * Receive segment response
 */
static bool
probeReceive(ProbeConnectionInfo *probeInfo)
{
	int ret;
	if ((ret = pqReadData(probeInfo->conn)) == -1)
	{
		write_log("FTS: error reading probe response from libpq socket "
				  "(content=%d, dbid=%d): %s",
				  probeInfo->segmentId, probeInfo->dbId,
				  probeInfo->conn->errorMessage.data);
	}
	else if (ret == 0)
	{
		write_log("FTS: no probe response available on libpq socket "
				  "(content=%d, dbid=%d)",
				  probeInfo->segmentId, probeInfo->dbId);
	}
	return (ret > 0);
}
#endif

/*
 * Check if probe timeout has expired
 */
static bool
probeTimeout(ProbeConnectionInfo *probeInfo, const char* calledFrom)
{
	uint64 elapsed_ms = gp_get_elapsed_ms(&probeInfo->startTime);
	const uint64 debug_elapsed_ms = 5000;

	if (gp_log_fts >= GPVARS_VERBOSITY_VERBOSE && elapsed_ms > debug_elapsed_ms)
	{
		write_log("FTS: probe '%s' elapsed time: " UINT64_FORMAT " ms for segment (content=%d, dbid=%d).",
		          calledFrom, elapsed_ms, probeInfo->segmentId, probeInfo->dbId);
	}

	/* If connection takes more than the gp_fts_probe_timeout, we fail. */
	if (elapsed_ms > (uint64)gp_fts_probe_timeout * 1000)
	{
		write_log("FTS: failed to probe segment (content=%d, dbid=%d) due to timeout expiration, "
				  "probe elapsed time: " UINT64_FORMAT " ms.",
				  probeInfo->segmentId,
				  probeInfo->dbId,
				  elapsed_ms);

		return true;
	}

	return false;
}

#ifdef USE_SEGWALREP
#else
/*
 * Process segment response
 */
static bool
probeProcessResponse(ProbeConnectionInfo *probeInfo)
{
	int32 responseLen;
	if (pqGetInt(&responseLen, sizeof(PacketLen), probeInfo->conn))
	{
		write_log("FTS: invalid response from segment "
				  "(content=%d, dbid=%d): %s", probeInfo->segmentId,
				  probeInfo->dbId, probeInfo->conn->errorMessage.data);
		return false;
	}

	if (responseLen != PROBE_RESPONSE_LEN)
	{
		write_log("FTS: invalid response length %d from segment "
				  "(content=%d, dbid=%d): %s", responseLen, probeInfo->segmentId,
				  probeInfo->dbId, probeInfo->conn->errorMessage.data);
		return false;
	}
	/* segment responded to probe, mark it as alive */
	probeInfo->segmentStatus = PROBE_ALIVE;
	write_log("FTS: segment (content=%d, dbid=%d) reported segmentstatus %x to the prober.",
			  probeInfo->segmentId, probeInfo->dbId, probeInfo->segmentStatus);

	int32 role;
	int32 state;
	int32 mode;
	int32 fault;
	
	if (pqGetInt(&role, 4, probeInfo->conn) ||
		pqGetInt(&state, 4, probeInfo->conn) ||
		pqGetInt(&mode, 4, probeInfo->conn) ||
		pqGetInt(&fault, 4, probeInfo->conn))
	{
		write_log("FTS: invalid response from segment "
				  "(content=%d, dbid=%d): %s", probeInfo->segmentId,
				  probeInfo->dbId, probeInfo->conn->errorMessage.data);
		return false;
	}

	if (gp_log_fts > GPVARS_VERBOSITY_VERBOSE)
	{
		write_log("FTS: probe result for (content=%d, dbid=%d): %s %s %s %s.",
				  probeInfo->segmentId, probeInfo->dbId,
				  getMirrorModeLabel(role),
				  getSegmentStateLabel(state),
				  getDataStateLabel(mode),
				  getFaultTypeLabel(fault));
	}

	/* check if segment has completed re-synchronizing */
	if (mode == DataStateInSync && probeInfo->mode == 'r')
	{
		probeInfo->segmentStatus |= PROBE_RESYNC_COMPLETE;
	}
	else
	{
		/* check for inconsistent segment state */
		char probeRole = getRole(role);
		char probeMode = getMode(mode);
		if ((probeRole != probeInfo->role || probeMode != probeInfo->mode))
		{
			write_log("FTS: segment (content=%d, dbid=%d) has not reached new state yet, "
			          "expected state: ('%c','%c'), "
			          "reported state: ('%c','%c').",
					  probeInfo->segmentId,
					  probeInfo->dbId,
					  probeInfo->role,
					  probeInfo->mode,
					  probeRole,
					  probeMode);
		}
	}

	/* check if segment reported a fault */
	if (state == SegmentStateFault)
	{
		/* get fault type - this will be used to decide the next segment state */
		switch(fault)
		{
			case FaultTypeNet:
				probeInfo->segmentStatus |= PROBE_FAULT_NET;
				break;

			case FaultTypeMirror:
				probeInfo->segmentStatus |= PROBE_FAULT_MIRROR;
				break;

			case FaultTypeDB:
			case FaultTypeIO:
				probeInfo->segmentStatus |= PROBE_FAULT_CRASH;
				break;

			default:
				Assert(!"Unexpected segment fault type");
		}
		write_log("FTS: segment (content=%d, dbid=%d) reported fault %s segment status %x to the prober.",
				  probeInfo->segmentId, probeInfo->dbId, getFaultTypeLabel(fault), probeInfo->segmentStatus);
	}
	return true;
}
#endif

/* EOF */
