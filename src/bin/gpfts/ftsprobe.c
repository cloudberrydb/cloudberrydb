/*-------------------------------------------------------------------------
 *
 * ftsprobe.c
 *	  Implementation of segment probing interface
 *
 * Portions Copyright (c) 2006-2011, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/fts/ftsprobe.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#ifndef USE_INTERNAL_FTS

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "fe_utils/log.h"
#include "libpq-fe.h"
#include "libpq/pqcomm.h"
#include "libpq-int.h"
#include "fts.h"
#include "pgtime.h"
#include "ftsprobe.h"
#include <sys/param.h>			/* for MAXHOSTNAMELEN */
#include "fts_etcd.h"
#include <assert.h>
#include "postmaster/postmaster.h"

static struct pollfd *PollFds;

extern bool fts_k8s_compatibility_enabled;

CdbComponentDatabaseInfo * getActiveMaster(fts_context *context)
{
	fts_segment_info *fts_info = NULL;
	CdbComponentDatabaseInfo *master_db = NULL;

	for (int response_index = 0;
		response_index < context->num_pairs && FtsIsActive();
		response_index ++)
	{
		fts_info = &(context->perSegInfos[response_index]);

		/* no master nor standby*/ 
		if (PRIMARY_CONFIG(fts_info)->segindex != MASTER_CONTENT_ID)
			continue;
		
		if (SEGMENT_IS_ACTIVE_PRIMARY(fts_info->primary_cdbinfo) &&
			SEGMENT_IS_ALIVE(fts_info->primary_cdbinfo))
		{
			master_db = fts_info->primary_cdbinfo;
			break;
		}

		if (fts_info->has_mirror_configured &&
			SEGMENT_IS_ACTIVE_PRIMARY(fts_info->mirror_cdbinfo) && 
			SEGMENT_IS_ALIVE(fts_info->mirror_cdbinfo))
		{
			master_db = fts_info->mirror_cdbinfo;
			break;
		}
	}
	return master_db;
}

static void
FtsWalRepInitProbeContext(CdbComponentDatabases *cdbs, fts_context *context, fts_config * context_config);

static void
InitPollFds(size_t size)
{
	PollFds = (struct pollfd *) palloc0(size * sizeof(struct pollfd));
}

CdbComponentDatabaseInfo *
FtsGetPeerSegment(CdbComponentDatabases *cdbs,
				  int content, int dbid)
{
	int i;

	for (i=0; i < cdbs->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *segInfo = &cdbs->segment_db_info[i];

		if (segInfo->config->segindex == content && segInfo->config->dbid != dbid)
		{
			/* found it */
			return segInfo;
		}
	}

	return NULL;
}

CdbComponentDatabaseInfo *
FtsGetPeerMaster(CdbComponentDatabases *cdbs)
{
	int i;

	for (i=0; i < cdbs->total_entry_dbs; i++)
	{
		CdbComponentDatabaseInfo *segInfo = &cdbs->entry_db_info[i];

		if (segInfo->config->segindex == MASTER_CONTENT_ID && segInfo->config->preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
		{
			/* found it */
			return segInfo;
		}
	}

	return NULL;
}

CdbComponentDatabaseInfo *
FtsGetPeerStandby(CdbComponentDatabases *cdbs)
{
	int i;

	for (i=0; i < cdbs->total_entry_dbs; i++)
	{
		CdbComponentDatabaseInfo *segInfo = &cdbs->entry_db_info[i];

		if (segInfo->config->segindex == MASTER_CONTENT_ID && segInfo->config->preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_MIRROR)
		{
			/* found it */
			return segInfo;
		}
	}	
	return NULL;
}


static FtsMessageState
nextSuccessState(FtsMessageState state)
{
	FtsMessageState result = FTS_PROBE_FAILED; /* to shut up compiler */
	switch(state)
	{
		case FTS_PROBE_SEGMENT:
			result = FTS_PROBE_SUCCESS;
			break;
		case FTS_SYNCREP_OFF_SEGMENT:
			result = FTS_SYNCREP_OFF_SUCCESS;
			break;
		case FTS_PROMOTE_SEGMENT:
			result = FTS_PROMOTE_SUCCESS;
			break;
		case FTS_PROBE_RETRY_WAIT:
		case FTS_SYNCREP_OFF_RETRY_WAIT:
		case FTS_PROMOTE_RETRY_WAIT:
		case FTS_PROBE_SUCCESS:
		case FTS_SYNCREP_OFF_SUCCESS:
		case FTS_PROMOTE_SUCCESS:
		case FTS_PROBE_FAILED:
		case FTS_SYNCREP_OFF_FAILED:
		case FTS_PROMOTE_FAILED:
		case FTS_RESPONSE_PROCESSED:
			cbdb_log_error("cannot determine next success state for %d", state);
	}
	return result;
}

static FtsMessageState
nextFailedState(FtsMessageState state)
{
	FtsMessageState result = FTS_PROBE_FAILED; /* to shut up compiler */
	switch(state)
	{
		case FTS_PROBE_SEGMENT:
			result = FTS_PROBE_FAILED;
			break;
		case FTS_SYNCREP_OFF_SEGMENT:
			result = FTS_SYNCREP_OFF_FAILED;
			break;
		case FTS_PROMOTE_SEGMENT:
			result = FTS_PROMOTE_FAILED;
			break;
		case FTS_PROBE_FAILED:
			result = FTS_PROBE_FAILED;
			break;
		case FTS_SYNCREP_OFF_FAILED:
			result = FTS_SYNCREP_OFF_FAILED;
			break;
		case FTS_PROMOTE_FAILED:
			result = FTS_PROMOTE_FAILED;
			break;
		case FTS_PROBE_RETRY_WAIT:
			result = FTS_PROBE_FAILED;
			break;
		case FTS_SYNCREP_OFF_RETRY_WAIT:
			result = FTS_SYNCREP_OFF_FAILED;
			break;
		case FTS_PROMOTE_RETRY_WAIT:
			result = FTS_PROMOTE_FAILED;
			break;
		case FTS_PROBE_SUCCESS:
		case FTS_SYNCREP_OFF_SUCCESS:
		case FTS_PROMOTE_SUCCESS:
		case FTS_RESPONSE_PROCESSED:
			cbdb_log_error("cannot determine next failed state for %d", state);
	}
	return result;
}

static bool
allDone(fts_context *context)
{
	bool result = true;
	int i;
	for (i = 0; i < context->num_pairs && result; i++)
	{
		/*
		 * If any segment is not in a final state, return false.
		 */
		result = (context->perSegInfos[i].state == FTS_RESPONSE_PROCESSED);
	}
	return result;
}

/*
 * Establish async libpq connection to a segment
 */
static bool
ftsConnectStart(fts_segment_info *fts_info)
{
	char conninfo[1024];
	char *hostip;

	/*
	 * No events should be pending on the connection that hasn't started
	 * yet.
	 */
	Assert(fts_info->poll_revents == 0);

	hostip = PRIMARY_CONFIG(fts_info)->hostip;
	snprintf(conninfo, 1024, "host=%s port=%d gpconntype=%s",
			 hostip ? hostip : "", PRIMARY_CONFIG(fts_info)->port,
			 GPCONN_TYPE_FTS);

	fts_info->conn = PQconnectStart(conninfo);
	
	if (fts_info->conn == NULL)
	{	
		cbdb_log_error("cannot create libpq connection object, possibly out"
			 " of memory (content=%d, dbid=%d)",
			 PRIMARY_CONFIG(fts_info)->segindex,
			 PRIMARY_CONFIG(fts_info)->dbid);
		return false;
	}
	if (fts_info->conn->status == CONNECTION_BAD)
	{
		cbdb_log_error("cannot establish libpq connection to "
			 "(content=%d, dbid=%d): %s",
			 PRIMARY_CONFIG(fts_info)->segindex, PRIMARY_CONFIG(fts_info)->dbid,
			 PQerrorMessage(fts_info->conn));
		return false;
	}

	/*
	 * Connection started, we must wait until the socket becomes ready for
	 * writing before anything can be written on this socket.  Therefore, mark
	 * the connection to be considered for subsequent poll step.
	 */
	fts_info->poll_events |= POLLOUT;
	/*
	 * Start the timer.
	 */
	fts_info->startTime = (pg_time_t) time(NULL);

	return true;
}

static bool
checkIfConnFailedDueToRecoveryInProgress(PGconn *conn)
{
	return strstr(PQerrorMessage(conn), _(POSTMASTER_IN_RECOVERY_MSG))
			&& strstr(PQerrorMessage(conn),POSTMASTER_AFTER_PROMOTE_STANDBY_IN_RECOVERY_DETAIL_MSG);
}

/*
 * Check if the primary segment is restarting normally by examing the PQ error message.
 * It could be that they are in RESET (waiting for the children to exit) or making 
 * progress in RECOVERY. Note there is no good source of RESET progress indications 
 * that we could check, so we simply always allow it. Normally RESET should be fast 
 * and there's a timeout in postmaster to guard against long wait.
 */
static void
checkIfFailedDueToNormalRestart(fts_segment_info *fts_info)
{
	if (strstr(PQerrorMessage(fts_info->conn), _(POSTMASTER_IN_RECOVERY_MSG)) ||
		strstr(PQerrorMessage(fts_info->conn), _(POSTMASTER_IN_STARTUP_MSG)))
	{
		XLogRecPtr tmpptr;
		char *ptr = strstr(PQerrorMessage(fts_info->conn),
				_(POSTMASTER_IN_RECOVERY_DETAIL_MSG));
		uint32		tmp_xlogid;
		uint32		tmp_xrecoff;

		if ((ptr == NULL) ||
		    sscanf(ptr, POSTMASTER_IN_RECOVERY_DETAIL_MSG " %X/%X\n",
		    &tmp_xlogid, &tmp_xrecoff) != 2)
		{
			cbdb_log_error(
				"invalid in-recovery message %s "
				"(content=%d, dbid=%d) state=%d",
				PQerrorMessage(fts_info->conn),
				PRIMARY_CONFIG(fts_info)->segindex,
				PRIMARY_CONFIG(fts_info)->dbid,
				fts_info->state);
			return;
		}
		tmpptr = ((uint64) tmp_xlogid) << 32 | (uint64) tmp_xrecoff;

		/*
		 * If the xlog record returned from the primary is less than or
		 * equal to the xlog record we had saved from the last probe
		 * then we assume that recovery is not making progress. In the
		 * case of rolling panics on the primary the returned xlog
		 * location can be less than the recorded xlog location. In
		 * these cases of rolling panic or recovery hung we want to
		 * mark the primary as down.
		 */
		if (tmpptr <= fts_info->xlogrecptr)
		{
			fts_info->restart_state = PM_IN_RECOVERY_NOT_MAKING_PROGRESS;
			cbdb_log_debug("detected segment is in recovery mode and not making progress (content=%d) "
				 "primary dbid=%d, mirror dbid=%d",
				 PRIMARY_CONFIG(fts_info)->segindex,
				 PRIMARY_CONFIG(fts_info)->dbid,
				 fts_info->has_mirror_configured ? MIRROR_CONFIG(fts_info)->dbid : -1);
		}
		else
		{
			fts_info->restart_state = PM_IN_RECOVERY_MAKING_PROGRESS;
			fts_info->xlogrecptr = tmpptr;

			cbdb_log_debug("detected segment is in recovery mode replayed (%X/%X) (content=%d) "
				   "primary dbid=%d, mirror dbid=%d",
				   (uint32) (tmpptr >> 32),
				   (uint32) tmpptr,
				   PRIMARY_CONFIG(fts_info)->segindex,
				   PRIMARY_CONFIG(fts_info)->dbid,
				   fts_info->has_mirror_configured ? MIRROR_CONFIG(fts_info)->dbid : -1);
		}
	}
	else if (strstr(PQerrorMessage(fts_info->conn), _(POSTMASTER_IN_RESET_MSG)))
	{
		fts_info->restart_state = PM_IN_RESETTING;
		cbdb_log_debug("FTS: detected segment is in RESET state (content=%d) "
				"primary dbid=%d, mirror dbid=%d",
				fts_info->primary_cdbinfo->config->segindex,
				fts_info->primary_cdbinfo->config->dbid,
				fts_info->mirror_cdbinfo->config->dbid);
	}
}

/*
 * Start a libpq connection for each "per segment" object in context.  If the
 * connection is already started for an object, advance libpq state machine for
 * that object by calling PQconnectPoll().  An established libpq connection
 * (authentication complete and ready-for-query received) is identified by: (1)
 * state of the "per segment" object is any of FTS_PROBE_SEGMENT,
 * FTS_SYNCREP_OFF_SEGMENT, FTS_PROMOTE_SEGMENT and (2) PQconnectPoll() returns
 * PGRES_POLLING_OK for the connection.
 *
 * Upon failure, transition that object to a failed state.
 */
static void
ftsConnect(fts_context *context)
{
	int i;
	for (i = 0; i < context->num_pairs; i++)
	{
		fts_segment_info *fts_info = &context->perSegInfos[i];
		cbdb_log_debug("ftsConnect (content=%d, dbid=%d) state=%d, "
			   "retry_count=%d, conn->status=%d",
			   PRIMARY_CONFIG(fts_info)->segindex,
			   PRIMARY_CONFIG(fts_info)->dbid,
			   fts_info->state, fts_info->retry_count,
			   fts_info->conn ? fts_info->conn->status : -1);
		if (fts_info->conn && PQstatus(fts_info->conn) == CONNECTION_OK)
			continue;
		switch(fts_info->state)
		{
			case FTS_PROBE_SEGMENT:
			case FTS_SYNCREP_OFF_SEGMENT:
			case FTS_PROMOTE_SEGMENT:
				/*
				 * We always default to PM_NOT_IN_RESTART.  If connect fails, we then check
				 * the primary's restarting state, so we can skip promoting mirror if it's in
				 * PM_IN_RESETTING or PM_IN_RECOVERY_MAKING_PROGRESS.
				 */
				fts_info->restart_state = PM_NOT_IN_RESTART;
				if (fts_info->conn == NULL)
				{
					Assert(fts_info->retry_count <= context->config->probe_retries);
					if (!ftsConnectStart(fts_info))
						fts_info->state = nextFailedState(fts_info->state);
				}
				else if (fts_info->poll_revents & (POLLOUT | POLLIN))
				{
					switch(PQconnectPoll(fts_info->conn))
					{
						case PGRES_POLLING_OK:
							/*
							 * Response-state is already set and now the
							 * connection is also ready with authentication
							 * completed.  Next step should now be able to send
							 * the appropriate FTS message.
							 */
							cbdb_log_debug("established libpq connection "
								   "(content=%d, dbid=%d) state=%d, "
								   "retry_count=%d, conn->status=%d",
								   PRIMARY_CONFIG(fts_info)->segindex,
								   PRIMARY_CONFIG(fts_info)->dbid,
								   fts_info->state, fts_info->retry_count,
								   fts_info->conn->status);
							fts_info->poll_events = POLLOUT;
							break;

						case PGRES_POLLING_READING:
							/*
							 * The connection can now be polled for reading and
							 * if the poll() returns POLLIN in revents, data
							 * has arrived.
							 */
							fts_info->poll_events |= POLLIN;
							break;

						case PGRES_POLLING_WRITING:
							/*
							 * The connection can now be polled for writing and
							 * may be written to, if ready.
							 */
							fts_info->poll_events |= POLLOUT;
							break;

						case PGRES_POLLING_FAILED:
							fts_info->state = nextFailedState(fts_info->state);
							checkIfFailedDueToNormalRestart(fts_info); 
							cbdb_log_debug("cannot establish libpq connection "
								 "(content=%d, dbid=%d): %s, retry_count=%d",
								 PRIMARY_CONFIG(fts_info)->segindex,
								 PRIMARY_CONFIG(fts_info)->dbid,
								 PQerrorMessage(fts_info->conn),
								 fts_info->retry_count);
							break;

						default:
							cbdb_log_error( "invalid response to PQconnectPoll"
								 " (content=%d, dbid=%d): %s",
								 PRIMARY_CONFIG(fts_info)->segindex,
								 PRIMARY_CONFIG(fts_info)->dbid,
								 PQerrorMessage(fts_info->conn));
							break;
					}
				}
				else
					cbdb_log_debug("ftsConnect (content=%d, dbid=%d) state=%d, "
						   "retry_count=%d, conn->status=%d pollfd.revents unset",
						   PRIMARY_CONFIG(fts_info)->segindex,
						   PRIMARY_CONFIG(fts_info)->dbid,
						   fts_info->state, fts_info->retry_count,
						   fts_info->conn ? fts_info->conn->status : -1);
				break;
			case FTS_PROBE_SUCCESS:
			case FTS_SYNCREP_OFF_SUCCESS:
			case FTS_PROMOTE_SUCCESS:
			case FTS_PROBE_FAILED:
			case FTS_SYNCREP_OFF_FAILED:
			case FTS_PROMOTE_FAILED:
			case FTS_PROBE_RETRY_WAIT:
			case FTS_SYNCREP_OFF_RETRY_WAIT:
			case FTS_PROMOTE_RETRY_WAIT:
			case FTS_RESPONSE_PROCESSED:
				break;
		}
	}
}

/*
 * Timeout is said to have occurred if greater than probe_timeout
 * seconds have elapsed since connection start and a response is not received.
 * Segments for which a response is received already are exempted from timeout
 * evaluation.
 */
static void
ftsCheckTimeout(fts_segment_info *fts_info, pg_time_t now, int probe_timeout_limit)
{
	if (!IsFtsMessageStateSuccess(fts_info->state) &&
		(int) (now - fts_info->startTime) > probe_timeout_limit)
	{
		cbdb_log_warning("timeout detected for (content=%d, dbid=%d) "
			 "state=%d, retry_count=%d",
			 PRIMARY_CONFIG(fts_info)->segindex,
			 PRIMARY_CONFIG(fts_info)->dbid, fts_info->state,
			 fts_info->retry_count);
		fts_info->state = nextFailedState(fts_info->state);
	}
}

static void
ftsPoll(fts_context *context)
{
	int i;
	int nfds=0;
	int nready;
	for (i = 0; i < context->num_pairs; i++)
	{
		fts_segment_info *fts_info = &context->perSegInfos[i];
		if (fts_info->poll_events & (POLLIN|POLLOUT))
		{
			PollFds[nfds].fd = PQsocket(fts_info->conn);
			PollFds[nfds].events = fts_info->poll_events;
			PollFds[nfds].revents = 0;
			fts_info->fd_index = nfds;
			nfds++;
		}
		else
			fts_info->fd_index = -1; /*
									 * This socket is not considered for
									 * polling.
									 */
	}
	if (nfds == 0)
		return;

	nready = poll(PollFds, nfds, 50);
	if (nready < 0)
	{
		if (errno == EINTR)
		{
			cbdb_log_debug("ftsPoll() interrupted, nfds %d", nfds);
		}
		else
			cbdb_log_error( "ftsPoll() failed: nfds %d, %m", nfds);
	}
	else if (nready == 0)
	{
		cbdb_log_debug("ftsPoll() timed out, nfds %d", nfds);
	}

	cbdb_log_debug("ftsPoll() found %d out of %d sockets ready",
		   nready, nfds);

	pg_time_t now = (pg_time_t) time(NULL);

	/* Record poll() response poll_revents for each "per_segment" object. */
	for (i = 0; i < context->num_pairs; i++)
	{
		fts_segment_info *fts_info = &context->perSegInfos[i];

		if (fts_info->poll_events & (POLLIN|POLLOUT))
		{
			Assert(PollFds[fts_info->fd_index].fd == PQsocket(fts_info->conn));
			fts_info->poll_revents = PollFds[fts_info->fd_index].revents;
			/*
			 * Reset poll_events for fds that were found ready.  Assume
			 * that at the most one bit is set in poll_events (POLLIN
			 * or POLLOUT).
			 */
			if (fts_info->poll_revents & fts_info->poll_events)
			{
				fts_info->poll_events = 0;
			}
			else if (fts_info->poll_revents & (POLLHUP | POLLERR))
			{
				fts_info->state = nextFailedState(fts_info->state);
				cbdb_log_info("poll failed (revents=%d, events=%d) for "
					 "(content=%d, dbid=%d) state=%d, retry_count=%d, "
					 "libpq status=%d, asyncStatus=%d",
					 fts_info->poll_revents, fts_info->poll_events,
					 PRIMARY_CONFIG(fts_info)->segindex,
					 PRIMARY_CONFIG(fts_info)->dbid, fts_info->state,
					 fts_info->retry_count, fts_info->conn->status,
					 fts_info->conn->asyncStatus);
			}
			else if (fts_info->poll_revents)
			{
				fts_info->state = nextFailedState(fts_info->state);
				cbdb_log_warning(
					 "unexpected events (revents=%d, events=%d) for "
					 "(content=%d, dbid=%d) state=%d, retry_count=%d, "
					 "libpq status=%d, asyncStatus=%d",
					 fts_info->poll_revents, fts_info->poll_events,
					 PRIMARY_CONFIG(fts_info)->segindex,
					 PRIMARY_CONFIG(fts_info)->dbid, fts_info->state,
					 fts_info->retry_count, fts_info->conn->status,
					 fts_info->conn->asyncStatus);
			}
			/* If poll timed-out above, check timeout */
			ftsCheckTimeout(fts_info, now, context->config->probe_timeout);
		}
	}
}

/*
 * Send FTS query
 */
static void
ftsSend(fts_context *context)
{
	fts_segment_info *fts_info;
	const char *message_type;
	char message[FTS_MSG_MAX_LEN];
	int i;

	for (i = 0; i < context->num_pairs; i++)
	{
		fts_info = &context->perSegInfos[i];
		cbdb_log_debug(
			   "ftsSend (content=%d, dbid=%d) state=%d, "
			   "retry_count=%d, conn->asyncStatus=%d, conn->status=%d",
			   PRIMARY_CONFIG(fts_info)->segindex,
			   PRIMARY_CONFIG(fts_info)->dbid,
			   fts_info->state, fts_info->retry_count,
			   fts_info->conn ? fts_info->conn->asyncStatus : -1, PQstatus(fts_info->conn));
		switch(fts_info->state)
		{
			case FTS_PROBE_SEGMENT:
			case FTS_SYNCREP_OFF_SEGMENT:
			case FTS_PROMOTE_SEGMENT:
				/*
				 * The libpq connection must be ready for accepting query and
				 * the socket must be writable.
				 */
				if (PQstatus(fts_info->conn) != CONNECTION_OK ||
					fts_info->conn->asyncStatus != PGASYNC_IDLE ||
				    !(fts_info->poll_revents & POLLOUT))
					break;
				if (fts_info->state == FTS_PROBE_SEGMENT)
					message_type = FTS_MSG_PROBE;
				else if (fts_info->state == FTS_SYNCREP_OFF_SEGMENT)
					message_type = FTS_MSG_SYNCREP_OFF;
				else
				{
					/*
					 * If set disable_promote_standby.
					 * Then do not send promote message to standby.
					 */
					if (context->config->disable_promote_standby && 
						PRIMARY_CONFIG(fts_info)->segindex == MASTER_CONTENT_ID)
					{
						cbdb_log_info("disable promote standby, skipped promote (content=%d, dbid=%d).", 
							PRIMARY_CONFIG(fts_info)->segindex, 
							PRIMARY_CONFIG(fts_info)->dbid);
						fts_info->state = FTS_RESPONSE_PROCESSED;
						break;
					}

					message_type = FTS_MSG_PROMOTE;
				}

				snprintf(message, FTS_MSG_MAX_LEN, FTS_MSG_FORMAT,
						 message_type,
						 PRIMARY_CONFIG(fts_info)->dbid,
						 PRIMARY_CONFIG(fts_info)->segindex);

				if (PQsendQuery(fts_info->conn, message))
				{
					/*
					 * Message sent successfully, mark the socket to be polled
					 * for reading so we will be ready to read response when it
					 * arrives.
					 */
					fts_info->poll_events = POLLIN;
					cbdb_log_debug(
						   "sent %s to (content=%d, dbid=%d), retry_count=%d",
						   message, PRIMARY_CONFIG(fts_info)->segindex,
						   PRIMARY_CONFIG(fts_info)->dbid, fts_info->retry_count);
				}
				else
				{
					cbdb_log_info(
						 "failed to send %s to segment (content=%d, "
						 "dbid=%d) state=%d, retry_count=%d, "
						 "conn->asyncStatus=%d %s", message,
						 PRIMARY_CONFIG(fts_info)->segindex,
						 PRIMARY_CONFIG(fts_info)->dbid,
						 fts_info->state, fts_info->retry_count,
						 fts_info->conn->asyncStatus,
						 PQerrorMessage(fts_info->conn));
					fts_info->state = nextFailedState(fts_info->state);
				}
				break;
			default:
				/* Cannot send messages in any other state. */
				break;
		}
	}
}

/*
 * Record FTS handler's response from libpq result into fts_result
 */
static void
probeRecordResponse(fts_segment_info *fts_info, PGresult *result)
{
	fts_info->result.isPrimaryAlive = true;

	fts_info->result.isMirrorAlive = *PQgetvalue(result, 0,
			Anum_fts_message_response_is_mirror_up);

	fts_info->result.isInSync = *PQgetvalue(result, 0,
			Anum_fts_message_response_is_in_sync);

	fts_info->result.isSyncRepEnabled = *PQgetvalue(result, 0,
			Anum_fts_message_response_is_syncrep_enabled);

	fts_info->result.isRoleMirror = *PQgetvalue(result, 0,
			Anum_fts_message_response_is_role_mirror);

	fts_info->result.retryRequested = *PQgetvalue(result, 0,
			Anum_fts_message_response_request_retry);

	cbdb_log_debug("segment (content=%d, dbid=%d, role=%c) reported "
		   "isMirrorUp %d, isInSync %d, isSyncRepEnabled %d, "
		   "isRoleMirror %d, and retryRequested %d to the prober.",
		   PRIMARY_CONFIG(fts_info)->segindex,
		   PRIMARY_CONFIG(fts_info)->dbid,
		   PRIMARY_CONFIG(fts_info)->role,
		   fts_info->result.isMirrorAlive,
		   fts_info->result.isInSync,
		   fts_info->result.isSyncRepEnabled,
		   fts_info->result.isRoleMirror,
		   fts_info->result.retryRequested);
}

/*
 * Receive segment response
 */
static void
ftsReceive(fts_context *context)
{
	fts_segment_info *fts_info;
	PGresult *result = NULL;
	int ntuples;
	int nfields;
	int i;

	for (i = 0; i < context->num_pairs; i++)
	{
		fts_info = &context->perSegInfos[i];
		cbdb_log_debug(
			   "ftsReceive (content=%d, dbid=%d) state=%d, "
			   "retry_count=%d, conn->asyncStatus=%d",
			   PRIMARY_CONFIG(fts_info)->segindex,
			   PRIMARY_CONFIG(fts_info)->dbid,
			   fts_info->state, fts_info->retry_count,
			   fts_info->conn ? fts_info->conn->asyncStatus : -1);
		switch(fts_info->state)
		{
			case FTS_PROBE_SEGMENT:
			case FTS_SYNCREP_OFF_SEGMENT:
			case FTS_PROMOTE_SEGMENT:
				/*
				 * The libpq connection must be established and a message must
				 * have arrived on the socket.
				 */
				if (PQstatus(fts_info->conn) != CONNECTION_OK ||
					!(fts_info->poll_revents & POLLIN))
					break;
				/* Read the response that has arrived. */
				if (!PQconsumeInput(fts_info->conn))
				{
					cbdb_log_info("failed to read from (content=%d, dbid=%d)"
						 " state=%d, retry_count=%d, conn->asyncStatus=%d %s",
						 PRIMARY_CONFIG(fts_info)->segindex,
						 PRIMARY_CONFIG(fts_info)->dbid,
						 fts_info->state, fts_info->retry_count,
						 fts_info->conn->asyncStatus,
						 PQerrorMessage(fts_info->conn));
					fts_info->state = nextFailedState(fts_info->state);
					break;
				}
				/* Parse the response. */
				if (PQisBusy(fts_info->conn))
				{
					/*
					 * There is not enough data in the buffer.
					 */
					break;
				}

				/*
				 * Response parsed, PQgetResult() should not block for I/O now.
				 */
				result = PQgetResult(fts_info->conn);

				if (!result || PQstatus(fts_info->conn) == CONNECTION_BAD)
				{
					cbdb_log_info("error getting results from (content=%d, "
						 "dbid=%d) state=%d, retry_count=%d, "
						 "conn->asyncStatus=%d conn->status=%d %s",
						 PRIMARY_CONFIG(fts_info)->segindex,
						 PRIMARY_CONFIG(fts_info)->dbid,
						 fts_info->state, fts_info->retry_count,
						 fts_info->conn->asyncStatus,
						 fts_info->conn->status,
						 PQerrorMessage(fts_info->conn));
					fts_info->state = nextFailedState(fts_info->state);
					break;
				}

				if (PQresultStatus(result) != PGRES_TUPLES_OK)
				{
					cbdb_log_info("error response from (content=%d, dbid=%d)"
						 " state=%d, retry_count=%d, conn->asyncStatus=%d %s",
						 PRIMARY_CONFIG(fts_info)->segindex,
						 PRIMARY_CONFIG(fts_info)->dbid,
						 fts_info->state, fts_info->retry_count,
						 fts_info->conn->asyncStatus,
						 PQresultErrorMessage(result));
					fts_info->state = nextFailedState(fts_info->state);
					break;
				}
				ntuples = PQntuples(result);
				nfields = PQnfields(result);
				if (nfields != Natts_fts_message_response ||
					ntuples != FTS_MESSAGE_RESPONSE_NTUPLES)
				{
					/*
					 * XXX: Investigate: including conn->asyncStatus generated
					 * a format string warning at compile time.
					 */
					cbdb_log_info("invalid response from (content=%d, dbid=%d)"
						 " state=%d, retry_count=%d, expected %d tuple with "
						 "%d fields, got %d tuples with %d fields",
						 PRIMARY_CONFIG(fts_info)->segindex,
						 PRIMARY_CONFIG(fts_info)->dbid,
						 fts_info->state, fts_info->retry_count,
						 FTS_MESSAGE_RESPONSE_NTUPLES,
						 Natts_fts_message_response, ntuples, nfields);
					fts_info->state = nextFailedState(fts_info->state);
					break;
				}
				/*
				 * Result received and parsed successfully.  Record it so that
				 * subsequent step processes it and transitions to next state.
				 */
				probeRecordResponse(fts_info, result);
				fts_info->state = nextSuccessState(fts_info->state);
				break;
			default:
				/* Cannot receive response in any other state. */
				break;
		}

		/*
		 * Reference to the result should already be stored in
		 * connection object. If it is not then free explicitly.
		 */
		if (result && result != fts_info->conn->result)
		{
			PQclear(result);
			result = NULL;
		}
	}
}

static void
retryForFtsFailed(fts_segment_info *fts_info, pg_time_t now, int probe_retries_limit)
{
	if (fts_info->retry_count == probe_retries_limit)
	{
		cbdb_log_info( "max (%d) retries exhausted "
			"(content=%d, dbid=%d) state=%d",
			fts_info->retry_count,
			PRIMARY_CONFIG(fts_info)->segindex,
			PRIMARY_CONFIG(fts_info)->dbid, fts_info->state);
		return;
	}

	fts_info->retry_count++;
	if (fts_info->state == FTS_PROBE_SUCCESS ||
		fts_info->state == FTS_PROBE_FAILED)
		fts_info->state = FTS_PROBE_RETRY_WAIT;
	else if (fts_info->state == FTS_SYNCREP_OFF_FAILED)
		fts_info->state = FTS_SYNCREP_OFF_RETRY_WAIT;
	else
		fts_info->state = FTS_PROMOTE_RETRY_WAIT;
	fts_info->retryStartTime = now;
	cbdb_log_debug(
		"initialized retry start time to now "
		"(content=%d, dbid=%d) state=%d",
		PRIMARY_CONFIG(fts_info)->segindex,
		PRIMARY_CONFIG(fts_info)->dbid, fts_info->state);

	PQfinish(fts_info->conn);
	fts_info->conn = NULL;
	fts_info->poll_events = fts_info->poll_revents = 0;
	/* Reset result before next attempt. */
	memset(&fts_info->result, 0, sizeof(fts_result));
}

/*
 * If retry attempts are available, transition the segments to the start state
 * corresponding to their failure state.  If retries have exhausted, leave the
 * segment in the failure state.
 */
static void
processRetry(fts_context *context)
{
	fts_segment_info *fts_info;
	int i;
	pg_time_t now = (pg_time_t) time(NULL);
	bool IsMirrorConfigured = true;

	for (i = 0; i < context->num_pairs; i++)
	{
		fts_info = &context->perSegInfos[i];
		IsMirrorConfigured = fts_info->has_mirror_configured;
		switch(fts_info->state)
		{
			case FTS_PROBE_SUCCESS:
				/*
				 * Purpose of retryRequested flag is to avoid considering
				 * mirror as down prematurely.  If mirror is already marked
				 * down in configuration, there is no need to retry.
				 */
				if (IsMirrorConfigured && !(fts_info->result.retryRequested &&
					  SEGMENT_IS_ALIVE(fts_info->mirror_cdbinfo)))
					break;
				else if (!fts_info->result.retryRequested)
					break;

				/* else, fallthrough */
			case FTS_PROBE_FAILED:
			case FTS_SYNCREP_OFF_FAILED:
			case FTS_PROMOTE_FAILED:
				retryForFtsFailed(fts_info, now, context->config->probe_retries);
				break;
			case FTS_PROBE_RETRY_WAIT:
			case FTS_SYNCREP_OFF_RETRY_WAIT:
			case FTS_PROMOTE_RETRY_WAIT:
				/* Wait for 1 second before making another attempt. */
				if ((int) (now - fts_info->retryStartTime) < 1)
					break;
				/*
				 * We have remained in retry state for over a second, it's time
				 * to make another attempt.
				 */
				cbdb_log_info(
					   "retrying attempt %d (content=%d, dbid=%d) "
					   "state=%d", fts_info->retry_count,
					   PRIMARY_CONFIG(fts_info)->segindex,
					   PRIMARY_CONFIG(fts_info)->dbid, fts_info->state);
				if (fts_info->state == FTS_PROBE_RETRY_WAIT)
					fts_info->state = FTS_PROBE_SEGMENT;
				else if (fts_info->state == FTS_SYNCREP_OFF_RETRY_WAIT)
					fts_info->state = FTS_SYNCREP_OFF_SEGMENT;
				else
					fts_info->state = FTS_PROMOTE_SEGMENT;
				break;
			default:
				break;
		}
	}
}

/*
 * Return true for segments whose response is ready to be processed.  Segments
 * whose response is already processed should have response->conn set to NULL.
 */
static bool
ftsResponseReady(fts_segment_info *fts_info)
{
	return (IsFtsMessageStateSuccess(fts_info->state) ||
			IsFtsMessageStateFailed(fts_info->state));
}

static int dumpSegConfigInfo(GpSegConfigEntry *config, char * buf)
{
	return sprintf(buf, "%d %d %c %c %c %c %d %s %s %s\n", config->dbid, 
			config->segindex, config->role, config->preferred_role,
			config->mode, config->status, config->port,
			config->hostname, config->address, config->datadir);
}

static 
bool writeIntoBackend(fts_context *context, char * buff)
{
	char * rewrite_sql = NULL;
	PGconn *db_conn = NULL;
	PGresult *res = NULL;
	char connstr_source[1024] = {0};
	CdbComponentDatabaseInfo * master_segment;

	master_segment = getActiveMaster(context);
	if (!master_segment)
	{
		/* FTS_TODO: for safe, Back up the original FTS info then overwrite it
		 * Also need lock the KEY and check the origin counts same with now one.
		 */ 
		cbdb_log_warning("no exist alive master/standby node. Write directly to etcd.");
		writeFTSDumpFromETCD(buff);
		goto finish;
	}

	if (context->config->connstr_source)
	{
		snprintf(connstr_source, 1024, 
			CONNECTION_SOURCE_TEMPLATE_WITH_UP, 
			context->config->connstr_source,
			master_segment->config->address, 
			master_segment->config->port);
	}
	else
	{
		snprintf(connstr_source, 1024, 
			CONNECTION_SOURCE_TEMPLATE, 
			master_segment->config->address, 
			master_segment->config->port);
	}
	
	rewrite_sql = palloc0(strlen(REWRITE_SEGMENTS_INFO_SQL) + strlen(buff) + 1);
	snprintf(rewrite_sql,strlen(REWRITE_SEGMENTS_INFO_SQL) + strlen(buff) + 1, REWRITE_SEGMENTS_INFO_SQL, buff);
	
	db_conn = PQconnectdb(connstr_source);

	if (PQstatus(db_conn) == CONNECTION_BAD)
	{
		/* When master down + promote standby + segment down 
		 * After standby been promoted, then it don't know segment down but can't create gang.
		 * Then recovery background process keep asking FTS update segment status.
		 * In that time, FTS can't write into standby, because standby in recovery.
		 */
		if (checkIfConnFailedDueToRecoveryInProgress(db_conn))
		{
			cbdb_log_warning("unable to update catalog when master/standby is recovering. Write directly to etcd.");
			writeFTSDumpFromETCD(buff);
			goto finish;
		}
		else
		{
			cbdb_log_error("fail to update segment, %s", PQerrorMessage(db_conn));
			goto finish;
		}
	}

	res = PQexec(db_conn, rewrite_sql);
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		cbdb_log_error("rewrite message: %s", rewrite_sql);
		cbdb_log_error("rewrite message exec failed: %s", PQerrorMessage(db_conn));
		goto finish;
	}

	PQclear(res);
	pfree(rewrite_sql);
	PQfinish(db_conn);

	return true;

finish:
	if (db_conn)
		PQfinish(db_conn);
	if (rewrite_sql)
		pfree(rewrite_sql);

	return false;
}

static 
bool dumpFtsToETCD(fts_context *context)
{
	int line_length = SEGMENT_CONFIGURATION_ONE_LINE_LENGTH;
	int rc;
	int line_number = 0;
	char *buf;
	int index = 0;
	int response_index;
	int next_index = 0;
	fts_segment_info *fts_info;
	CdbComponentDatabaseInfo * primary_cdbinfo, *mirror_cdbinfodbinfo;

	for (response_index = 0;
		 response_index < context->num_pairs && FtsIsActive();
		 response_index ++)
	{
		fts_info = &(context->perSegInfos[response_index]);
		line_number += fts_info->has_mirror_configured ? 2 : 1;
	}

	buf = palloc0(line_length * line_number + 1);

	for (response_index = 0;
		 response_index < context->num_pairs && FtsIsActive();
		 response_index ++)
	{
		fts_info = &(context->perSegInfos[response_index]);
		primary_cdbinfo = fts_info->primary_cdbinfo;
		mirror_cdbinfodbinfo = fts_info->mirror_cdbinfo;

		rc = dumpSegConfigInfo(primary_cdbinfo->config, buf + next_index);
		if (rc <= 0)
		{
			cbdb_log_error("Fail to dump primary DataBaseInfo to string, [dbid=%d, content=%d]", 
				primary_cdbinfo->config->dbid, primary_cdbinfo->config->segindex);
			goto error;
		}
		index++;
		next_index += rc;

		if (fts_info->has_mirror_configured)
		{
			rc = dumpSegConfigInfo(mirror_cdbinfodbinfo->config, buf + next_index);
			if (rc <= 0)
			{
				cbdb_log_error("Fail to dump mirror DataBaseInfo to string, [dbid=%d, content=%d]", 
					mirror_cdbinfodbinfo->config->dbid, mirror_cdbinfodbinfo->config->segindex);
				goto error;
			}
			index++;
			next_index += rc;
		}
	}


	if (index != line_number || next_index > line_length)
	{
		cbdb_log_error("The index can't match the cdb counts, [index=%d, counts=%d, line_length=%d]", 
			index, line_number, line_length);
		goto error;
	}

	if (writeIntoBackend(context, buf))
	{
		cbdb_log_info("Already updated segment infos.");
	}

	pfree(buf);
	return true;
error:
	pfree(buf);
	return false;
}

static bool
updateConfiguration(fts_context *context,
					CdbComponentDatabaseInfo *primary,
					CdbComponentDatabaseInfo *mirror,
					char newPrimaryRole, char newMirrorRole,
					bool IsInSync, bool IsPrimaryAlive, bool IsMirrorAlive)
{
	bool IsMirrorConfigured = mirror != NULL;
	bool UpdatePrimary = (IsPrimaryAlive != SEGMENT_IS_ALIVE(primary));
	bool UpdateMirror = (IsMirrorConfigured && IsMirrorAlive != SEGMENT_IS_ALIVE(mirror));
	
	if (IsInSync != SEGMENT_IS_IN_SYNC(primary))
		UpdatePrimary = UpdateMirror = true;

	bool UpdateNeeded = UpdatePrimary || UpdateMirror;
	if (!UpdateNeeded)
	{
		return UpdateNeeded;
	}

	if (UpdatePrimary)
	{
		primary->config->role = newPrimaryRole;
		primary->config->status = IsPrimaryAlive ? GP_SEGMENT_CONFIGURATION_STATUS_UP : GP_SEGMENT_CONFIGURATION_STATUS_DOWN;
		primary->config->mode = IsInSync ? GP_SEGMENT_CONFIGURATION_MODE_INSYNC : GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC;
	}

	if (IsMirrorConfigured && UpdateMirror)
	{
		mirror->config->role = newMirrorRole;
		mirror->config->status = IsMirrorAlive ? GP_SEGMENT_CONFIGURATION_STATUS_UP : GP_SEGMENT_CONFIGURATION_STATUS_DOWN;
		mirror->config->mode = IsInSync ? GP_SEGMENT_CONFIGURATION_MODE_INSYNC : GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC;
	}

	dumpFtsToETCD(context);

	return UpdateNeeded;
}

/*
 * Process responses from primary segments:
 * (a) Transition internal state so that segments can be messaged subsequently
 * (e.g. promotion and turning off syncrep).
 * (b) Update gp_segment_configuration catalog table, if needed.
 */
static bool
processResponse(fts_context *context)
{
	bool is_updated = false;

	for (int response_index = 0;
		 response_index < context->num_pairs && FtsIsActive();
		 response_index ++)
	{
		fts_segment_info *fts_info = &(context->perSegInfos[response_index]);

		/*
		 * Consider segments that are in final state (success / failure) and
		 * that are not already processed.
		 */
		if (!ftsResponseReady(fts_info))
			continue;

		CdbComponentDatabaseInfo *primary = fts_info->primary_cdbinfo;
		CdbComponentDatabaseInfo *mirror = fts_info->mirror_cdbinfo;

		bool IsPrimaryAlive = fts_info->result.isPrimaryAlive;
		bool IsMirrorConfigured = fts_info->has_mirror_configured;
		/* Trust a response from primary only if it's alive. */
		bool IsMirrorAlive =  IsPrimaryAlive ?
			fts_info->result.isMirrorAlive : IsMirrorConfigured ? SEGMENT_IS_ALIVE(mirror) : false;
		bool IsInSync = IsPrimaryAlive ?
			fts_info->result.isInSync : false;

		switch(fts_info->state)
		{
			case FTS_PROBE_SUCCESS:
				assert(IsPrimaryAlive);
				if (fts_info->result.isSyncRepEnabled && !IsMirrorAlive)
				{
					if (!fts_info->result.retryRequested)
					{
						/*
						 * Primaries that have syncrep enabled continue to block
						 * commits until FTS update the mirror status as down.
						 */
						is_updated |= updateConfiguration(
							context, primary, mirror,
							GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY,
							GP_SEGMENT_CONFIGURATION_ROLE_MIRROR,
							IsInSync, IsPrimaryAlive, IsMirrorAlive);
						/*
						 * Now that the configuration is updated, FTS must notify
						 * the primaries to unblock commits by sending syncrep off
						 * message.
						 */
						fts_info->state = FTS_SYNCREP_OFF_SEGMENT;
						cbdb_log_info(
							   "turning syncrep off on (content=%d, dbid=%d)",
							   primary->config->segindex, primary->config->dbid);
					}
					else
					{
						cbdb_log_info(
							   "skipping mirror down update for (content=%d) as retryRequested",
							   primary->config->segindex);
						fts_info->state = FTS_RESPONSE_PROCESSED;
					}
				}
				else if (fts_info->result.isRoleMirror)
				{
					/*
					 * A promote message sent previously didn't make it to the
					 * mirror.  Catalog must have been updated before sending
					 * the previous promote message.
					 */
					assert(!IsMirrorAlive);
					assert(SEGMENT_IS_NOT_INSYNC(primary));
					assert(!fts_info->result.isSyncRepEnabled);
					cbdb_log_debug(
						   "resending promote request to (content=%d,"
						   " dbid=%d)", primary->config->segindex, primary->config->dbid);
					fts_info->state = FTS_PROMOTE_SEGMENT;
				}
				else
				{
					/*
					 * No subsequent state transition needed, update catalog if
					 * necessary.  The cases are mirror status found to change
					 * from down to up, mode found to change from not in-sync
					 * to in-sync or syncrep found to change from off to on.
					 */
					is_updated |= updateConfiguration(
						context, primary, mirror,
						GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY,
						GP_SEGMENT_CONFIGURATION_ROLE_MIRROR,
						IsInSync, IsPrimaryAlive, IsMirrorAlive);
					fts_info->state = FTS_RESPONSE_PROCESSED;
				}
				break;
			case FTS_PROBE_FAILED:
				/* Primary is down */

				/* If primary is in resetting or making progress in recovery, do not mark it down and promote mirror */
				if (fts_info->restart_state == PM_IN_RESETTING)
				{
					Assert(strstr(PQerrorMessage(fts_info->conn), _(POSTMASTER_IN_RESET_MSG)));
					cbdb_log_debug(
						 "FTS: detected segment is in resetting mode "
						 "(content=%d) primary dbid=%d, mirror dbid=%d",
						 primary->config->segindex, primary->config->dbid, mirror->config->dbid);

					fts_info->state = FTS_RESPONSE_PROCESSED;
					break;
				}
				else if (fts_info->restart_state == PM_IN_RECOVERY_MAKING_PROGRESS)
				{
					assert(strstr(PQerrorMessage(fts_info->conn), _(POSTMASTER_IN_RECOVERY_MSG)) ||
						   strstr(PQerrorMessage(fts_info->conn), _(POSTMASTER_IN_STARTUP_MSG)));
					cbdb_log_debug(
						 "detected segment is in recovery mode and making "
						 "progress (content=%d) primary dbid=%d, mirror dbid=%d",
						 primary->config->segindex, primary->config->dbid, IsMirrorConfigured ? mirror->config->dbid : -1);

					fts_info->state = FTS_RESPONSE_PROCESSED;
					break;
				}

				assert(!IsPrimaryAlive);
				if (!IsMirrorConfigured)
				{
					cbdb_log_debug(
						   "skipping mirror down update for (content=%d, dbid=%d), Becuase no mirror configured." 
						   	, primary->config->segindex, primary->config->dbid);

					is_updated |= updateConfiguration(
						context, primary, mirror,
						GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY,
						GP_SEGMENT_CONFIGURATION_ROLE_MIRROR,
						IsInSync, IsPrimaryAlive, IsMirrorAlive);
					fts_info->state = FTS_RESPONSE_PROCESSED;

					break;
				}

				/* See if mirror can be promoted. */
				if (SEGMENT_IS_IN_SYNC(mirror))
				{
					/*
					 * Flip the roles and mark the failed primary as down in
					 * FTS configuration before sending promote message.
					 * Dispatcher should no longer consider the failed primary
					 * for gang creation, FTS should no longer probe the failed
					 * primary.
					 */
					is_updated |= updateConfiguration(
						context, primary, mirror,
						GP_SEGMENT_CONFIGURATION_ROLE_MIRROR, /* newPrimaryRole */
						GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY, /* newMirrorRole */
						IsInSync, IsPrimaryAlive, IsMirrorAlive);

					/*
					 * Swap the primary and mirror references so that the
					 * mirror will be promoted in subsequent connect, poll,
					 * send, receive steps.
					 */
					cbdb_log_debug(
						   "promoting mirror (content=%d, dbid=%d) "
						   "to be the new primary",
						   mirror->config->segindex, mirror->config->dbid);
					fts_info->state = FTS_PROMOTE_SEGMENT;
					fts_info->primary_cdbinfo = mirror;
					fts_info->mirror_cdbinfo = primary;
				}
				else
				{
					/*
					 * Only log here, will handle it later, having an "ERROR"
					 * keyword here for customer convenience
					 */
					cbdb_log_warning("double fault detected (content=%d) "
						 "primary dbid=%d, mirror dbid=%d",
						 primary->config->segindex, primary->config->dbid, IsMirrorConfigured ? mirror->config->dbid : -1);
					
					is_updated |= updateConfiguration(
						context, primary, mirror,
						GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY,
						GP_SEGMENT_CONFIGURATION_ROLE_MIRROR,
						IsInSync, IsPrimaryAlive, IsMirrorAlive);
					fts_info->state = FTS_RESPONSE_PROCESSED;
				}
				break;
			case FTS_SYNCREP_OFF_FAILED:
				/*
				 * Another attempt to turn off syncrep will be made in the next
				 * probe cycle.  Until then, leave the transactions waiting for
				 * syncrep.  A worse alternative is to PANIC.
				 */
				cbdb_log_warning("failed to turn off syncrep on (content=%d,"
					 " dbid=%d)", primary->config->segindex, primary->config->dbid);
				fts_info->state = FTS_RESPONSE_PROCESSED;
				break;
			case FTS_PROMOTE_FAILED:
				/*
				 * Only log here, will handle it later, having an "ERROR"
				 * keyword here for customer convenience
				 */
				cbdb_log_warning("double fault detected (content=%d) "
					 "primary dbid=%d, mirror dbid=%d",
					 primary->config->segindex, primary->config->dbid, IsMirrorConfigured ? mirror->config->dbid : -1);
				is_updated |= updateConfiguration(
						context, primary, mirror,
						GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY,
						GP_SEGMENT_CONFIGURATION_ROLE_MIRROR,
						IsInSync, IsPrimaryAlive, IsMirrorAlive);
					fts_info->state = FTS_RESPONSE_PROCESSED;
				fts_info->state = FTS_RESPONSE_PROCESSED;
				break;
			case FTS_PROMOTE_SUCCESS:
				cbdb_log_info(
					   "mirror (content=%d, dbid=%d) promotion "
					   "triggered successfully",
					   primary->config->segindex, primary->config->dbid);
				fts_info->state = FTS_RESPONSE_PROCESSED;
				break;
			case FTS_SYNCREP_OFF_SUCCESS:
				cbdb_log_info(
					   "primary (content=%d, dbid=%d) notified to turn "
					   "syncrep off", primary->config->segindex, primary->config->dbid);
				fts_info->state = FTS_RESPONSE_PROCESSED;
				break;
			default:
				cbdb_log_error("invalid internal state %d for (content=%d)"
					 "primary dbid=%d, mirror dbid=%d", fts_info->state,
					 primary->config->segindex, primary->config->dbid, IsMirrorConfigured ? mirror->config->dbid : -1);
				break;
		}
		
		/* Close connection and reset result for next message, if any. */
		memset(&fts_info->result, 0, sizeof(fts_result));
		PQfinish(fts_info->conn);
		fts_info->conn = NULL;
		fts_info->poll_events = fts_info->poll_revents = 0;
		fts_info->retry_count = 0;
	}

	return is_updated;
}

#ifdef USE_ASSERT_CHECKING
static bool
FtsIsSegmentAlive(CdbComponentDatabaseInfo *segInfo)
{
	if (SEGMENT_IS_ACTIVE_MIRROR(segInfo) && SEGMENT_IS_ALIVE(segInfo))
		return true;

	if (SEGMENT_IS_ACTIVE_PRIMARY(segInfo))
		return true;

	return false;
}
#endif

/*
 * Initialize context before a probe cycle based on cluster configuration in
 * cdbs.
 */
static void
FtsWalRepInitProbeContext(CdbComponentDatabases *cdbs, fts_context *context, fts_config * context_config)
{
	context->num_pairs = cdbs->total_segments;
	context->config = context_config;
	/*
	 * context->num_pairs = segment counts
	 * +1 for the master/standby
	 */
	context->perSegInfos = (fts_segment_info *) palloc0(
		(context->num_pairs + 1) * sizeof(fts_segment_info));

	int fts_index = 0;
	int cdb_index = 0;
	for(; cdb_index < cdbs->total_segment_dbs; cdb_index++)
	{
		CdbComponentDatabaseInfo *primary = &(cdbs->segment_db_info[cdb_index]);
		if (!SEGMENT_IS_ACTIVE_PRIMARY(primary))
			continue;
		CdbComponentDatabaseInfo *mirror = FtsGetPeerSegment(cdbs,
															 primary->config->segindex,
															 primary->config->dbid);
#ifdef USE_ASSERT_CHECKING
		/* primary in catalog will NEVER be marked down. */
		Assert(FtsIsSegmentAlive(primary));
#endif

		fts_segment_info *fts_info = &(context->perSegInfos[fts_index]);
		/*
		 * Initialize the response object.  Response from a segment will be
		 * processed only if fts_info->state is one of SUCCESS states.  If a
		 * failure is encountered in messaging a segment, its response will not
		 * be processed.
		 */
		fts_info->result.isPrimaryAlive = false;
		fts_info->result.isMirrorAlive = false;
		fts_info->result.isInSync = false;
		fts_info->result.isSyncRepEnabled = false;
		fts_info->result.retryRequested = false;
		fts_info->result.isRoleMirror = false;
		fts_info->result.dbid = primary->config->dbid;
		if (fts_k8s_compatibility_enabled && primary->config->hostip[0] == '\0')
			fts_info->state = FTS_PROBE_FAILED;
		else
			fts_info->state = FTS_PROBE_SEGMENT;
		fts_info->restart_state = PM_NOT_IN_RESTART;
		fts_info->xlogrecptr = InvalidXLogRecPtr;

		fts_info->primary_cdbinfo = primary;
		fts_info->mirror_cdbinfo = mirror;
		fts_info->has_mirror_configured = mirror != NULL;

		assert(fts_index < context->num_pairs);
		fts_index ++;
	}

	assert(cdbs->total_entry_dbs == 1 || cdbs->total_entry_dbs == 2);

	CdbComponentDatabaseInfo *master = FtsGetPeerMaster(cdbs);
	CdbComponentDatabaseInfo *standby = FtsGetPeerStandby(cdbs);
	fts_segment_info *fts_info = &(context->perSegInfos[context->num_pairs]);
	fts_info->result.isPrimaryAlive = false;
	fts_info->result.isMirrorAlive = false;
	fts_info->result.isInSync = false;
	fts_info->result.isSyncRepEnabled = false;
	fts_info->result.retryRequested = false;
	fts_info->result.isRoleMirror = false;
	fts_info->result.dbid = master->config->dbid;
	if (fts_k8s_compatibility_enabled &&
	   ((master->config->hostip[0] == '\0') ||(standby && standby->config->hostip[0] == '\0')))
		fts_info->state = FTS_PROBE_FAILED;
	else
		fts_info->state = FTS_PROBE_SEGMENT;
	fts_info->restart_state = PM_NOT_IN_RESTART;
	fts_info->xlogrecptr = InvalidXLogRecPtr;

	fts_info->primary_cdbinfo = master;
	fts_info->mirror_cdbinfo = standby;
	fts_info->has_mirror_configured = standby != NULL;

	/* After inited master/standby fts_info
	 * Added master/standby pair number
	 */ 
	context->num_pairs += 1;
}

bool
FtsWalRepMessageSegments(CdbComponentDatabases *cdbs, fts_config * context_config)
{
	bool is_updated = false;
	fts_context context;

	FtsWalRepInitProbeContext(cdbs, &context, context_config);
	InitPollFds(cdbs->total_segments + 1);

	int i = 0;

	while (!allDone(&context) && FtsIsActive())
	{
		ftsConnect(&context);
		ftsPoll(&context);
		ftsSend(&context);
		ftsReceive(&context);
		processRetry(&context);
		is_updated |= processResponse(&context);
	}

	for (i = 0; i < context.num_pairs; i++)	
	{
		if (context.perSegInfos[i].conn)
		{
			PQfinish(context.perSegInfos[i].conn);
			context.perSegInfos[i].conn = NULL;
		}
	}

	pfree(context.perSegInfos);
	pfree(PollFds);
	return is_updated;
}

/* EOF */
#endif 
