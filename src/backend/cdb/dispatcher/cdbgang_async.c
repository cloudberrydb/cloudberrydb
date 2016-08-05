
/*-------------------------------------------------------------------------
 *
 * cdbgang_async.c
 *	  Functions for asynchronous implementation of creating gang.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <limits.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "storage/ipc.h"		/* For proc_exit_inprogress  */
#include "tcop/tcopprot.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/gp_atomic.h"

static int getTimeout(const struct timeval* startTS);
static Gang *createGang_async(GangType type, int gang_id, int size, int content);

CreateGangFunc pCreateGangFuncAsync = createGang_async;

/*
 * Creates a new gang by logging on a session to each segDB involved.
 *
 * call this function in GangContext memory context.
 * elog ERROR or return a non-NULL gang.
 */
static Gang*
createGang_async(GangType type, int gang_id, int size, int content)
{
	Gang *newGangDefinition;
	SegmentDatabaseDescriptor *segdbDesc = NULL;
	int i = 0;
	int create_gang_retry_counter = 0;
	int in_recovery_mode_count = 0;
	int successful_connections = 0;

	ELOG_DISPATCHER_DEBUG("createGang type = %d, gang_id = %d, size = %d, content = %d",
			type, gang_id, size, content);

	/* check arguments */
	Assert(size == 1 || size == getgpsegmentCount());
	Assert(CurrentResourceOwner != NULL);
	Assert(CurrentMemoryContext == GangContext);

create_gang_retry:
	/* If we're in a retry, we may need to reset our initial state, a bit */
	newGangDefinition = NULL;
	successful_connections = 0;
	in_recovery_mode_count = 0;

	/* Check the writer gang first. */
	if (type != GANGTYPE_PRIMARY_WRITER && !isPrimaryWriterGangAlive())
	{
		elog(LOG, "primary writer gang is broken");
		goto exit;
	}

	/* allocate and initialize a gang structure */
	newGangDefinition = buildGangDefinition(type, gang_id, size, content);
	Assert(newGangDefinition != NULL);
	Assert(newGangDefinition->size == size);
	Assert(newGangDefinition->perGangContext != NULL);
	MemoryContextSwitchTo(newGangDefinition->perGangContext);

	PG_TRY();
	{
		struct pollfd *fds;
		struct timeval startTS;

		for (i = 0; i < size; i++)
		{
			char gpqeid[100];
			char *options;

			/*
			 * Create the connection requests.	If we find a segment without a
			 * valid segdb we error out.  Also, if this segdb is invalid, we must
			 * fail the connection.
			 */
			segdbDesc = &newGangDefinition->db_descriptors[i];

			/*
			 * Build the connection string.  Writer-ness needs to be processed
			 * early enough now some locks are taken before command line options
			 * are recognized.
			 */
			build_gpqeid_param(gpqeid, sizeof(gpqeid),
							   segdbDesc->segindex,
							   type == GANGTYPE_PRIMARY_WRITER,
							   gang_id);

			options = makeOptions();

			cdbconn_doConnectStart(segdbDesc, gpqeid, options);
			if (cdbconn_isBadConnection(segdbDesc))
			{
				/*
				 * Log the details to the server log, but give a more
				 * generic error to the client. XXX: The user would
				 * probably prefer to see a bit more details too.
				 */
				ereport(LOG, (errcode(ERRCODE_GP_INTERNAL_ERROR),
							  errmsg("Master unable to connect to %s with options %s: %s",
								segdbDesc->whoami,
								options,
								PQerrorMessage(segdbDesc->conn))));

				if (!segment_failure_due_to_recovery(segdbDesc))
					ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
									errmsg("failed to acquire resources on one or more segments")));
				else
					in_recovery_mode_count++;
			}
		}

		/*
		 * Ok, we've now launched all the connection attempts. Start the
		 * timeout clock (= get the start timestamp), and poll until they're
		 * all completed or we reach timeout.
		 */
		gettimeofday(&startTS, NULL);
		fds = (struct pollfd *) palloc0(sizeof(struct pollfd) * size);

		for(;;)
		{
			int nready;
			int timeout;
			int nfds = 0;

			for (i = 0; i < size; i++)
			{
				PostgresPollingStatusType pollStatus;

				segdbDesc = &newGangDefinition->db_descriptors[i];

				if (cdbconn_isConnectionOk(segdbDesc))
					continue;

				pollStatus = PQconnectPoll(segdbDesc->conn);
				switch (pollStatus)
				{
					case PGRES_POLLING_OK:
						cdbconn_doConnectComplete(segdbDesc);
						successful_connections++;
						break;

					case PGRES_POLLING_READING:
						fds[nfds].fd = PQsocket(segdbDesc->conn);
						fds[nfds].events = POLLIN;
						nfds++;
						break;

					case PGRES_POLLING_WRITING:
						fds[nfds].fd = PQsocket(segdbDesc->conn);
						fds[nfds].events = POLLOUT;
						nfds++;
						break;

					case PGRES_POLLING_FAILED:
						elog(LOG, "Failed to connect to %s", segdbDesc->whoami);
						ereport(ERROR,
								(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								 errmsg("failed to acquire resources on one or more segments")));
						break;

					default:
						elog(LOG, "Get wrong poll status when connect to %s", segdbDesc->whoami);
						ereport(ERROR,
								(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								 errmsg("failed to acquire resources on one or more segments")));
						break;
				}
			}

			if (nfds == 0)
				break;

			timeout = getTimeout(&startTS);

			/* Wait until something happens */
			nready = poll(fds, nfds, timeout);

			if (nready < 0)
			{
				int	sock_errno = SOCK_ERRNO;
				if (sock_errno == EINTR)
					continue;

				ereport(LOG, (errcode_for_socket_access(),
							  errmsg("poll() failed while connecting to segments")));
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("failed to acquire resources on one or more segments")));
			}
			else if (nready == 0)
			{
				if (timeout != 0)
					continue;

				elog(LOG, "poll() timeout while connecting to segments");
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("failed to acquire resources on one or more segments")));
			}
		}

		if (successful_connections != size)
			ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
							errmsg("failed to acquire resources on one or more segments")));

		MemoryContextSwitchTo(GangContext);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(GangContext);
		/*
		 * If this is a reader gang and the writer gang is invalid, destroy all gangs.
		 * This happens when one segment is reset.
		 */
		if (type != GANGTYPE_PRIMARY_WRITER && !isPrimaryWriterGangAlive())
		{
			elog(LOG, "primary writer gang is broken");
			goto exit;
		}

		/* FTS shows some segment DBs are down, destroy all gangs. */
		if (isFTSEnabled() &&
			FtsTestSegmentDBIsDown(newGangDefinition->db_descriptors, size))
		{
			elog(LOG, "FTS detected some segments are down");
			goto exit;
		}

		/* Writer gang is created before reader gangs. */
		if (type == GANGTYPE_PRIMARY_WRITER)
			Insist(!gangsExist());

		ELOG_DISPATCHER_DEBUG("createGang: %d processes requested; %d successful connections %d in recovery",
				size, successful_connections, in_recovery_mode_count);

		/*
		 * Retry when any of the following condition is met:
		 * 1) This is the writer gang.
		 * 2) This is the first reader gang.
		 * 3) All failed segments are in recovery mode.
		 */
		if(gp_gang_creation_retry_count &&
		   create_gang_retry_counter++ < gp_gang_creation_retry_count &&
		   (type == GANGTYPE_PRIMARY_WRITER ||
		    !readerGangsExist() ||
		    successful_connections + in_recovery_mode_count == size))
		{
			disconnectAndDestroyGang(newGangDefinition);
			newGangDefinition = NULL;

			ELOG_DISPATCHER_DEBUG("createGang: gang creation failed, but retryable.");

			CHECK_FOR_INTERRUPTS();
			pg_usleep(gp_gang_creation_retry_timer * 1000);
			CHECK_FOR_INTERRUPTS();

			goto create_gang_retry;
		}
		else
		{
			goto exit;
		}
	}
	PG_END_TRY();

	setLargestGangsize(size);
	return newGangDefinition;

exit:
	disconnectAndDestroyGang(newGangDefinition);
	disconnectAndDestroyAllGangs(true);
	CheckForResetSession();
	ereport(ERROR,
			(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
			errmsg("failed to acquire resources on one or more segments")));
	return NULL;
}

static int getTimeout(const struct timeval* startTS)
{
	struct timeval now;
	int timeout;
	int64 diff_us;

	gettimeofday(&now, NULL);

	if (gp_segment_connect_timeout > 0)
	{
		diff_us = (now.tv_sec - startTS->tv_sec) * 1000000;
		diff_us += (int) now.tv_usec - (int) startTS->tv_usec;
		if (diff_us > (int64) gp_segment_connect_timeout * 1000000)
			timeout = 0;
		else
			timeout = gp_segment_connect_timeout * 1000 - diff_us / 1000;
	}
	else
		timeout = -1;

	return timeout;
}

