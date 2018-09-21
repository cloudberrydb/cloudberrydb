/*-------------------------------------------------------------------------
 *
 * compat.c
 *		Reimplementations of various backend functions.
 *
 * Portions Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pg_xlogdump/compat.c
 *
 * This file contains client-side implementations for various backend
 * functions that the rm_desc functions in *desc.c files rely on.
 *
 *-------------------------------------------------------------------------
 */

/* ugly hack, same as in e.g pg_controldata */
#define FRONTEND 1
#include "postgres.h"

#include <time.h>

#include "utils/datetime.h"
#include "lib/stringinfo.h"

#include "access/twophase.h"
#include "access/xlog.h"
#include "catalog/pg_tablespace.h"
#include "common/relpath.h"

/* copied from timestamp.c */
pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	pg_time_t	result;

#ifdef HAVE_INT64_TIMESTAMP
	result = (pg_time_t) (t / USECS_PER_SEC +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#else
	result = (pg_time_t) (t +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#endif
	return result;
}

/*
 * Stopgap implementation of timestamptz_to_str that doesn't depend on backend
 * infrastructure.	This will work for timestamps that are within the range
 * of the platform time_t type.  (pg_time_t is compatible except for possibly
 * being wider.)
 *
 * XXX the return value points to a static buffer, so beware of using more
 * than one result value concurrently.
 *
 * XXX: The backend timestamp infrastructure should instead be split out and
 * moved into src/common.  That's a large project though.
 */
const char *
timestamptz_to_str(TimestampTz dt)
{
	static char buf[MAXDATELEN + 1];
	char		ts[MAXDATELEN + 1];
	char		zone[MAXDATELEN + 1];
	time_t		result = (time_t) timestamptz_to_time_t(dt);
	struct tm  *ltime = localtime(&result);

	strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", ltime);
	strftime(zone, sizeof(zone), "%Z", ltime);

#ifdef HAVE_INT64_TIMESTAMP
	sprintf(buf, "%s.%06d %s", ts, (int) (dt % USECS_PER_SEC), zone);
#else
	sprintf(buf, "%s.%.6f %s", ts, fabs(dt - floor(dt)), zone);
#endif

	return buf;
}

/*
 * Provide a hacked up compat layer for StringInfos so xlog desc functions can
 * be linked/called.
 */
void
appendStringInfo(StringInfo str, const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	vprintf(fmt, args);
	va_end(args);
}

void
appendStringInfoString(StringInfo str, const char *string)
{
	appendStringInfo(str, "%s", string);
}

void
appendStringInfoChar(StringInfo str, char ch)
{
	appendStringInfo(str, "%c", ch);
}


const char *
tablespace_version_directory(void)
{
	static char path[MAXPGPATH] = "";

	// GPDB_93_MERGE_FIXME: I hardcoded dbid 0 here, just to make this compile.
	// Where do we get the actual value?
	if (!path[0])
		snprintf(path, MAXPGPATH, "%s_db%d", GP_TABLESPACE_VERSION_DIRECTORY,
				 0 /* GpIdentity.dbid */);

	return path;
}

/* copied from xlog.c */
void
UnpackCheckPointRecord(XLogRecord *record, CheckpointExtendedRecord *ckptExtended)
{
	char *current_record_ptr;
	int remainderLen;

	if (record->xl_len == sizeof(CheckPoint))
	{
		/* Special (for bootstrap, xlog switch, maybe others) */
		ckptExtended->dtxCheckpoint = NULL;
		ckptExtended->dtxCheckpointLen = 0;
		ckptExtended->ptas = NULL;
		return;
	}

	/* Normal checkpoint Record */
	Assert(record->xl_len > sizeof(CheckPoint));

	current_record_ptr = ((char*)XLogRecGetData(record)) + sizeof(CheckPoint);
	remainderLen = record->xl_len - sizeof(CheckPoint);

	/* Start of distributed transaction information */
	ckptExtended->dtxCheckpoint = (TMGXACT_CHECKPOINT *)current_record_ptr;
	ckptExtended->dtxCheckpointLen =
		TMGXACT_CHECKPOINT_BYTES((ckptExtended->dtxCheckpoint)->committedCount);

	/*
	 * The master prepared transaction aggregate state (ptas) will be skipped
	 * when gp_before_filespace_setup is ON.
	 */
	if (remainderLen > ckptExtended->dtxCheckpointLen)
	{
		current_record_ptr = current_record_ptr + ckptExtended->dtxCheckpointLen;
		remainderLen -= ckptExtended->dtxCheckpointLen;

		/* Finally, point to prepared transaction information */
		ckptExtended->ptas = (prepared_transaction_agg_state *) current_record_ptr;
		Assert(remainderLen == PREPARED_TRANSACTION_CHECKPOINT_BYTES(ckptExtended->ptas->count));
	}
	else
	{
		Assert(remainderLen == ckptExtended->dtxCheckpointLen);
		ckptExtended->ptas = NULL;
	}
}
