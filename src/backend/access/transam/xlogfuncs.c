/*-------------------------------------------------------------------------
 *
 * xlogfuncs.c
 *
 * PostgreSQL transaction log manager user interface functions
 *
 * This file contains WAL control and information functions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/*
 * pg_start_backup: set up for taking an on-line backup dump
 *
 * Essentially what this does is to create a backup label file in $PGDATA,
 * where it will be archived as part of the backup dump.  The label file
 * contains the user-supplied label string (typically this would be used
 * to tell where the backup dump will be stored) and the starting time and
 * starting WAL location for the dump.
 *
 * **Note :- Currently this functionality is not supported.**
 */
Datum
pg_start_backup(PG_FUNCTION_ARGS)
{
	XLogRecPtr	startpoint = {0,0};
	char		startxlogstr[MAXFNAMELEN];

	ereport(NOTICE,
			(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_start_backup() is not supported in Greenplum Database"),
			 errhint("Contact support to get more information and resolve the issue")));

	snprintf(startxlogstr, sizeof(startxlogstr), "%X/%X",
			 startpoint.xlogid, startpoint.xrecoff);
	PG_RETURN_TEXT_P(cstring_to_text(startxlogstr));
}

/*
 * pg_stop_backup: finish taking an on-line backup dump
 *
 * We write an end-of-backup WAL record, and remove the backup label file
 * created by pg_start_backup, creating a backup history file in pg_xlog
 * instead (whence it will immediately be archived). The backup history file
 * contains the same info found in the label file, plus the backup-end time
 * and WAL location. Before 9.0, the backup-end time was read from the backup
 * history file at the beginning of archive recovery, but we now use the WAL
 * record for that and the file is for informational and debug purposes only.
 *
 * Note: different from CancelBackup which just cancels online backup mode.
 *
 * **Note :- Currently this functionality is not supported.**
 */
Datum
pg_stop_backup(PG_FUNCTION_ARGS)
{
	XLogRecPtr	stoppoint = {0,0};
	char		stopxlogstr[MAXFNAMELEN];

	ereport(NOTICE,
			(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_stop_backup() is not supported in Greenplum Database"),
			 errhint("Contact support to get more information and resolve the issue")));

	snprintf(stopxlogstr, sizeof(stopxlogstr), "%X/%X",
			 stoppoint.xlogid, stoppoint.xrecoff);
	PG_RETURN_TEXT_P(cstring_to_text(stopxlogstr));
}
