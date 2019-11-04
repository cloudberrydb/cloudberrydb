/*-------------------------------------------------------------------------
 *
 * xlogfuncs_gp.c
 *
 * GPDB-specific transaction log manager user interface functions
 *
 * This file contains WAL control and information functions.
 *
 * Portions Copyright (c) 2017-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog_fn.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "utils/faultinjector.h"

/*
 * gp_create_restore_point: a distributed named point for cluster restore
 */
Datum
gp_create_restore_point(PG_FUNCTION_ARGS)
{
	text *restore_name = PG_GETARG_TEXT_P(0);
	char *restore_name_str;
	char *restore_command;

	restore_name_str = text_to_cstring(restore_name);

	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "cannot use gp_create_restore_point() when not in QD mode");

	restore_command = psprintf("SELECT pg_catalog.pg_create_restore_point(%s)",
								   quote_literal_cstr(restore_name_str));

	/*
	 * Acquire TwophaseCommitLock in EXCLUSIVE mode. This is to ensure
	 * cluster-wide restore point consistency by blocking distributed commit
	 * prepared broadcasts from concurrent twophase transactions where a QE
	 * segment has written WAL.
	 */
	LWLockAcquire(TwophaseCommitLock, LW_EXCLUSIVE);

	SIMPLE_FAULT_INJECTOR("gp_create_restore_point_acquired_lock");

	CdbDispatchCommand(restore_command, DF_NEED_TWO_PHASE | DF_CANCEL_ON_ERROR, NULL);
	DirectFunctionCall1(pg_create_restore_point, PointerGetDatum(restore_name));

	LWLockRelease(TwophaseCommitLock);

	pfree(restore_command);

	PG_RETURN_BOOL(true);
}
