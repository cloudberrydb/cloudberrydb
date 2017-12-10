/*-------------------------------------------------------------------------
 *
 * cdbfilerepprimaryrecovery.c
 *
 * Portions Copyright (c) 2009-2010 Greenplum Inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbfilerepprimaryrecovery.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>

#include "access/xlog.h"
#include "access/twophase.h"
#include "access/slru.h"
#include "cdb/cdbfilerep.h"
#include "cdb/cdbfilerepprimary.h"
#include "cdb/cdbfilerepprimaryack.h"
#include "cdb/cdbfilerepprimaryrecovery.h"
#include "cdb/cdbfilerepservice.h"
#include "cdb/cdbmirroredflatfile.h"
#include "utils/flatfiles.h"

static void FileRepPrimary_StartRecoveryInSync(void);
static int	FileRepPrimary_RunRecoveryInSync(void);

static void FileRepPrimary_RunHeartBeat(void);

/****************************************************************
 * FILEREP SUB-PROCESS (FileRep Primary RECOVERY Process)
 ****************************************************************/

void
FileRepPrimary_StartRecovery(void)
{
	FileRep_InsertConfigLogEntry("start recovery");

	switch (dataState)
	{
		case DataStateInSync:
			FileRepPrimary_StartRecoveryInSync();
			FileRepPrimary_RunHeartBeat();
			break;

		case DataStateInResync:
			FileRepPrimary_RunHeartBeat();
			break;

		default:
			Assert(0);
			break;
	}
}

/****************************************************************
 * DataStateInSync
 ****************************************************************/

/*
 *
 * FileRepPrimary_StartRecoveryInSync()
 *
 *
 */
static void
FileRepPrimary_StartRecoveryInSync(void)
{
	int			status = STATUS_OK;

	FileRep_InsertConfigLogEntry("run recovery");

	while (1)
	{

		if (status != STATUS_OK)
		{
			FileRep_SetSegmentState(SegmentStateFault, FaultTypeMirror);
			FileRepSubProcess_SetState(FileRepStateFault);
		}

		while (FileRepSubProcess_GetState() == FileRepStateFault ||

			   (fileRepShmemArray[0]->state == FileRepStateNotInitialized &&
				FileRepSubProcess_GetState() != FileRepStateShutdownBackends &&
				FileRepSubProcess_GetState() != FileRepStateShutdown))
		{

			FileRepSubProcess_ProcessSignals();
			pg_usleep(50000L);	/* 50 ms */
		}

		if (FileRepSubProcess_GetState() == FileRepStateShutdown ||
			FileRepSubProcess_GetState() == FileRepStateShutdownBackends)
		{

			break;
		}

		if (FileRepSubProcess_GetState() == FileRepStateReady)
		{
			break;
		}

		Insist(fileRepRole == FileRepPrimaryRole);

		Insist(dataState == DataStateInSync);

		status = FileRepPrimary_RunRecoveryInSync();


	}
}

/*-----------------------------------------
 * FileRepPrimary_RunRecoveryInSync()
 *
 *		1) Recover Flat Files
 *			a) pg_control file
 *			b) pg_database file
 *			c) pg_auth file
 *			d) pg_twophase directory
 *			e) Slru directories
 *					*) pg_clog
 *					*) pg_multixact
 *					*) pg_distributedlog
 *					*) pg_distributedxidmap
 *					*) pg_subtrans
 *
 *		2) Reconcile xlog EOF
 *-----------------------------------------
 */
static int
FileRepPrimary_RunRecoveryInSync(void)
{
	int			status = STATUS_OK;

	FileRep_InsertConfigLogEntry("run recovery of flat files");

	while (1)
	{

		status = XLogRecoverMirrorControlFile();

		if (status != STATUS_OK)
		{
			break;
		}

		FileRepSubProcess_ProcessSignals();
		if (FileRepSubProcess_GetState() != FileRepStateInitialization)
		{
			break;
		}

		status = XLogReconcileEofPrimary();

		if (status != STATUS_OK)
		{
			break;
		}

		FileRepSubProcess_ProcessSignals();
		if (FileRepSubProcess_GetState() != FileRepStateInitialization)
		{
			break;
		}

		MirroredFlatFile_DropTemporaryFiles();

		FileRepSubProcess_ProcessSignals();
		if (FileRepSubProcess_GetState() != FileRepStateInitialization)
		{
			break;
		}

		MirroredFlatFile_MirrorDropTemporaryFiles();

		FileRepSubProcess_ProcessSignals();
		if (FileRepSubProcess_GetState() != FileRepStateInitialization)
		{
			break;
		}

		status = FlatFilesRecoverMirror();

		if (status != STATUS_OK)
		{
			break;
		}

		FileRepSubProcess_ProcessSignals();
		if (FileRepSubProcess_GetState() != FileRepStateInitialization)
		{
			break;
		}

		status = TwoPhaseRecoverMirror();

		if (status != STATUS_OK)
		{
			break;
		}

		FileRepSubProcess_ProcessSignals();
		if (FileRepSubProcess_GetState() != FileRepStateInitialization)
		{
			break;
		}

		status = SlruRecoverMirror();

		if (status != STATUS_OK)
		{
			break;
		}

		FileRepSubProcess_ProcessSignals();
		if (FileRepSubProcess_GetState() != FileRepStateInitialization)
		{
			break;
		}

		FileRepSubProcess_SetState(FileRepStateReady);
		break;
	}


	return status;
}

/*
 *
 * FileRepPrimary_RunHeartBeat()
 *
 *
 */
static void
FileRepPrimary_RunHeartBeat(void)
{
	int			retry = 0;

	Insist(fileRepRole == FileRepPrimaryRole);

	Insist(dataState == DataStateInSync ||
		   dataState == DataStateInResync);

	while (1)
	{
		FileRepSubProcess_ProcessSignals();

		while (FileRepSubProcess_GetState() == FileRepStateFault ||

			   (fileRepShmemArray[0]->state == FileRepStateNotInitialized &&
				FileRepSubProcess_GetState() != FileRepStateShutdownBackends &&
				FileRepSubProcess_GetState() != FileRepStateShutdown))
		{

			FileRepSubProcess_ProcessSignals();
			pg_usleep(50000L);	/* 50 ms */
		}

		if (FileRepSubProcess_GetState() == FileRepStateShutdown ||
			FileRepSubProcess_GetState() == FileRepStateShutdownBackends)
		{

			break;
		}

		/*
		 * verify if flow from primary to mirror and back is alive once per
		 * minute
		 */
		pg_usleep(50000L);		/* 50 ms */

		if (FileRepSubProcess_ProcessSignals() == true ||
			FileRepSubProcess_GetState() == FileRepStateFault)
		{
			continue;
		}

		retry++;
		if (retry == 1200)		/* 1200 * 50 ms = 60 sec */
		{
			FileRepPrimary_MirrorHeartBeat(FileRepMessageTypeXLog);
			continue;
		}

		if (retry == 1201)		/* 1200 * 50 ms = 60 sec */
		{
			FileRepPrimary_MirrorHeartBeat(FileRepMessageTypeWriter);
			continue;
		}

		if (retry == 1202)		/* 1200 * 50 ms = 60 sec */
		{
			FileRepPrimary_MirrorHeartBeat(FileRepMessageTypeAO01);
			retry = 0;
		}
	}
}
