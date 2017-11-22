/*-------------------------------------------------------------------------
 *
 * gp_replication.c
 *
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/replication/gp_replication.c
 *
 *-------------------------------------------------------------------------
 */

#include "replication/gp_replication.h"
#include "utils/guc.h"

#include "replication/walsender.h"
#include "replication/walsender_private.h"

/*
 * Checking the WalSndCtl to figure out whether the mirror is up or not.
 *
 * True: if any mirror is up.
 * False: if none of the mirrors is up.
 */
void GetMirrorStatus(bool *IsMirrorUp, bool *IsInSync)
{
	*IsMirrorUp = false;
	*IsInSync = false;

	/*
	 * Greenplum currently supports only ONE mirror per primary.
	 * If there are more mirrors, this logic in this function need to be revised.
	 */
	Assert(max_wal_senders == 1);

	LWLockAcquire(SyncRepLock, LW_SHARED);

	for (int i = 0; i < max_wal_senders; i++)
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile WalSnd *walsnd = &WalSndCtl->walsnds[i];

		if (walsnd->pid != 0)
		{
			if(walsnd->state == WALSNDSTATE_CATCHUP
			   || walsnd->state == WALSNDSTATE_STREAMING)
			{
				*IsMirrorUp = true;

				if (walsnd->state == WALSNDSTATE_STREAMING)
					*IsInSync = true;

				break;
			}
		}
	}

	LWLockRelease(SyncRepLock);
}

/*
 * Set WalSndCtl->sync_standbys_defined to true to enable synchronous segment
 * WAL replication and insert synchronous_standby_names="*" into the
 * gp_replication.conf to persist this state in case of segment crash.
 */
void
SetSyncStandbysDefined(void)
{
	LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

	if (!WalSndCtl->sync_standbys_defined)
	{
		SyncRepStandbyNames = "*";
		set_gp_replication_config("synchronous_standby_names", SyncRepStandbyNames);
		SyncRepUpdateSyncStandbysDefined();
	}

	LWLockRelease(SyncRepLock);
}
