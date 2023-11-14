#include "utils/sharedqueryplan.h"

static ResourceOwner SharedQueryPlanResOwner = NULL;
static HTAB *SharedQueryPlanHash;

#define SHARED_QUERY_PLAN_TABLE_SIZE MaxConnections

static void
destroy_entry(SharedQueryPlanEntry *entry, bool isCommit);

Size
SharedQueryPlanHashSize(void)
{
	return hash_estimate_size(SHARED_QUERY_PLAN_TABLE_SIZE, sizeof(SharedQueryPlanEntry));
}

void
InitSharedQueryPlanHash(void)
{
	HASHCTL ctl;
	ctl.keysize = sizeof(gp_session_id);
	ctl.entrysize = sizeof(SharedQueryPlanEntry);
	SharedQueryPlanHash = ShmemInitHash("Shared Query Plans Hash",
	                                    SHARED_QUERY_PLAN_TABLE_SIZE,
	                                    SHARED_QUERY_PLAN_TABLE_SIZE,
	                                    &ctl,
	                                    HASH_ELEM | HASH_BLOBS);
}

void
ReadSharedQueryPlan(char *serializedPlantree, int serializedPlantreelen, char *serializedQueryDispatchDesc, int serializedQueryDispatchDesclen)
{
	SharedQueryPlanEntry *entry;
	dsm_segment *seg;
	char *addr;
	long sleep_per_check_us = 1 * 1000;
	long timeout_us = 5 * 1000 * 1000;
	long total_sleep_time_us = 0;
	long warning_sleep_time_us = 0;
	for (;;)
	{
		LWLockAcquire(SharedQueryPlanLock, LW_EXCLUSIVE);
		entry = (SharedQueryPlanEntry *) hash_search(SharedQueryPlanHash,
		                                             &gp_session_id,
		                                             HASH_FIND,
		                                             NULL);
		if (entry && entry->commandCount == gp_command_count)
		{
			seg = dsm_attach(entry->handle);
			if (seg == NULL)
				ereport(ERROR, errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				        errmsg("Dispatch by shmem commandCount matched but dsm_attach returns NULL"),
				        errdetail("May due to out of memory. Please retry. "
				                  "commandCount = %d (local %d), handle = %u",
				                  entry->commandCount, gp_command_count, entry->handle));

			addr = dsm_segment_address(seg);
			memcpy(serializedPlantree, addr, serializedPlantreelen);
			memcpy(serializedQueryDispatchDesc, addr + serializedPlantreelen, serializedQueryDispatchDesclen);
			dsm_detach(seg);

			entry->reference--;
			Assert(entry->reference >= 0);
			if (entry->reference == 0)
				destroy_entry(entry, true);
			LWLockRelease(SharedQueryPlanLock);
			return;
		}
		if (warning_sleep_time_us > 1000 * 1000)
		{
			/* log a warning every second */
			ereport(LOG, errmsg("ReadSharedQueryPlan did not find shared plan. "
			                    "We are waiting for writer QE of session %d to set shared "
			                    "serializedPlantree and serializedQueryDispatchDesc for commandCount %d "
			                    "but it is currently %d. Already waited for %ldus and will time out at %ldus.",
			                    gp_session_id,
			                    gp_command_count,
			                    entry ? entry->commandCount : -1,
			                    total_sleep_time_us,
			                    timeout_us));
			warning_sleep_time_us = 0;
		}
		LWLockRelease(SharedQueryPlanLock);

		if (total_sleep_time_us >= timeout_us)
			ereport(ERROR, errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
			        errmsg("ReadSharedQueryPlan timed out after %ldus waiting for writer QE to set shared plan.", total_sleep_time_us),
			        errdetail("We are waiting for writer QE of session %d to set shared "
			                  "serializedPlantree and serializedQueryDispatchDesc for commandCount %d "
			                  "but it is currently %d. This may due to network congestion, please retry.",
			                  gp_session_id,
			                  gp_command_count,
			                  entry ? entry->commandCount : -1));

		pg_usleep(sleep_per_check_us);

		CHECK_FOR_INTERRUPTS();

		total_sleep_time_us += sleep_per_check_us;
		warning_sleep_time_us += sleep_per_check_us;
	}
}

void
WriteSharedQueryPlan(const char *serializedPlantree, int serializedPlantreelen, const char *serializedQueryDispatchDesc, int serializedQueryDispatchDesclen, const SliceTable *sliceTable)
{
	int i;
	ExecSlice *slice = NULL;
	ExecSlice *local = NULL;

	LWLockAcquire(SharedQueryPlanLock, LW_EXCLUSIVE);
	local = &sliceTable->slices[sliceTable->localSlice];
	int referenced = local->parallel_workers - 1; // referenced counts how many *other* QE need to read from this SharedQueryPlan
	for (i = 0; i < sliceTable->numSlices; i++)
	{
		slice = &sliceTable->slices[i];
		if (slice->sliceIndex == sliceTable->localSlice)
			continue;
		// there can be multiple roots when plan contains InitPlan, only consider those who're in the same root
		if (slice->rootIndex != local->rootIndex)
			continue;
		if (slice->gangType == GANGTYPE_PRIMARY_WRITER || slice->gangType == GANGTYPE_PRIMARY_READER || slice->gangType == GANGTYPE_SINGLETON_READER)
		{
			ListCell *segment_cell;
			foreach(segment_cell, slice->segments)
			{
				referenced += lfirst_int(segment_cell) == GpIdentity.segindex;
			}
		}
	}
	Assert(referenced >= 0);

	if (referenced > 0)
	{
		bool found;
		SharedQueryPlanEntry *entry;
		ResourceOwner oldowner;
		Size size = serializedPlantreelen + serializedQueryDispatchDesclen;

		entry = (SharedQueryPlanEntry *) hash_search(SharedQueryPlanHash,
		                                             &gp_session_id,
		                                             HASH_ENTER,
		                                             &found);
		Assert(!found);
		if (!SharedQueryPlanResOwner)
			SharedQueryPlanResOwner = ResourceOwnerCreate(NULL, "SharedQueryPlanResOwner");

		oldowner = CurrentResourceOwner;
		CurrentResourceOwner = SharedQueryPlanResOwner;
		dsm_segment *seg = dsm_create(size, 0);
		dsm_handle handle = dsm_segment_handle(seg);
		char *addr = dsm_segment_address(seg);
		memcpy(addr, serializedPlantree, serializedPlantreelen);
		memcpy(addr + serializedPlantreelen, serializedQueryDispatchDesc, serializedQueryDispatchDesclen);
		entry->session_id = gp_session_id;
		entry->reference = referenced;
		entry->commandCount = gp_command_count;
		entry->handle = handle;

		/* pin seg to avoid being reclaimed, do that later in ReadSharedQueryPlan */
		dsm_pin_segment(seg);
		dsm_detach(seg);
		CurrentResourceOwner = oldowner;
	}
	LWLockRelease(SharedQueryPlanLock);
}

/* this routine must be called with SharedQueryPlanLock held in exclusive mode */
static void
destroy_entry(SharedQueryPlanEntry *entry, bool isCommit)
{
	bool found;
	dsm_unpin_segment(entry->handle);
	ResourceOwnerRelease(SharedQueryPlanResOwner, RESOURCE_RELEASE_BEFORE_LOCKS, isCommit, true);
	entry->handle = DSM_HANDLE_INVALID;
	hash_search(SharedQueryPlanHash,
	            &gp_session_id,
	            HASH_REMOVE,
	            &found);
	Assert(found);
}

static void
at_abort_discard_entry(void)
{
	SharedQueryPlanEntry *entry;
	LWLockAcquire(SharedQueryPlanLock, LW_EXCLUSIVE);
	entry = (SharedQueryPlanEntry *) hash_search(SharedQueryPlanHash,
	                                             &gp_session_id,
	                                             HASH_FIND,
	                                             NULL);
	if (entry)
	{
		destroy_entry(entry, false);
	}
	LWLockRelease(SharedQueryPlanLock);
}

void
AtAbort_SharedQueryPlan(void)
{
	at_abort_discard_entry();
}