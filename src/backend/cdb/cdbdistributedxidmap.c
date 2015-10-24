/*-------------------------------------------------------------------------
 *
 * cdbdistributedxidmap.c
 *		Maps distributed (DTM) XIDs to local xids 
 *
 * The pg_distributedxidmap manager is a pg_subtrans-like manager that stores the
 * local transaction id for each global transaction.
 *
 * We only need to remember pg_distributedxidmap information for currently-open 
 * global transactions.  Thus, there is no need to preserve data over a crash
 * and restart.
 *
 * There are no XLOG interactions since we do not care about preserving
 * the map across crashes.  During database startup, we simply delete all
 * currently-active pages of DistributedXidMap and create an initial first page
 * of zeroes.
 *
 * Copyright (c) 2007-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/slru.h"
#include "access/transam.h"
#include "access/cdbdistributedxidmap.h"
#include "utils/tqual.h"
#include "utils/guc.h"
#include "storage/shmem.h"


/*
 * Defines for DistributedTransactionId page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: We are assuming that no more than 4 billion distributed transactions can
 * be generated during one greenplum array start.
 */


typedef struct DISTRIBUTEDXIDMAP_ENTRY
{
	DistributedMapState	state;

	int				    pid;
	TransactionId		xid;
}	DISTRIBUTEDXIDMAP_ENTRY;

#define ENTRIES_PER_PAGE (BLCKSZ / sizeof(DISTRIBUTEDXIDMAP_ENTRY))

#define DistributedTransactionIdToPage(gxid) ((gxid) / (DistributedTransactionId) ENTRIES_PER_PAGE)
#define DistributedTransactionIdToEntry(gxid) ((gxid) % (DistributedTransactionId) ENTRIES_PER_PAGE)

/*
 * Shared memory variables.
 */
typedef struct DISTRIBUTEDXIDMAP_SHARED
{
	int DistributedXidMapHighestPageNo;
	
	DistributedTransactionId MaxDistributedXid;

} DISTRIBUTEDXIDMAP_SHARED;



/*
 * Link to shared-memory data structures for DistributedTransactionId control
 */
static SlruCtlData DistributedXidMapCtlData;

#define DistributedXidMapCtl  (&DistributedXidMapCtlData)

/*
 * We initialize the highest page to 0 since we allocate the first page
 * when starting.
 */
static int *shmDistributedXidMapHighestPageNo = NULL;
static DistributedTransactionId *shmMaxDistributedXid = NULL;

static int	ZeroDistributedXidMapPage(int pageno);
static bool DistributedXidMapPagePrecedes(int page1, int page2);
static void DistributedXidMapMakeMorePages(int pageno);
static char* DistributedMapStateToString(DistributedMapState state);

DistributedTransactionId GetMaxDistributedXid(void)
{
	return (shmMaxDistributedXid == NULL ? 0 : *shmMaxDistributedXid);

}

static char* DistributedMapStateToString(DistributedMapState state)
{
	switch (state)
	{
	case DISTRIBUTEDXIDMAP_STATE_NONE:	
		return "None";
	case DISTRIBUTEDXIDMAP_STATE_PREALLOC_FOR_OPEN_TRANS:	
		return "Preallocated for Open Transaction";
	case DISTRIBUTEDXIDMAP_STATE_IN_PROGRESS:	
		return "In-Progress";
	case DISTRIBUTEDXIDMAP_STATE_PREPARED:	
		return "Prepared";
	case DISTRIBUTEDXIDMAP_STATE_COMMITTED:	
		return "Committed";
	case DISTRIBUTEDXIDMAP_STATE_ABORTED:	
		return "Aborted";
	default:
		return "Unknown";
	}
}

static void
DistributedXidMapMakeMorePages(int pageno)
{
	int			newpageno;
	
	for (newpageno = *shmDistributedXidMapHighestPageNo + 1; newpageno <= pageno; newpageno++)
	{
		elog((Debug_print_full_dtm ? LOG : DEBUG5), "DistributedXidMapMakeMorePages zeroing out page = %d", newpageno);
		
		/* Zero the page */
		ZeroDistributedXidMapPage(newpageno);
	}

	*shmDistributedXidMapHighestPageNo = pageno;
}

/*
 * Get the local XID associated with a distributed transaction.  We will
 * allocate the local XID and assign its value in the map if necessary.
 */
void
AllocOrGetLocalXidForStartDistributedTransaction(DistributedTransactionId gxid, TransactionId *xid)
{
	int			pageno = DistributedTransactionIdToPage(gxid);
	int			entryno = DistributedTransactionIdToEntry(gxid);
	int			slotno;
	DISTRIBUTEDXIDMAP_ENTRY *ptr;

	Assert(gxid != InvalidDistributedTransactionId);
	Assert(xid != NULL);

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "Entering AllocOrGetLocalXidForStartDistributedTransaction with distributed xid = %d (pageno = %d, entryno = %d, DistributedXidMapHighestPageNo = %d)",
		 gxid, pageno, entryno, *shmDistributedXidMapHighestPageNo);
	
	LWLockAcquire(DistributedXidMapControlLock, LW_EXCLUSIVE);

	if (pageno > *shmDistributedXidMapHighestPageNo)
	{
		/*
		 * Zero out the new page(s).
		 */
		DistributedXidMapMakeMorePages(pageno);
	}

	if (*shmMaxDistributedXid < gxid)
	{
		*shmMaxDistributedXid = gxid;
	}

	slotno = SimpleLruReadPage(DistributedXidMapCtl, pageno, InvalidTransactionId);
	ptr = (DISTRIBUTEDXIDMAP_ENTRY *) DistributedXidMapCtl->shared->page_buffer[slotno];
	ptr += entryno;

	if (ptr->state == DISTRIBUTEDXIDMAP_STATE_NONE)
	{
		/*
		 * Need to allocate a local XID and assign the map entry.
		 */
		*xid = GetNewTransactionId(false, false);	// NOT subtrans, DO NOT Set PROC struct xid
		ptr->state = DISTRIBUTEDXIDMAP_STATE_IN_PROGRESS;
		ptr->pid = MyProcPid;
		ptr->xid = *xid;
		
		elog((Debug_print_full_dtm ? LOG : DEBUG5), "AllocOrGetLocalXidForStartDistributedTransaction allocated local XID = %d", *xid);

		DistributedXidMapCtl->shared->page_dirty[slotno] = true;
	}
	else if (ptr->state == DISTRIBUTEDXIDMAP_STATE_PREALLOC_FOR_OPEN_TRANS)
	{
		/*
		 * The local XID was pre-allocated by another QE when it
		 * received a distributed snapshot that listed the distributed transaction
		 * in its in-doubt list.
		 */
		ptr->state = DISTRIBUTEDXIDMAP_STATE_IN_PROGRESS;
		ptr->pid = MyProcPid;
		*xid = ptr->xid;
		elog((Debug_print_full_dtm ? LOG : DEBUG5), "AllocOrGetLocalXidForStartDistributedTransaction found pre-allocated local XID = %d", *xid);

		DistributedXidMapCtl->shared->page_dirty[slotno] = true;
	}
	else
	{
		int pid = ptr->pid;
		TransactionId reuseXid = ptr->xid;
		DistributedMapState state = ptr->state;
		LWLockRelease(DistributedXidMapControlLock);

		elog(ERROR,"Attempting re-use local xid %u and distributed xid %u again (original start pid was %d, state = %s, pageno %d, entryno %d, slotno %d)",
			 reuseXid, gxid, pid, DistributedMapStateToString(state), pageno, entryno, slotno);
	}

	SetProcXid(*xid, gxid);

	LWLockRelease(DistributedXidMapControlLock);
}

/*
 * Add local XID for any new distributed transactions.
 */
void
PreallocLocalXidsForOpenDistributedTransactions(DistributedTransactionId *gxidArray, uint32 count)
{
	int							i;
	DistributedTransactionId 	gxid;
	int							pageno;
	int							entryno;
	int							slotno;
	DISTRIBUTEDXIDMAP_ENTRY		*ptr;

	LWLockAcquire(DistributedXidMapControlLock, LW_EXCLUSIVE);
	
	for (i = 0; i < count; i++)
	{
		gxid = gxidArray[i];
		
		pageno = DistributedTransactionIdToPage(gxid);
		entryno = DistributedTransactionIdToEntry(gxid);

		elog((Debug_print_full_dtm ? LOG : DEBUG5), "PreallocLocalXidsForOpenDistributedTransactions: gxidArray[%d] is %u (pageno %d, entryno %d)",
			 i, gxid, pageno, entryno);

		if (pageno > *shmDistributedXidMapHighestPageNo)
		{
			/*
			 * Zero out the new page(s).
			 */
			DistributedXidMapMakeMorePages(pageno);
		}
		
		if (*shmMaxDistributedXid < gxid)
		{
			*shmMaxDistributedXid = gxid;
		}
		
		slotno = SimpleLruReadPage(DistributedXidMapCtl, pageno, InvalidTransactionId);
		ptr = (DISTRIBUTEDXIDMAP_ENTRY *) DistributedXidMapCtl->shared->page_buffer[slotno];
		ptr += entryno;

		if (ptr->state == DISTRIBUTEDXIDMAP_STATE_NONE)
		{
			ptr->state = DISTRIBUTEDXIDMAP_STATE_PREALLOC_FOR_OPEN_TRANS;
			ptr->pid = MyProcPid;
			ptr->xid = GetNewTransactionId(false, false);	// NOT subtrans, DO NOT Set PROC struct xid
			DistributedXidMapCtl->shared->page_dirty[slotno] = true;
			elog((Debug_print_full_dtm ? LOG : DEBUG5), "PreallocLocalXidsForOpenDistributedTransactions: Allocated local XID = %u for global XID = %u",
				 ptr->xid, gxid);
		}
	}
	
	LWLockRelease(DistributedXidMapControlLock);
}

/*
 * Get the local XID associated with a distributed transaction.
 */
TransactionId
GetLocalXidForDistributedTransaction(DistributedTransactionId gxid)
{
	int			pageno = DistributedTransactionIdToPage(gxid);
	int			entryno = DistributedTransactionIdToEntry(gxid);
	int			slotno;
	DISTRIBUTEDXIDMAP_ENTRY *ptr;
	TransactionId xid;
	DistributedMapState state;

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "Entering GetLocalXidForDistributedTransaction with distributed xid = %d (pageno = %d, entryno = %d, DistributedXidMapHighestPageNo = %d)",
		 gxid, pageno, entryno, *shmDistributedXidMapHighestPageNo);
	
	LWLockAcquire(DistributedXidMapControlLock, LW_EXCLUSIVE);

	if (pageno > *shmDistributedXidMapHighestPageNo)
	{
		LWLockRelease(DistributedXidMapControlLock);
		
		elog((Debug_print_full_dtm ? LOG : DEBUG5), "GetLocalXidForDistributedTransaction returning InvalidTransactionId for local xid for distributed xid = %d (pageno = %d, entryno = %d, DistributedXidMapHighestPageNo = %d)",
			 gxid, pageno, entryno, *shmDistributedXidMapHighestPageNo);
		return InvalidTransactionId;
	}

	slotno = SimpleLruReadPage(DistributedXidMapCtl, pageno, InvalidTransactionId);
	ptr = (DISTRIBUTEDXIDMAP_ENTRY *) DistributedXidMapCtl->shared->page_buffer[slotno];
	ptr += entryno;

	xid = ptr->xid;
	state = ptr->state;

	LWLockRelease(DistributedXidMapControlLock);

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "GetLocalXidForDistributedTransaction found local xid = %u for distributed xid = %d in state %s (pageno = %d, entryno = %d, DistributedXidMapHighestPageNo = %d)",
		 xid, gxid, DistributedMapStateToString(state), pageno, entryno, *shmDistributedXidMapHighestPageNo);
	return xid;
}

void
UpdateDistributedXidMapState(DistributedTransactionId gxid, DistributedMapState newState)
{
	int			pageno = DistributedTransactionIdToPage(gxid);
	int			entryno = DistributedTransactionIdToEntry(gxid);
	int			slotno;
	DISTRIBUTEDXIDMAP_ENTRY *ptr;
	DistributedMapState oldState;
	TransactionId xid;

	Assert(newState == DISTRIBUTEDXIDMAP_STATE_PREPARED ||
		   newState == DISTRIBUTEDXIDMAP_STATE_COMMITTED ||
		   newState == DISTRIBUTEDXIDMAP_STATE_ABORTED);

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "Entering UpdateDistributedXidMapState with distributed xid = %d (pageno = %d, entryno = %d, DistributedXidMapHighestPageNo = %d)",
		 gxid, pageno, entryno, *shmDistributedXidMapHighestPageNo);
	
	LWLockAcquire(DistributedXidMapControlLock, LW_EXCLUSIVE);

	if (pageno > *shmDistributedXidMapHighestPageNo)
	{
		LWLockRelease(DistributedXidMapControlLock);
		
		elog(ERROR, "UpdateDistributedXidMapState distributed xid = %d invalid (pageno = %d, entryno = %d, DistributedXidMapHighestPageNo = %d)",
			 gxid, pageno, entryno, *shmDistributedXidMapHighestPageNo);
	}

	slotno = SimpleLruReadPage(DistributedXidMapCtl, pageno, InvalidTransactionId);
	ptr = (DISTRIBUTEDXIDMAP_ENTRY *) DistributedXidMapCtl->shared->page_buffer[slotno];
	ptr += entryno;

	oldState = ptr->state;
	if (oldState == DISTRIBUTEDXIDMAP_STATE_IN_PROGRESS)
	{
		ptr->state = newState;
	}
	else if (oldState == DISTRIBUTEDXIDMAP_STATE_PREPARED)
	{
		Assert(newState != DISTRIBUTEDXIDMAP_STATE_PREPARED);
		ptr->state = newState;
	}
	xid = ptr->xid;
	DistributedXidMapCtl->shared->page_dirty[slotno] = true;
	LWLockRelease(DistributedXidMapControlLock);

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "UpdateDistributedXidMapState state for local xid = %u and distributed xid = %d from state %s to new state %s (pageno = %d, entryno = %d, DistributedXidMapHighestPageNo = %d)",
		 xid, gxid, DistributedMapStateToString(oldState), DistributedMapStateToString(newState),pageno, entryno, *shmDistributedXidMapHighestPageNo);
}

void 
RecordMaxDistributedXid(DistributedTransactionId gxid)
{
	LWLockAcquire(DistributedXidMapControlLock, LW_EXCLUSIVE);

	if (*shmMaxDistributedXid < gxid)
	{
		*shmMaxDistributedXid = gxid;
	}
	LWLockRelease(DistributedXidMapControlLock);
}

/*
 * Initialization of shared memory for DistributedXidMap SLRU
 */
Size
DistributedXidMapShmemSize_SLru(void)
{
	return SimpleLruShmemSize(NUM_DISTRIBUTEDXIDMAP_BUFFERS);
}

/*
 * Initialization of shared memory for DistributedXidMap shared memory.
 */
Size
DistributedXidMapShmemSize(void)
{
	return MAXALIGN(sizeof(DISTRIBUTEDXIDMAP_SHARED));
}

void
DistributedXidMapShmemInit_SLru(void)
{
	DistributedXidMapCtl->PagePrecedes = DistributedXidMapPagePrecedes;
	SimpleLruInit(DistributedXidMapCtl, "DistributedXidMap Ctl", NUM_DISTRIBUTEDXIDMAP_BUFFERS,
				  DistributedXidMapControlLock, DISTRIBUTEDXIDMAP_DIR);
	/* Override default assumption that writes should be fsync'd */
	DistributedXidMapCtl->do_fsync = false;
}

void
DistributedXidMapShmemInit(void)
{
	bool		found;
	DISTRIBUTEDXIDMAP_SHARED *shared;
	
	shared = (DISTRIBUTEDXIDMAP_SHARED *) ShmemInitStruct("DistributedXidMap Shared", DistributedXidMapShmemSize(), &found);
	if (!shared)
		elog(FATAL, "Could not initialize Distributed XIP Map shared memory");

	if (!found)
	{
		MemSet(shared, 0, sizeof(DISTRIBUTEDXIDMAP_SHARED));
	}
	shmDistributedXidMapHighestPageNo = &shared->DistributedXidMapHighestPageNo;
	shmMaxDistributedXid = &shared->MaxDistributedXid;
}

/*
 * This func must be called ONCE on system install.  It verifies
 * the DistributedXidMap directory exists.  (The DistributedXidMap directory is assumed to
 * have been created by the initdb shell script, and DistributedXidMapShmemInit
 * must have been called already.)
 */
void
BootStrapDistributedXidMap(void)
{
	LWLockAcquire(DistributedXidMapControlLock, LW_EXCLUSIVE);

//  SimpleLruVerifyDirExists(DistributedXidMapCtl);

	LWLockRelease(DistributedXidMapControlLock);
}

/*
 * Initialize (or reinitialize) a page of DistributedXidMap to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroDistributedXidMapPage(int pageno)
{
	return SimpleLruZeroPage(DistributedXidMapCtl, pageno);
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
void
StartupDistributedXidMap(void)
{
	/*
	 * Since we don't expect pg_distributedxidmap to be valid across crashes, we
	 * delete all segments.
	 */
	LWLockAcquire(DistributedXidMapControlLock, LW_EXCLUSIVE);

//	SimpleLruTruncate(DistributedXidMapCtl, 0, false);

	/* Zero the first page */
	ZeroDistributedXidMapPage(0);

	LWLockRelease(DistributedXidMapControlLock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownDistributedXidMap(void)
{
	/*
	 * Flush dirty DistributedXidMap pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely as a debugging aid.
	 */
	SimpleLruFlush(DistributedXidMapCtl, false);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointDistributedXidMap(void)
{
	/*
	 * Flush dirty DistributedXidMap pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 */
	SimpleLruFlush(DistributedXidMapCtl, true);
}


/*
 * Remove all DistributedXidMap segments before the one holding the passed
 * DTM Global XID.
 *
 * This is normally called when a distributed snapshot arrives indicating the
 * xmin of all current distributed snapshots has moved forward.
 */
void
TruncateDistributedXidMap(DistributedTransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = DistributedTransactionIdToPage(oldestXact);

	// UNDONE: Save cutoff page and don't call SimpleLruTruncate when there
	// is no change to the cuttoff page. 

	SimpleLruTruncate(DistributedXidMapCtl, cutoffPage, false);
}


/*
 * Decide which of two DistributedXidMap page numbers is "older" for truncation
 * purposes.
 *
 */
static bool
DistributedXidMapPagePrecedes(int page1, int page2)
{
	return (page1 < page2);
}


