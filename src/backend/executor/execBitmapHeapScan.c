/*
 * execHeapScan.c
 *   Support routines for scanning Heap tables.
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Copyright (c) 2014 Pivotal, Inc.
 */
#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "executor/execdebug.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "cdb/cdbvars.h" /* gp_select_invisible */
#include "cdb/cdbfilerepprimary.h"
#include "nodes/tidbitmap.h"

static void bitgetpage(HeapScanDesc scan, TBMIterateResult *tbmres);

/*
 * Prepares for a new heap scan.
 */
void
BitmapHeapScanBegin(ScanState *scanState)
{
	BitmapTableScanState *node = (BitmapTableScanState *)scanState;
	Relation currentRelation = node->ss.ss_currentRelation;
	EState *estate = node->ss.ps.state;

	Assert(node->scanDesc == NULL);

	/*
	 * Even though we aren't going to do a conventional seqscan, it is useful
	 * to create a HeapScanDesc --- this checks the relation size and sets up
	 * statistical infrastructure for us.
	 */
	node->scanDesc = heap_beginscan(currentRelation,
												   estate->es_snapshot,
												   0,
												   NULL);

	/*
	 * One problem is that heap_beginscan counts a "sequential scan" start,
	 * when we actually aren't doing any such thing.  Reverse out the added
	 * scan count.	(Eventually we may want to count bitmap scans separately.)
	 */
	pgstat_discount_heap_scan(currentRelation);

	/*
	 * Heap always needs rechecking each tuple because of potential
	 * visibility issue (we don't store MVCC info in the index).
	 */
	node->recheckTuples = true;
}

/*
 * Cleans up after the scanning is done.
 */
void
BitmapHeapScanEnd(ScanState *scanState)
{
	BitmapTableScanState *node = (BitmapTableScanState *)scanState;

	/*
	 * We might call "End" method before even calling init method,
	 * in case we had an ERROR. Ignore scanDesc cleanup in such cases
	 */
	if (NULL != node->scanDesc)
	{
		heap_endscan((HeapScanDesc)node->scanDesc);
		node->scanDesc = NULL;
	}

	if (NULL != node->iterator)
	{
		pfree(node->iterator);
		node->iterator = NULL;
	}
}

/*
 * Returns the next matching tuple.
 */
TupleTableSlot *
BitmapHeapScanNext(ScanState *scanState)
{
	BitmapTableScanState *node = (BitmapTableScanState *)scanState;
	Assert((node->ss.scan_state & SCAN_SCAN) != 0);

	/*
	 * extract necessary information from index scan node
	 */
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	HeapScanDesc scan = node->scanDesc;

	TBMIterateResult *tbmres = (TBMIterateResult *)node->tbmres;
	Assert(tbmres != NULL && tbmres->ntuples != 0);
	Assert(node->needNewBitmapPage == false);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (node->iterator == NULL)
		{
			/*
			 * Fetch the current heap page and identify candidate tuples.
			 */
			bitgetpage(scan, tbmres);

			/*
		 	* Set rs_cindex to first slot to examine
		 	*/
			scan->rs_cindex = 0;
			/*
			 * The nullity of the iterator is used to check if
			 * we need a new iterator to process a new bitmap page.
			 * Note: the bitmap page is provided by BitmapTableScan.
			 * This iterator is supposed to maintain the cursor position
			 * in the heap page that it is scanning. However, for heap
			 * tables we already have such cursor state as part of ScanState,
			 * and so, we just use a dummy allocation here to indicate
			 * ourselves that we have finished initialization for processing
			 * a new bitmap page.
			 */
			node->iterator = palloc(0);
		}
		else
		{
			/*
			 * Continuing in previously obtained page; advance rs_cindex
			 */
			scan->rs_cindex++;
		}

		/*
		 * If we reach the end of the relation or if we are out of range or
		 * nothing more to look at on this page, then request a new bitmap page.
		 */
		if (tbmres->blockno >= scan->rs_nblocks || scan->rs_cindex < 0 ||
				scan->rs_cindex >= scan->rs_ntuples)
		{
			Assert(NULL != node->iterator);
			pfree(node->iterator);
			node->iterator = NULL;

			node->needNewBitmapPage = true;
			return ExecClearTuple(slot);
		}

		/*
		 * Okay to fetch the tuple
		 */
		OffsetNumber targoffset = scan->rs_vistuples[scan->rs_cindex];
		Page		dp = (Page) BufferGetPage(scan->rs_cbuf);
		ItemId		lp = PageGetItemId(dp, targoffset);
		Assert(ItemIdIsUsed(lp));

		scan->rs_ctup.t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
		scan->rs_ctup.t_len = ItemIdGetLength(lp);
		ItemPointerSet(&scan->rs_ctup.t_self, tbmres->blockno, targoffset);

		pgstat_count_heap_fetch(scan->rs_rd);

		/*
		 * Set up the result slot to point to this tuple. Note that the slot
		 * acquires a pin on the buffer.
		 */
		ExecStoreHeapTuple(&scan->rs_ctup,
					   slot,
					   scan->rs_cbuf,
					   false);

		if (!BitmapTableScanRecheckTuple(node, slot))
		{
			ExecClearTuple(slot);
			continue;
		}

		return slot;
	}

	/*
	 * We should never reach here as the termination is handled
	 * from nodeBitmapTableScan.
	 */
	Assert(false);
	return NULL;
}

/*
 * Prepares for a re-scan.
 */
void
BitmapHeapScanReScan(ScanState *scanState)
{
	BitmapTableScanState *node = (BitmapTableScanState *)scanState;
	Assert(node->scanDesc != NULL);

	/* rescan to release any page pin */
	heap_rescan(node->scanDesc, NULL);
	/* undo bogus "seq scan" count (see notes in ExecInitBitmapHeapScan) */
	pgstat_discount_heap_scan(node->ss.ss_currentRelation);
}

/*
 * This routine reads and pins the specified page of the relation, then
 * builds an array indicating which tuples on the page are both potentially
 * interesting according to the bitmap, and visible according to the snapshot.
 */
static void
bitgetpage(HeapScanDesc scan, TBMIterateResult *tbmres)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	BlockNumber page = tbmres->blockno;
	Buffer		buffer;
	Snapshot	snapshot;
	Page		dp;
	int			ntup;
	int			curslot;
	int			minslot;
	int			maxslot;
	int			maxoff;

	/*
	 * Acquire pin on the target heap page, trading in any pin we held before.
	 */
	Assert(page < scan->rs_nblocks);

	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;

	scan->rs_cbuf = ReleaseAndReadBuffer(scan->rs_cbuf,
										 scan->rs_rd,
										 page);

	buffer = scan->rs_cbuf;
	snapshot = scan->rs_snapshot;

	/*
	 * We must hold share lock on the buffer content while examining tuple
	 * visibility.	Afterwards, however, the tuples we have found to be
	 * visible are guaranteed good as long as we hold the buffer pin.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = (Page) BufferGetPage(buffer);
	maxoff = PageGetMaxOffsetNumber(dp);

	/*
	 * Determine how many entries we need to look at on this page. If the
	 * bitmap is lossy then we need to look at each physical item pointer;
	 * otherwise we just look through the offsets listed in tbmres.
	 */
	if (tbmres->ntuples >= 0)
	{
		/* non-lossy case */
		minslot = 0;
		maxslot = tbmres->ntuples - 1;
	}
	else
	{
		/* lossy case */
		minslot = FirstOffsetNumber;
		maxslot = maxoff;
	}

	ntup = 0;
	for (curslot = minslot; curslot <= maxslot; curslot++)
	{
		OffsetNumber targoffset;
		ItemId		lp;
		HeapTupleData loctup;
		bool		valid;

		if (tbmres->ntuples >= 0)
		{
			/* non-lossy case */
			targoffset = tbmres->offsets[curslot];
		}
		else
		{
			/* lossy case */
			targoffset = (OffsetNumber) curslot;
		}

		/*
		 * We'd better check for out-of-range offnum in case of VACUUM since
		 * the TID was obtained.
		 */
		if (targoffset < FirstOffsetNumber || targoffset > maxoff)
			continue;

		lp = PageGetItemId(dp, targoffset);

		/*
		 * Must check for deleted tuple.
		 */
		if (!ItemIdIsUsed(lp))
			continue;

		/*
		 * check time qualification of tuple, remember it if valid
		 */
		loctup.t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
		loctup.t_len = ItemIdGetLength(lp);
		ItemPointerSet(&(loctup.t_self), page, targoffset);

		valid = HeapTupleSatisfiesVisibility(scan->rs_rd, &loctup, snapshot, buffer);
		if (valid)
			scan->rs_vistuples[ntup++] = targoffset;
	}

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------

	Assert(ntup <= MaxHeapTuplesPerPage);
	scan->rs_ntuples = ntup;
}

