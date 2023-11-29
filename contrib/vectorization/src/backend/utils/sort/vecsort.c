
/*--------------------------------------------------------------------
 * vecsort.h
 *	  Generalized tuple sorting routines.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/utils/sort/vecsort.c
 *
 *--------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_collation.h"
#include "utils/tuplesort.h"
#include "nodes/execnodes.h"
#include "storage/buffile.h"
#include "utils/logtape.h"
#include "utils/palloc.h"

#include "utils/pg_locale.h"
#include "utils/syscache.h"
#include "utils/vecsort.h"
#include "utils/arrow.h"
#include "utils/tuptable_vec.h"
#include "vecexecutor/execslot.h"

typedef enum
{
	TSS_INITIAL,				/* Loading tuples; still within memory limit */
	TSS_BOUNDED,				/* Loading tuples into bounded-size heap */
	TSS_BUILDRUNS,				/* Loading tuples; writing to tape */
	TSS_SORTEDINMEM,			/* Sort completed entirely in memory */
	TSS_SORTEDONTAPE,			/* Sort completed, final run is on tape */
	TSS_FINALMERGE				/* Performing final merge on-the-fly */
} TupSortStatus;

#define LACKMEM(state)		((state)->availMem < 0 && !(state)->slabAllocatorUsed)
#define USEMEM(state,amt)	((state)->availMem -= (amt))
#define WORKER(state)		((state)->shared && (state)->worker != -1)
#define LEADER(state)		((state)->shared && (state)->worker == -1)


/*
 * Override SortTuple for Arrow arrays.
 */
typedef struct SortBatch
{
	GArrowRecordBatch   *batch;	/* the sorting batch */
	GArrowRecordBatch   *keybatch;	/* the sorting keys */
	GArrowUInt64Array *sorted_indices; /* sorted indices batch*/
	GArrowArray	*array1;	    /* value of first key column */
	int			 tupindex;		/* see notes for SortTuple in tuplesort.c */
} SortBatch;


struct VecTuplesortstate
{
	TupSortStatus status;		/* enumerated value as shown above */
	int			nKeys;			/* number of columns in sort key */
	bool		randomAccess;	/* did caller request random access? */
	bool		bounded;		/* did caller specify a maximum number of
								 * tuples to return? */
	bool		boundUsed;		/* true if we made use of a bounded heap */
	int			bound;			/* if bounded, the maximum number of tuples */
	bool		tuples;			/* Can SortTuple.tuple ever be set? */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	int			maxTapes;		/* number of tapes (Knuth's T) */
	int			tapeRange;		/* maxTapes-1 (Knuth's P) */
	int64		maxSpace;		/* maximum amount of space occupied among sort
								 * of groups, either in-memory or on-disk */
	bool		isMaxSpaceDisk; /* true when maxSpace is value for on-disk
								 * space, false when it's value for in-memory
								 * space */
	TupSortStatus maxSpaceStatus;	/* sort status when maxSpace was reached */
	MemoryContext maincontext;	/* memory context for tuple sort metadata that
								 * persists across multiple batches */
	MemoryContext sortcontext;	/* memory context holding most sort data */
	MemoryContext tuplecontext; /* sub-context of sortcontext for tuple data */
	LogicalTapeSet *tapeset;	/* logtape.c object for tapes in a temp file */

	/*
	 * This array holds the tuples now in sort memory.  If we are in state
	 * INITIAL, the tuples are in no particular order; if we are in state
	 * SORTEDINMEM, the tuples are in final sorted order; in states BUILDRUNS
	 * and FINALMERGE, the tuples are organized in "heap" order per Algorithm
	 * H.  In state SORTEDONTAPE, the array is not used.
	 */
	SortBatch  *membatchs;		/* array of SortBatch structs */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/*
	 * Memory for tuples is sometimes allocated using a simple slab allocator,
	 * rather than with palloc().  Currently, we switch to slab allocation
	 * when we start merging.  Merging only needs to keep a small, fixed
	 * number of tuples in memory at any time, so we can avoid the
	 * palloc/pfree overhead by recycling a fixed number of fixed-size slots
	 * to hold the tuples.
	 *
	 * For the slab, we use one large allocation, divided into SLAB_SLOT_SIZE
	 * slots.  The allocation is sized to have one slot per tape, plus one
	 * additional slot.  We need that many slots to hold all the tuples kept
	 * in the heap during merge, plus the one we have last returned from the
	 * sort, with tuplesort_gettuple.
	 *
	 * Initially, all the slots are kept in a linked list of free slots.  When
	 * a tuple is read from a tape, it is put to the next available slot, if
	 * it fits.  If the tuple is larger than SLAB_SLOT_SIZE, it is palloc'd
	 * instead.
	 *
	 * When we're done processing a tuple, we return the slot back to the free
	 * list, or pfree() if it was palloc'd.  We know that a tuple was
	 * allocated from the slab, if its pointer value is between
	 * slabMemoryBegin and -End.
	 *
	 * When the slab allocator is used, the USEMEM/LACKMEM mechanism of
	 * tracking memory usage is not used.
	 */
	bool		slabAllocatorUsed;

	/*
	 * While building initial runs, this is the current output run number.
	 * Afterwards, it is the number of initial runs we made.
	 */
	int			currentRun;

	/*
	 * These variables are used after completion of sorting to keep track of
	 * the next tuple to return.  (In the tape case, the tape's current read
	 * position is also critical state.)
	 */
	int			result_tape;	/* actual tape number of finished output */
	int			current;		/* array index (only used if SORTEDINMEM) */
	bool		eof_reached;	/* reached EOF (needed for cursors) */

	/* markpos_xxx holds marked position for mark and restore */
	long		markpos_block;	/* tape block# (only used if SORTEDONTAPE) */
	int			markpos_offset; /* saved "current", or offset in tape block */
	bool		markpos_eof;	/* saved "eof_reached" */

	/*
	 * These variables are used during parallel sorting.
	 *
	 * worker is our worker identifier.  Follows the general convention that
	 * -1 value relates to a leader tuplesort, and values >= 0 worker
	 * tuplesorts. (-1 can also be a serial tuplesort.)
	 *
	 * shared is mutable shared memory state, which is used to coordinate
	 * parallel sorts.
	 *
	 * nParticipants is the number of worker Tuplesortstates known by the
	 * leader to have actually been launched, which implies that they must
	 * finish a run leader can merge.  Typically includes a worker state held
	 * by the leader process itself.  Set in the leader Tuplesortstate only.
	 */
	int			worker;
	Sharedsort *shared;
	int			nParticipants;

	/*
	 * The sortKeys variable is used by every case other than the hash index
	 * case; it is set by tuplesort_begin_xxx.  tupDesc is only used by the
	 * MinimalTuple and CLUSTER routines, though.
	 */
	TupleDesc	tupDesc;
	SortSupport sortKeys;		/* array of length nKeys */

	/*
	 * This variable is shared by the single-key MinimalTuple case and the
	 * Datum case (which both use qsort_ssup()).  Otherwise it's NULL.
	 */
	SortSupport onlyKey;

	/*
	 * Additional state for managing "abbreviated key" sortsupport routines
	 * (which currently may be used by all cases except the hash index case).
	 * Tracks the intervals at which the optimization's effectiveness is
	 * tested.
	 */
	int64		abbrevNext;		/* Tuple # at which to next check
								 * applicability */
	EState	   *estate;			/* for evaluating index expressions */

	/* -------- vector sort state ----------*/
	GList *vs_reslist;   /*results indices list, GArrowUInt64Array * type */
	GArrowChunkedArray *vs_resindices; /*the final sorted indices for all*/
	GArrowFunction *vs_function;
	GArrowExecuteContext *vs_ectx;

	GArrowArray *sorted_indices; /* sorted indices batch*/
	GArrowArray **arrays;
	GArrowTable *array_table;

	int64 rowcnt;
	int64 offset;
	/* disk workfile */
	BufFile **batch_record_files;
	int BufFileCnt;
	int BufFileCur;
	int *read_size;

	/* max vectorized tuple rows in memory */
	int64 max_mem_vec_rows;

	VecTupSortStatus vtss;
	bool  isWorkfile;
	int   spill_count;
	int64 sent_tuples;

	void *plan; /* GArrowExecutePlan* */
	void *sinkreader; /* *GArrowRecordBatchReader* */
	SortGetNextBatch getnext;
};

static GArrowSortEncoding
get_arrow_locale_from_collation(Oid collid)
{
	HeapTuple       tp;
	Form_pg_collation collform;
	const char *collcollate;

	/* Callers must pass a valid OID */
	if (!OidIsValid(collid))
		return GARROW_SORT_ORDER_Default;

	/* Return GARROW_SORT_ORDER_Default for "default" collation */
	if (collid == DEFAULT_COLLATION_OID)
	{
		const char *localeptr;
		localeptr = setlocale(LC_COLLATE, NULL);
		if (!localeptr)
			elog(ERROR, "invalid LC_COLLATE setting");
		if (strcmp(localeptr, "C") == 0)
			return GARROW_SORT_ORDER_Default;
		else if(strcasecmp(localeptr, "en_US.utf8") == 0 || strcasecmp(localeptr, "en_US.UTF-8") == 0)
			return GARROW_SORT_ORDER_En_US_UTF8;
		/* The correct fallback does not trigger the logic here.*/
		else
			elog(ERROR, "not support this collation %u", collid);
	}
	else
	{
		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation %u", collid);

		collform = (Form_pg_collation) GETSTRUCT(tp);
		collcollate = NameStr(collform->collcollate);

		if(strcasecmp(collcollate, "en_US.utf8") == 0)
		{
			ReleaseSysCache(tp);
			return GARROW_SORT_ORDER_En_US_UTF8;
		}
	}
	ReleaseSysCache(tp);
	return GARROW_SORT_ORDER_Default;
}

GList *
create_sort_keys(SortSupport ssup,
				 int nkeys,
				 GArrowSchema *schema)
{
	GList *lkey = NULL;
	GArrowSortOrder order;
	GArrowSortEncoding encoding;
	const char *attr_name = NULL;
	int attr = 0;
	int i = 0;

	for (i = 0; i < nkeys; i++)
	{
		GArrowSortNullPlacement null_placement = GARROW_SORT_ORDER_AT_END;
		SortSupport sortKey = ssup + i;
		g_autoptr(GArrowField) field = NULL;
		g_autoptr(GArrowSortKey) key = NULL;

		attr = sortKey->ssup_attno - 1;

		if (sortKey->ssup_reverse)
			order = GARROW_SORT_ORDER_DESCENDING;
		else
			order = GARROW_SORT_ORDER_ASCENDING;
		encoding = get_arrow_locale_from_collation(sortKey->ssup_collation);
		garrow_store_func(field, garrow_schema_get_field(schema, attr));

		attr_name = garrow_field_get_name(field);

		if (sortKey->ssup_nulls_first) 
			null_placement = GARROW_SORT_ORDER_AT_START;

		garrow_store_func(key, garrow_sort_key_new(attr_name, order, encoding, null_placement));

		lkey = garrow_list_append_ptr(lkey, key);
	}

	return lkey;
}

/*
 * tuplesort_updatemax
 *
 *	Update maximum resource usage statistics.
 */
static void
tuplesort_updatemax(VecTuplesortstate *state)
{
	int64		spaceUsed;
	bool		isSpaceDisk;

	/*
	 * Note: it might seem we should provide both memory and disk usage for a
	 * disk-based sort.  However, the current code doesn't track memory space
	 * accurately once we have begun to return tuples to the caller (since we
	 * don't account for pfree's the caller is expected to do), so we cannot
	 * rely on availMem in a disk sort.  This does not seem worth the overhead
	 * to fix.  Is it worth creating an API for the memory context code to
	 * tell us how much is actually used in sortcontext?
	 */
	if (state->tapeset)
	{
		isSpaceDisk = true;
		spaceUsed = LogicalTapeSetBlocks(state->tapeset) * BLCKSZ;
	}
	else
	{
		isSpaceDisk = false;
		spaceUsed = state->allowedMem - state->availMem;
	}

	/*
	 * Sort evicts data to the disk when it wasn't able to fit that data into
	 * main memory.  This is why we assume space used on the disk to be more
	 * important for tracking resource usage than space used in memory. Note
	 * that the amount of space occupied by some tupleset on the disk might be
	 * less than amount of space occupied by the same tupleset in memory due
	 * to more compact representation.
	 */
	if ((isSpaceDisk && !state->isMaxSpaceDisk) ||
		(isSpaceDisk == state->isMaxSpaceDisk && spaceUsed > state->maxSpace))
	{
		state->maxSpace = spaceUsed;
		state->isMaxSpaceDisk = isSpaceDisk;
		state->maxSpaceStatus = state->status;
	}
}

/*
 * vecsort_get_stats - extract summary statistics
 *
 * This can be called after tuplesort_performsort() finishes to obtain
 * printable summary information about how the sort was performed.
 */
void 
vecsort_get_stats(VecTuplesortstate *state,
				 TuplesortInstrumentation *stats)
{
	/*
	 * Note: it might seem we should provide both memory and disk usage for a
	 * disk-based sort.  However, the current code doesn't track memory space
	 * accurately once we have begun to return tuples to the caller (since we
	 * don't account for pfree's the caller is expected to do), so we cannot
	 * rely on availMem in a disk sort.  This does not seem worth the overhead
	 * to fix.  Is it worth creating an API for the memory context code to
	 * tell us how much is actually used in sortcontext?
	 *
	 * GPDB: we have such accounting in memory contexts, so we store the
	 * memory usage in 'workmemused'.
	 */
	tuplesort_updatemax(state);

	if (state->isMaxSpaceDisk)
		stats->spaceType = SORT_SPACE_TYPE_DISK;
	else
		stats->spaceType = SORT_SPACE_TYPE_MEMORY;
	stats->spaceUsed = (state->maxSpace + 1023) / 1024;
	stats->workmemused = MemoryContextGetPeakSpace(state->sortcontext);

	switch (state->maxSpaceStatus)
	{
		case TSS_SORTEDINMEM:
			if (state->boundUsed)
				stats->sortMethod = SORT_TYPE_TOP_N_HEAPSORT;
			else
				stats->sortMethod = SORT_TYPE_QUICKSORT;
			break;
		case TSS_SORTEDONTAPE:
			stats->sortMethod = SORT_TYPE_EXTERNAL_SORT;
			break;
		case TSS_FINALMERGE:
			stats->sortMethod = SORT_TYPE_EXTERNAL_MERGE;
			break;
		default:
			stats->sortMethod = SORT_TYPE_STILL_IN_PROGRESS;
			break;
	}
}
