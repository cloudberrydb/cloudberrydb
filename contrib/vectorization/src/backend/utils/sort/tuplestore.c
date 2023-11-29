/*-------------------------------------------------------------------------
 *
 * tuplestore.c
 *	  Generalized routines for temporary tuple storage.
 *
 * This module handles temporary storage of tuples for purposes such
 * as Materialize nodes, hashjoin batch files, etc.  It is essentially
 * a dumbed-down version of tuplesort.c; it does no sorting of tuples
 * but can only store and regurgitate a sequence of tuples.  However,
 * because no sort is required, it is allowed to start reading the sequence
 * before it has all been written.  This is particularly useful for cursors,
 * because it allows random access within the already-scanned portion of
 * a query without having to process the underlying scan to completion.
 * Also, it is possible to support multiple independent read pointers.
 *
 * A temporary file is used to handle the data if it exceeds the
 * space limit specified by the caller.
 *
 * The (approximate) amount of memory allowed to the tuplestore is specified
 * in kilobytes by the caller.  We absorb tuples and simply store them in an
 * in-memory array as long as we haven't exceeded maxKBytes.  If we do exceed
 * maxKBytes, we dump all the tuples into a temp file and then read from that
 * when needed.
 *
 * Upon creation, a tuplestore supports a single read pointer, numbered 0.
 * Additional read pointers can be created using tuplestore_alloc_read_pointer.
 * Mark/restore behavior is supported by copying read pointers.
 *
 * When the caller requests backward-scan capability, we write the temp file
 * in a format that allows either forward or backward scan.  Otherwise, only
 * forward scan is allowed.  A request for backward scan must be made before
 * putting any tuples into the tuplestore.  Rewind is normally allowed but
 * can be turned off via tuplestore_set_eflags; turning off rewind for all
 * read pointers enables truncation of the tuplestore at the oldest read point
 * for minimal memory usage.  (The caller must explicitly call tuplestore_trim
 * at appropriate times for truncation to actually happen.)
 *
 * Note: in TSS_WRITEFILE state, the temp file's seek position is the
 * current write position, and the write-position variables in the tuplestore
 * aren't kept up to date.  Similarly, in TSS_READFILE state the temp file's
 * seek position is the active read pointer's position, and that read pointer
 * isn't kept up to date.  We update the appropriate variables using ftell()
 * before switching to the other state or activating a different read pointer.
 *
 *
 * Cloudberry changes
 * -----------------
 *
 * In Cloudberry, tuplestores have one extra capability: a tuplestore can
 * be created and filled in one process, and opened for reading in another
 * process. To do this, call tuplestore_make_shared() immediately
 * after creating the tuplestore, in the writer process. Then populate the
 * tuplestore as usual, by calling tuplestore_puttupleslot(). When you're
 * finished writing to it, call tuplestore_freeze(). tuplestore_freeze()
 * flushes all the tuples to the file. No new rows may be added after
 * freezing it.
 *
 * After freezing, you can open the tupletore for reading in the other
 * process by calling tuplestore_open_shared(). It may be opened for reading
 * as many times as you want, in different processes, until it is destroyed
 * by the original writer process by calling tuplestore_end().
 *
 * Note that tuplestore doesn't do any synchronization across processes!
 * It is up to the calling code to do the freezing, opening for reading, and
 * destroying the tuplestore in the right order!
 *
 * Portions Copyright (c) 2007-2010, Cloudberry Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/sort/tuplestore.c
 *
 *-------------------------------------------------------------------------
 */

#include "utils/tuplestore_vec.h"
#include "utils/wrapper.h"
/*
 * Possible states of a Tuplestore object.  These denote the states that
 * persist between calls of Tuplestore routines.
 */
typedef enum
{
	TSS_INMEM,					/* Tuples still fit in memory */
	TSS_WRITEFILE,				/* Writing to temp file */
	TSS_READFILE				/* Reading from temp file */
} TupStoreStatus;

/*
 * State for a single read pointer.  If we are in state INMEM then all the
 * read pointers' "current" fields denote the read positions.  In state
 * WRITEFILE, the file/offset fields denote the read positions.  In state
 * READFILE, inactive read pointers have valid file/offset, but the active
 * read pointer implicitly has position equal to the temp file's seek position.
 *
 * Special case: if eof_reached is true, then the pointer's read position is
 * implicitly equal to the write position, and current/file/offset aren't
 * maintained.  This way we need not update all the read pointers each time
 * we write.
 */
typedef struct
{
	int			eflags;			/* capability flags */
	bool		eof_reached;	/* read has reached EOF */
	int			current;		/* next array index to read */
	int			file;			/* temp file# */
	off_t		offset;			/* byte offset in file */
} TSReadPointer;

typedef enum
{
	TSHARE_NOT_SHARED,
	TSHARE_WRITER,
	TSHARE_READER
} TSSharedStatus;

/*
 * Private state of a Tuplestore operation.
 */
struct Tuplestorestate
{
	TupStoreStatus status;		/* enumerated value as shown above */
	int			eflags;			/* capability flags (OR of pointers' flags) */
	bool		backward;		/* store extra length words in file? */
	bool		interXact;		/* keep open through transactions? */
	bool		truncated;		/* tuplestore_trim has removed tuples? */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	int64		tuples;			/* number of tuples added */
	BufFile    *myfile;			/* underlying file, or NULL if none */
	MemoryContext context;		/* memory context for holding tuples */
	ResourceOwner resowner;		/* resowner for holding temp files */

	TSSharedStatus share_status;
	bool		frozen;
	SharedFileSet *fileset;
	char	   *shared_filename;
	workfile_set *work_set; /* workfile set to use when using workfile manager */
	int64		remaining_tuples; /* number of tuples remaining */

	/*
	 * These function pointers decouple the routines that must know what kind
	 * of tuple we are handling from the routines that don't need to know it.
	 * They are set up by the tuplestore_begin_xxx routines.
	 *
	 * (Although tuplestore.c currently only supports heap tuples, I've copied
	 * this part of tuplesort.c so that extension to other kinds of objects
	 * will be easy if it's ever needed.)
	 *
	 * Function to copy a supplied input tuple into palloc'd space. (NB: we
	 * assume that a single pfree() is enough to release the tuple later, so
	 * the representation must be "flat" in one palloc chunk.) state->availMem
	 * must be decreased by the amount of space used.
	 */
	void	   *(*copytup) (Tuplestorestate *state, void *tup);

	/*
	 * Function to write a stored tuple onto tape.  The representation of the
	 * tuple on tape need not be the same as it is in memory; requirements on
	 * the tape representation are given below.  After writing the tuple,
	 * pfree() it, and increase state->availMem by the amount of memory space
	 * thereby released.
	 */
	void		(*writetup) (Tuplestorestate *state, void *tup);

	/*
	 * Function to read a stored tuple from tape back into memory. 'len' is
	 * the already-read length of the stored tuple.  Create and return a
	 * palloc'd copy, and decrease state->availMem by the amount of memory
	 * space consumed.
	 */
	void	   *(*readtup) (Tuplestorestate *state, unsigned int len);

	/*
	 * This array holds pointers to tuples in memory if we are in state INMEM.
	 * In states WRITEFILE and READFILE it's not used.
	 *
	 * When memtupdeleted > 0, the first memtupdeleted pointers are already
	 * released due to a tuplestore_trim() operation, but we haven't expended
	 * the effort to slide the remaining pointers down.  These unused pointers
	 * are set to NULL to catch any invalid accesses.  Note that memtupcount
	 * includes the deleted pointers.
	 */
	void	  **memtuples;		/* array of pointers to palloc'd tuples */
	int			memtupdeleted;	/* the first N slots are currently unused */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/*
	 * These variables are used to keep track of the current positions.
	 *
	 * In state WRITEFILE, the current file seek position is the write point;
	 * in state READFILE, the write position is remembered in writepos_xxx.
	 * (The write position is the same as EOF, but since BufFileSeek doesn't
	 * currently implement SEEK_END, we have to remember it explicitly.)
	 */
	TSReadPointer *readptrs;	/* array of read pointers */
	int			activeptr;		/* index of the active read pointer */
	int			readptrcount;	/* number of pointers currently valid */
	int			readptrsize;	/* allocated length of readptrs array */

	int			writepos_file;	/* file# (valid if READFILE state) */
	off_t		writepos_offset;	/* offset (valid if READFILE state) */

    /*
     * CDB: EXPLAIN ANALYZE reporting interface and statistics.
     */
	struct Instrumentation *instrument;
	long        availMemMin;    /* availMem low water mark (bytes) */
	int64       spilledBytes;   /* memory used for spilled tuples */
};

#define COPYTUP(state,tup)	((*(state)->copytup) (state, tup))
#define WRITETUP(state,tup) ((*(state)->writetup) (state, tup))
#define READTUP(state,len)	((*(state)->readtup) (state, len))
#define LACKMEM(state)		((state)->availMem < 0)
#define USEMEM(state,amt)	((state)->availMem -= (amt))
#define FREEMEM(state,amt)	\
	do { \
		if ((state)->availMemMin > (state)->availMem) \
			(state)->availMemMin = (state)->availMem; \
		(state)->availMem += (amt); \
	} while(0)

/*--------------------
 *
 * NOTES about on-tape representation of tuples:
 *
 * We require the first "unsigned int" of a stored tuple to be the total size
 * on-tape of the tuple, including itself (so it is never zero).
 * The remainder of the stored tuple
 * may or may not match the in-memory representation of the tuple ---
 * any conversion needed is the job of the writetup and readtup routines.
 *
 * If state->backward is true, then the stored representation of
 * the tuple must be followed by another "unsigned int" that is a copy of the
 * length --- so the total tape space used is actually sizeof(unsigned int)
 * more than the stored length value.  This allows read-backwards.  When
 * state->backward is not set, the write/read routines may omit the extra
 * length word.
 *
 * writetup is expected to write both length words as well as the tuple
 * data.  When readtup is called, the tape is positioned just after the
 * front length word; readtup must read the tuple data and advance past
 * the back length word (if present).
 *
 * The write/read routines can make use of the tuple description data
 * stored in the Tuplestorestate record, if needed. They are also expected
 * to adjust state->availMem by the amount of memory space (not tape space!)
 * released or consumed.  There is no error return from either writetup
 * or readtup; they should ereport() on failure.
 *
 *
 * NOTES about memory consumption calculations:
 *
 * We count space allocated for tuples against the maxKBytes limit,
 * plus the space used by the variable-size array memtuples.
 * Fixed-size space (primarily the BufFile I/O buffer) is not counted.
 * We don't worry about the size of the read pointer array, either.
 *
 * Note that we count actual space used (as shown by GetMemoryChunkSpace)
 * rather than the originally-requested size.  This is important since
 * palloc can add substantial overhead.  It's not a complete answer since
 * we won't count any wasted space in palloc allocation blocks, but it's
 * a lot better than what we were doing before 7.3.
 *
 *--------------------
 */


static Tuplestorestate *tuplestore_begin_common(int eflags,
												bool interXact,
												int maxKBytes);
static void tuplestore_puttuple_common(Tuplestorestate *state, void *batch);
static void dumptuples(Tuplestorestate *state);
static unsigned int getlen(Tuplestorestate *state, bool eofOK);
static void writetup_batch(Tuplestorestate *state, void *batch);
static void *readtup_batch(Tuplestorestate *state, unsigned int len);

/*
 *		tuplestore_begin_xxx
 *
 * Initialize for a tuple store operation.
 */
static Tuplestorestate *
tuplestore_begin_common(int eflags, bool interXact, int maxKBytes)
{
	Tuplestorestate *state;

	state = (Tuplestorestate *) palloc0(sizeof(Tuplestorestate));

	state->status = TSS_INMEM;
	state->eflags = eflags;
	state->interXact = interXact;
	state->truncated = false;
	state->allowedMem = maxKBytes * 1024L;
	state->availMem = state->allowedMem;
	state->availMemMin = state->availMem;
	state->myfile = NULL;
	state->context = CurrentMemoryContext;
	state->resowner = CurrentResourceOwner;

	state->memtupdeleted = 0;
	state->memtupcount = 0;
	state->tuples = 0;
	state->remaining_tuples = 0;

	/*
	 * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
	 * see comments in grow_memtuples().
	 */
	state->memtupsize = Max(16384 / sizeof(void *),
							ALLOCSET_SEPARATE_THRESHOLD / sizeof(void *) + 1);

	state->growmemtuples = true;
	state->memtuples = (void **) palloc(state->memtupsize * sizeof(void *));

	USEMEM(state, GetMemoryChunkSpace(state->memtuples));

	state->activeptr = 0;
	state->readptrcount = 1;
	state->readptrsize = 8;		/* arbitrary */
	state->readptrs = (TSReadPointer *)
		palloc(state->readptrsize * sizeof(TSReadPointer));

	state->readptrs[0].eflags = eflags;
	state->readptrs[0].eof_reached = false;
	state->readptrs[0].current = 0;

	return state;
}

/*
 * tuplestore_begin_batch
 *
 * Create a new tuplestore; other types of tuple stores (other than
 * "heap" tuple stores, for heap tuples) are possible, but not presently
 * implemented.
 *
 * randomAccess: if true, both forward and backward accesses to the
 * tuple store are allowed.
 *
 * interXact: if true, the files used for on-disk storage persist beyond the
 * end of the current transaction.  NOTE: It's the caller's responsibility to
 * create such a tuplestore in a memory context and resource owner that will
 * also survive transaction boundaries, and to ensure the tuplestore is closed
 * when it's no longer wanted.
 *
 * maxKBytes: how much data to store in memory (any data beyond this
 * amount is paged to disk).  When in doubt, use work_mem.
 */
Tuplestorestate *
tuplestore_begin_batch(bool randomAccess, bool interXact, int maxKBytes)
{
	Tuplestorestate *state;
	int			eflags;

	/*
	 * This interpretation of the meaning of randomAccess is compatible with
	 * the pre-8.3 behavior of tuplestores.
	 */
	eflags = randomAccess ?
		(EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND) :
		(EXEC_FLAG_REWIND);

	state = tuplestore_begin_common(eflags, interXact, maxKBytes);

	state->copytup = NULL;
	state->writetup = writetup_batch;
	state->readtup = readtup_batch;

	return state;
}

/*
 * Grow the memtuples[] array, if possible within our memory constraint.  We
 * must not exceed INT_MAX tuples in memory or the caller-provided memory
 * limit.  Return true if we were able to enlarge the array, false if not.
 *
 * Normally, at each increment we double the size of the array.  When doing
 * that would exceed a limit, we attempt one last, smaller increase (and then
 * clear the growmemtuples flag so we don't try any more).  That allows us to
 * use memory as fully as permitted; sticking to the pure doubling rule could
 * result in almost half going unused.  Because availMem moves around with
 * tuple addition/removal, we need some rule to prevent making repeated small
 * increases in memtupsize, which would just be useless thrashing.  The
 * growmemtuples flag accomplishes that and also prevents useless
 * recalculations in this function.
 */
static bool
grow_memtuples(Tuplestorestate *state)
{
	int			newmemtupsize;
	int			memtupsize = state->memtupsize;
	int64		memNowUsed = state->allowedMem - state->availMem;

	/* Forget it if we've already maxed out memtuples, per comment above */
	if (!state->growmemtuples)
		return false;

	/* Select new value of memtupsize */
	if (memNowUsed <= state->availMem)
	{
		/*
		 * We've used no more than half of allowedMem; double our usage,
		 * clamping at INT_MAX tuples.
		 */
		if (memtupsize < INT_MAX / 2)
			newmemtupsize = memtupsize * 2;
		else
		{
			newmemtupsize = INT_MAX;
			state->growmemtuples = false;
		}
	}
	else
	{
		/*
		 * This will be the last increment of memtupsize.  Abandon doubling
		 * strategy and instead increase as much as we safely can.
		 *
		 * To stay within allowedMem, we can't increase memtupsize by more
		 * than availMem / sizeof(void *) elements. In practice, we want to
		 * increase it by considerably less, because we need to leave some
		 * space for the tuples to which the new array slots will refer.  We
		 * assume the new tuples will be about the same size as the tuples
		 * we've already seen, and thus we can extrapolate from the space
		 * consumption so far to estimate an appropriate new size for the
		 * memtuples array.  The optimal value might be higher or lower than
		 * this estimate, but it's hard to know that in advance.  We again
		 * clamp at INT_MAX tuples.
		 *
		 * This calculation is safe against enlarging the array so much that
		 * LACKMEM becomes true, because the memory currently used includes
		 * the present array; thus, there would be enough allowedMem for the
		 * new array elements even if no other memory were currently used.
		 *
		 * We do the arithmetic in float8, because otherwise the product of
		 * memtupsize and allowedMem could overflow.  Any inaccuracy in the
		 * result should be insignificant; but even if we computed a
		 * completely insane result, the checks below will prevent anything
		 * really bad from happening.
		 */
		double		grow_ratio;

		grow_ratio = (double) state->allowedMem / (double) memNowUsed;
		if (memtupsize * grow_ratio < INT_MAX)
			newmemtupsize = (int) (memtupsize * grow_ratio);
		else
			newmemtupsize = INT_MAX;

		/* We won't make any further enlargement attempts */
		state->growmemtuples = false;
	}

	/* Must enlarge array by at least one element, else report failure */
	if (newmemtupsize <= memtupsize)
		goto noalloc;

	/*
	 * On a 32-bit machine, allowedMem could exceed MaxAllocHugeSize.  Clamp
	 * to ensure our request won't be rejected.  Note that we can easily
	 * exhaust address space before facing this outcome.  (This is presently
	 * impossible due to guc.c's MAX_KILOBYTES limitation on work_mem, but
	 * don't rely on that at this distance.)
	 */
	if ((Size) newmemtupsize >= MaxAllocHugeSize / sizeof(void *))
	{
		newmemtupsize = (int) (MaxAllocHugeSize / sizeof(void *));
		state->growmemtuples = false;	/* can't grow any more */
	}

	/*
	 * We need to be sure that we do not cause LACKMEM to become true, else
	 * the space management algorithm will go nuts.  The code above should
	 * never generate a dangerous request, but to be safe, check explicitly
	 * that the array growth fits within availMem.  (We could still cause
	 * LACKMEM if the memory chunk overhead associated with the memtuples
	 * array were to increase.  That shouldn't happen because we chose the
	 * initial array size large enough to ensure that palloc will be treating
	 * both old and new arrays as separate chunks.  But we'll check LACKMEM
	 * explicitly below just in case.)
	 */
	if (state->availMem < (int64) ((newmemtupsize - memtupsize) * sizeof(void *)))
		goto noalloc;

	/* OK, do it */
	FREEMEM(state, GetMemoryChunkSpace(state->memtuples));
	state->memtupsize = newmemtupsize;
	state->memtuples = (void **)
		repalloc_huge(state->memtuples,
					  state->memtupsize * sizeof(void *));
	USEMEM(state, GetMemoryChunkSpace(state->memtuples));
	if (LACKMEM(state))
		elog(ERROR, "unexpected out-of-memory situation in tuplestore");
	return true;

noalloc:
	/* If for any reason we didn't realloc, shut off future attempts */
	state->growmemtuples = false;
	return false;
}

/*
 * Accept one tuple and append it to the tuplestore.
 *
 * Note that the input tuple is always copied; the caller need not save it.
 *
 * If the active read pointer is currently "at EOF", it remains so (the read
 * pointer implicitly advances along with the write pointer); otherwise the
 * read pointer is unchanged.  Non-active read pointers do not move, which
 * means they are certain to not be "at EOF" immediately after puttuple.
 * This curious-seeming behavior is for the convenience of nodeMaterial.c and
 * nodeCtescan.c, which would otherwise need to do extra pointer repositioning
 * steps.
 *
 * tuplestore_putvecslot() is a convenience routine to collect data from
 * a TupleTableSlot without an extra copy operation.
 */
void tuplestore_putvecslot(Tuplestorestate *state,
						   TupleTableSlot *slot)
{
    tuplestore_puttuple_common(state, (void *) GetBatch(slot));
}


static void
tuplestore_puttuple_common(Tuplestorestate *state, void *batch)
{
	TSReadPointer *readptr;
	int			i;
	ResourceOwner oldowner;

	if (state->frozen)
		elog(ERROR, "cannot write new tuples to frozen tuplestore");

	state->tuples++;
	state->remaining_tuples++;

	switch (state->status)
	{
		case TSS_INMEM:

			/*
			 * Update read pointers as needed; see API spec above.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					readptr->current = state->memtupcount;
				}
			}

			/*
			 * Grow the array as needed.  Note that we try to grow the array
			 * when there is still one free slot remaining --- if we fail,
			 * there'll still be room to store the incoming tuple, and then
			 * we'll switch to tape-based operation.
			 */
			if (state->memtupcount >= state->memtupsize - 1)
			{
				(void) grow_memtuples(state);
				Assert(state->memtupcount < state->memtupsize);
			}

			/* Stash the tuple in the in-memory array */
			state->memtuples[state->memtupcount++] = garrow_copy_ptr(batch);

			/*
			 * Done if we still fit in available memory and have array slots.
			 */
			if (state->memtupcount < state->memtupsize && !LACKMEM(state))
				return;
			/*
			 * Nope; time to switch to tape-based operation.  Make sure that
			 * the temp file(s) are created in suitable temp tablespaces.
			 */
			PrepareTempTablespaces();

			/* associate the file with the store's resource owner */
			oldowner = CurrentResourceOwner;
			CurrentResourceOwner = state->resowner;

			char tmpprefix[50];
			snprintf(tmpprefix, 50, "slice%d_tuplestore", currentSliceId);
			state->myfile = BufFileCreateTemp(tmpprefix, state->interXact);

			CurrentResourceOwner = oldowner;

			/*
			 * Freeze the decision about whether trailing length words will be
			 * used.  We can't change this choice once data is on tape, even
			 * though callers might drop the requirement.
			 */
			state->backward = (state->eflags & EXEC_FLAG_BACKWARD) != 0;
			state->status = TSS_WRITEFILE;
			dumptuples(state);
			break;
		case TSS_WRITEFILE:

			/*
			 * Update read pointers as needed; see API spec above. Note:
			 * BufFileTell is quite cheap, so not worth trying to avoid
			 * multiple calls.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					BufFileTell(state->myfile,
								&readptr->file,
								&readptr->offset);
				}
			}

			WRITETUP(state, batch);
			break;
		case TSS_READFILE:

			/*
			 * Switch from reading to writing.
			 */
			if (!state->readptrs[state->activeptr].eof_reached)
				BufFileTell(state->myfile,
							&state->readptrs[state->activeptr].file,
							&state->readptrs[state->activeptr].offset);
			if (BufFileSeek(state->myfile,
							state->writepos_file, state->writepos_offset,
							SEEK_SET) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in tuplestore temporary file")));
			state->status = TSS_WRITEFILE;

			/*
			 * Update read pointers as needed; see API spec above.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					readptr->file = state->writepos_file;
					readptr->offset = state->writepos_offset;
				}
			}

			WRITETUP(state, batch);
			break;
		default:
			elog(ERROR, "invalid tuplestore state");
			break;
	}
}

/*
 * Fetch the next tuple in either forward or back direction.
 * Returns NULL if no more tuples.  If should_free is set, the
 * caller must pfree the returned tuple when done with it.
 *
 * Backward scan is only allowed if randomAccess was set true or
 * EXEC_FLAG_BACKWARD was specified to tuplestore_set_eflags().
 */
static void *
tuplestore_getbatch(Tuplestorestate *state, bool forward,
					bool *should_free)
{
	TSReadPointer *readptr = &state->readptrs[state->activeptr];
	unsigned int tuplen;
	void	   *batch;

	Assert(forward || (readptr->eflags & EXEC_FLAG_BACKWARD));

	switch (state->status)
	{
		case TSS_INMEM:
			*should_free = false;
			if (forward)
			{
				if (readptr->eof_reached)
					return NULL;
				if (readptr->current < state->memtupcount)
				{
					/* We have another tuple, so return it */
					return state->memtuples[readptr->current++];
				}
				readptr->eof_reached = true;
				return NULL;
			}
			else
			{
				/*
				 * if all tuples are fetched already then we return last
				 * tuple, else tuple before last returned.
				 */
				if (readptr->eof_reached)
				{
					readptr->current = state->memtupcount;
					readptr->eof_reached = false;
				}
				else
				{
					if (readptr->current <= state->memtupdeleted)
					{
						Assert(!state->truncated);
						return NULL;
					}
					readptr->current--; /* last returned tuple */
				}
				if (readptr->current <= state->memtupdeleted)
				{
					Assert(!state->truncated);
					return NULL;
				}
				return state->memtuples[readptr->current - 1];
			}
			break;

		case TSS_WRITEFILE:
			/* Skip state change if we'll just return NULL */
			if (readptr->eof_reached && forward)
				return NULL;

			/*
			 * Switch from writing to reading.
			 */
			BufFileTell(state->myfile,
						&state->writepos_file, &state->writepos_offset);
			if (!readptr->eof_reached)
				if (BufFileSeek(state->myfile,
								readptr->file, readptr->offset,
								SEEK_SET) != 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not seek in tuplestore temporary file")));
			state->status = TSS_READFILE;
			/* FALLTHROUGH */

		case TSS_READFILE:
			*should_free = true;
			if (forward)
			{
				if ((tuplen = getlen(state, true)) != 0)
				{
					batch = READTUP(state, tuplen);
					return batch;
				}
				else
				{
					readptr->eof_reached = true;
					return NULL;
				}
			}

			/*
			 * Backward.
			 *
			 * if all tuples are fetched already then we return last tuple,
			 * else tuple before last returned.
			 *
			 * Back up to fetch previously-returned tuple's ending length
			 * word. If seek fails, assume we are at start of file.
			 */
			if (BufFileSeek(state->myfile, 0, -(long) sizeof(unsigned int),
							SEEK_CUR) != 0)
			{
				/* even a failed backwards fetch gets you out of eof state */
				readptr->eof_reached = false;
				Assert(!state->truncated);
				return NULL;
			}
			tuplen = getlen(state, false);

			if (readptr->eof_reached)
			{
				readptr->eof_reached = false;
				/* We will return the tuple returned before returning NULL */
			}
			else
			{
				/*
				 * Back up to get ending length word of tuple before it.
				 */
				if (BufFileSeek(state->myfile, 0,
								-(long) (tuplen + 2 * sizeof(unsigned int)),
								SEEK_CUR) != 0)
				{
					/*
					 * If that fails, presumably the prev tuple is the first
					 * in the file.  Back up so that it becomes next to read
					 * in forward direction (not obviously right, but that is
					 * what in-memory case does).
					 */
					if (BufFileSeek(state->myfile, 0,
									-(long) (tuplen + sizeof(unsigned int)),
									SEEK_CUR) != 0)
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not seek in tuplestore temporary file")));
					Assert(!state->truncated);
					return NULL;
				}
				tuplen = getlen(state, false);
			}

			/*
			 * Now we have the length of the prior tuple, back up and read it.
			 * Note: READTUP expects we are positioned after the initial
			 * length word of the tuple, so back up to that point.
			 */
			if (BufFileSeek(state->myfile, 0,
							-(long) tuplen,
							SEEK_CUR) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in tuplestore temporary file")));
			batch = READTUP(state, tuplen);
			return batch;

		default:
			elog(ERROR, "invalid tuplestore state");
			return NULL;		/* keep compiler quiet */
	}
}

/*
 * tuplestore_getvecslot - exported function to fetch a MinimalTuple
 *
 * If successful, put tuple in slot and return true; else, clear the slot
 * and return false.
 *
 * If copy is true, the slot receives a copied tuple (allocated in current
 * memory context) that will stay valid regardless of future manipulations of
 * the tuplestore's state.  If copy is false, the slot may just receive a
 * pointer to a tuple held within the tuplestore.  The latter is more
 * efficient but the slot contents may be corrupted if additional writes to
 * the tuplestore occur.  (If using tuplestore_trim, see comments therein.)
 */
bool tuplestore_getvecslot(Tuplestorestate *state, bool forward,
						   bool copy, TupleTableSlot *slot)
{
    GArrowRecordBatch *batch = NULL;
	bool		should_free;

	batch = (GArrowRecordBatch *) tuplestore_getbatch(state, forward, &should_free);

	if (batch)
	{
		ExecStoreBatch(slot, batch);
		if (should_free)
			free_batch((void **)&batch);
		return true;
	}
	else
	{
		ExecClearTuple(slot);
		return false;
	}
}

/*
 * tuplestore_advance_vec - exported function to adjust position without fetching
 *
 * We could optimize this case to avoid palloc/pfree overhead, but for the
 * moment it doesn't seem worthwhile.
 */
bool
tuplestore_advance_vec(Tuplestorestate *state, bool forward)
{
	void	   *batch;
	bool		should_free;

	batch = tuplestore_getbatch(state, forward, &should_free);

	if (batch)
	{
		if (should_free)
			free_batch(&batch);
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * tuplestore_trim	- remove all no-longer-needed tuples
 *
 * Calling this function authorizes the tuplestore to delete all tuples
 * before the oldest read pointer, if no read pointer is marked as requiring
 * REWIND capability.
 *
 * Note: this is obviously safe if no pointer has BACKWARD capability either.
 * If a pointer is marked as BACKWARD but not REWIND capable, it means that
 * the pointer can be moved backward but not before the oldest other read
 * pointer.
 */
void 
tuplestore_trim_vec(Tuplestorestate *state)
{
	int			oldest;
	int			nremove;
	int			i;

	/*
	 * Truncation is disallowed if any read pointer requires rewind
	 * capability.
	 */
	if (state->eflags & EXEC_FLAG_REWIND)
		return;

	/* Cannot trim tuplestore if another process might be reading it */
	if (state->frozen)
		return;

	/*
	 * We don't bother trimming temp files since it usually would mean more
	 * work than just letting them sit in kernel buffers until they age out.
	 */
	if (state->status != TSS_INMEM)
		return;

	/* Find the oldest read pointer */
	oldest = state->memtupcount;
	for (i = 0; i < state->readptrcount; i++)
	{
		if (!state->readptrs[i].eof_reached)
			oldest = Min(oldest, state->readptrs[i].current);
	}

	/*
	 * Note: you might think we could remove all the tuples before the oldest
	 * "current", since that one is the next to be returned.  However, since
	 * tuplestore_gettuple returns a direct pointer to our internal copy of
	 * the tuple, it's likely that the caller has still got the tuple just
	 * before "current" referenced in a slot. So we keep one extra tuple
	 * before the oldest "current".  (Strictly speaking, we could require such
	 * callers to use the "copy" flag to tuplestore_gettupleslot, but for
	 * efficiency we allow this one case to not use "copy".)
	 */
	nremove = oldest - 1;
	if (nremove <= 0)
		return;					/* nothing to do */

	Assert(nremove >= state->memtupdeleted);
	Assert(nremove <= state->memtupcount);

	/* Release no-longer-needed tuples */
	for (i = state->memtupdeleted; i < nremove; i++)
	{
		free_batch(&state->memtuples[i]);
		state->memtuples[i] = NULL;
	}
	state->memtupdeleted = nremove;

	/* mark tuplestore as truncated (used for Assert crosschecks only) */
	state->truncated = true;

	/*
	 * If nremove is less than 1/8th memtupcount, just stop here, leaving the
	 * "deleted" slots as NULL.  This prevents us from expending O(N^2) time
	 * repeatedly memmove-ing a large pointer array.  The worst case space
	 * wastage is pretty small, since it's just pointers and not whole tuples.
	 */
	if (nremove < state->memtupcount / 8)
		return;

	/*
	 * Slide the array down and readjust pointers.
	 *
	 * In mergejoin's current usage, it's demonstrable that there will always
	 * be exactly one non-removed tuple; so optimize that case.
	 */
	if (nremove + 1 == state->memtupcount)
		state->memtuples[0] = state->memtuples[nremove];
	else
		memmove(state->memtuples, state->memtuples + nremove,
				(state->memtupcount - nremove) * sizeof(void *));

	state->memtupdeleted = 0;
	state->memtupcount -= nremove;
	for (i = 0; i < state->readptrcount; i++)
	{
		if (!state->readptrs[i].eof_reached)
			state->readptrs[i].current -= nremove;
	}
}

/*
 * Tape interface routines
 */
static unsigned int
getlen(Tuplestorestate *state, bool eofOK)
{
	unsigned int len;
	size_t		nbytes;

	nbytes = BufFileRead(state->myfile, (void *) &len, sizeof(len));
	if (nbytes == sizeof(len))
		return len;
	if (nbytes != 0 || !eofOK)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
						nbytes, sizeof(len))));
	return 0;
}


static void
writetup_batch(Tuplestorestate *state, void *batch)
{
	int64 			buf_size = 0;
	void 			*buffer = NULL;
	const void 		*data_pos;
	/* serielized the record batch */
	buffer = SerializeRecordBatch(batch);
	data_pos = GetSerializedRecordBatchDataBufDataAndSize(buffer, &buf_size);
	/* total on-disk footprint: */
	unsigned int tuplen = buf_size + sizeof(int);
	BufFileWrite(state->myfile, (void *) &tuplen, sizeof(tuplen));
	BufFileWrite(state->myfile, (void *) data_pos, buf_size);
	if (state->backward)		/* need trailing length word? */
		BufFileWrite(state->myfile, (void *) &tuplen, sizeof(tuplen));
	free_array_buffer(&buffer);
}

static void *
readtup_batch(Tuplestorestate *state, unsigned int len)
{
	size_t		nread;
	g_autoptr(GArrowRecordBatch) batch = NULL;
	unsigned int tupbodylen = len - sizeof(int);
	g_autoptr(GArrowBuffer) buffer = NULL;
	g_autoptr(GError) error = NULL;
	void *data;
	g_autoptr(GBytes) gbytes = NULL;
	gsize data_szie;

	buffer = garrow_buffer_new_default_mem(tupbodylen, &error);
	if (error)
		elog(ERROR, "Allocate deserialize buffer from defualt pool failed: %s",
				error->message);

	gbytes = garrow_buffer_get_data(buffer);
	data = (void *) g_bytes_get_data(gbytes, &data_szie);
	nread = BufFileRead(state->myfile, data, tupbodylen);
	if (nread != (size_t) tupbodylen)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
						nread, (size_t) tupbodylen)));
    batch = DeserializeRecordBatch(buffer, tupbodylen, false);
	if (state->backward)		/* need trailing length word? */
	{
		nread = BufFileRead(state->myfile, (void *) &tupbodylen, sizeof(tupbodylen));
		if (nread != sizeof(tupbodylen))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
							nread, sizeof(tupbodylen))));
	}
	return garrow_move_ptr(batch);
}


static void
writetup_forbidden(Tuplestorestate *state, void *tup)
{
	elog(ERROR, "cannot write to tuplestore, it is already frozen");
}

/*
 * tuplestore_open_shared_vec
 *
 * Open a shared tuplestore that has been populated in another process
 * for reading.
 */
Tuplestorestate *
tuplestore_open_shared_vec(SharedFileSet *fileset, const char *filename)
{
	Tuplestorestate *state;
	int			eflags;

	eflags = EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND;

	state = tuplestore_begin_common(eflags,
									false /* interXact, ignored because we open existing files */,
									10 /* no need for memory buffers */);

	state->backward = true;

	state->copytup = NULL;
	state->writetup = writetup_forbidden;
	state->readtup = readtup_batch;

	state->myfile = BufFileOpenShared(fileset, filename, O_RDONLY);
	state->readptrs[0].file = 0;
	state->readptrs[0].offset = 0L;
	state->status = TSS_READFILE;

	state->share_status = TSHARE_READER;
	state->frozen = false;
	state->fileset = fileset;
	state->shared_filename = pstrdup(filename);

	return state;
}

/*
 * dumptuples - remove tuples from memory and write to tape
 *
 * As a side effect, we must convert each read pointer's position from
 * "current" to file/offset format.  But eof_reached pointers don't
 * need to change state.
 */
static void
dumptuples(Tuplestorestate *state)
{
	int			i;

	for (i = state->memtupdeleted;; i++)
	{
		TSReadPointer *readptr = state->readptrs;
		int			j;

		for (j = 0; j < state->readptrcount; readptr++, j++)
		{
			if (i == readptr->current && !readptr->eof_reached)
				BufFileTell(state->myfile,
							&readptr->file, &readptr->offset);
		}
		if (i >= state->memtupcount)
			break;
		WRITETUP(state, state->memtuples[i]);
	}
	state->memtupdeleted = 0;
	state->memtupcount = 0;
}

/*
 * tuplestore_end_vec
 *
 *	Release resources and clean up.
 */
void
tuplestore_end_vec(Tuplestorestate *state)
{
	int			i;

	/*
	 * CDB: Report statistics to EXPLAIN ANALYZE.
	 */
	if (state->instrument && state->instrument->need_cdb)
	{
		double  nbytes;

		/* How close did we come to the work_mem limit? */
		FREEMEM(state, 0);              /* update low-water mark */
		nbytes = state->allowedMem - state->availMemMin;
		state->instrument->workmemused = Max(state->instrument->workmemused, nbytes);

		/* How much work_mem would be enough to hold all tuples in memory? */
		if (state->spilledBytes > 0)
		{
			nbytes = state->allowedMem - state->availMem + state->spilledBytes;
			state->instrument->workmemwanted =
				Max(state->instrument->workmemwanted, nbytes);
		}

		if (state->myfile)
			state->instrument->workfileCreated = true;
	}

	if (state->myfile)
		BufFileClose(state->myfile);
	if (state->share_status == TSHARE_WRITER)
		BufFileDeleteShared(state->fileset, state->shared_filename);
	if (state->work_set)
		workfile_mgr_close_set(state->work_set);
	if (state->shared_filename)
		pfree(state->shared_filename);
	if (state->memtuples)
	{
		for (i = state->memtupdeleted; i < state->memtupcount; i++)
			free_batch(&state->memtuples[i]);
		pfree(state->memtuples);
	}
	pfree(state->readptrs);
	pfree(state);
}
