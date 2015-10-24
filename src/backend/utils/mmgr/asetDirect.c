/*-------------------------------------------------------------------------
 *
 * asetdirect.c
 *    A specialized implementation of the abstract MemoryContext type,
 *    which allocates directly from malloc() and does not support pfree().
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memutils.h"

#include "cdb/cdbptrbuf.h"              /* CdbPtrBuf */

/* Define this to detail debug alloc information */
/* #define HAVE_ALLOCINFO */


#ifdef CDB_PALLOC_CALLER_ID
#define CDB_MCXT_WHERE(context) (context)->callerFile, (context)->callerLine
#else
#define CDB_MCXT_WHERE(context) __FILE__, __LINE__
#endif


/*
 * AsetDirectContext
 */
typedef struct AsetDirectContext
{
    MemoryContextData   header;         /* standard memory-context fields */
    Size                size_total;     /* total size of all allocated areas */
    unsigned            narea_total;    /* number of allocated areas */
    CdbPtrBuf           areas;          /* collection of allocated area ptrs */

    /* variably-sized array, must be last */
    CdbPtrBuf_Ptr       areaspace[10];
} AsetDirectContext;

#define ASETDIRECTCONTEXT_BYTES(nareaspace) \
            (MAXALIGN(SIZEOF_VARSTRUCT(nareaspace, AsetDirectContext, areaspace)))


/*
 * These functions implement the MemoryContext API for AsetDirect contexts.
 */
static void *AsetDirectAlloc(MemoryContext context, Size size);
static void AsetDirectInit(MemoryContext context);
static void AsetDirectReset(MemoryContext context);
static void AsetDirectDelete(MemoryContext context);
static bool AsetDirectIsEmpty(MemoryContext context);
static void AsetDirectStats(MemoryContext context, uint64 *nBlocks, uint64 *nChunks,
		uint64 *currentAvailable, uint64 *allAllocated, uint64 *allFreed, uint64 *maxHeld);

#ifdef MEMORY_CONTEXT_CHECKING
static void AsetDirectCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for AsetDirect contexts.
 */
static MemoryContextMethods AsetDirectMethods = {
    AsetDirectAlloc,
    NULL,                               /* pfree */
    NULL,                               /* repalloc */
    AsetDirectInit,
    AsetDirectReset,
    AsetDirectDelete,
    NULL,                               /* GetChunkSpace */
    AsetDirectIsEmpty,
    AsetDirectStats,
#ifdef MEMORY_CONTEXT_CHECKING
    AsetDirectCheck
#endif
};


/* ----------
 * Debug macros
 * ----------
 */
#ifdef HAVE_ALLOCINFO
#define AllocAllocInfo(_cxt, _chunk) \
            fprintf(stderr, "AsetDirectAlloc: %s: %p, %d\n", \
				(_cxt)->header.name, (_chunk), (_cxt)->chunksize)
#else
#define AllocAllocInfo(_cxt, _chunk)
#endif


/*
 * Public routines
 */


/*
 * AsetDirectContextCreate
 *      Create a new AsetDirect context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (for debugging --- string will be copied)
 */
MemoryContext
AsetDirectContextCreate(MemoryContext parent, const char *name)
{
    AsetDirectContext  *set;
    Size                namesize = MAXALIGN(strlen(name) + 1);
    Size                allocsize;
    Size                setsize;        /* #bytes to request for new context */
    int                 nareaspace;     /* num of slots in areaspace array */

    /*
     * Determine amount of memory to request for the AsetDirectContext struct.
     *
     * Assume the total allocation will be rounded up to a power of 2, and
     * will include the AsetDirectContext with variably sized 'areaspace' array
     * and the context 'name' string.  Size the 'areaspace' array to use up any
     * extra space in the expected allocation.
     */
    allocsize = 1 << ceil_log2_Size(MAXALIGN(sizeof(AsetDirectContext)) + namesize);
    nareaspace = VARELEMENTS_TO_FIT(allocsize - namesize, AsetDirectContext, areaspace);
    setsize = ASETDIRECTCONTEXT_BYTES(nareaspace);

    /*
     * Create the new memory context and hook up to parent context.
     */
    set = (AsetDirectContext *)MemoryContextCreate(T_AsetDirectContext,
                                                   setsize,
                                                   &AsetDirectMethods,
                                                   parent,
                                                   name);

    /*
     * Initialize empty collection of ptrs to allocated areas.
     */
    CdbPtrBuf_Init(&set->areas,
                   set->areaspace,
                   nareaspace,
                   50,                      /* num_cells_expand */
                   set->header.parent);
    return (MemoryContext)set;
}                               /* AsetDirectContextCreate */


/*
 * AsetDirectInit
 *      Context-type-specific initialization routine.
 *
 * This is called by MemoryContextCreate() after setting up the
 * generic MemoryContext fields and before linking the new context
 * into the context tree.  We must do whatever is needed to make the
 * new context minimally valid for deletion.  We must *not* risk
 * failure --- thus, for example, allocating more memory is not cool.
 * (AsetDirectContextCreate can allocate memory when it gets control
 * back, however.)
 */
static void
AsetDirectInit(MemoryContext context)
{
	/*
	 * Since MemoryContextCreate already zeroed the context node, we don't
	 * have to do anything here: it's already OK.
	 */
}                               /* AsetDirectInit */


/*
 * AsetDirectReset
 *      Frees all memory which is allocated in the given set.
 */
static void
AsetDirectReset(MemoryContext context)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;
    CdbPtrBuf_Iterator  it;
    CdbPtrBuf_Ptr      *pp;
    CdbPtrBuf_Ptr       p;

    Assert(set && IsA(set, AsetDirectContext));
    Assert(CdbPtrBuf_IsOk(&set->areas));

    /* Free allocated areas. */
    CdbPtrBuf_Iterator_Init(&it, &set->areas);
    while (NULL != (pp = CdbPtrBuf_Iterator_NextCell(&it)))
    {
        p = *pp;
        *pp = NULL;
        if (p)
        {
#ifdef CLOBBER_FREED_MEMORY
            /* Wipe first few bytes of freed memory for debugging purposes */
            memset(p, 0x7F, MAXALIGN(1));   /* don't know actual size of area */
#endif
            free(p);
        }
    }

    /* Empty the 'areas' collection. */
    CdbPtrBuf_Reset(&set->areas);

    /* Update statistics. */
    MemoryContextNoteFree(&set->header, set->size_total);
    set->narea_total = 0;
    set->size_total = 0;
}                               /* AsetDirectReset */


/*
 * AsetDirectDelete
 *      Frees all memory which is allocated in the given set,
 *      in preparation for deletion of the set.
 *
 * Unlike AsetDirectReset, this *must* free all resources of the set.
 * But note we are not responsible for deleting the context node itself.
 */
static void
AsetDirectDelete(MemoryContext context)
{
    AsetDirectReset(context);
}                               /* AsetDirectDelete */


/*
 * AsetDirectAlloc
 *      Returns pointer to allocated memory of given size; memory is added
 *      to the set.
 */
static void *
AsetDirectAlloc(MemoryContext context, Size size)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;
    CdbPtrBuf_Ptr      *pp;

    Assert(set && IsA(set, AsetDirectContext));

    if (size < MAXALIGN(1))
        size = MAXALIGN(1);

    /* Obtain a slot in 'areas' collection to point to the new allocation. */
    pp = CdbPtrBuf_Append(&set->areas, NULL);

    /* Allocate the memory. */
    *pp = malloc(size);
    if (!*pp)
        MemoryContextError(ERRCODE_OUT_OF_MEMORY,
                           &set->header, CDB_MCXT_WHERE(&set->header),
                           "Out of memory.  Failed on request of size %lu bytes.",
                           (unsigned long)size);

    /* Update statistics. */
    set->size_total += size;
    set->narea_total++;
    MemoryContextNoteAlloc(&set->header, size);
    AllocAllocInfo(set, chunk);
    return *pp;
}                               /* AsetDirectAlloc */


/*
 * AsetDirectIsEmpty
 *      Is an allocset empty of any allocated space?
 */
static bool
AsetDirectIsEmpty(MemoryContext context)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;

    return set->narea_total == 0;
}                               /* AsetDirectIsEmpty */

/*
 * AsetDirectStats
 *		Returns stats about memory consumption of an AsetDirectContext.
 *
 *	Input parameters:
 *		context: the context of interest
 *
 *	Output parameters:
 *		nBlocks: number of blocks in the context
 *		nChunks: number of chunks in the context
 *
 *		currentAvailable: free space across all blocks
 *
 *		allAllocated: total bytes allocated during lifetime (including
 *		blocks that was dropped later on, e.g., freeing a large chunk
 *		in an exclusive block would drop the block)
 *
 *		allFreed: total bytes that was freed during lifetime
 *		maxHeld: maximum bytes held during lifetime
 */
static void
AsetDirectStats(MemoryContext context, uint64 *nBlocks, uint64 *nChunks,
		uint64 *currentAvailable, uint64 *allAllocated, uint64 *allFreed, uint64 *maxHeld)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;

    Assert(set && IsA(set, AsetDirectContext));

    *nBlocks = 0;
    *nChunks = set->narea_total;
    *currentAvailable = 0;
    *allAllocated = set->header.allBytesAlloc;
    *allFreed = set->header.allBytesFreed;
    *maxHeld = set->header.maxBytesHeld;
}


#ifdef MEMORY_CONTEXT_CHECKING
/*
 * AsetDirectCheck
 *      Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
AsetDirectCheck(MemoryContext context)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;
    const char         *name = set->header.name;

    if (!IsA(set, AsetDirectContext))
        elog(WARNING, "problem in alloc set %s: type=%d",
             name, set->header.type);

    else if (!CdbPtrBuf_IsOk(&set->areas))
        elog(WARNING, "problem in alloc set %s: CdbPtrBuf error",
             name);

    else if (set->narea_total < 0 ||
             set->narea_total > CdbPtrBuf_Length(&set->areas))
        elog(WARNING, "problem in alloc set %s: narea=%d",
             name, set->narea_total);

}                               /* AsetDirectCheck */
#endif   /* MEMORY_CONTEXT_CHECKING */
