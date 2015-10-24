/*-------------------------------------------------------------------------
 *
 * cdbcellbuf.c
 *    a container for small items of constant size
 *
 * Copyright (c) 2007-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbcellbuf.h"             /* me */
#include "utils/memutils.h"             /* STANDARDCHUNKHEADERSIZE */


/*-------------------------------------------------------------------------
 *                  Initialize/Reset caller's CdbCellBuf
 *-------------------------------------------------------------------------
 */

/*
 * CdbCellBuf_InitEasy
 *
 * Initialize caller's CdbCellBuf struct (easy-to-call version).
 *
 * 'cellbuf' is the area to be initialized as a CdbCellBuf.
 * 'cellbytes' is the size of each cell.
 */
void
CdbCellBuf_InitEasy(CdbCellBuf *cellbuf, Size cellbytes)
{
    CdbCellBuf_Init(cellbuf,
                    cellbytes,
                    NULL,       /* initial_cell_area */
                    0,          /* num_initial_cells */
                    25,         /* num_cells_expand  */
                    NULL);      /* context */
}                               /* CdbCellBuf_InitEasy */


/*
 * CdbCellBuf_Init
 *
 * Initialize caller's CdbCellBuf struct.
 *
 * 'cellbuf' is the area to be initialized as a CdbCellBuf.
 * 'cellbytes' is the size of each cell.
 * 'initial_cell_area' is an optional memory area, properly aligned, where
 *      the first few cells can be stored.  It must be nearby the CdbCellBuf
 *      itself, typically in the same struct or local variable stack frame.
 *      This allows a small number of cells to be managed with good locality
 *      and no palloc/pfree.  If NULL, cell memory is allocated from 'context'
 *      when needed.
 * 'num_initial_cells' is the exact number of cells in the initial_cell_area.
 * 'num_cells_expand' is the suggested number of cell slots to be allocated
 *      whenever additional memory is needed.  The actual number will be
 *      rounded to fully utilize a power-of-2 chunk.  However, no expansion
 *      is done if num_cells_expand is zero; instead, after the initial
 *      cells are exhausted an error is raised.
 * 'context' is the memory context from which to allocate chunks of space
 *      for additional cells when there is no more room in initial_cell_area.
 *      If NULL, the CurrentMemoryContext is used.
 */
void
CdbCellBuf_Init(CdbCellBuf     *cellbuf,
                size_t          cellbytes,
                void           *initial_cell_area,
                int             num_initial_cells,
                int             num_cells_expand,
                MemoryContext   context)
{
    Assert(cellbuf != NULL &&
           cellbytes > 0 &&
           cellbytes < (65535U) &&
           num_initial_cells >= 0 &&
           num_cells_expand >= 0);

    memset(cellbuf, 0, sizeof(*cellbuf));

    cellbuf->cellbytes = (uint16)cellbytes;

    /* Did caller provide some nearby memory to be used for first few cells? */
    if (num_initial_cells > 0)
    {
        ptrdiff_t   offset_nearby = (char *)initial_cell_area - (char *)cellbuf;

        cellbuf->offset_nearby = (int16)offset_nearby;
        cellbuf->nearby_bunch_len = (int)(num_initial_cells * cellbytes);
        cellbuf->cbeg = (char *)initial_cell_area;
        cellbuf->cfree = cellbuf->cbeg;
        cellbuf->cend = cellbuf->cfree + cellbuf->nearby_bunch_len;

        /* Make sure the initial_cell_area is truly nearby. */
        Assert(initial_cell_area &&
               (char *)cellbuf + cellbuf->offset_nearby == (char *)initial_cell_area);
    }

    /* Does caller want us to get more space when initial cells are used up? */
    if (num_cells_expand > 0)
    {
        cellbuf->expand_bunch_len = (int)(num_cells_expand * cellbytes);
        cellbuf->context = context ? context : CurrentMemoryContext;
    }
}                               /* CdbCellBuf_Init */


/*
 * CdbCellBuf_Reset
 *
 * Free memory held by a cellbuf.  Does not free the CdbCellBuf struct.
 *
 * Should be used to clean up cellbufs initialized by CdbCellBuf_Init()
 * or CdbCellBuf_InitEasy().  May be used to empty any cellbuf for reuse.
 *
 * (NB: An acceptable alternative for cleaning up would be to destroy or
 * reset the associated MemoryContext.)
 */
void
CdbCellBuf_Reset(CdbCellBuf *cellbuf)
{
    cellbuf->nfull_total = 0;

    /* Empty the initial cell area (if any). */
    if (cellbuf->nearby_bunch_len > 0)
    {
        cellbuf->cbeg = (char *)cellbuf + cellbuf->offset_nearby;
        cellbuf->cfree = cellbuf->cbeg;
        cellbuf->cend = cellbuf->cbeg + cellbuf->nearby_bunch_len;
    }
    else
    {
        cellbuf->cbeg = NULL;
        cellbuf->cfree = NULL;
        cellbuf->cend = NULL;
    }

    /* Free all expansion bunches. */
    if (cellbuf->head)
    {
        CdbCellBuf_Bumper  *prefix = cellbuf->head;
        CdbCellBuf_Bumper  *tailprefix = cellbuf->tail;
        Size                cellspace;

        cellbuf->head = NULL;
        cellbuf->tail = NULL;
        cellspace = cellbuf->expand_bunch_len;

        /* Free all but the last expansion bunch. */
        while (prefix != tailprefix)
        {
            char               *cbeg = (char *)prefix + CDBCELLBUF_BUMPER_BYTES;
            CdbCellBuf_Bumper  *suffix = (CdbCellBuf_Bumper *)(cbeg + cellspace);
            CdbCellBuf_Bumper  *nextprefix = suffix->link;

            if (prefix->cellbuf != cellbuf ||
                suffix->cellbuf != cellbuf ||
                !nextprefix ||
                nextprefix->cellbuf != cellbuf ||
                nextprefix->link != prefix)
                break;

            pfree(prefix);
            prefix = nextprefix;
        }

        /* Free the last one. */
        if (prefix == tailprefix &&
            prefix->cellbuf == cellbuf)
            pfree(prefix);

        /* Warn if bad linkage. */
        else
        {
            char    mcname[100];

            ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
                              errmsg("Unexpected internal error"),
                              errdetail("Cellbuf error in context '%s'.",
                                        MemoryContextName(cellbuf->context,
                                                          NULL,
                                                          mcname,
                                                          sizeof(mcname))) ));
        }
    }
}                               /* CdbCellBuf_Reset */

/*-------------------------------------------------------------------------
 *              Create/Destroy dynamically allocated CdbCellBuf
 *-------------------------------------------------------------------------
 */

/*
 * CdbCellBuf_Create
 *
 * Returns a newly allocated CdbCellBuf.
 *
 * 'cellbytes' is the size of each cell.
 * 'num_initial_cells' is the suggested number of cell slots to be created
 *      immediately following the CdbCellBuf.  This allows a small number
 *      of cells to be managed with good locality and no palloc/pfree.
 *      If NULL, all cells are stored in chunks allocated from 'context'.
 *      The actual number will be rounded up to minimize wasted space.
 * 'num_cells_expand' is the suggested number of cell slots to be allocated
 *      whenever additional memory is needed.  The actual number will be
 *      rounded to fully utilize a power-of-2 chunk.  However, no expansion
 *      is done if num_cells_expand is zero; instead, after the initial
 *      cells are exhausted an error is raised.
 * 'context' is the memory context from which the CdbCellBuf and cells are
 *      to be allocated.  If NULL, the CurrentMemoryContext is used.
 */
CdbCellBuf *
CdbCellBuf_Create(size_t          cellbytes,
                  int             num_initial_cells,
                  int             num_cells_expand,
                  MemoryContext   context)
{
    return (CdbCellBuf *)CdbCellBuf_CreateWrap(sizeof(CdbCellBuf),
                                               0,   /* offsetof_cellbuf_in_struct */
                                               cellbytes,
                                               num_initial_cells,
                                               num_cells_expand,
                                               context);
}                               /* CdbCellBuf_Create */


/*
 * CdbCellBuf_Destroy
 *
 * Free a CdbCellBuf struct allocated by CdbCellBuf_Create().
 *
 * Do not use this to free a cellbuf allocated by CdbCellBuf_CreateWrap()
 * unless offsetof_cellbuf_in_struct == 0.
 *
 * NB: CdbCellBuf_Reset() can be used to free the memory owned by a cellbuf
 * without freeing the CdbCellBuf struct itself.  That is safe for all
 * cellbufs.
 *
 * NB: An acceptable alternative for cleaning up would be to destroy or
 * reset the associated MemoryContext.
 */
void
CdbCellBuf_Destroy(CdbCellBuf *cellbuf)
{
    if (cellbuf)
    {
        CdbCellBuf_Reset(cellbuf);
        cellbuf->cellbytes = 0;
        pfree(cellbuf);
    }
}                               /* CdbCellBuf_Destroy */


/*
 * CdbCellBuf_CreateWrap
 *
 * Returns zeroed, newly allocated memory for a structure of given size,
 * with a CdbCellBuf initialized at a given offset within the structure,
 * and followed by space for initial cells.
 *
 * 'sizeof_outer_struct' is the size of the struct in which the CdbCellBuf is
 *      embedded.
 * 'offsetof_cellbuf_in_struct' is the offset of the CdbCellBuf within that.
 * 'cellbytes' is the size of each cell.
 * 'num_initial_cells' is the suggested number of cell slots to be created
 *      immediately following the outer struct.  This allows a small number
 *      of cells to be managed with good locality and no palloc/pfree.
 *      If NULL, all cells are stored in chunks allocated from 'context'.
 *      The actual number will be rounded up to minimize wasted space.
 * 'num_cells_expand' is the suggested number of cell slots to be allocated
 *      whenever additional memory is needed.  The actual number will be
 *      rounded to fully utilize a power-of-2 chunk.  However, no expansion
 *      is done if num_cells_expand is zero; instead, after the initial
 *      cells are exhausted an error is raised.
 * 'context' is the memory context from which the CdbCellBuf and cells are
 *      to be allocated.  If NULL, the CurrentMemoryContext is used.
 */
void *
CdbCellBuf_CreateWrap(size_t        sizeof_outer_struct,
                      size_t        offsetof_cellbuf_in_struct,
                      size_t        cellbytes,
                      int           num_initial_cells,
                      int           num_cells_expand,
                      MemoryContext context)
{
    char       *outer_struct;

    Assert(cellbytes > 0 &&
           num_initial_cells >= 0 &&
           num_cells_expand >= 0);
    Assert(offsetof_cellbuf_in_struct >= 0 &&
           offsetof_cellbuf_in_struct + sizeof(CdbCellBuf) <= sizeof_outer_struct);

    sizeof_outer_struct = MAXALIGN(sizeof_outer_struct);

    if (!context)
        context = CurrentMemoryContext;

    /* No initial cells?  That's easy... */
    if (num_initial_cells == 0)
    {
        /* Allocate memory for outer struct. */
        outer_struct = (char *)MemoryContextAlloc(context, sizeof_outer_struct);
        memset(outer_struct, 0, sizeof_outer_struct);

        /* Set up CdbCellBuf structure within the outer struct. */
        CdbCellBuf_Init((CdbCellBuf *)(outer_struct + offsetof_cellbuf_in_struct),
                        cellbytes,
                        NULL,               /* initial_cell_area */
                        0,                  /* num_initial_cells */
                        num_cells_expand,
                        context);
    }

    /* Allocate enough space to include initial cells following the struct. */
    else
    {
        Size    allocbytes;
        Size    chunkbytes;
        Size    overhead = sizeof_outer_struct;

        /* Round up number of initial cells to fill a power-of-2 chunk. */
        chunkbytes = 32;
        while (chunkbytes < overhead + cellbytes * num_initial_cells)
            chunkbytes <<= 1;
        num_initial_cells = (int)((chunkbytes - overhead) / cellbytes);

        /* Allocate memory for outer struct plus initial cells. */
        allocbytes = sizeof_outer_struct + cellbytes * num_initial_cells;
        outer_struct = (char *)MemoryContextAlloc(context, allocbytes);
        memset(outer_struct, 0, sizeof_outer_struct);

        /* Set up CdbCellBuf structure within the outer struct. */
        CdbCellBuf_Init((CdbCellBuf *)(outer_struct + offsetof_cellbuf_in_struct),
                        cellbytes,
                        outer_struct + sizeof_outer_struct,
                        num_initial_cells,
                        num_cells_expand,
                        context);
    }

    return outer_struct;
}                               /* CdbCellBuf_CreateWrap */


/*-------------------------------------------------------------------------
 *                          Examine cellbuf state
 *-------------------------------------------------------------------------
 */

/*
 * CdbCellBuf_IsOk
 *
 * Returns true if the cellbuf passes a consistency check.
 *
 * 'cellbytes' must be the cell size which was given at initialization.
 */
bool
CdbCellBuf_IsOk(const CdbCellBuf *cellbuf, Size cellbytes)
{
    bool    ok;

    ok = (cellbuf->cbeg <= cellbuf->cfree &&
          cellbuf->cfree <= cellbuf->cend &&
          cellbuf->cellbytes == cellbytes);

    /* No nearby or expansion bunch */
    if (cellbuf->cbeg == NULL)
        ok = (cellbuf->cend == NULL &&
              cellbuf->head == NULL &&
              cellbuf->tail == NULL &&
              cellbuf->nearby_bunch_len == 0 &&
              cellbuf->nfull_total == 0);

    /* Does cellbuf have only a nearby bunch? */
    else if (cellbuf->cbeg == (char *)cellbuf + cellbuf->offset_nearby)
        ok = (cellbuf->cend == cellbuf->cbeg + cellbuf->nearby_bunch_len &&
              cellbuf->cfree == cellbuf->nfull_total * cellbytes + cellbuf->cbeg);

    /* Does cellbuf end in an expansion bunch? */
    else if (cellbuf->head)
        ok = (cellbuf->tail &&
              cellbuf->cend == cellbuf->cbeg + cellbuf->expand_bunch_len &&
              (long)(cellbuf->cfree - cellbuf->cbeg) + cellbuf->nearby_bunch_len <=
                cellbuf->nfull_total * cellbytes);
    else
        ok = false;

    return ok;
}                               /* CdbCellBuf_IsOk */


/*
 * CdbCellBuf_LastCellHelper
 *
 * Helper called by inline function CdbCellBuf_LastCell() when cfree points
 * to the first cell in a bunch and the cellbuf is nonempty.  Returns ptr to
 * the last cell in the next-to-last bunch (never NULL).
 */
void *
CdbCellBuf_LastCellHelper(CdbCellBuf *cellbuf)
{
    CdbCellBuf_Iterator it;

    CdbCellBuf_Iterator_InitToEnd(&it, cellbuf);
    return CdbCellBuf_Iterator_CellAtHelper(&it, -cellbuf->cellbytes);
}                               /* CdbCellBuf_LastCellHelper */


/*-------------------------------------------------------------------------
 *                              Cell operations
 *-------------------------------------------------------------------------
 */

/*
 * CdbCellBuf_Append_More
 *
 * Helper called by inline function CdbCellBuf_Append() to expand cellbuf.
 * Returns ptr to an uninitialized, newly allocated cell at tail of cellbuf.
 */
void *
CdbCellBuf_AppendMore(CdbCellBuf *cellbuf)
{
    CdbCellBuf_Bumper  *bunch;
    char               *cell;
    Size                allocbytes;
    int                 overhead = 2 * CDBCELLBUF_BUMPER_BYTES;

    Assert(cellbuf->cfree == cellbuf->cend);

    /*
     * On first call, round the suggested number of cells per bunch to
     * fully utilize a power-of-2 chunk.
     *
     * At the end of every expansion bunch is a suffix bumper, immediately
     * adjacent to the last cell.  The number of cells per bunch will be
     * adjusted if necessary to ensure proper alignment of the suffix bumper.
     * For instance, if cellbytes == 6 and MAXALIGN(1) == 8, we adjust the
     * number of cells per bunch to a multiple of 4, making expand_bunch_len
     * a multiple of 6*4 = 24 bytes.
     */
    if (!cellbuf->tail)
    {
        int     bunchbytes;
        int     rounddown;
        int     adjcellbytes;

        Assert(!cellbuf->head);

        /* Error if not enough initial cells and expansion not enabled. */
        if (cellbuf->expand_bunch_len == 0)
            elog(ERROR, "Cellbuf overflow; %ld cells in use",
                 cellbuf->nfull_total);

        /* Adjust bytes per bunch so suffix bumper will be MAXALIGNed. */
        adjcellbytes = cellbuf->cellbytes;
        while (adjcellbytes & (MAXALIGN(1)-1))
        {
            adjcellbytes <<= 1;
            cellbuf->expand_bunch_len += adjcellbytes;
        }

        /* Expansion bunch consists of prefix bumper + cells + suffix bumper. */
        allocbytes = overhead + cellbuf->expand_bunch_len;

        /* Round to closest power of 2, but don't round down if small. */
        bunchbytes = 128;
        while (bunchbytes < allocbytes)
            bunchbytes <<= 1;

        rounddown = bunchbytes >> 1;
        if (bunchbytes - allocbytes > allocbytes - rounddown &&
            rounddown >= 50 * cellbuf->cellbytes + overhead)
            bunchbytes = rounddown;

        /* Now deduct overhead and round down to a multiple of the cell size. */
        bunchbytes -= overhead;
        bunchbytes -= bunchbytes % adjcellbytes;
        Assert(bunchbytes >= adjcellbytes);

        cellbuf->expand_bunch_len = bunchbytes;
    }
    else
        Assert(cellbuf->head &&
               cellbuf->head->cellbuf == cellbuf &&
               cellbuf->tail->cellbuf == cellbuf);

    /* Allocate memory for prefix bumper + cells + suffix bumper. */
    allocbytes = overhead + cellbuf->expand_bunch_len;
    bunch = (CdbCellBuf_Bumper *)MemoryContextAlloc(cellbuf->context, allocbytes);

    /* Will this be the first expansion bunch? */
    if (cellbuf ->tail == NULL)
    {
        /* Initialize prefix bumper at beginning of new bunch. */
        bunch->cellbuf = cellbuf;
        bunch->link = NULL;

        cellbuf->head = bunch;
    }

    /* Add to tail of bunch list. */
    else
    {
        CdbCellBuf_Bumper  *oldtailsuffix = (CdbCellBuf_Bumper *)cellbuf->cend;

        /* Fill in suffix bumper at end of old tail (uninitialized until now) */
        Assert(cellbuf->cend == (char *)cellbuf->tail + allocbytes - CDBCELLBUF_BUMPER_BYTES);
        oldtailsuffix->cellbuf = cellbuf;
        oldtailsuffix->link = bunch;

        /* Initialize prefix bumper at beginning of new bunch. */
        bunch->cellbuf = cellbuf;
        bunch->link = cellbuf->tail;
    }
    cellbuf->tail = bunch;

    /* All cells in new bunch are free. */
    cellbuf->cbeg = (char *)bunch + CDBCELLBUF_BUMPER_BYTES;
    cellbuf->cend = (char *)bunch + allocbytes - CDBCELLBUF_BUMPER_BYTES;

    /* Allocate a cell for caller. */
    cell = cellbuf->cbeg;
    cellbuf->cfree = cell + cellbuf->cellbytes;
    return cell;
}                               /* CdbCellBuf_AppendMore */


/*
 * CdbCellBuf_PopMore
 *
 * Helper called by inline function CdbCellBuf_Pop() when cfree points
 * to the first cell in a bunch and the cellbuf is nonempty.  (Note that
 * this can occur only when there is a full bunch preceding the empty tail
 * bunch.)
 */
void *
CdbCellBuf_PopMore(CdbCellBuf *cellbuf)
{
    CdbCellBuf_Bumper  *poptail = cellbuf->tail;
    CdbCellBuf_Bumper  *predecessor = poptail->link;

    Assert(cellbuf->cbeg == (char *)poptail + CDBCELLBUF_BUMPER_BYTES &&
           cellbuf->cfree == cellbuf->cbeg &&
           cellbuf->nfull_total > 0);

    /* Freeing the only expansion bunch? */
    if (poptail == cellbuf->head)
    {
        /* Validate before freeing. */
        Insist(!predecessor && poptail->cellbuf == cellbuf);

        /* Empty the list. */
        cellbuf->head = NULL;
        cellbuf->tail = NULL;

        /* There have to be some nearby cells and they are all occupied. */
        Assert(cellbuf->nearby_bunch_len == cellbuf->nfull_total * cellbuf->cellbytes);
        cellbuf->cbeg = (char *)cellbuf + cellbuf->offset_nearby;
        cellbuf->cend = cellbuf->cbeg + cellbuf->nearby_bunch_len;
    }

    /* Retreat to the full expansion bunch preceding the empty tail. */
    else
    {
        CdbCellBuf_Bumper  *suffix;

        /* Validate tail's prefix before following the backward link. */
        Insist(predecessor && poptail->cellbuf == cellbuf);

        /* Predecessor's prefix isn't touched here except in a debug build. */
        Assert(predecessor->cellbuf == cellbuf &&
               cellbuf->nfull_total * cellbuf->cellbytes >=
                    cellbuf->nearby_bunch_len + cellbuf->expand_bunch_len);

        cellbuf->tail = predecessor;
        cellbuf->cbeg = (char *)predecessor + CDBCELLBUF_BUMPER_BYTES;
        cellbuf->cend = cellbuf->cbeg + cellbuf->expand_bunch_len;

        /* Caller is about to touch last cell; might as well validate suffix */
        suffix = (CdbCellBuf_Bumper *)cellbuf->cend;
        Insist(suffix->cellbuf == cellbuf &&
               suffix->link == poptail);
        suffix->link = NULL;
    }

    /* Pop the last cell of the full bunch which is now at the tail. */
    cellbuf->cfree = cellbuf->cend - cellbuf->cellbytes;
    cellbuf->nfull_total--;

    /* Free the empty former tail bunch. */
    poptail->cellbuf = NULL;
    poptail->link = NULL;
    pfree(poptail);

    /* Let the caller examine the popped cell. */
    return cellbuf->cfree;
}                               /* CdbCellBuf_PopMore */


/*-------------------------------------------------------------------------
 *                            CdbCellBuf_Iterator
 *-------------------------------------------------------------------------
 */

/*
 * CdbCellBuf_Iterator_Init
 *
 * Initializes caller's CdbCellBuf_Iterator struct, positioning the iterator
 * before the first cell.
 */
void
CdbCellBuf_Iterator_Init(CdbCellBuf_Iterator *it, CdbCellBuf *cellbuf)
{
    char   *beg;
    int     len;

    Assert(it &&
           cellbuf &&
           cellbuf->cellbytes > 0);

    it->cellbuf = cellbuf;
    it->begoff = 0;

    /* Position before first nearby cell. */
    if (cellbuf->nearby_bunch_len > 0)
    {
        beg = (char *)cellbuf + cellbuf->offset_nearby;
        len = cellbuf->nearby_bunch_len;
    }

    /* No nearby cells... position before first cell of first expansion bunch */
    else if (cellbuf->head)
    {
        Assert(cellbuf->tail &&
               cellbuf->tail->cellbuf == cellbuf &&
               cellbuf->head->cellbuf == cellbuf);

        /* Start at first bunch */
        beg = (char *)cellbuf->head + CDBCELLBUF_BUMPER_BYTES;
        len = cellbuf->expand_bunch_len;
    }

    /* No nearby cells and no bunches */
    else
    {
        Assert(cellbuf->tail == NULL &&
               cellbuf->nfull_total == 0);

        beg = NULL;
        len = 0;
    }

    it->cbeg = beg;
    it->cnext = beg;
    it->cend = beg + len;
}                               /* CdbCellBuf_Iterator_Init */


/*
 * CdbCellBuf_Iterator_NextCellHelper
 *
 * Private helper called by inline function CdbCellBuf_Iterator_Next().
 *
 * Advances the iterator 'it' to the beginning of the next bunch.  If the
 * bunch is empty, returns NULL.  Otherwise returns a pointer to the first
 * cell of the bunch and advances the iterator past that cell.
 */
char *
CdbCellBuf_Iterator_NextCellHelper(CdbCellBuf_Iterator *it)
{
    CdbCellBuf         *cellbuf = it->cellbuf;
    CdbCellBuf_Bumper  *nextbunch;
    char               *cell;

    /* Advancing from initial state or nearby bunch into 1st expansion bunch? */
    if (it->begoff == 0 &&
        (!it->cbeg ||
         it->cbeg == (char *)cellbuf + cellbuf->offset_nearby))
    {
        it->begoff = cellbuf->nearby_bunch_len;
        nextbunch = cellbuf->head;

        /* Caller is about to touch next cell, so might as well check prefix. */
        Insist(nextbunch &&
               nextbunch->cellbuf == cellbuf);
        Assert(nextbunch->link == NULL);
    }

    /* Advancing into an expansion bunch after the first. */
    else
    {
        CdbCellBuf_Bumper  *suffix = (CdbCellBuf_Bumper *)it->cend;

        nextbunch = suffix->link;

        /* Caller is about to touch next cell, so might as well check prefix. */
        Insist(nextbunch &&
               suffix->cellbuf == cellbuf &&
               nextbunch->cellbuf == cellbuf);
        Assert((char *)nextbunch->link == it->cbeg - CDBCELLBUF_BUMPER_BYTES);

        it->begoff += cellbuf->expand_bunch_len;
    }

    /* Compute beginning and ending cell ptrs for the new bunch. */
    it->cbeg = (char *)nextbunch + CDBCELLBUF_BUMPER_BYTES;
    it->cend = it->cbeg + cellbuf->expand_bunch_len;

    /* If new bunch is tail, it might be empty; if so, this is end of data. */
    if (it->cbeg == cellbuf->cfree)
    {
        it->cnext = it->cbeg;
        cell = NULL;
    }
    else
    {
        cell = it->cbeg;
        it->cnext = cell + cellbuf->cellbytes;
    }

    return cell;
}                               /* CdbCellBuf_Iterator_NextCellHelper */


/*
 * CdbCellBuf_Iterator_PrevCellHelper
 *
 * Private helper called by inline function CdbCellBuf_Iterator_Prev().
 *
 * Suppose C is a bunch of cells in which lies the current position of
 * iterator 'it'.  If C has no predecessor, this function returns NULL and
 * leaves the state unchanged.  If bunch B is C's immediate predecessor,
 * this function positions the iterator before the last cell of B and
 * returns a pointer to that cell.
 */
char *
CdbCellBuf_Iterator_PrevCellHelper(CdbCellBuf_Iterator *it)
{
    CdbCellBuf         *cellbuf = it->cellbuf;
    CdbCellBuf_Bumper  *prefix;
    CdbCellBuf_Bumper  *suffix;

    /* Return NULL if in initial state or first bunch. */
    if (it->begoff == 0)
        return NULL;

    Assert(it->cend - it->cbeg == cellbuf->expand_bunch_len &&
           it->begoff <= cellbuf->nfull_total * cellbuf->cellbytes);

    /* Step back to last cell of nearby bunch. */
    if (it->begoff == cellbuf->nearby_bunch_len)
    {
        it->begoff = 0;
        it->cbeg = (char *)cellbuf + cellbuf->offset_nearby;
        it->cend = it->cbeg + cellbuf->nearby_bunch_len;
    }

    /* Step back to last cell of an expansion bunch. */
    else
    {
        Assert(it->begoff >= cellbuf->nearby_bunch_len + cellbuf->expand_bunch_len);

        /* Find prefix of current bunch.  It points to predecessor's prefix. */
        prefix = (CdbCellBuf_Bumper *)(it->cbeg - CDBCELLBUF_BUMPER_BYTES);
        Insist(prefix->link &&
               prefix->cellbuf == cellbuf);

        it->cbeg = (char *)prefix->link + CDBCELLBUF_BUMPER_BYTES;
        it->cend = it->cbeg + cellbuf->expand_bunch_len;
        it->begoff -= cellbuf->expand_bunch_len;

        /* Caller is about to touch last cell, so might as well check suffix. */
        suffix = (CdbCellBuf_Bumper *)it->cend;
        Insist(suffix->cellbuf == cellbuf &&
               suffix->link == prefix);

        /* In debug build, check the new front bumper too. */
        Assert(prefix->link->cellbuf == cellbuf);
    }

    it->cnext = it->cend - cellbuf->cellbytes;
    return it->cnext;
}                               /* CdbCellBuf_Iterator_PrevCellHelper */


/*
 * CdbCellBuf_Iterator_CellAtHelper
 *
 * Private helper called by inline function CdbCellBuf_Iterator_CellAt().
 *
 * Returns a pointer to the cell at a specified offset relative to the
 * iterator's current position; or NULL if out of bounds.  Does not
 * affect the position or state of the iterator.
 */
char *
CdbCellBuf_Iterator_CellAtHelper(CdbCellBuf_Iterator *it, long relCellOff)
{
    CdbCellBuf             *cellbuf = it->cellbuf;
    long                    absCellOff;
    CdbCellBuf_Iterator     tempit;

    /* Compute offset to requested cell from beginning of cellbuf. */
    absCellOff = it->cnext - it->cbeg + it->begoff + relCellOff;

    /* Return NULL if request is beyond the bounds of the cellbuf. */
    if (absCellOff < 0 ||
        absCellOff >= cellbuf->nfull_total * cellbuf->cellbytes)
        return NULL;

    /* Find the cell, using a local copy of caller's iterator. */
    tempit = *it;
    CdbCellBuf_Iterator_SetPosHelper(&tempit, absCellOff);
    return CdbCellBuf_Iterator_NextCell(&tempit);
}                               /* CdbCellBuf_Iterator_CellAtHelper */


/*
 * CdbCellBuf_Iterator_SetPosHelper
 *
 * Private helper called by inline function CdbCellBuf_Iterator_SetPos()
 * to reposition into a bunch other than the iterator's current bunch.
 *
 * An internal error is raised if nextCellIndex is outside the bounds
 *      0 <= nextCellIndex <= CdbCellBuf_Length(it->cellbuf)
 */
void
CdbCellBuf_Iterator_SetPosHelper(CdbCellBuf_Iterator *it, long nextCellOff)
{
    CdbCellBuf         *cellbuf = it->cellbuf;
    CdbCellBuf_Bumper  *prefix;
    CdbCellBuf_Bumper  *suffix;

    /* Backward */
    if (nextCellOff < it->begoff)
    {
        /* Position into nearby bunch */
        if (nextCellOff < cellbuf->nearby_bunch_len)
        {
            Insist(nextCellOff >= 0);
            it->begoff = 0;
            it->cbeg = (char *)cellbuf + cellbuf->offset_nearby;
            it->cend = it->cbeg + cellbuf->nearby_bunch_len;
            it->cnext = it->cbeg + nextCellOff;
            return;
        }

        /* If nextCellOff is closer to head, jump to head and advance. */
        if (nextCellOff < cellbuf->nearby_bunch_len + it->begoff - nextCellOff)
        {
            it->begoff = cellbuf->nearby_bunch_len;
            prefix = cellbuf->head;
        }

        /* Retreat from current position. */
        else
            prefix = (CdbCellBuf_Bumper *)(it->cbeg - CDBCELLBUF_BUMPER_BYTES);
    }

    /* Forward */
    else
    {
        long    endoff = cellbuf->nfull_total * cellbuf->cellbytes;
        long    tailbegoff = endoff - (cellbuf->cfree - cellbuf->cbeg);

        Assert(nextCellOff >= it->cend - it->cbeg + it->begoff);
        Insist(nextCellOff <= endoff);

        /* If currently in nearby bunch, advance to first expansion bunch. */
        if (it->begoff <= cellbuf->nearby_bunch_len)
        {
            it->begoff = cellbuf->nearby_bunch_len;
            prefix = cellbuf->head;
        }

        /* Current position is in an expansion bunch.  Get its prefix. */
        else
            prefix = (CdbCellBuf_Bumper *)(it->cbeg - CDBCELLBUF_BUMPER_BYTES);

        /* If nextCellOff is closer to tail, jump to tail and retreat. */
        if (nextCellOff - it->begoff > tailbegoff - nextCellOff)
        {
            it->begoff = tailbegoff;
            prefix = cellbuf->tail;
        }
    }

    Insist(it->cbeg &&
           cellbuf->head &&
           cellbuf->tail);

    /* Retreat to bunch which contains nextCellOff. */
    while (nextCellOff < it->begoff)
    {
        Insist(prefix->cellbuf == cellbuf &&
               prefix->link);
        Assert(prefix->link->cellbuf == cellbuf);
        prefix = prefix->link;
        it->begoff -= cellbuf->expand_bunch_len;
    }

    /* Advance to bunch which contains nextCellOff. */
    while (prefix != cellbuf->tail &&
           nextCellOff >= it->begoff + cellbuf->expand_bunch_len)
    {
        suffix = (CdbCellBuf_Bumper *)((char *)prefix + cellbuf->expand_bunch_len);
        Insist(suffix->cellbuf == cellbuf &&
               suffix->link);
        Assert(suffix->link->cellbuf == cellbuf &&
               suffix->link->link == prefix);
        prefix = suffix->link;
        it->begoff += cellbuf->expand_bunch_len;
    }

    /* Position before specified cell index. */
    it->cbeg = (char *)prefix + CDBCELLBUF_BUMPER_BYTES;
    it->cend = it->cbeg + cellbuf->expand_bunch_len;
    it->cnext = nextCellOff - it->begoff + it->cbeg;

    Assert(it->cbeg <= it->cnext &&
           it->cnext <= it->cend);
}                               /* CdbCellBuf_Iterator_SetPosHelper */
