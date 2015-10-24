/*-------------------------------------------------------------------------
 *
 * cdbcellbuf.h
 *    a container for small items of constant size
 *
 * Copyright (c) 2007-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBCELLBUF_H
#define CDBCELLBUF_H

#include "cdb/cdbpublic.h"              /* CdbCellBuf */


/* Private helper functions needed by inline functions below... */
void *CdbCellBuf_AppendMore(CdbCellBuf *cellbuf);
void *CdbCellBuf_PopMore(CdbCellBuf *cellbuf);
void *CdbCellBuf_LastCellHelper(CdbCellBuf *cellbuf);


/*-------------------------------------------------------------------------
 *                           CdbCellBuf_Iterator
 *-------------------------------------------------------------------------
 * For iterating over the cells of a CdbCellBuf.
 */
typedef struct CdbCellBuf_Iterator
{
    char            *cnext;             /* next cell */
    char            *cend;              /* end of last cell of current bunch */
    CdbCellBuf      *cellbuf;
    char            *cbeg;              /* first cell of current bunch */
    long             begoff;            /* total size (bytes) of all cells in
                                         *   bunches preceding current bunch */
} CdbCellBuf_Iterator;

/* Private helper functions needed by inline functions below... */
char *CdbCellBuf_Iterator_NextCellHelper(CdbCellBuf_Iterator *it);
char *CdbCellBuf_Iterator_PrevCellHelper(CdbCellBuf_Iterator *it);
char *CdbCellBuf_Iterator_CellAtHelper(CdbCellBuf_Iterator *it, long relCellOff);
void CdbCellBuf_Iterator_SetPosHelper(CdbCellBuf_Iterator *it, long nextCellOff);


/*-------------------------------------------------------------------------
 *                            CdbCellBuf_Bumper
 *-------------------------------------------------------------------------
 * Each dynamically allocated expansion bunch of cells begins and ends with
 * a bumper.  At the beginning is a prefix bumper whose 'link' field points
 * to the prefix bumper of the previous expansion bunch (or NULL in the first
 * expansion bunch).  At the end of the bunch is a suffix bumper whose 'link'
 * field points to the prefix bumper of the next expansion bunch.  The suffix
 * bumper of the last expansion bunch is uninitialized.  Between the prefix
 * and suffix bumper are cells in the quantity 'cellbuf->num_cells_expand'.
 */
typedef struct CdbCellBuf_Bumper
{
    CdbCellBuf                 *cellbuf;    /* owner of bunch */
    struct CdbCellBuf_Bumper   *link;       /* prefix: -> prev bunch pfx or NULL.
                                             * suffix: -> prefix of next bunch.
                                             */
} CdbCellBuf_Bumper;
#define CDBCELLBUF_BUMPER_BYTES  (MAXALIGN(sizeof(CdbCellBuf_Bumper)))



/*-------------------------------------------------------------------------
 *                    Initialize/Reset caller's CdbCellBuf
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
CdbCellBuf_InitEasy(CdbCellBuf *cellbuf, Size cellbytes);

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
 * 'context' is the memory context from which to allocate space for
 *      additional cells when there is no more room in initial_cell_area.
 *      If NULL, the CurrentMemoryContext is used.
 */
void
CdbCellBuf_Init(CdbCellBuf     *cellbuf,
                size_t          cellbytes,
                void           *initial_cell_area,
                int             num_initial_cells,
                int             num_cells_expand,
                MemoryContext   context);

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
 *
 * Iterators (CdbCellBuf_Iterator) become invalid when their cellbuf is
 * reset; afterward the caller should abandon or reinitialize them.
 */
void
CdbCellBuf_Reset(CdbCellBuf *cellbuf);

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
                  MemoryContext   context);

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
CdbCellBuf_Destroy(CdbCellBuf *cellbuf);

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
 *      immediately following the struct.  This allows a small number
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
                      MemoryContext context);


/*-------------------------------------------------------------------------
 *                          Examine cellbuf state
 *-------------------------------------------------------------------------
 */

/*
 * CdbCellBuf_Context
 *
 * Returns the memory context used for allocating additional bunches of cells.
 */
static inline MemoryContext
CdbCellBuf_Context(const CdbCellBuf *cellbuf)
{
    return cellbuf->context;
}                               /* CdbCellBuf_Context */


/*
 * CdbCellBuf_IsEmpty
 *
 * Returns true if cellbuf is empty.
 */
static inline bool
CdbCellBuf_IsEmpty(const CdbCellBuf *cellbuf)
{
    return cellbuf->nfull_total == 0;
}                               /* CdbCellBuf_IsEmpty */


/*
 * CdbCellBuf_IsOk
 *
 * Returns true if the cellbuf passes a consistency check.
 *
 * 'cellbytes' must be the cell size which was given at initialization.
 */
bool
CdbCellBuf_IsOk(const CdbCellBuf *cellbuf, Size cellbytes);


/*
 * CdbCellBuf_Length
 *
 * Returns the number of cells in use.
 */
static inline long
CdbCellBuf_Length(const CdbCellBuf *cellbuf)
{
    return cellbuf->nfull_total;
}                               /* CdbCellBuf_Length */


/*
 * CdbCellBuf_FirstCell
 *
 * Returns a pointer to the first cell, or NULL if the cellbuf is empty.
 */
static inline void *
CdbCellBuf_FirstCell(CdbCellBuf *cellbuf)
{
    char   *firstcell = NULL;

    if (cellbuf->nearby_bunch_len > 0)
        firstcell = (char *)cellbuf + cellbuf->offset_nearby;
    else if (cellbuf->head)
    {
        Assert(cellbuf->head->cellbuf == cellbuf);
        firstcell = (char *)cellbuf->head + CDBCELLBUF_BUMPER_BYTES;
    }

    if (firstcell == cellbuf->cfree)
        firstcell = NULL;

    Assert((firstcell != NULL) == (cellbuf->nfull_total > 0));

    return firstcell;
}                               /* CdbCellBuf_FirstCell */


/*
 * CdbCellBuf_LastCell
 *
 * Returns a pointer to the last cell, or NULL if the cellbuf is empty.
 */
static inline void *
CdbCellBuf_LastCell(CdbCellBuf *cellbuf)
{
    char   *lastcell;

    if (cellbuf->cbeg < cellbuf->cfree)
    {
        Assert(cellbuf->cbeg + cellbuf->cellbytes <= cellbuf->cfree &&
               cellbuf->cfree <= cellbuf->cend);
        lastcell = cellbuf->cfree - cellbuf->cellbytes;
    }
    else if (cellbuf->nfull_total > 0)
        lastcell = CdbCellBuf_LastCellHelper(cellbuf);
    else
        lastcell = NULL;

    return lastcell;
}                               /* CdbCellBuf_LastCell */


/*-------------------------------------------------------------------------
 *                              Cell operations
 *-------------------------------------------------------------------------
 */

/*
 * CdbCellBuf_AppendCell
 *
 * Allocate a cell at tail of cellbuf.
 *
 * Returns pointer to the cell.  The cell has not been initialized.
 */
static inline void *
CdbCellBuf_AppendCell(CdbCellBuf *cellbuf)
{
    void   *cell;

    Assert(cellbuf &&
           cellbuf->cellbytes > 0);

    if (cellbuf->cfree < cellbuf->cend)
    {
        Assert(cellbuf->cbeg &&
               cellbuf->cbeg <= cellbuf->cfree &&
               cellbuf->cfree + cellbuf->cellbytes <= cellbuf->cend);

        cell = cellbuf->cfree;
        cellbuf->cfree += cellbuf->cellbytes;
    }
    else
        cell = CdbCellBuf_AppendMore(cellbuf);

    cellbuf->nfull_total++;
    return cell;
}                               /* CdbCellBuf_AppendCell */


/*
 * CdbCellBuf_PopCell
 *
 * Deallocate a cell at tail of cellbuf.
 *
 * Returns pointer to the cell, or NULL if cellbuf is empty.  The contents
 * of the popped cell remain valid until the next cell operation, or until
 * CdbCellBuf_Reset() or CdbCellBuf_Destroy().
 */
static inline void *
CdbCellBuf_PopCell(CdbCellBuf *cellbuf)
{
    void   *cell;

    Assert(cellbuf &&
           cellbuf->cellbytes > 0);

    if (cellbuf->cbeg < cellbuf->cfree)
    {
        Assert(cellbuf->cbeg &&
               cellbuf->cbeg + cellbuf->cellbytes <= cellbuf->cfree &&
               cellbuf->cfree <= cellbuf->cend);

        cell = cellbuf->cfree - cellbuf->cellbytes;
        cellbuf->cfree = cell;
        cellbuf->nfull_total--;
    }
    else if (cellbuf->nfull_total > 0)
        cell = CdbCellBuf_PopMore(cellbuf);
    else
        cell = NULL;

    return cell;
}                               /* CdbCellBuf_PopCell */


/*-------------------------------------------------------------------------
 *                            CdbCellBuf_Iterator
 *-------------------------------------------------------------------------
 */

/*
 * CdbCellBuf_Iterator_Init
 *
 * Initializes caller's CdbCellBuf_Iterator struct, positioning the iterator
 * before the first cell.
 *
 * It is ok to delete items (via CdbCellBuf_PopCell()) that are after the
 * iterator position.  However, deletion of an item that precedes the iterator
 * position can leave the iterator in an inconsistent state; you must take care
 * not to use that iterator any more after popping a preceding item.
 *
 * The following should not be called while iterating: CdbCellBuf_Reset(),
 * CdbCellBuf_Destroy().
 */
void
CdbCellBuf_Iterator_Init(CdbCellBuf_Iterator *it, CdbCellBuf *cellbuf);


/*
 * CdbCellBuf_Iterator_InitToEnd
 *
 * Initializes caller's CdbCellBuf_Iterator struct, positioning the iterator
 * after the last cell.  Equivalent to:
 *
 *      CdbCellBuf_Iterator_Init(it, cellbuf);
 *      CdbCellBuf_Iterator_SetPos(it, CdbCellBuf_Length(cellbuf));
 */
static inline void
CdbCellBuf_Iterator_InitToEnd(CdbCellBuf_Iterator *it, CdbCellBuf *cellbuf)
{
    Assert(it &&
           cellbuf &&
           cellbuf->cellbytes > 0);

    it->cnext = cellbuf->cfree;
    it->cend = cellbuf->cend;
    it->cellbuf = cellbuf;
    it->cbeg = cellbuf->cbeg;
    it->begoff = cellbuf->nfull_total * cellbuf->cellbytes -
                    (cellbuf->cfree - cellbuf->cbeg);
}                               /* CdbCellBuf_Iterator_InitToEnd */


/*
 * CdbCellBuf_Iterator_NextCell
 *
 * If iterator 'it' is positioned before a cell, returns a pointer to the cell
 * and advances the iterator past that cell.  Else returns NULL and leaves the
 * iterator unchanged.
 *
 * The iterator will reach new items added by CdbCellBuf_Append() during
 * or after the traversal.  Even when the end has been reached, the caller
 * can add more items and continue calling CdbCellBuf_Iterator_NextCell() to
 * retrieve them.
 */
static inline void *
CdbCellBuf_Iterator_NextCell(CdbCellBuf_Iterator *it)
{
    char   *cell = it->cnext;

    if (cell == it->cellbuf->cfree)
        cell = NULL;
    else if (cell < it->cend)
    {
        Assert(cell >= it->cbeg);
        it->cnext += it->cellbuf->cellbytes;
    }
    else
    {
        Assert(cell == it->cend);
        cell = CdbCellBuf_Iterator_NextCellHelper(it);
    }

    return cell;
}                               /* CdbCellBuf_Iterator_NextCell */


/*
 * CdbCellBuf_Iterator_PrevCell
 *
 * If iterator 'it' is positioned after a cell, returns a pointer to the
 * cell and repositions the iterator before that cell.  Else returns NULL
 * and leaves the iterator unchanged.
 */
static inline void *
CdbCellBuf_Iterator_PrevCell(CdbCellBuf_Iterator *it)
{
    char   *cell;

    if (it->cnext > it->cbeg)
    {
        Assert(it->cnext <= it->cend);
        it->cnext -= it->cellbuf->cellbytes;
        cell = it->cnext;
    }
    else
    {
        Assert(it->cnext == it->cbeg);
        cell = CdbCellBuf_Iterator_PrevCellHelper(it);
    }

    return cell;
}                               /* CdbCellBuf_Iterator_PrevCell */


/*
 * CdbCellBuf_Iterator_CellAt
 *
 * Returns a pointer to the cell at a specified position relative to the
 * iterator's current position; or NULL if the specified position would
 * be out of bounds.  Does not affect the position or state of the iterator.
 *
 * relCellIndex == 0 returns the same value that CdbCellBuf_Iterator_NextCell
 * would return, but does not advance the iterator.
 *
 * relCellIndex == -1 returns the same value that CdbCellBuf_Iterator_PrevCell
 * would return, without repositioning the iterator.
 */
static inline void *
CdbCellBuf_Iterator_CellAt(CdbCellBuf_Iterator *it, long relCellIndex)
{
    long    relCellOff = relCellIndex * it->cellbuf->cellbytes;
    char   *cell = it->cnext + relCellOff;

    if (cell >= it->cbeg &&
        cell < it->cend)
    {
        if (relCellIndex >= 0 &&
            it->cend == it->cellbuf->cend &&
            cell >= it->cellbuf->cfree)
            cell = NULL;
    }
    else
        cell = CdbCellBuf_Iterator_CellAtHelper(it, relCellOff);

    return cell;
}                               /* CdbCellBuf_Iterator_CellAt */


/*
 * CdbCellBuf_Iterator_GetPos
 *
 * Returns the number of cells preceding the current iterator position.
 */
static inline long
CdbCellBuf_Iterator_GetPos(const CdbCellBuf_Iterator *it)
{
    CdbCellBuf         *cellbuf = it->cellbuf;
    long                pos;

    Assert(cellbuf &&
           cellbuf->cellbytes > 0 &&
           it->cnext >= it->cbeg &&
           it->cnext <= it->cend);

    pos = (long)(it->cnext - it->cbeg + it->begoff) / cellbuf->cellbytes;

    Assert(pos >= 0 &&
           pos <= cellbuf->nfull_total);

    return pos;
}                               /* CdbCellBuf_Iterator_GetPos */


/*
 * CdbCellBuf_Iterator_SetPos
 *
 * Positions the iterator before the cell whose 0-based index is
 * 'nextCellIndex'.  The first call to CdbCellBuf_Iterator_NextCell()
 * will return a pointer to the specified cell (or NULL if the starting
 * position equals the number of occupied cells.)
 *
 * If nextCellIndex == 0, positions the iterator before the first cell.
 * If nextCellIndex == CdbCellBuf_Length(), positions the iterator after
 * the last cell.
 *
 * An internal error is raised if nextCellIndex is outside the bounds
 *      0 <= nextCellIndex <= CdbCellBuf_Length(it->cellbuf)
 */
static inline void
CdbCellBuf_Iterator_SetPos(CdbCellBuf_Iterator *it, long nextCellIndex)
{
    long    nextCellOff;

    Insist(nextCellIndex <= it->cellbuf->nfull_total);

    nextCellOff = nextCellIndex * it->cellbuf->cellbytes;

    /* Reposition within current block? */
    if (nextCellOff >= it->begoff &&
        nextCellOff <= (long)(it->cend - it->cbeg) + it->begoff)
        it->cnext = nextCellOff - it->begoff + it->cbeg;

    else
        CdbCellBuf_Iterator_SetPosHelper(it, nextCellOff);
}                               /* CdbCellBuf_Iterator_SetPos */


/*
 * CdbCellBuf_Iterator_AtBegin
 *
 * Returns 'true' if iterator is positioned before the first cell or if
 * the cellbuf is empty.  Returns 'false' if there is a cell preceding
 * the iterator position.
 */
static inline bool
CdbCellBuf_Iterator_AtBegin(const CdbCellBuf_Iterator *it)
{
    return (it->cbeg == it->cnext &&
            it->begoff == 0);
}                               /* CdbCellBuf_Iterator_AtBegin */


/*
 * CdbCellBuf_Iterator_AtEnd
 *
 * Returns 'true' if iterator is positioned after the last cell or if
 * the cellbuf is empty, i.e., if CdbCellBuf_Iterator_NextCell() would
 * return NULL.  Returns 'false' if iterator is positioned before a cell.
 */
static inline bool
CdbCellBuf_Iterator_AtEnd(const CdbCellBuf_Iterator *it)
{
    CdbCellBuf *cellbuf = it->cellbuf;
    bool        atend;

    if (it->cnext == cellbuf->cfree)
        atend = true;

    /* Maybe tail bunch is empty and iterator is at end of previous bunch */
    else if (it->cnext == it->cend &&
             it->cnext - it->cbeg + it->begoff == cellbuf->nfull_total * cellbuf->cellbytes)
        atend = true;

    else
        atend = false;

    return atend;
}                               /* CdbCellBuf_Iterator_AtEnd */


/*
 * CdbCellBuf_Iterator_Length
 *
 * Returns the number of cells in the associated cellbuf.
 *
 * Equivalent to
 *      CdbCellBuf_Length(it->cellbuf)
 */
static inline long
CdbCellBuf_Iterator_Length(const CdbCellBuf_Iterator *it)
{
    return it->cellbuf->nfull_total;
}                               /* CdbCellBuf_Iterator_Length */


#endif   /* CDBCELLBUF_H */
