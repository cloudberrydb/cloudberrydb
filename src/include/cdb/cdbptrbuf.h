/*-------------------------------------------------------------------------
 *
 * cdbptrbuf.h
 *    a container for pointers, layered on CdbPtrBuf
 *
 * Copyright (c) 2007-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPTRBUF_H
#define CDBPTRBUF_H

#include "cdb/cdbcellbuf.h"             /* CdbCellBuf */

typedef void   *CdbPtrBuf_Ptr;          /* type of pointer held by CdbPtrBuf */


typedef struct CdbPtrBuf
{
    CdbCellBuf              cellbuf;
} CdbPtrBuf;

typedef struct CdbPtrBuf_Iterator
{
    CdbCellBuf_Iterator     it;
} CdbPtrBuf_Iterator;

/*-------------------------------------------------------------------------
 *                    Initialize/Reset caller's CdbPtrBuf
 *-------------------------------------------------------------------------
 */

/*
 * CdbPtrBuf_InitEasy
 *
 * Initialize caller's CdbPtrBuf struct (easy-to-call version).
 *
 * 'ptrbuf' is the area to be initialized as a CdbPtrBuf.
 */
static inline void
CdbPtrBuf_InitEasy(CdbPtrBuf *ptrbuf)
{
    CdbCellBuf_InitEasy(&ptrbuf->cellbuf, sizeof(CdbPtrBuf_Ptr));
}

/*
 * CdbPtrBuf_Init
 *
 * Initialize caller's CdbPtrBuf struct.
 *
 * 'ptrbuf' is the area to be initialized as a CdbPtrBuf.
 * 'initial_cell_area' is an optional memory area, properly aligned, where
 *      the first few pointers can be stored.  It must be nearby the CdbPtrBuf
 *      itself, typically in the same struct or local variable stack frame.
 *      This allows a small number of cells to be managed with good locality
 *      and no palloc/pfree.  If NULL, memory is allocated from 'context'
 *      when needed.
 * 'num_initial_cells' is the exact number of cells in the initial_cell_area.
 * 'num_cells_expand' is the suggested number of ptr cells to be allocated
 *      whenever additional memory is needed.  The actual number will be
 *      rounded to fully utilize a power-of-2.  However, no expansion is
 *      done if num_cells_expand is zero; instead, after the initial cells
 *      are exhausted an error is raised.
 * 'context' is the memory context from which to allocate chunks of space
 *      for additional cells when there is no more room in initial_cell_area.
 *      If NULL, the CurrentMemoryContext is used.
 */
static inline void
CdbPtrBuf_Init(CdbPtrBuf       *ptrbuf,
               CdbPtrBuf_Ptr   *initial_cell_area,
               int              num_initial_cells,
               int              num_cells_expand,
               MemoryContext    context)
{
    CdbCellBuf_Init(&ptrbuf->cellbuf,
                    sizeof(*initial_cell_area),
                    initial_cell_area,
                    num_initial_cells,
                    num_cells_expand,
                    context);
}

/*
 * CdbPtrBuf_Reset
 *
 * Free memory held by a ptrbuf.  Does not free the CdbPtrBuf struct.
 *
 * Should be used to clean up a ptrbuf initialized by CdbPtrBuf_Init().
 * (NB: An acceptable alternative for cleaning up would be to destroy or
 * reset the associated MemoryContext.)
 *
 * Iterators (CdbPtrBuf_Iterator) become invalid when their ptrbuf is
 * reset; afterward the caller should abandon or reinitialize them.
 */
static inline void
CdbPtrBuf_Reset(CdbPtrBuf *ptrbuf)
{
    CdbCellBuf_Reset(&ptrbuf->cellbuf);
}

/*-------------------------------------------------------------------------
 *              Create/Destroy dynamically allocated CdbPtrBuf
 *-------------------------------------------------------------------------
 */

/*
 * CdbPtrBuf_CreateWrap
 *
 * Returns zeroed, newly allocated memory for a structure of given size,
 * with a CdbPtrBuf initialized at a given offset within the structure,
 * and followed by space for initial cells.
 *
 * 'sizeof_outer_struct' is the size of the struct in which the CdbPtrBuf is
 *      embedded.
 * 'offsetof_ptrbuf_in_struct' is the offset of the CdbPtrBuf within that.
 * 'num_initial_cells' is the suggested number of ptr cells to be created
 *      immediately following the struct.  This allows a small number
 *      of cells to be managed with good locality and no palloc/pfree.
 *      If NULL, all cells are stored in chunks allocated from 'context'.
 *      The actual number will be rounded up to minimize wasted space.
 * 'num_cells_expand' is the suggested number of ptr cells to be allocated
 *      whenever additional memory is needed.  The actual number will be
 *      rounded to fully utilize a power-of-2 chunk assuming 'context'
 *      is a standard memory context as implemented by aset.c.  However, no
 *      expansion is done if num_cells_expand is zero; instead, after the
 *      initial cells are exhausted an error is raised.
 * 'context' is the memory context from which the CdbPtrBuf and cells are
 *      to be allocated.  If NULL, the CurrentMemoryContext is used.
 */
static inline void *
CdbPtrBuf_CreateWrap(size_t         sizeof_outer_struct,
                     size_t         offsetof_ptrbuf_in_struct,
                     int            num_initial_cells,
                     int            num_cells_expand,
                     MemoryContext  context)
{
    Assert(offsetof_ptrbuf_in_struct >= 0 &&
           offsetof_ptrbuf_in_struct + sizeof(CdbPtrBuf) <= sizeof_outer_struct);

    return CdbCellBuf_CreateWrap(sizeof_outer_struct,
                                 offsetof_ptrbuf_in_struct + offsetof(CdbPtrBuf, cellbuf),
                                 sizeof(CdbPtrBuf_Ptr),
                                 num_initial_cells,
                                 num_cells_expand,
                                 context);
}

/*
 * CdbPtrBuf_Create
 *
 * Returns a newly allocated CdbPtrBuf.
 *
 * 'num_initial_cells' is the number of ptr cells to be created immediately
 *      following the CdbPtrBuf.  This allows a small number of cells to be
 *      managed with good locality and no palloc/pfree.  If NULL, all cells
 *      are stored in chunks allocated from 'context'.
 *      The actual number will be rounded up to minimize wasted space.
 * 'num_cells_expand' is the suggested number of ptr cells to be allocated
 *      whenever additional memory is needed.  The actual number will be
 *      rounded to fully utilize a power-of-2 chunk assuming 'context'
 *      is a standard memory context as implemented by aset.c.  However, no
 *      expansion is done if num_cells_expand is zero; instead, after the
 *      initial cells are exhausted an error is raised.
 * 'context' is the memory context from which the CdbPtrBuf and cells are
 *      to be allocated.  If NULL, the CurrentMemoryContext is used.
 */
static inline CdbPtrBuf *
CdbPtrBuf_Create(int            num_initial_cells,
                 int            num_cells_expand,
                 MemoryContext  context)
{
    return (CdbPtrBuf *)CdbCellBuf_CreateWrap(sizeof(CdbPtrBuf),
                                              offsetof(CdbPtrBuf, cellbuf),
                                              sizeof(CdbPtrBuf_Ptr),
                                              num_initial_cells,
                                              num_cells_expand,
                                              context);
}

/*
 * CdbPtrBuf_Destroy
 *
 * Free a CdbPtrBuf struct allocated by CdbPtrBuf_Create().
 *
 * Do not use this to free a cellbuf allocated by CdbPtrBuf_CreateWrap()
 * unless offsetof_ptrbuf_in_struct == 0.
 *
 * NB: CdbPtrBuf_Reset() can be used to free the memory owned by a ptrbuf
 * without freeing the CdbPtrBuf struct itself.  That is safe for all
 * ptrbufs.
 *
 * (NB: An acceptable alternative for cleaning up would be to destroy or
 * reset the associated MemoryContext.)
 */
static inline void
CdbPtrBuf_Destroy(CdbPtrBuf *ptrbuf)
{
    CdbCellBuf_Destroy((CdbCellBuf *)ptrbuf);
}

/*-------------------------------------------------------------------------
 *                            Examine ptrbuf state
 *-------------------------------------------------------------------------
 */

/*
 * CdbPtrBuf_Context
 *
 * Returns the memory context used for allocating additional bunches of cells.
 */
static inline MemoryContext
CdbPtrBuf_Context(const CdbCellBuf *cellbuf)
{
    return CdbCellBuf_Context(cellbuf);
}                               /* CdbPtrBuf_Context */

/*
 * CdbPtrBuf_IsEmpty
 *
 * Returns true if ptrbuf is empty.
 */
static inline bool
CdbPtrBuf_IsEmpty(const CdbPtrBuf *ptrbuf)
{
    return CdbCellBuf_IsEmpty(&ptrbuf->cellbuf);
}                               /* CdbPtrBuf_IsEmpty */

/*
 * CdbPtrBuf_IsOk
 *
 * Returns true if the ptrbuf passes a consistency check.
 */
static inline bool
CdbPtrBuf_IsOk(const CdbPtrBuf *ptrbuf)
{
    return CdbCellBuf_IsOk(&ptrbuf->cellbuf, sizeof(CdbPtrBuf_Ptr));
}                               /* CdbPtrBuf_IsOk */

/*
 * CdbPtrBuf_FirstCell
 *
 * Returns a pointer to the head cell, or NULL if the ptrbuf is empty.
 */
static inline CdbPtrBuf_Ptr *
CdbPtrBuf_FirstCell(CdbPtrBuf *ptrbuf)
{
    return (CdbPtrBuf_Ptr *)CdbCellBuf_FirstCell(&ptrbuf->cellbuf);
}                               /* CdbPtrBuf_FirstCell */

/*
 * CdbPtrBuf_LastCell
 *
 * Returns a pointer to the tail cell, or NULL if the ptrbuf is empty.
 */
static inline CdbPtrBuf_Ptr *
CdbPtrBuf_LastCell(CdbPtrBuf *ptrbuf)
{
    return (CdbPtrBuf_Ptr *)CdbCellBuf_LastCell(&ptrbuf->cellbuf);
}                               /* CdbPtrBuf_LastCell */

/*
 * CdbPtrBuf_First
 *
 * Returns contents of the head cell, or NULL if the ptrbuf is empty.
 *
 * Note that a NULL return value is ambiguous: it could be that the ptrbuf
 * is empty, or that the requested cell contains NULL.
 */
static inline CdbPtrBuf_Ptr
CdbPtrBuf_First(CdbPtrBuf *ptrbuf)
{
    CdbPtrBuf_Ptr  *pp = CdbPtrBuf_FirstCell(ptrbuf);

    return pp ? *pp : NULL;
}                               /* CdbPtrBuf_First */

/*
 * CdbPtrBuf_Last
 *
 * Returns contents of the tail cell, or NULL if the ptrbuf is empty.
 *
 * Note that a NULL return value is ambiguous: it could be that the ptrbuf
 * is empty, or that the requested cell contains NULL.
 */
static inline CdbPtrBuf_Ptr
CdbPtrBuf_Last(CdbPtrBuf *ptrbuf)
{
    CdbPtrBuf_Ptr  *pp = CdbPtrBuf_LastCell(ptrbuf);

    return pp ? *pp : NULL;
}                               /* CdbPtrBuf_Last */

/*
 * CdbPtrBuf_Length
 *
 * Returns the number of ptrs in use.
 */
static inline unsigned
CdbPtrBuf_Length(CdbPtrBuf *ptrbuf)
{
    return CdbCellBuf_Length(&ptrbuf->cellbuf);
}                               /* CdbPtrBuf_Length */

/*-------------------------------------------------------------------------
 *                              Cell operations
 *-------------------------------------------------------------------------
 */

/*
 * CdbPtrBuf_Append
 *
 * Append an item at tail of ptrbuf.  Returns pointer to the new cell.
 */
static inline CdbPtrBuf_Ptr *
CdbPtrBuf_Append(CdbPtrBuf *ptrbuf, CdbPtrBuf_Ptr item)
{
    CdbPtrBuf_Ptr  *pp = (CdbPtrBuf_Ptr *)CdbCellBuf_AppendCell(&ptrbuf->cellbuf);

    *pp = item;
    return pp;
}                               /* CdbPtrBuf_Append */

/*
 * CdbPtrBuf_Pop
 *
 * Remove last item from tail of ptrbuf.  Returns the item, or NULL if
 * ptrbuf is empty.
 */
static inline CdbPtrBuf_Ptr
CdbPtrBuf_Pop(CdbPtrBuf *ptrbuf)
{
    CdbPtrBuf_Ptr  *pp = (CdbPtrBuf_Ptr *)CdbCellBuf_PopCell(&ptrbuf->cellbuf);

    return pp ? *pp : NULL;
}                               /* CdbPtrBuf_Pop */

/*
 * CdbPtrBuf_PopCell
 *
 * Remove last item from tail of ptrbuf.  Returns pointer to the cell
 * containing the item; it remains valid until the next append, pop,
 * reset or destroy.  Returns NULL if ptrbuf is empty.
 */
static inline CdbPtrBuf_Ptr *
CdbPtrBuf_PopCell(CdbPtrBuf *ptrbuf)
{
    return (CdbPtrBuf_Ptr *)CdbCellBuf_PopCell(&ptrbuf->cellbuf);
}                               /* CdbPtrBuf_PopCell */


/*-------------------------------------------------------------------------
 *                             CdbPtrBuf_Iterator
 *-------------------------------------------------------------------------
 */

/*
 * CdbPtrBuf_Iterator_Init
 *
 * Initializes caller's CdbPtrBuf_Iterator struct, positioning the iterator
 * before the first cell.
 */
static inline void
CdbPtrBuf_Iterator_Init(CdbPtrBuf_Iterator *it, CdbPtrBuf *ptrbuf)
{
    CdbCellBuf_Iterator_Init(&it->it, &ptrbuf->cellbuf);
}                               /* CdbPtrBuf_Iterator_Init */

/*
 * CdbPtrBuf_Iterator_InitToEnd
 *
 * Initializes caller's CdbPtrBuf_Iterator struct, positioning the iterator
 * after the last cell.  Equivalent to:
 *
 *      CdbPtrBuf_Iterator_Init(it, ptrbuf);
 *      CdbPtrBuf_Iterator_SetPos(it, CdbPtrBuf_Length(ptrbuf));
 */
static inline void
CdbPtrBuf_Iterator_InitToEnd(CdbPtrBuf_Iterator *it, CdbPtrBuf *ptrbuf)
{
    CdbCellBuf_Iterator_InitToEnd(&it->it, &ptrbuf->cellbuf);
}                               /* CdbPtrBuf_Iterator_InitToEnd */


/*
 * CdbPtrBuf_Iterator_NextCell
 *
 * If iterator 'it' is positioned before a cell, returns a pointer to the cell
 * and advances the iterator past that cell.  Else returns NULL and leaves the
 * iterator unchanged.
 *
 * This can be used to modify the values stored in the ptrbuf.  It can also
 * be used in case there are NULL pointers in the ptrbuf because the return
 * value is non-NULL until the end of iteration.
 *
 * NB: The iterator will reach new items added by CdbPtrBuf_Append() during
 * or after the traversal.  Even when the end has been reached, the caller
 * can add more items and continue calling CdbPtrBuf_Iterator_Next() or
 * CdbPtrBuf_Iterator_NextCell() to retrieve them.
 */
static inline CdbPtrBuf_Ptr *
CdbPtrBuf_Iterator_NextCell(CdbPtrBuf_Iterator *it)
{
    return (CdbPtrBuf_Ptr *)CdbCellBuf_Iterator_NextCell(&it->it);
}                               /* CdbPtrBuf_Iterator_NextCell */

/*
 * CdbPtrBuf_Iterator_Next
 *
 * If iterator 'it' is positioned before a cell, returns the contents of the
 * cell and advances the iterator past that cell.  Else returns NULL and leaves
 * the iterator unchanged.
 *
 * Note that a NULL return value is ambiguous if there are NULL values in the
 * ptrbuf: the caller can't tell whether the result is an actual NULL ptr value
 * or the end of iteration.  Suggested solutions: (a) store only non-NULL
 * values in the ptrbuf; or (b) use CdbPtrBuf_Iterator_NextCell() instead of
 * CdbPtrBuf_Iterator_Next(); or (c) use CdbPtrBuf_Iterator_AtEnd() to detect
 * end of iteration; or (d) iterate from [0..CdbPtrBuf_Iterator_Length()-1] or
 * [0..CdbPtrBuf_Length()-1].
 *
 * NB: The iterator will reach new items added by CdbPtrBuf_Append() during
 * or after the traversal.  Even when the end has been reached, the caller
 * can add more items and continue calling CdbPtrBuf_Iterator_Next() or
 * CdbPtrBuf_Iterator_NextCell() to retrieve them.
 */
static inline CdbPtrBuf_Ptr
CdbPtrBuf_Iterator_Next(CdbPtrBuf_Iterator *it)
{
    CdbPtrBuf_Ptr  *pp = CdbPtrBuf_Iterator_NextCell(it);

    return pp ? *pp : NULL;
}                               /* CdbPtrBuf_Iterator_Next */

/*
 * CdbPtrBuf_Iterator_PrevCell
 *
 * If iterator 'it' is positioned after a cell, returns a pointer to the
 * cell and repositions the iterator before that cell.  Else returns NULL
 * and leaves the iterator unchanged.
 *
 * This can be used to modify the values stored in the ptrbuf.  It can also
 * be used in case there are NULL pointers in the ptrbuf because the return
 * value is non-NULL until the end of iteration.
 */
static inline CdbPtrBuf_Ptr *
CdbPtrBuf_Iterator_PrevCell(CdbPtrBuf_Iterator *it)
{
    return (CdbPtrBuf_Ptr *)CdbCellBuf_Iterator_PrevCell(&it->it);
}                               /* CdbPtrBuf_Iterator_PrevCell */

/*
 * CdbPtrBuf_Iterator_Prev
 *
 * If iterator 'it' is positioned after a cell, returns the contents of the
 * cell and repositions the iterator before that cell.  Else returns NULL
 * and leaves the iterator unchanged.
 *
 * Note that a NULL return value is ambiguous if there are NULL values in the
 * ptrbuf: the caller can't tell whether the result is an actual NULL ptr value
 * or the end of iteration.  Suggested solutions: (a) store only non-NULL
 * values in the ptrbuf; or (b) use CdbPtrBuf_Iterator_NextCell() instead of
 * CdbPtrBuf_Iterator_Next(); or (c) use CdbPtrBuf_Iterator_AtEnd() to detect
 * end of iteration; or (d) iterate from [0..CdbPtrBuf_Iterator_Length()-1] or
 * [0..CdbPtrBuf_Length()-1].
 */
static inline CdbPtrBuf_Ptr
CdbPtrBuf_Iterator_Prev(CdbPtrBuf_Iterator *it)
{
    CdbPtrBuf_Ptr  *pp = CdbPtrBuf_Iterator_PrevCell(it);

    return pp ? *pp : NULL;
}                               /* CdbPtrBuf_Iterator_Prev */

/*
 * CdbPtrBuf_Iterator_CellAt
 *
 * Returns a pointer to the cell at a specified position relative to the
 * iterator's current position; or NULL if the specified position would
 * be out of bounds.  Does not affect the position or state of the iterator.
 *
 * relCellIndex == 0 returns the same value that CdbPtrBuf_Iterator_NextCell
 * would return, but does not advance the iterator.
 *
 * relCellIndex == -1 returns the same value that CdbPtrBuf_Iterator_PrevCell
 * would return, without repositioning the iterator.
 *
 * This can be used to modify the values stored in the ptrbuf.  It can also
 * be used in case there are NULL pointers in the ptrbuf because the return
 * value is non-NULL for relCellIndex in bounds.
 */
static inline CdbPtrBuf_Ptr *
CdbPtrBuf_Iterator_CellAt(CdbPtrBuf_Iterator *it, long relCellIndex)
{
    return (CdbPtrBuf_Ptr *)CdbCellBuf_Iterator_CellAt(&it->it, relCellIndex);
}                               /* CdbPtrBuf_Iterator_CellAt */

/*
 * CdbPtrBuf_Iterator_Peek
 *
 * Returns the contents of the cell at a specified position relative to the
 * iterator's current position; or NULL if the specified position would
 * be out of bounds.  Does not affect the position or state of the iterator.
 *
 * relCellIndex == 0 returns the same value that CdbPtrBuf_Iterator_Next
 * would return, but does not advance the iterator.
 *
 * relCellIndex == -1 returns the same value that CdbPtrBuf_Iterator_Prev
 * would return, without repositioning the iterator.
 *
 * Note that a NULL return value is ambiguous if there are NULL values in the
 * ptrbuf: the caller can't tell whether the result is an actual NULL ptr value
 * or an indication that relCellIndex is out of bounds.  If this is a concern,
 * you can use CdbPtrBuf_Iterator_CellAt instead of CdbPtrBuf_Iterator_Peek.
 */
static inline CdbPtrBuf_Ptr
CdbPtrBuf_Iterator_Peek(CdbPtrBuf_Iterator *it, long relCellIndex)
{
    CdbPtrBuf_Ptr  *pp = CdbPtrBuf_Iterator_CellAt(it, relCellIndex);

    return pp ? *pp : NULL;
}                               /* CdbPtrBuf_Iterator_Peek */


/*
 * CdbPtrBuf_Iterator_Poke
 *
 * Stores 'item' into the cell at a specified position relative to the
 * iterator's current position.  Does not affect the position or state
 * of the iterator.
 *
 * 'relCellIndex' must designate a cell within the cellbuf bounds:
 *  0 <= relCellIndex + CdbPtrBuf_Iterator_GetPos(it) < CdbPtrBuf_Iterator_Length(it)
 */
static inline void
CdbPtrBuf_Iterator_Poke(CdbPtrBuf_Iterator *it, long relCellIndex, CdbPtrBuf_Ptr item)
{
    *CdbPtrBuf_Iterator_CellAt(it, relCellIndex) = item;
}                               /* CdbPtrBuf_Iterator_Poke */


/*
 * CdbPtrBuf_Iterator_GetPos
 *
 * Returns the number of cells preceding the current iterator position.
 */
static inline long
CdbPtrBuf_Iterator_GetPos(const CdbPtrBuf_Iterator *it)
{
    return CdbCellBuf_Iterator_GetPos(&it->it);
}                               /* CdbPtrBuf_Iterator_GetPos */

/*
 * CdbPtrBuf_Iterator_SetPos
 *
 * Positions the iterator before the cell whose 0-based index is
 * 'nextCellIndex'.  The first call to CdbPtrBuf_Iterator_NextCell()
 * will return a pointer to the specified cell (or NULL if the starting
 * position equals the number of occupied cells.)
 *
 * If nextCellIndex == 0, positions the iterator before the first cell.
 * If nextCellIndex == CdbPtrBuf_Length(), positions the iterator after
 * the last cell.
 *
 * An internal error is raised if nextCellIndex is outside the bounds
 *      0 <= nextCellIndex <= CdbPtrBuf_Iterator_Length(it)
 */
static inline void
CdbPtrBuf_Iterator_SetPos(CdbPtrBuf_Iterator *it, long nextCellIndex)
{
    CdbCellBuf_Iterator_SetPos(&it->it, nextCellIndex);
}                               /* CdbCellBuf_Iterator_SetPos */

/*
 * CdbPtrBuf_Iterator_Length
 *
 * Returns the number of cells in the associated ptrbuf.
 */
static inline unsigned
CdbPtrBuf_Iterator_Length(CdbPtrBuf_Iterator *it)
{
    return CdbCellBuf_Iterator_Length(&it->it);
}                               /* CdbPtrBuf_Iterator_Length */

/*
 * CdbPtrBuf_Iterator_AtBegin
 *
 * Returns 'true' if iterator is positioned before the first cell or if
 * the ptrbuf is empty.  Returns 'false' if iterator has advanced.
 */
static inline bool
CdbPtrBuf_Iterator_AtBegin(const CdbPtrBuf_Iterator *it)
{
    return CdbCellBuf_Iterator_AtBegin(&it->it);
}                               /* CdbPtrBuf_Iterator_AtBegin */

/*
 * CdbPtrBuf_Iterator_AtEnd
 *
 * Returns 'true' if iterator is positioned after the last cell or if
 * the cellbuf is empty, i.e., if CdbCellBuf_Iterator_NextCell() would
 * return NULL.  Returns 'false' if iterator is positioned before a cell.
 */
static inline bool
CdbPtrBuf_Iterator_AtEnd(CdbPtrBuf_Iterator *it)
{
    return CdbCellBuf_Iterator_AtEnd(&it->it);
}                               /* CdbPtrBuf_Iterator_AtEnd */

#endif   /* CDBPTRBUF_H */
