/*-------------------------------------------------------------------------
 *
 * vecheap.c
 *	  A simple binary heap implementation
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/lib/vecheap.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "utils/vecheap.h"
#include "utils/arrow.h"

static inline void swap_nodes(vecheap *heap, int a, int b);
static void heapify(vecheap *heap, int i);

/*
 * vecheap_allocate
 *
 * Returns a pointer to a newly-allocated heap that has the capacity to
 * store the given number of nodes, with the heap property defined by
 * the given comparator function, which will be invoked with the additional
 * argument specified by 'arg'.
 */
vecheap *
vecheap_allocate(int capacity, SortSupport sortKeys,
                 FormData_pg_attribute *attrs, int nkey)
{
	int		i;
	vecheap *heap;

	heap = (vecheap *) palloc(sizeof(vecheap));
	heap->vh_noderoutes = (int *)palloc(sizeof(int) * capacity);
	heap->vh_nodes = (HeapNode *)palloc0(sizeof(HeapNode) * capacity * nkey);
	heap->vh_sortkeys = (void **) palloc0(sizeof(void*) * capacity * 2 * nkey);
	heap->vh_keybitmaps = (void **) palloc0(sizeof(void*) * capacity * 2 * nkey);
	heap->vh_schema = NULL;
	heap->vh_rtbatches = (void **) palloc0(sizeof(void*) * capacity * 2);
	heap->vh_rttotalrows = (int *) palloc0(sizeof(int) * capacity * 2);
	heap->vh_rtstartrows = (int *) palloc0(sizeof(int) * capacity);
	heap->vh_rtcurrows = (int *) palloc0(sizeof(int) * capacity);
	heap->vh_indices = (int *) palloc(sizeof(int) * max_batch_size);
	heap->vh_keysize = (int *)palloc(sizeof(int) * nkey);
	heap->vh_atttypeid = (Oid *)palloc(sizeof(Oid) * nkey);

	heap->vh_nkey = nkey;
	heap->vh_num_indices = 0;
	heap->vh_num_route = 0;
	heap->vh_space = capacity;
	heap->vh_ssup = sortKeys;
	for (i = 0; i < nkey; i++)
	{
		SortSupport ssup = &sortKeys[i];

		heap->vh_keysize[i] = attrs[ssup->ssup_attno - 1].attlen;
		heap->vh_atttypeid[i] = attrs[ssup->ssup_attno - 1].atttypid;

		/* tid should be treated special */
		if (heap->vh_atttypeid[i] == TIDOID)
			heap->vh_atttypeid[i] = INT8OID;

	}

	heap->vh_size = 0;
	heap->vh_has_heap_property = true;

	return heap;
}

/*
 * vecheap_reset
 *
 * Resets the heap to an empty state, losing its data content but not the
 * parameters passed at allocation.
 */
void
vecheap_reset(vecheap *heap)
{
	heap->vh_size = 0;
	heap->vh_has_heap_property = true;
}

/*
 * vecheap_free
 *
 * Releases memory used by the given vecheap.
 */
void
vecheap_free(vecheap *heap)
{
	int i;

	pfree(heap->vh_sortkeys);
	pfree(heap->vh_keybitmaps);
	for (i = 0; i < heap->vh_space * 2; i++)
	{
		ARROW_FREE(GArrowRecordBatch, &heap->vh_rtbatches[i]);
	}
	pfree(heap->vh_rtbatches);
	pfree(heap->vh_rttotalrows);
	pfree(heap->vh_rtstartrows);
	pfree(heap->vh_indices);
	pfree(heap->vh_nodes);
	pfree(heap->vh_noderoutes);
	pfree(heap);
}

static inline int
left_offset(int i)
{
	return 2 * i + 1;
}

static inline int
right_offset(int i)
{
	return 2 * i + 2;
}

static inline int
parent_offset(int i)
{
	return (i - 1) / 2;
}

/*
 * vecheap_build
 *
 * Assembles a valid heap in O(n) from the nodes added by
 * vecheap_add_unordered(). Not needed otherwise.
 */
void
vecheap_build(vecheap *heap)
{
	int			i;

	for (i = parent_offset(heap->vh_size - 1); i >= 0; i--)
        heapify(heap, i);
    heap->vh_has_heap_property = true;
}

/*
 * vecheap_replace_first
 *
 * Replace the topmost element of a non-empty heap, preserving the heap
 * property.  O(1) in the best case, or O(log n) if it must fall back to
 * sifting the new node down.
 */
bool
vecheap_get_first(vecheap *heap)
{
	int i;

	Assert(!vecheap_empty(heap) && heap->vh_has_heap_property);

	if (heap->vh_size <= 0)
		return false;

	heap->vh_num_indices = 0;
	/* merge sort to indices */
	for (i = 0; i < max_batch_size; i++)
	{
		int route = heap->vh_noderoutes[0];

		/* store index to result */
		heap->vh_indices[i] =
				heap->vh_rtstartrows[route] + heap->vh_rtcurrows[route];
		heap->vh_num_indices++;
		heap->vh_rtcurrows[route]++;

		/* no more*/
		if ((heap->vh_rtcurrows[route] > heap->vh_rttotalrows[route * 2] - 1)
				&& heap->vh_rttotalrows[route * 2 + 1] <= 0)
		{
			swap_nodes(heap, 0, heap->vh_size - 1);
			heap->vh_size--;
		} else {
			if (heap->vh_rtcurrows[route] < heap->vh_rttotalrows[route * 2])
				setData(heap, route, 0, 0);
			else
				setData(heap, route, 1, 0);
		}

		if (heap->vh_size > 0)
		{
			heapify(heap, 0);

			/* the second batch has been touched bottom
			 * need load the follow batch */
			if (heap->vh_rtcurrows[route] >= (heap->vh_rttotalrows[route * 2] +
				heap->vh_rttotalrows[route * 2 + 1] - 1))
				break;
		}
		else
			break;
	}
	return true;
}

static inline bool
merge_issmall(vecheap *heap, int left, int right,
              SortSupport ssup, int *keysize, int nkey)
{
	int i;
	HeapNode *nodes = heap->vh_nodes;

	for (i = 0; i < nkey; i++)
	{
		int compare;

		if ((keysize[i] > 0) || heap->vh_atttypeid[i] == NUMERICOID)
            compare = ApplySortComparator(nodes[left * nkey + i].data,
                                          nodes[left * nkey + i].isnull,
                                          nodes[right * nkey + i].data,
                                          nodes[right * nkey + i].isnull,
                                          ssup + i);
        /*variable size, string only now */
		else
		{
			char *lstring = (char *)nodes[left * nkey + i].data;
			char *rstring = (char *)nodes[right * nkey + i].data;

			bool lnull = nodes[left * nkey + i].isnull;
			bool rnull = nodes[right * nkey + i].isnull;

			if (lnull && rnull)
			{
				compare = 0;
			}
			else if(lnull && !rnull)
			{
				compare = (ssup + i)->ssup_nulls_first ? -1 : 1;
			}
			else if (!lnull && rnull)
			{
				compare = (ssup + i)->ssup_nulls_first ? 1 : -1;
			}
			else
			{
				compare = strcmp(lstring, rstring);
				if ((ssup + i)->ssup_reverse)
								INVERT_COMPARE_RESULT(compare);
			}
		}

		if (compare < 0)
			return true;
		else if(compare > 0)
			return false;
	}
	return true;
}

/*
 * Sift a node down from its current position to satisfy the heap
 * property.
 */
static inline void
heapify(vecheap *heap, int i)
{
	int left = 2 * i + 1;
	int right = 2 *i + 2;
	int smallest = i;

	if (left < heap->vh_size &&
		 merge_issmall(heap,
			 left,
			 smallest,
			 heap->vh_ssup,
			 heap->vh_keysize,
			 heap->vh_nkey))
	{
		smallest = left;
	}

	if (right < heap->vh_size &&
		 merge_issmall(heap,
			 right,
			 smallest,
			 heap->vh_ssup,
			 heap->vh_keysize,
			 heap->vh_nkey))
	{
		smallest = right;
	}

	/* swap */
	if (smallest != i)
	{
		swap_nodes(heap, i, smallest);
		heapify(heap, smallest);
	}
}

/*
 * Swap the contents of two nodes.
 */
static inline void
swap_nodes(vecheap *heap, int a, int b)
{
	int i;
	int route;
	int nkey = heap->vh_nkey;

	route = heap->vh_noderoutes[a];
	heap->vh_noderoutes[a] = heap->vh_noderoutes[b];
	heap->vh_noderoutes[b] = route;
	for (i = 0; i < heap->vh_nkey; i++)
	{
		HeapNode	swap;
		swap.data = heap->vh_nodes[a * nkey + i].data;
		swap.isnull = heap->vh_nodes[a * nkey + i].isnull;
		heap->vh_nodes[a * nkey + i].data = heap->vh_nodes[b * nkey + i].data;
		heap->vh_nodes[a * nkey + i].isnull = heap->vh_nodes[b * nkey + i].isnull;
		heap->vh_nodes[b * nkey + i].data = swap.data;
		heap->vh_nodes[b * nkey + i].isnull = swap.isnull;
	}
}
