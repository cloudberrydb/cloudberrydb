/*-------------------------------------------------------------------------
 *
 * vecheap.h
 *	  A simple binary heap implementation
 *
 * Portions Copyright (c) 2016-2020, HashData
 *
 * src/include/utils/vecheap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECHEAP_H
#define VECHEAP_H

#include "utils/sortsupport.h"

#include "utils/decimal.h"
#include "utils/arrow.h"

/*
 * For a max-heap, the comparator must return <0 iff a < b, 0 iff a == b,
 * and >0 iff a > b.  For a min-heap, the conditions are reversed.
 */
typedef int (*vecheap_comparator) (Datum a, Datum b, void *arg);

typedef struct HeapNode
{
	Datum	data;
	bool	isnull;;
} HeapNode;

/*
 * vecheap
 *
 *		vh_size			how many nodes are currently in "nodes"
 *		vh_space		how many nodes can be stored in "nodes"
 *		vh_has_heap_property	no unordered operations since last heap build
 *		vh_compare		comparison function to define the heap property
 *		vh_arg			user data for comparison function
 *		vh_nodes		variable-length array of "space" nodes
 */
typedef struct vecheap
{
	int			vh_size;
	int			vh_space;
	bool		vh_has_heap_property;	/* debugging cross-check */
	SortSupport vh_ssup;

	int			vh_nkey;
	void	  **vh_sortkeys; /* pointer of sort keys for each route
								and each key.
								array for key data of route i key j:
								sortkeys[i * nkey + j]
								*/
	void	  **vh_keybitmaps; /* valid bitmap of each route and key.
	 	 	 	 	 	 	 	* bitmap for route i key j:
	 	 	 	 	 	 	 	* bitmaps[i * nkey + j]
	 	 	 	 	 	 	 	*/
	int		   *vh_keysize;
	Oid		   *vh_atttypeid;

	void	   *vh_schema;
	void      **vh_rtbatches; /* two batch for each route*/
	int		   *vh_rttotalrows;
	int		   *vh_rtstartrows;
	int		   *vh_rtcurrows; /* rows of each route batch */
	int			vh_num_route;

	int	 	   *vh_indices; /* result indices */
	int			vh_num_indices;

	int		   *vh_noderoutes; /* heap node routes */
	HeapNode   *vh_nodes; /* nodes datums, datum of heap node i key j:
							 vh_nodes[i * nkey + j] */
} vecheap;

extern vecheap *vecheap_allocate(int capacity,
                                 SortSupport sortKeys,
                                 FormData_pg_attribute *attrs,
                                 int nkey);
extern void vecheap_reset(vecheap *heap);
extern void vecheap_free(vecheap *heap);
extern void vecheap_add_unordered(vecheap *heap, Datum d);
extern void vecheap_build(vecheap *heap);
extern bool vecheap_get_first(vecheap *heap);

static inline void
setData(vecheap *vp, int route, int offset, int heapid)
{
	/* current index in key array*/
	int nkey = vp->vh_nkey;
    int index = vp->vh_rtcurrows[route] -
                vp->vh_rttotalrows[route * 2] * offset;

    for (int i = 0; i < nkey; i++)
	{
		void *keys = vp->vh_sortkeys[(route * 2 + offset) * nkey + i];
		char *bitmap = vp->vh_keybitmaps[(route * 2 + offset) * nkey + i];

		switch(vp->vh_keysize[i])
		{
			case 1:
			{
				int8 *data = (int8 *)keys;
				vp->vh_nodes[heapid * nkey + i].data = Int8GetDatum(data[index]);
				if (bitmap)
                    vp->vh_nodes[heapid * nkey + i].isnull =
                        ((bitmap[index / 8] & 1 << (index % 8)) == 0);
                else
					vp->vh_nodes[heapid * nkey + i].isnull = false;
			}
			break;
			case 2:
			{
				int16 *data = (int16 *)keys;
				vp->vh_nodes[heapid * nkey + i].data = Int16GetDatum(data[index]);
				if (bitmap)
                    vp->vh_nodes[heapid * nkey + i].isnull =
                        ((bitmap[index / 8] & 1 << (index % 8)) == 0);
                else
					vp->vh_nodes[heapid * nkey + i].isnull = false;
			}
			break;
			case 4:
			{
				int32 *data = (int32 *)keys;
				vp->vh_nodes[heapid * nkey + i].data = Int32GetDatum(data[index]);
				if (bitmap)
                    vp->vh_nodes[heapid * nkey + i].isnull =
                        ((bitmap[index / 8] & 1 << (index % 8)) == 0);
                else
					vp->vh_nodes[heapid * nkey + i].isnull = false;
			}
			break;
			case 6: // tid -> int64 in vectorization
			case 8:
			{
				int64 *data = (int64 *)keys;
				vp->vh_nodes[heapid * nkey + i].data = Int64GetDatum(data[index]);
				if (bitmap)
                    vp->vh_nodes[heapid * nkey + i].isnull =
                        ((bitmap[index / 8] & 1 << (index % 8)) == 0);
                else
					vp->vh_nodes[heapid * nkey + i].isnull = false;
			}
			break;
			case -1:
			{
				if ((vp->vh_atttypeid[i] == BPCHAROID)
						|| (vp->vh_atttypeid[i] == VARCHAROID)
						|| (vp->vh_atttypeid[i] == TEXTOID))
				{
					GArrowStringArray *array = GARROW_STRING_ARRAY(keys);
					char *string;
					int is_null = garrow_array_is_null(GARROW_ARRAY(array), index);

					if (is_null)
					{
						vp->vh_nodes[heapid * nkey + i].isnull = true;
						vp->vh_nodes[heapid * nkey + i].data = (Datum)0;
					}
					else
					{
						string = garrow_string_array_get_string(array, index);
						vp->vh_nodes[heapid * nkey + i].data = PointerGetDatum((void *)string);
						vp->vh_nodes[heapid * nkey + i].isnull = false;
					}

				}
				else if ((vp->vh_atttypeid[i] == NUMERICOID))
				{
					GArrowNumeric128Array *array = GARROW_NUMERIC128_ARRAY(keys);
					int is_null = garrow_array_is_null(GARROW_ARRAY(array), index);

					if (is_null)
					{
						vp->vh_nodes[heapid * nkey + i].isnull = true;
						vp->vh_nodes[heapid * nkey + i].data = (Datum)0;
					}
					else
					{
						vp->vh_nodes[heapid * nkey + i].data = Numeric128ArrayGetDatum(array, index);
						vp->vh_nodes[heapid * nkey + i].isnull = false;
					}
                }
			}
			break;
			default:
				elog(ERROR, "unsupported sort key data size: %d", vp->vh_keysize[i]);}
	}
}

#define vecheap_empty(h)			((h)->vh_size == 0)

#endif   /* VECHEAP_H */
