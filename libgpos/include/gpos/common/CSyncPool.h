//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncPool.h
//
//	@doc:
//		Template-based synchronized object pool class with minimum synchronization
//		overhead; it provides thread-safe object retrieval and release through
//		atomic primitives (lock-free);
//
//		Object pool is dynamically created during construction and released at
//		destruction; users retrieve objects without incurring the construction
//		cost (memory allocation, constructor invocation)
//
//		In order for the objects to be used in lock-free lists, the class uses
//		the clock algorithm to recycle objects.
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncPool_H
#define GPOS_CSyncPool_H

#include "gpos/types.h"
#include "gpos/utils.h"

#include "gpos/sync/atomic.h"
#include "gpos/common/CAutoP.h"

#define BYTES_PER_ULONG (GPOS_SIZEOF(ULONG))
#define BITS_PER_ULONG  (BYTES_PER_ULONG * 8)

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncPool<class T>
	//
	//	@doc:
	//		Synchronized object pool class
	//
	//---------------------------------------------------------------------------
	template <class T>
	class CSyncPool
	{
		private:

			// memory pool
			IMemoryPool *m_pmp;

			// array of preallocated objects
			T *m_rgtObjs;

			// bitmap indicating object reservation
			volatile ULONG *m_rgulBitMapReserved;

			// bitmap indicating object recycle
			volatile ULONG *m_rgulBitMapRecycled;

			// number of allocated objects
			ULONG m_ulObjs;

			// number of elements (ULONG) in bitmap
			ULONG m_ulBitMapSize;

			// offset of last lookup - clock index
			volatile ULONG_PTR m_ulpLast;

			// offset of id inside the object
			ULONG m_ulIdOffset;

			// atomically set bit if it is unset
			BOOL FSetBit(volatile ULONG *ulDest, ULONG ulBitVal)
            {
                GPOS_ASSERT(NULL != ulDest);

                ULONG ulOld = *ulDest;

                // keep trying while the bit is unset
                while (0 == (ulBitVal & ulOld))
                {
                    ULONG ulNew = ulBitVal | ulOld;

                    // attempt to set the bit
                    if (FCompareSwap(ulDest, ulOld, ulNew))
                    {
                        return true;
                    }

                    ulOld = *ulDest;
                }

                return false;
            }

			// atomically unset bit if it is set
			BOOL FUnsetBit(volatile ULONG *ulDest, ULONG ulBitVal)
            {
                GPOS_ASSERT(NULL != ulDest);

                ULONG ulOld = *ulDest;

                // keep trying while the bit is set
                while (ulBitVal == (ulBitVal & ulOld))
                {
                    ULONG ulNew = ulBitVal ^ ulOld;

                    // attempt to set the bit
                    if (FCompareSwap(ulDest, ulOld, ulNew))
                    {
                        return true;
                    }

                    ulOld = *ulDest;
                }

                return false;
            }

			// no copy ctor
			CSyncPool(const CSyncPool&);

		public:

			// ctor
			CSyncPool
				(
				IMemoryPool *pmp,
				ULONG ulSize
				)
            :
            m_pmp(pmp),
            m_rgtObjs(NULL),
            m_rgulBitMapReserved(NULL),
            m_rgulBitMapRecycled(NULL),
            m_ulObjs(ulSize),
            m_ulBitMapSize(ulSize / BITS_PER_ULONG + 1),
            m_ulpLast(0),
            m_ulIdOffset(ULONG_MAX)
            {}

			// dtor
			~CSyncPool()
            {
                if (ULONG_MAX != m_ulIdOffset)
                {
                    GPOS_ASSERT(NULL != m_rgtObjs);
                    GPOS_ASSERT(NULL != m_rgulBitMapReserved);
                    GPOS_ASSERT(NULL != m_rgulBitMapRecycled);

    #ifdef GPOS_DEBUG
                    if (!ITask::PtskSelf()->FPendingExc())
                    {
                        for (ULONG i = 0; i < m_ulObjs; i++)
                        {
                            ULONG ulElemOffset = i / BITS_PER_ULONG;
                            ULONG ulBitOffset = i % BITS_PER_ULONG;
                            ULONG ulBitVal = 1 << ulBitOffset;

                            BOOL fReserved = (ulBitVal == (m_rgulBitMapReserved[ulElemOffset] & ulBitVal));
                            BOOL fRecycled = (ulBitVal == (m_rgulBitMapRecycled[ulElemOffset] & ulBitVal));

                            GPOS_ASSERT((!fReserved || fRecycled) && "Object is still in use");
                        }
                    }
    #endif // GPOS_DEBUG

                    GPOS_DELETE_ARRAY(m_rgtObjs);
                    GPOS_DELETE_ARRAY(m_rgulBitMapReserved);
                    GPOS_DELETE_ARRAY(m_rgulBitMapRecycled);
                }
            }

			// init function to facilitate arrays
			void Init(ULONG ulIdOffset)
            {
                GPOS_ASSERT(ALIGNED_32(ulIdOffset));

                m_rgtObjs = GPOS_NEW_ARRAY(m_pmp, T, m_ulObjs);
                m_rgulBitMapReserved = GPOS_NEW_ARRAY(m_pmp, ULONG, m_ulBitMapSize);
                m_rgulBitMapRecycled = GPOS_NEW_ARRAY(m_pmp, ULONG, m_ulBitMapSize);

                m_ulIdOffset = ulIdOffset;

                // initialize object ids
                for (ULONG i = 0; i < m_ulObjs; i++)
                {
                    ULONG *pulId = (ULONG *) (((BYTE *) &m_rgtObjs[i]) + m_ulIdOffset);
                    *pulId = i;
                }

                // initialize bitmaps
                for (ULONG i = 0; i < m_ulBitMapSize; i++)
                {
                    m_rgulBitMapReserved[i] = 0;
                    m_rgulBitMapRecycled[i] = 0;
                }
            }

			// find unreserved object and reserve it
			T *PtRetrieve()
            {
                GPOS_ASSERT(ULONG_MAX != m_ulIdOffset && "Id offset not initialized.");

                // iterate over all objects twice (two full clock rotations);
                // objects marked as recycled cannot be reserved on the first round;
                for (ULONG i = 0; i < 2 * m_ulObjs; i++)
                {
                    // move clock index
                    ULONG_PTR ulpIndex = UlpExchangeAdd(&m_ulpLast, 1) % m_ulObjs;

                    ULONG ulElemOffset = (ULONG) ulpIndex / BITS_PER_ULONG;
                    ULONG ulBitOffset = (ULONG) ulpIndex % BITS_PER_ULONG;
                    ULONG ulBitVal = 1 << ulBitOffset;

                    // attempt to reserve object
                    if (FSetBit(&m_rgulBitMapReserved[ulElemOffset], ulBitVal))
                    {
                        // set id in corresponding object
                        T *pt = &m_rgtObjs[ulpIndex];

    #ifdef GPOS_DEBUG
                        ULONG *pulId = (ULONG *) (((BYTE *) pt) + m_ulIdOffset);
                        GPOS_ASSERT(ulpIndex == *pulId);
    #endif // GPOS_DEBUG

                        return pt;
                    }

                    // object is reserved, check if it has been marked for recycling
                    if (ulBitVal == (ulBitVal & m_rgulBitMapRecycled[ulElemOffset]))
                    {
                        // attempt to unset the recycle bit
                        if (FUnsetBit(&m_rgulBitMapRecycled[ulElemOffset], ulBitVal))
                        {
    #ifdef GPOS_DEBUG
                            BOOL fRecycled =
    #endif // GPOS_DEBUG
                            // unset the reserve bit - must succeed
                            FUnsetBit(&m_rgulBitMapReserved[ulElemOffset], ulBitVal);

                            GPOS_ASSERT(fRecycled && "Object was reserved before being recycled");
                        }
                    }
                }

                // no object is currently available, create a new one
                T *pt = GPOS_NEW(m_pmp) T();
                *(ULONG*) (((BYTE *) pt) + m_ulIdOffset) = ULONG_MAX;

                return pt;
            }

			// recycle reserved object
			void Recycle(T *pt)
            {
                GPOS_ASSERT(ULONG_MAX != m_ulIdOffset && "Id offset not initialized.");

                ULONG ulOffset = *(ULONG *) (((BYTE *) pt) + m_ulIdOffset);
                if (ULONG_MAX == ulOffset)
                {
                    // object does not belong to the array, delete it
                    GPOS_DELETE(pt);
                    return;
                }

                GPOS_ASSERT(ulOffset < m_ulObjs);

                ULONG ulElemOffset = ulOffset / BITS_PER_ULONG;
                ULONG ulBitOffset = ulOffset % BITS_PER_ULONG;
                ULONG ulBitVal = 1 << ulBitOffset;

    #ifdef GPOS_DEBUG
                ULONG ulReserved = m_rgulBitMapReserved[ulElemOffset];
                GPOS_ASSERT((ulBitVal == (ulBitVal & ulReserved)) && "Object is not reserved");

                BOOL fMarkRecycled =
    #endif // GPOS_DEBUG
                FSetBit(&m_rgulBitMapRecycled[ulElemOffset], ulBitVal);

                GPOS_ASSERT(fMarkRecycled && "Object has already been marked for recycling");
            }

	}; // class CSyncPool
}

#endif // !GPOS_CSyncPool_H


// EOF
