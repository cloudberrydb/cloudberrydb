//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncPool.inl
//
//	@doc:
//		Implementation of synchronized object pool class
//---------------------------------------------------------------------------

#include "gpos/sync/atomic.h"
#include "gpos/common/CAutoP.h"

#define BYTES_PER_ULONG (GPOS_SIZEOF(ULONG))
#define BITS_PER_ULONG  (BYTES_PER_ULONG * 8)

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncPool<T>::CSyncPool
	//
	//	@doc:
	//		Constructor
	//
	//---------------------------------------------------------------------------
	template<class T>
	CSyncPool<T>::CSyncPool
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncPool<T>::~CSyncPool
	//
	//	@doc:
	//		Destructor
	//
	//---------------------------------------------------------------------------
	template<class T>
	CSyncPool<T>::~CSyncPool()
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncPool<T>::Init
	//
	//	@doc:
	//		Init function to facilitate arrays
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CSyncPool<T>::Init
		(
		ULONG ulIdOffset
		)
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncPool<T>::PtReserve
	//
	//	@doc:
	//		Find unreserved object and reserve it
	//
	//---------------------------------------------------------------------------
	template<class T>
	T*
	CSyncPool<T>::PtRetrieve()
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncPool<T>::Recycle
	//
	//	@doc:
	//		Recycle reserved object
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CSyncPool<T>::Recycle
		(
		T *pt
		)
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




	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncPool<T>::FSetBit
	//
	//	@doc:
	//		Atomically set bit if it is unset
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CSyncPool<T>::FSetBit
		(
		volatile ULONG *ulDest,
		ULONG ulBitVal
		)
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncPool<T>::FUnsetBit
	//
	//	@doc:
	//		Atomically unset bit if it is set
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CSyncPool<T>::FUnsetBit
		(
		volatile ULONG *ulDest,
		ULONG ulBitVal
		)
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
}

// EOF
