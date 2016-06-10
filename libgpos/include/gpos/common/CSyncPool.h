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
			BOOL FSetBit(volatile ULONG *ulDest, ULONG ulBitVal);

			// atomically unset bit if it is set
			BOOL FUnsetBit(volatile ULONG *ulDest, ULONG ulBitVal);

			// no copy ctor
			CSyncPool(const CSyncPool&);

		public:

			// ctor
			CSyncPool
				(
				IMemoryPool *pmp,
				ULONG ulSize
				);

			// dtor
			~CSyncPool();

			// init function to facilitate arrays
			void Init(ULONG ulIdOffset);

			// find unreserved object and reserve it
			T *PtRetrieve();

			// recycle reserved object
			void Recycle(T *pt);

	}; // class CSyncPool
}


// include implementation
#include "CSyncPool.inl"

#endif // !GPOS_CSyncPool_H


// EOF
