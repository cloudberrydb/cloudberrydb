//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMemoryPool.h
//
//	@doc:
//		Memory pool wrapper that cleans up the pool automatically
//
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoMemoryPool_H
#define GPOS_CAutoMemoryPool_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/common/CStackObject.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoMemoryPool
	//
	//	@doc:
	//		Automatic memory pool interface;
	//		tears down memory pool when going out of scope;
	//
	//		For cleanliness, do not provide an automatic cast to IMemoryPool
	//
	//---------------------------------------------------------------------------
	class CAutoMemoryPool : public CStackObject
	{
		public:
		
			enum ELeakCheck
			{
				ElcNone,	// no leak checking -- to be deprecated
				
				ElcExc,		// check for leaks unless an exception is pending (default)
				ElcStrict	// always check for leaks
			};
		
		private:

			// private copy ctor
			CAutoMemoryPool(const CAutoMemoryPool &);

			// memory pool to protect
			IMemoryPool *m_pmp;
			
			// type of leak check to perform
			ELeakCheck m_elc;

		public:

			// ctor
			CAutoMemoryPool
				(
				ELeakCheck elc = ElcExc,
				CMemoryPoolManager::EAllocType ept = CMemoryPoolManager::EatTracker,
				BOOL fThreadSafe = true,
				ULLONG ullCapacity = gpos::ullong_max
				);

			// dtor
			~CAutoMemoryPool();

			// accessor
			IMemoryPool *Pmp() const
			{
				return m_pmp;
			}
			
			// detach from pool
			IMemoryPool *PmpDetach();

	}; // CAutoMemoryPool
}

#endif // GPOS_CAutoMemoryPool_H

// EOF

