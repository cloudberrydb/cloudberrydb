//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAtomicCounter.h
//
//	@doc:
//		Implements an atomic ULONG counter
//---------------------------------------------------------------------------
#ifndef GPOS_CAtomicCounter_H
#define GPOS_CAtomicCounter_H

#include "gpos/base.h"
#include "gpos/sync/atomic.h"

namespace gpos
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CAtomicCounter
	//
	//	@doc:
	//		Implements atomic ULONG counter as wrapper around a ULONG_PTR;
	//
	//---------------------------------------------------------------------------
	template <class T>
	class CAtomicCounter
	{
		private:

			// counter
			volatile ULONG_PTR m_ulp;

			// private copy ctor
			CAtomicCounter(const CAtomicCounter &);
			
		public:
		
			// ctor
			explicit
			CAtomicCounter
				(
				T tSeed = 0
				)
				: 
				m_ulp(tSeed)
			{
				GPOS_ASSERT(sizeof(T) <= sizeof(m_ulp) && "Type too large for wrapper");
			}

			// incr
			T TIncr()
			{
#ifdef GPOS_DEBUG
				T t = (T)-1;
				GPOS_ASSERT(m_ulp < t - 1 && "Counter overflow");
#endif // GPOS_DEBUG
				return (T) UlpExchangeAdd(&m_ulp, 1);
			}			
			
	}; // class CAtomicCounter


	// ULONG, ULONG_PTR counters
	typedef CAtomicCounter<ULONG> CAtomicULONG;
	typedef CAtomicCounter<ULONG_PTR> CAtomicULONG_PTR;
	
}

#endif // !GPOS_CAtomicCounter_H

// EOF

