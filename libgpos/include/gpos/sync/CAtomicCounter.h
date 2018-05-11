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
			volatile ULONG_PTR m_counter;

			// private copy ctor
			CAtomicCounter(const CAtomicCounter &);
			
		public:
		
			// ctor
			explicit
			CAtomicCounter
				(
				T seed = 0
				)
				: 
				m_counter(seed)
			{
				GPOS_ASSERT(sizeof(T) <= sizeof(m_counter) && "Type too large for wrapper");
			}

			// incr
			T Incr()
			{
#ifdef GPOS_DEBUG
				T t = (T)-1;
				GPOS_ASSERT(m_counter < t - 1 && "Counter overflow");
#endif // GPOS_DEBUG
				return (T) ExchangeAddUlongPtrWithInt(&m_counter, 1);
			}			
			
	}; // class CAtomicCounter


	// ULONG, ULONG_PTR counters
	typedef CAtomicCounter<ULONG> CAtomicULONG;
	typedef CAtomicCounter<ULONG_PTR> CAtomicULONG_PTR;
	
}

#endif // !GPOS_CAtomicCounter_H

// EOF

