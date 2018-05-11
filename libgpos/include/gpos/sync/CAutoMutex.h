//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename: 
//		CAutoMutex.h
//
//	@doc:
//		Auto wrapper around base mutex class; destructor unlocks mutex
//		if it acquired the lock previously;
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoMutex_H
#define GPOS_CAutoMutex_H

#include "gpos/base.h"
#include "gpos/sync/CMutex.h"
#include "gpos/common/CStackObject.h"


namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoMutex
	//
	//	@doc:
	//		Auto wrapper;
	//
	//---------------------------------------------------------------------------
	class CAutoMutex : public CStackObject
	{
		private:

			// actual mutex
			CMutexBase &m_mutex;

			// lock count
			ULONG m_lock_count;

			// no copy ctor
			CAutoMutex
				(
				const CAutoMutex&
				);
			
		public:
		
			// ctor
			explicit
			CAutoMutex
				(
				CMutexBase &mutex
				)
				:
				m_mutex(mutex),
				m_lock_count(0)
			{}


			// ctor
			~CAutoMutex ();
			

			// acquire lock
			void Lock()
			{
				m_mutex.Lock();
				++m_lock_count;
			}
			
			// attempt locking
			BOOL TryLock()
			{
				if (m_mutex.TryLock())
				{
					++m_lock_count;
					return true;
				}
				return false;
			}
			
			// release lock
			void Unlock()
			{
				GPOS_ASSERT(0 < m_lock_count && "GetMutex not locked");
				
				--m_lock_count;
				m_mutex.Unlock();
			}

	}; // class CAutoMutex
}

#endif // !GPOS_CAutoMutex_H

// EOF

