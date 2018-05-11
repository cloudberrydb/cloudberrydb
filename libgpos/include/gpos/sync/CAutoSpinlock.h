//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoSpinlock.h
//
//	@doc:
//		Auto class to be put on stack to ensure spinlock is released when
//		going out of scope;
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoSpinlock_H
#define GPOS_CAutoSpinlock_H

#include "gpos/common/CStackObject.h"
#include "gpos/sync/CSpinlock.h"


namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoSpinlock
	//
	//	@doc:
	//		Wrapper around spinlock base class; releases spinlock in destructor
	//		if acquired previously;
	//
	//---------------------------------------------------------------------------
	class CAutoSpinlock : public CStackObject
	{
		private:
		
			// the actual spin lock
			CSpinlockBase &m_lock;

			// flag to indicate ownership
			BOOL m_locked;
			
		public:

			// ctor
			explicit
			CAutoSpinlock
				(
				CSpinlockBase &lock
				) 
				: m_lock(lock), m_locked(false) {}
			
			// dtor
			~CAutoSpinlock();
			
			// acquire lock
			void Lock()
			{
				m_lock.Lock();
				m_locked = true;
			}
			
			// release lock
			void Unlock()
			{
				GPOS_ASSERT(m_locked);
				
				m_lock.Unlock();
				m_locked = false;
			}

	}; // class CAutoSpinlock
}

#endif // !GPOS_CAutoSpinlock_H

// EOF

