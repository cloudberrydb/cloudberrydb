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
			CSpinlockBase &m_slock;

			// flag to indicate ownership
			BOOL m_fLocked;
			
		public:

			// ctor
			explicit
			CAutoSpinlock
				(
				CSpinlockBase &slock
				) 
				: m_slock(slock), m_fLocked(false) {}
			
			// dtor
			~CAutoSpinlock();
			
			// acquire lock
			void Lock()
			{
				m_slock.Lock();
				m_fLocked = true;
			}
			
			// release lock
			void Unlock()
			{
				GPOS_ASSERT(m_fLocked);
				
				m_slock.Unlock();
				m_fLocked = false;
			}

	}; // class CAutoSpinlock
}

#endif // !GPOS_CAutoSpinlock_H

// EOF

