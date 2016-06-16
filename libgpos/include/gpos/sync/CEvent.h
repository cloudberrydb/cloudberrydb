//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CEvent.h
//
//	@doc:
//		Abstraction of event primitives; maps error values from event operations
//		onto GPOS's exception framework
//---------------------------------------------------------------------------
#ifndef GPOS_CEvent_H
#define GPOS_CEvent_H

#include "gpos/types.h"
#include "gpos/common/pthreadwrapper.h"
#include "gpos/sync/CMutex.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CEvent
	//
	//	@doc:
	//		Base class condition variables
	//
	//---------------------------------------------------------------------------
	class CEvent
	{				
		private:
		
			//---------------------------------------------------------------------------
			//	@class:
			//		CAutoCounter
			//
			//	@doc:
			//		Ensures safe counter increment/decrement, even in the occurrence
			//		of an exception
			//
			//---------------------------------------------------------------------------
			class CAutoCounter : CStackObject
			{
				private:

					ULONG *m_pulCnt;

				public:

					// ctor - increments
					explicit
					CAutoCounter(ULONG *pulCnt)
						:
						m_pulCnt(pulCnt)
					{
						(*m_pulCnt)++;
					}

					// dtor - decrements
					~CAutoCounter()
					{
						(*m_pulCnt)--;
					}
			};

			// no copy ctor
			CEvent(const CEvent &);

			// external mutex
			CMutexBase *m_pmutex;
			
			// indicator that event is bound to mutex
			BOOL m_fInit;
			
			// waiters
			ULONG m_ulWaiters;

			// pending signals
			ULONG m_ulSignals;

			// total signals
			ULLONG m_ulSignalsTotal;

			// total broadcasts
			ULLONG m_ulBroadcastsTotal;

			// basic condition variable
			PTHREAD_COND_T m_tcond;		
					
			// internal wait function, used to implement TimedWait
			void InternalTimedWait(ULONG ulTimeoutMs);

		public:
		
			// ctor
			explicit
			CEvent();
							
			// dtor
			~CEvent();
						
			// bind to mutex
			void Init(CMutexBase *);

			// accessor to underlying mutex
			CMutexBase *Pmutex() const
			{
				return m_pmutex;
			}
			
			// accessor to waiter counter
			ULONG CWaiters() const
			{
				return m_ulWaiters;
			}
			
			// wait signal/broadcast
			void Wait();
			
			// wait signal/broadcast with timeout
			GPOS_RESULT EresTimedWait(ULONG ulTimeoutMs);

			// signal primitive
			void Signal();

			// broadcast primitive
			void Broadcast();
	
	}; // class CEvent
}


#endif // !GPOS_CEvent_H

// EOF

