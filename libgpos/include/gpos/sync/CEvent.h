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

					ULONG *m_count;

				public:

					// ctor - increments
					explicit
					CAutoCounter(ULONG *count)
						:
						m_count(count)
					{
						(*m_count)++;
					}

					// dtor - decrements
					~CAutoCounter()
					{
						(*m_count)--;
					}
			};

			// no copy ctor
			CEvent(const CEvent &);

			// external mutex
			CMutexBase *m_mutex;
			
			// indicator that event is bound to mutex
			BOOL m_inited;
			
			// waiters
			ULONG m_num_waiters;

			// pending signals
			ULONG m_num_signals;

			// total signals
			ULLONG m_num_total_signals;

			// total broadcasts
			ULLONG m_num_total_broadcasts;

			// basic condition variable
			PTHREAD_COND_T m_cond;		
					
			// internal wait function, used to implement TimedWait
			void InternalTimedWait(ULONG m_timeout_ms);

		public:
		
			// ctor
			explicit
			CEvent();
							
			// dtor
			~CEvent();
						
			// bind to mutex
			void Init(CMutexBase *);

			// accessor to underlying mutex
			CMutexBase *GetMutex() const
			{
				return m_mutex;
			}
			
			// accessor to waiter counter
			ULONG GetNumWaiters() const
			{
				return m_num_waiters;
			}
			
			// wait signal/broadcast
			void Wait();
			
			// wait signal/broadcast with timeout
			GPOS_RESULT TimedWait(ULONG m_timeout_ms);

			// signal primitive
			void Signal();

			// broadcast primitive
			void Broadcast();
	
	}; // class CEvent
}


#endif // !GPOS_CEvent_H

// EOF

