//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CEvent.cpp
//
//	@doc:
//		Implementation of CEvent
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/syslibwrapper.h"
#include "gpos/common/pthreadwrapper.h"
#include "gpos/sync/CEvent.h"
#include "gpos/sync/CAutoMutex.h"
#include "gpos/task/CWorkerPoolManager.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CEvent::CEvent
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CEvent::CEvent()
	:
	m_mutex(NULL),
	m_inited(false),
	m_num_waiters(0),
	m_num_signals(0),
	m_num_total_signals(0),
	m_num_total_broadcasts(0)
{}


//---------------------------------------------------------------------------
//	@function:
//		CEvent::~CEvent
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CEvent::~CEvent()
{
	GPOS_ASSERT(0 == m_num_waiters);

	// might not have initialized, therefore check
	if (m_inited)
	{
		pthread::CondDestroy(&m_cond);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEvent::Init
//
//	@doc:
//		Initialize event and bind to mutex
//
//---------------------------------------------------------------------------
void
CEvent::Init
	(
	CMutexBase *pmutex
	)
{
	GPOS_ASSERT(NULL != pmutex);
	GPOS_ASSERT(!m_inited && "Event already initialized.");
	
	m_mutex = pmutex;
	if (0 != pthread::CondInit(&m_cond, NULL))
	{
		// raise OOM exception
		GPOS_OOM_CHECK(NULL);
	}
	m_inited = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CEvent::Signal
//
//	@doc:
//		Wake up one waiter
//
//---------------------------------------------------------------------------
void
CEvent::Signal()
{
	GPOS_ASSERT(m_inited && "Event not initialized.");
	GPOS_ASSERT(m_mutex->IsOwned());

	// check if anyone is waiting
	if (0 < m_num_waiters)
	{
		// the condition variable is initialized - ignore returned value
		(void) pthread::CondSignal(&m_cond);

		m_num_signals++;
		m_num_total_signals++;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEvent::Broadcast
//
//	@doc:
//		Wake up all waiters
//
//---------------------------------------------------------------------------
void
CEvent::Broadcast()
{
	GPOS_ASSERT(m_inited && "Event not initialized.");
	GPOS_ASSERT(m_mutex->IsOwned());
	
	// check if anyone is waiting
	if (0 < m_num_waiters)
	{
		// the condition variable is initialized - ignore returned value
		(void) pthread::CondBroadcast(&m_cond);

		m_num_total_broadcasts++;

		// reset pending signal counter
		// all current waiters will receive the broadcast
		m_num_signals = 0;
	}
}



//---------------------------------------------------------------------------
//	@function:
//		CEvent::Wait
//
//	@doc:
//		Wait function, calls TimedWait with infinite timeout
//
//---------------------------------------------------------------------------
void
CEvent::Wait()
{
#ifdef GPOS_DEBUG
	GPOS_RESULT eres =
#endif // GPOS_DEBUG
	TimedWait(gpos::ulong_max);

	GPOS_ASSERT(GPOS_OK == eres && "Failed to receive a signal on the event");
}


//---------------------------------------------------------------------------
//	@function:
//		CEvent::TimedWait
//
//	@doc:
//		Wait function with timeout, calls internal wait function repeatedly
//		until a signal is received or timeout expires
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEvent::TimedWait
	(
	ULONG timeout_ms
	)
{
	GPOS_ASSERT(m_inited && "Event not initialized.");
	GPOS_ASSERT(m_mutex->IsOwned());

	CWallClock clock;

	ULLONG signals_previous = m_num_total_signals;
	ULLONG broadcasts_previous = m_num_total_broadcasts;

	GPOS_RESULT eres = GPOS_OK;

	// increment waiters - decremented by dtor
	CAutoCounter ac(&m_num_waiters);

	do
	{
		// check if timeout has expired
		ULONG elapsed_ms = clock.ElapsedMS();
		if (elapsed_ms >= timeout_ms)
		{
			return GPOS_TIMEOUT;
		}

		ULONG timeout_internal_ms = timeout_ms - elapsed_ms;
		if (GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC < timeout_internal_ms)
		{
			timeout_internal_ms = GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC;
		}

		InternalTimedWait(timeout_internal_ms);

#ifdef GPOS_DEBUG
		// do not enforce time slicing while waiting for event
		if (IWorker::m_enforce_time_slices)
		{
			CWorker::Self()->ResetTimeSlice();
		}
#endif // GPOS_DEBUG

		// check if broadcast was received
		if (broadcasts_previous < m_num_total_broadcasts)
		{
			eres = GPOS_OK;
		}
		else
		{
			// check if signal was received
			if (0 < m_num_signals && signals_previous < m_num_total_signals)
			{
				m_num_signals--;
				eres = GPOS_OK;
			}
			else
			{
				eres = GPOS_TIMEOUT;

				GPOS_CHECK_ABORT;
			}
		}

	}
	while (GPOS_OK != eres);

	GPOS_ASSERT(m_mutex->IsOwned());

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CEvent::InternalTimedWait
//
//	@doc:
//		Internal wait function, used to implement TimedWait;
//		relinquishes mutex and calls underlying condition variable primitives;
//		relinquish/regain functions are called while the mutex is actually held;
//		function blocks until signal is received or timeout expires;
//
//---------------------------------------------------------------------------
void
CEvent::InternalTimedWait
	(
	ULONG m_timeout_ms
	)
{
	GPOS_ASSERT(m_inited && "Event not initialized.");
	GPOS_ASSERT(m_mutex->IsOwned());

	// set expiration timer
	TIMEVAL tv;
	syslib::GetTimeOfDay(&tv, NULL/*timezone*/);
	ULLONG current_us = tv.tv_sec * GPOS_USEC_IN_SEC + tv.tv_usec;
	ULLONG expire_us = current_us + m_timeout_ms * GPOS_USEC_IN_MSEC;
	TIMESPEC ts;
	ts.tv_sec = (ULONG_PTR) (expire_us / GPOS_USEC_IN_SEC);
	ts.tv_nsec = (ULONG_PTR) ((expire_us % GPOS_USEC_IN_SEC) * (GPOS_NSEC_IN_SEC / GPOS_USEC_IN_SEC));

	m_mutex->Relinquish();

#ifdef GPOS_DEBUG
	INT ret =
#endif // GPOS_DEBUG
	pthread::CondTimedWait(&m_cond, m_mutex->GetMutex(), &ts);

	m_mutex->Regain();

	GPOS_ASSERT(m_mutex->IsOwned());
	GPOS_ASSERT(0 == ret || ETIMEDOUT == ret);
}

// EOF

