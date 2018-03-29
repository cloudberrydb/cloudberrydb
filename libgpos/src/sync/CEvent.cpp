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
	m_pmutex(NULL),
	m_fInit(false),
	m_ulWaiters(0),
	m_ulSignals(0),
	m_ulSignalsTotal(0),
	m_ulBroadcastsTotal(0)
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
	GPOS_ASSERT(0 == m_ulWaiters);

	// might not have initialized, therefore check
	if (m_fInit)
	{
		pthread::PthreadCondDestroy(&m_tcond);
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
	GPOS_ASSERT(!m_fInit && "Event already initialized.");
	
	m_pmutex = pmutex;
	if (0 != pthread::IPthreadCondInit(&m_tcond, NULL))
	{
		// raise OOM exception
		GPOS_OOM_CHECK(NULL);
	}
	m_fInit = true;
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
	GPOS_ASSERT(m_fInit && "Event not initialized.");
	GPOS_ASSERT(m_pmutex->FOwned());

	// check if anyone is waiting
	if (0 < m_ulWaiters)
	{
		// the condition variable is initialized - ignore returned value
		(void) pthread::IPthreadCondSignal(&m_tcond);

		m_ulSignals++;
		m_ulSignalsTotal++;
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
	GPOS_ASSERT(m_fInit && "Event not initialized.");
	GPOS_ASSERT(m_pmutex->FOwned());
	
	// check if anyone is waiting
	if (0 < m_ulWaiters)
	{
		// the condition variable is initialized - ignore returned value
		(void) pthread::IPthreadCondBroadcast(&m_tcond);

		m_ulBroadcastsTotal++;

		// reset pending signal counter
		// all current waiters will receive the broadcast
		m_ulSignals = 0;
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
		EresTimedWait(gpos::ulong_max);

	GPOS_ASSERT(GPOS_OK == eres && "Failed to receive a signal on the event");
}


//---------------------------------------------------------------------------
//	@function:
//		CEvent::EresTimedWait
//
//	@doc:
//		Wait function with timeout, calls internal wait function repeatedly
//		until a signal is received or timeout expires
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEvent::EresTimedWait
	(
	ULONG ulTimeoutMs
	)
{
	GPOS_ASSERT(m_fInit && "Event not initialized.");
	GPOS_ASSERT(m_pmutex->FOwned());

	CWallClock clock;

	ULLONG ulSignalsPrevious = m_ulSignalsTotal;
	ULLONG ulBroadcastsPrevious = m_ulBroadcastsTotal;

	GPOS_RESULT eres = GPOS_OK;

	// increment waiters - decremented by dtor
	CAutoCounter ac(&m_ulWaiters);

	do
	{
		// check if timeout has expired
		ULONG ulElapsedMs = clock.UlElapsedMS();
		if (ulElapsedMs >= ulTimeoutMs)
		{
			return GPOS_TIMEOUT;
		}

		ULONG ulTimeoutInternalMs = ulTimeoutMs - ulElapsedMs;
		if (GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC < ulTimeoutInternalMs)
		{
			ulTimeoutInternalMs = GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC;
		}

		InternalTimedWait(ulTimeoutInternalMs);

#ifdef GPOS_DEBUG
		// do not enforce time slicing while waiting for event
		if (IWorker::m_fEnforceTimeSlices)
		{
			CWorker::PwrkrSelf()->ResetTimeSlice();
		}
#endif // GPOS_DEBUG

		// check if broadcast was received
		if (ulBroadcastsPrevious < m_ulBroadcastsTotal)
		{
			eres = GPOS_OK;
		}
		else
		{
			// check if signal was received
			if (0 < m_ulSignals && ulSignalsPrevious < m_ulSignalsTotal)
			{
				m_ulSignals--;
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

	GPOS_ASSERT(m_pmutex->FOwned());

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
	ULONG ulTimeoutMs
	)
{
	GPOS_ASSERT(m_fInit && "Event not initialized.");
	GPOS_ASSERT(m_pmutex->FOwned());

	// set expiration timer
	TIMEVAL tv;
	syslib::GetTimeOfDay(&tv, NULL/*timezone*/);
	ULLONG ulCurrentUs = tv.tv_sec * GPOS_USEC_IN_SEC + tv.tv_usec;
	ULLONG ulExpireUs = ulCurrentUs + ulTimeoutMs * GPOS_USEC_IN_MSEC;
	TIMESPEC ts;
	ts.tv_sec = (ULONG_PTR) (ulExpireUs / GPOS_USEC_IN_SEC);
	ts.tv_nsec = (ULONG_PTR) ((ulExpireUs % GPOS_USEC_IN_SEC) * (GPOS_NSEC_IN_SEC / GPOS_USEC_IN_SEC));

	m_pmutex->Relinquish();

#ifdef GPOS_DEBUG
	INT iRet =
#endif // GPOS_DEBUG
	pthread::IPthreadCondTimedWait(&m_tcond, m_pmutex->Ptmutex(), &ts);

	m_pmutex->Regain();

	GPOS_ASSERT(m_pmutex->FOwned());
	GPOS_ASSERT(0 == iRet || ETIMEDOUT == iRet);
}

// EOF

