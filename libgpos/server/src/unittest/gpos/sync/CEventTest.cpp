//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CEventTest.cpp
//
//	@doc:
//		Tests for CEvent
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/sync/CEvent.h"
#include "gpos/sync/CAutoMutex.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/sync/CEventTest.h"

#define GPOS_EVENT_TEST_STEPS 100000

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CEventTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEventTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CEventTest::EresUnittest_ProducerConsumer),
		GPOS_UNITTEST_FUNC(CEventTest::EresUnittest_TimedWait)
		};
		
	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

ULONG CEventTest::ulValue = 1;

//---------------------------------------------------------------------------
//	@function:
//		CEventTest::EresUnittest_ProducerConsumer
//
//	@doc:
//		Back-and-forth between two threads
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEventTest::EresUnittest_ProducerConsumer()
{
	// abort simulation will lead to a deadlock
	if (ITask::Self()->IsTraceSet(EtraceSimulateAbort))
	{
		return GPOS_OK;
	}

	CMutex mutex;

	CEvent event;
	event.Init(&mutex);

	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *mp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

	// scope for tasks
	{
		CAutoTaskProxy atp(mp, pwpm);
		CTask *rgPtsk[2];
		rgPtsk[0] = atp.Create(CEventTest::PvUnittest_Consumer, &event);
		rgPtsk[1] = atp.Create(CEventTest::PvUnittest_Producer, &event);

		// OPT-58: take care to avoid race where producer overtakes consumer
		// synchronize startup through explicit handshake between consumer and main thread
		{
			CAutoMutex am(mutex);
			am.Lock();

			atp.Schedule(rgPtsk[0]);

			// wait for consumer to start up
			event.Wait();

			GPOS_ASSERT(rgPtsk[0]->CheckStatus(false /*fCompleted*/));
		}

		// We got the mutex back and Consumer is the only one waiting on the event;
		// now it's safe to start the producer

		atp.Schedule(rgPtsk[1]);

		for (ULONG i = 0; i < 2; i++)
		{
			atp.Wait(rgPtsk[i]);
		}
	}

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CEventTest::EresUnittest_ProducerConsumer
//
//	@doc:
//		Check timed wait
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEventTest::EresUnittest_TimedWait()
{
	CMutex mutex;

	CEvent event;
	event.Init(&mutex);

	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *mp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

	// scope for tasks
	{
		CAutoTaskProxy atp(mp, pwpm);

		CTask *ptsk = atp.Create(CEventTest::PvUnittest_TimedWait, &event);

		// scope for mutex
		{
			// wake up thread;
			CAutoMutex am(mutex);
			am.Lock();

			atp.Schedule(ptsk);

			// test wait with no timeout expiration
			if (GPOS_OK != event.TimedWait(gpos::ulong_max))
			{
				return GPOS_FAILED;
			}

			// test zero timeout
			if (GPOS_TIMEOUT != event.TimedWait(0))
			{
				return GPOS_FAILED;
			}

			// test short timeout
			if (GPOS_TIMEOUT != event.TimedWait(10))
			{
				return GPOS_FAILED;
			}

			// test medium timeout
			if (GPOS_TIMEOUT != event.TimedWait(100))
			{
				return GPOS_FAILED;
			}

			// test long timeout
			if (GPOS_TIMEOUT != event.TimedWait(1200))
			{
				return GPOS_FAILED;
			}

			GPOS_ASSERT(ptsk->CheckStatus(false /*fCompleted*/));

			// signal child to complete
			event.Signal();

			// test wait with no timeout expiration
			if (GPOS_OK != event.TimedWait(gpos::ulong_max))
			{
				return GPOS_FAILED;
			}
		}

		atp.Wait(ptsk);
	}

	return GPOS_OK;

}


//---------------------------------------------------------------------------
//	@function:
//		CEventTest::PvUnittest_Producer
//
//	@doc:
//		concurrent event test -- producer
//
//---------------------------------------------------------------------------
void *
CEventTest::PvUnittest_Producer
	(
	void *pv
	)
{
	CEvent *pevent = (CEvent*)pv;
	CMutexBase *pmutex = pevent->GetMutex();

	CAutoMutex am(*pmutex);
	am.Lock();

	for(ULONG i = 0; i < GPOS_EVENT_TEST_STEPS; i++)
	{
		if (0 == i % 10)
		{
			GPOS_CHECK_ABORT;
		}

		GPOS_ASSERT(1 == ulValue % 2);
		ulValue++;

		pevent->Signal();
		pevent->Wait();
	}

	return NULL;
}



//---------------------------------------------------------------------------
//	@function:
//		CEventTest::PvUnittest_Consumer
//
//	@doc:
//		concurrent event test -- consumer
//
//---------------------------------------------------------------------------
void *
CEventTest::PvUnittest_Consumer
	(
	void *pv
	)
{
	CEvent *pevent = (CEvent*)pv;
	CMutexBase *pmutex = pevent->GetMutex();

	CAutoMutex am(*pmutex);
	am.Lock();

	// signal to parent that we're up and running and will be waiting
	pevent->Signal();

	for(ULONG i = 0; i < GPOS_EVENT_TEST_STEPS; i++)
	{
		if (0 == i % 10)
		{
			GPOS_CHECK_ABORT;
		}

		pevent->Wait();

		GPOS_ASSERT(0 == ulValue % 2);
		ulValue++;
		pevent->Signal();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CEventTest::PvUnittest_TimedWait
//
//	@doc:
//		concurrent event test using timeout
//
//---------------------------------------------------------------------------
void *
CEventTest::PvUnittest_TimedWait
	(
	void *pv
	)
{
	CEvent *pevent = (CEvent *) pv;
	CMutexBase *pmutex = pevent->GetMutex();

	CAutoMutex am(*pmutex);
	am.Lock();

	// signal thread start
	pevent->Signal();

	// wait for signal - no expiration
	pevent->TimedWait(gpos::ulong_max);

	// signal complete
	pevent->Signal();

	return NULL;
}

// EOF

